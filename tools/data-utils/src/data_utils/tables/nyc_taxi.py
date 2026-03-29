"""nyc_taxi — NYC taxi trip data with location lookups and optional S3 tiered storage."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from clickhouse_driver import Client
from .helpers import (
    retry_on_drop_race, create_database, drop_database,
    generate_month_list, check_existing_rows, run_batched_insert,
    wait_for_table, ttl_clause, ttl_settings, is_sharded,
)
from .protocol import InsertMode, QuerySet
from data_utils.capabilities import Capabilities

if TYPE_CHECKING:
    from .helpers import ProgressTracker
    from .protocol import InsertConfig, Dataset


# ── helper functions for query generators ─────────────────────────


def _rand_days_ago() -> int:
    return random.randint(1, 30)


def _rand_pickup_location() -> int:
    return random.choice([132, 138, 161, 186, 237, 48, 79, 230, 170, 162])


def _rand_pickup_locations(n: int = 5) -> str:
    locs = random.sample([132, 138, 161, 186, 237, 48, 79, 230, 170, 162, 100, 234, 249, 113, 114], min(n, 15))
    return ', '.join(str(l) for l in locs)


def _rand_max_threads() -> int:
    return random.choice([1, 2, 4, 8, 16])


_BOROUGHS = [
    "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR",
]
_SERVICE_ZONES = ["Yellow Zone", "Boro Zone", "Airports", "EWR"]


def _sql_array(items: list[str]) -> str:
    return "['" + "','".join(items) + "']"


def drop_nyc_taxi(client: Client, caps: Capabilities | None = None) -> None:
    print("Dropping nyc_taxi...")
    sharded, cluster = is_sharded(caps)
    cluster_name = cluster or (caps.cluster_name if caps and caps.has_cluster else "")
    client.execute("DROP TABLE IF EXISTS nyc_taxi.locations SYNC")
    if sharded:
        client.execute("DROP TABLE IF EXISTS nyc_taxi.locations_local SYNC")
    client.execute("DROP TABLE IF EXISTS nyc_taxi.trips SYNC")
    if sharded:
        client.execute("DROP TABLE IF EXISTS nyc_taxi.trips_local SYNC")
    drop_database(client, "nyc_taxi", cluster=cluster_name)


def create_nyc_taxi(
    client: Client, caps: Capabilities | None = None, ttl_hours: int = 0,
) -> None:
    sharded, cluster = is_sharded(caps)
    replicated = caps.has_keeper if caps else False
    cluster_name = cluster or (caps.cluster_name if caps and caps.has_cluster else "")

    delete_ttl = ttl_clause(ttl_hours)
    ttl_s = ttl_settings(ttl_hours)

    use_s3 = caps.has_s3_storage_policy if caps else False
    if use_s3:
        s3_ttl = "TTL _inserted_at + INTERVAL 5 MINUTE TO VOLUME 's3cached'"
        combined_ttl = f"{s3_ttl}, _inserted_at + INTERVAL {ttl_hours} HOUR DELETE" if ttl_hours > 0 else s3_ttl
        settings_clause = "SETTINGS old_parts_lifetime = 60, storage_policy = 's3tiered', merge_with_ttl_timeout = 60"
    else:
        combined_ttl = delete_ttl
        extra = f", {ttl_s}" if ttl_s else ""
        settings_clause = f"SETTINGS old_parts_lifetime = 60{extra}"

    _TRIPS_SCHEMA = """
        (
            trip_id UInt64,
            pickup_datetime DateTime,
            dropoff_datetime DateTime,
            pickup_date Date DEFAULT toDate(pickup_datetime),
            passenger_count UInt8,
            trip_distance Float32,
            pickup_location_id UInt16,
            dropoff_location_id UInt16,
            payment_type LowCardinality(String),
            fare_amount Decimal(18, 2),
            tip_amount Decimal(18, 2),
            total_amount Decimal(18, 2),
            vendor_name LowCardinality(String) CODEC(ZSTD(3)),
            trip_duration_seconds UInt32 CODEC(Delta, ZSTD),
            rate_code LowCardinality(String) CODEC(LZ4HC),
            _inserted_at DateTime DEFAULT now()
        )
    """

    _TRIPS_ORDER = f"""
        PARTITION BY toYYYYMM(pickup_date)
        ORDER BY (pickup_date, pickup_location_id, pickup_datetime)
        {combined_ttl}
        {settings_clause}
    """

    if sharded:
        print(f"Creating nyc_taxi database (Replicated, cluster={cluster})...")
        create_database(client, "nyc_taxi", replicated=True, cluster=cluster)

        s3_label = ", storage: s3tiered" if use_s3 else ""
        print(f"Creating nyc_taxi.trips_local (ReplicatedMergeTree, per-shard{s3_label})...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS nyc_taxi.trips_local
            {_TRIPS_SCHEMA}
            ENGINE = ReplicatedMergeTree()
            {_TRIPS_ORDER}
        """))
        print("Creating nyc_taxi.trips (Distributed, sharded by pickup_location_id)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS nyc_taxi.trips
            AS nyc_taxi.trips_local
            ENGINE = Distributed('{cluster}', nyc_taxi, trips_local, sipHash64(pickup_location_id))
        """))
    else:
        if replicated:
            print(f"Creating nyc_taxi database (Replicated, cluster={cluster_name})...")
            create_database(client, "nyc_taxi", replicated=True, cluster=cluster_name)
            engine = "ReplicatedMergeTree()"
        else:
            print("Creating nyc_taxi database...")
            create_database(client, "nyc_taxi", replicated=False)
            engine = "MergeTree()"

        s3_label = ", storage: s3tiered" if use_s3 else ""
        print(f"Creating nyc_taxi.trips table (engine: {engine.split('(')[0]}{s3_label})...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS nyc_taxi.trips
            {_TRIPS_SCHEMA}
            ENGINE = {engine}
            {_TRIPS_ORDER}
        """))

    # ── Dimension: locations (265 taxi zones) ──
    # Small dimension table replicated to every shard so local JOINs work.
    _LOC_SCHEMA = """
        (
            location_id UInt16,
            borough LowCardinality(String),
            zone String,
            service_zone LowCardinality(String)
        )
    """
    if sharded:
        print("Creating nyc_taxi.locations_local (ReplicatedMergeTree, per-shard)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS nyc_taxi.locations_local
            {_LOC_SCHEMA}
            ENGINE = ReplicatedMergeTree()
            ORDER BY location_id
        """))
        print("Creating nyc_taxi.locations (Distributed, all shards)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS nyc_taxi.locations
            AS nyc_taxi.locations_local
            ENGINE = Distributed('{cluster}', nyc_taxi, locations_local, rand())
        """))
    else:
        dim_engine = "ReplicatedMergeTree()" if replicated else "MergeTree()"
        print(f"Creating nyc_taxi.locations (engine: {dim_engine.split('(')[0]})...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS nyc_taxi.locations
            {_LOC_SCHEMA}
            ENGINE = {dim_engine}
            ORDER BY location_id
        """))

    wait_for_table(client, "nyc_taxi.locations")
    existing = client.execute("SELECT count() FROM nyc_taxi.locations")[0][0]
    if existing > 0:
        print(f"  nyc_taxi.locations already has {existing} rows, skipping")
    else:
        print("  Inserting nyc_taxi.locations (265 zones)...")
        client.execute(f"""
            INSERT INTO nyc_taxi.locations
            SELECT
                number + 1 AS location_id,
                arrayElement(
                    {_sql_array(_BOROUGHS)},
                    (number % {len(_BOROUGHS)}) + 1
                ) AS borough,
                concat('Zone-', toString(number + 1)) AS zone,
                arrayElement(
                    {_sql_array(_SERVICE_ZONES)},
                    (number % {len(_SERVICE_ZONES)}) + 1
                ) AS service_zone
            FROM numbers(265)
        """)


def insert_nyc_taxi(
    client: Client, rows: int, partitions: int, batch_size: int,
    mode: InsertMode = InsertMode.RESUME,
    tracker: ProgressTracker | None = None, throttle_min: float = 0.0, throttle_max: float = 0.0,
) -> None:
    remaining = check_existing_rows(client, "nyc_taxi.trips", rows, mode)
    if remaining is None:
        if tracker:
            tracker.register("nyc_taxi", rows)
            tracker.skip("nyc_taxi")
        return

    months = generate_month_list(partitions)

    def build_sql(month_start: str, batch: int, bs: int, current_batch: int, month_rows: int, partition_offset: int) -> str:
        return f"""
            INSERT INTO nyc_taxi.trips
            SELECT
                {partition_offset} + {batch * bs} + number as trip_id,
                toDateTime('{month_start}') + toIntervalSecond(
                    ({batch} * {bs} + number) * 86400 * 28 / {month_rows}
                    + rand() % 3600
                ) as pickup_datetime,
                pickup_datetime + toIntervalSecond(300 + rand() % 3600) as dropoff_datetime,
                toDate(pickup_datetime) as pickup_date,
                1 + rand() % 6 as passenger_count,
                0.5 + (rand() % 2000) / 100.0 as trip_distance,
                1 + rand() % 265 as pickup_location_id,
                1 + rand() % 265 as dropoff_location_id,
                arrayElement(['Credit card', 'Cash', 'No charge', 'Dispute'], (rand() % 4) + 1) as payment_type,
                toDecimal64(2.5 + (rand() % 5000) / 100.0, 2) as fare_amount,
                toDecimal64(if(rand() % 3 = 0, (rand() % 2000) / 100.0, 0), 2) as tip_amount,
                fare_amount + tip_amount as total_amount,
                arrayElement(['Yellow Cab', 'Green Cab', 'Uber', 'Lyft', 'Via'], (rand() % 5) + 1) as vendor_name,
                toUInt32(dateDiff('second', pickup_datetime, dropoff_datetime)) as trip_duration_seconds,
                arrayElement(['Standard', 'JFK', 'Newark', 'Nassau', 'Negotiated', 'Group'], (rand() % 6) + 1) as rate_code,
                now() as _inserted_at
            FROM numbers({current_batch})
        """

    run_batched_insert(client, "nyc_taxi.trips", remaining, months, batch_size, build_sql, tracker=tracker, throttle_min=throttle_min, throttle_max=throttle_max)


# ── Plugin class ───────────────────────────────────────────────────


class NycTaxi:
    """Dataset implementation for nyc_taxi."""

    name = "nyc_taxi"
    flag = "taxi_only"

    def __init__(self, caps: Capabilities | None = None, ttl_hours: int = 0):
        self._caps = caps
        self._ttl_hours = ttl_hours

    def drop(self, client: Client) -> None:
        drop_nyc_taxi(client, caps=self._caps)

    def create(self, client: Client) -> None:
        create_nyc_taxi(client, caps=self._caps, ttl_hours=self._ttl_hours)

    def insert(
        self,
        client: Client,
        config: InsertConfig,
        tracker: ProgressTracker | None = None,
    ) -> None:
        insert_nyc_taxi(
            client, config.rows, config.partitions, config.batch_size,
            mode=config.mode, tracker=tracker,
            throttle_min=config.throttle_min, throttle_max=config.throttle_max,
        )

    @property
    def queries(self) -> QuerySet:
        return QuerySet(
            slow=[
                """
    SELECT
        pickup_location_id,
        count() as trips,
        sum(total_amount) as total_fares,
        avg(trip_distance) as avg_distance,
        sleepEachRow(0.0001) as _delay
    FROM nyc_taxi.trips
    GROUP BY pickup_location_id
    ORDER BY trips DESC
    SETTINGS max_threads = 1, use_uncompressed_cache = 0, use_query_cache = 0
    """,
                """
    SELECT
        toHour(pickup_datetime) as hour,
        toDayOfWeek(pickup_datetime) as dow,
        count() as trips,
        avg(total_amount) as avg_fare,
        sum(tip_amount) as total_tips
    FROM nyc_taxi.trips
    GROUP BY hour, dow
    ORDER BY trips DESC
    """,
            ],
            fast=[
                "SELECT count() FROM nyc_taxi.trips",
                "SELECT min(pickup_date), max(pickup_date) FROM nyc_taxi.trips",
                "SELECT payment_type, count() FROM nyc_taxi.trips WHERE pickup_date = today() GROUP BY payment_type",
                "SELECT count() FROM nyc_taxi.trips WHERE pickup_date = today() AND pickup_location_id = 100",
            ],
            pk_generators=[
                # Full key match — all 3 ORDER BY columns
                lambda: f"""
    SELECT count(), avg(total_amount), avg(trip_distance)
    FROM nyc_taxi.trips
    WHERE pickup_date = today() - {_rand_days_ago()}
      AND pickup_location_id = {_rand_pickup_location()}
      AND pickup_datetime >= now() - INTERVAL {random.randint(2, 10)} DAY
    SETTINGS use_query_cache = 0
    """,
                # Partial key (1/3) — leftmost only
                lambda: f"""
    SELECT payment_type, count() AS trips, sum(total_amount) AS revenue
    FROM nyc_taxi.trips
    WHERE pickup_date = today() - {_rand_days_ago()}
    GROUP BY payment_type
    ORDER BY trips DESC
    SETTINGS use_query_cache = 0
    """,
                # Partial key (2/3) — leftmost + second
                lambda: f"""
    SELECT toHour(pickup_datetime) AS hr, count(), avg(fare_amount)
    FROM nyc_taxi.trips
    WHERE pickup_date BETWEEN today() - {random.randint(3, 14)} AND today()
      AND pickup_location_id IN ({_rand_pickup_locations()})
    GROUP BY hr
    ORDER BY hr
    SETTINGS use_query_cache = 0
    """,
                # Skips leftmost key — filters on pickup_location_id only (2nd key column)
                lambda: f"""
    SELECT pickup_date, count(), avg(total_amount)
    FROM nyc_taxi.trips
    WHERE pickup_location_id = {_rand_pickup_location()}
    GROUP BY pickup_date
    ORDER BY pickup_date
    SETTINGS use_query_cache = 0
    """,
                # No key match — WHERE on non-key columns (vendor_name, payment_type)
                lambda: """
    SELECT toStartOfHour(pickup_datetime) AS hour, count()
    FROM nyc_taxi.trips
    WHERE vendor_name = 'Yellow Cab' AND payment_type = 'Credit card'
    GROUP BY hour
    ORDER BY hour
    SETTINGS use_query_cache = 0
    """,
            ],
            join_generators=[
                # INNER JOIN: trips × locations (pickup)
                lambda: f"""
    SELECT
        l.borough,
        count() AS trips,
        avg(t.total_amount) AS avg_fare,
        avg(t.trip_distance) AS avg_dist
    FROM nyc_taxi.trips AS t
    INNER JOIN nyc_taxi.locations AS l
        ON t.pickup_location_id = l.location_id
    WHERE t.pickup_date >= today() - {random.randint(7, 30)}
    GROUP BY l.borough
    ORDER BY trips DESC
    SETTINGS use_query_cache = 0
    """,
                # LEFT JOIN: trips × locations (dropoff)
                lambda: f"""
    SELECT
        l.zone,
        l.service_zone,
        count() AS dropoffs,
        sum(t.tip_amount) AS total_tips
    FROM nyc_taxi.trips AS t
    LEFT JOIN nyc_taxi.locations AS l
        ON t.dropoff_location_id = l.location_id
    WHERE t.pickup_date = today() - {random.randint(1, 14)}
    GROUP BY l.zone, l.service_zone
    ORDER BY dropoffs DESC
    LIMIT {random.choice([20, 50, 100])}
    SETTINGS use_query_cache = 0
    """,
                # Double JOIN: pickup + dropoff locations on same trip
                lambda: f"""
    SELECT
        p.borough AS pickup_borough,
        d.borough AS dropoff_borough,
        count() AS trips,
        avg(t.total_amount) AS avg_fare,
        avg(t.trip_duration_seconds) AS avg_duration_s
    FROM nyc_taxi.trips AS t
    INNER JOIN nyc_taxi.locations AS p ON t.pickup_location_id = p.location_id
    INNER JOIN nyc_taxi.locations AS d ON t.dropoff_location_id = d.location_id
    WHERE t.pickup_date >= today() - {random.randint(3, 14)}
    GROUP BY pickup_borough, dropoff_borough
    ORDER BY trips DESC
    LIMIT 30
    SETTINGS use_query_cache = 0
    """,
                # Self-JOIN: compare pickup vs dropoff stats per location
                lambda: f"""
    SELECT
        p.pickup_location_id AS location_id,
        p.pickups,
        d.dropoffs,
        p.pickups - d.dropoffs AS net_flow
    FROM (
        SELECT pickup_location_id, count() AS pickups
        FROM nyc_taxi.trips
        WHERE pickup_date >= today() - {random.randint(3, 14)}
        GROUP BY pickup_location_id
    ) AS p
    INNER JOIN (
        SELECT dropoff_location_id, count() AS dropoffs
        FROM nyc_taxi.trips
        WHERE pickup_date >= today() - {random.randint(3, 14)}
        GROUP BY dropoff_location_id
    ) AS d ON p.pickup_location_id = d.dropoff_location_id
    ORDER BY net_flow DESC
    LIMIT 20
    SETTINGS use_query_cache = 0
    """,
                # Multi-table: borough revenue by user tier
                lambda: f"""
    SELECT
        l.borough,
        u.tier,
        count() AS trip_count,
        avg(t.total_amount) AS avg_fare
    FROM nyc_taxi.trips AS t
    INNER JOIN nyc_taxi.locations AS l ON t.pickup_location_id = l.location_id
    INNER JOIN synthetic_data.user_tiers AS u ON t.passenger_count = u.user_id % 7
    WHERE t.pickup_date >= today() - {random.randint(3, 14)}
    GROUP BY l.borough, u.tier
    ORDER BY trip_count DESC
    LIMIT 40
    SETTINGS use_query_cache = 0
    """,
            ],
            settings_generators=[
                # Trip stats with variable max_threads
                lambda: f"""
    SELECT
        toHour(pickup_datetime) AS hour,
        count() AS trips,
        avg(total_amount) AS avg_fare,
        avg(trip_distance) AS avg_distance,
        sum(tip_amount) AS total_tips
    FROM nyc_taxi.trips
    WHERE pickup_date >= today() - {_rand_days_ago()}
    GROUP BY hour
    ORDER BY hour
    SETTINGS max_threads = {_rand_max_threads()}, use_query_cache = 0
    """,
            ],
        )


if TYPE_CHECKING:
    _: type[Dataset] = NycTaxi  # satisfies Dataset
