"""iceberg_nyc_taxi — NYC taxi trip data stored as Iceberg tables via IcebergS3 engine.

Same schema as nyc_taxi but uses the IcebergS3 table engine, writing Parquet
files to S3-compatible storage (MinIO). Designed to exercise ClickHouse's
Iceberg code paths for observability: metadata cache, S3 read/write counters,
partition pruning, and snapshot management.
"""

from __future__ import annotations

import json
import logging
import random
import urllib.request
import urllib.error
from typing import TYPE_CHECKING

from clickhouse_driver import Client
from .helpers import (
    generate_month_list, check_existing_rows, run_batched_insert,
    wait_for_table,
)
from .protocol import InsertMode, QuerySet
from data_utils.capabilities import Capabilities

if TYPE_CHECKING:
    from .helpers import ProgressTracker
    from .protocol import InsertConfig, Dataset

log = logging.getLogger(__name__)

_ICEBERG_INSERT_SETTINGS = (
    "allow_insert_into_iceberg = 1, "
    "write_full_path_in_iceberg_metadata = 1"
)


def _s3_url(caps: Capabilities, path: str) -> str:
    endpoint = caps.iceberg_s3_endpoint.rstrip("/")
    bucket = caps.iceberg_warehouse_bucket
    return f"{endpoint}/{bucket}/{path}"


def _engine_args(caps: Capabilities, path: str) -> str:
    url = _s3_url(caps, path)
    return f"'{url}', '{caps.iceberg_s3_key}', '{caps.iceberg_s3_secret}'"


def drop_iceberg_nyc_taxi(client: Client, caps: Capabilities | None = None) -> None:
    print("Dropping iceberg_nyc_taxi...")
    client.execute("DROP TABLE IF EXISTS iceberg_nyc_taxi.trips SYNC")
    client.execute("DROP TABLE IF EXISTS iceberg_nyc_taxi.locations SYNC")
    client.execute("DROP DATABASE IF EXISTS iceberg_nyc_taxi SYNC")


def create_iceberg_nyc_taxi(client: Client, caps: Capabilities | None = None) -> None:
    if not caps or not caps.has_iceberg_insert:
        print("  Skipping iceberg_nyc_taxi: Iceberg insert not supported")
        return

    print("Creating iceberg_nyc_taxi database...")
    client.execute("CREATE DATABASE IF NOT EXISTS iceberg_nyc_taxi")

    trips_engine = f"IcebergS3({_engine_args(caps, 'tracehouse/iceberg_nyc_taxi/trips/')})"

    print(f"Creating iceberg_nyc_taxi.trips (IcebergS3)...")
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS iceberg_nyc_taxi.trips
        (
            trip_id Int64,
            pickup_datetime DateTime,
            dropoff_datetime DateTime,
            pickup_date Date,
            passenger_count Int32,
            trip_distance Float32,
            pickup_location_id Int32,
            dropoff_location_id Int32,
            payment_type String,
            fare_amount Float64,
            tip_amount Float64,
            total_amount Float64,
            vendor_name String,
            trip_duration_seconds Int64,
            rate_code String
        )
        ENGINE = {trips_engine}
        SETTINGS {_ICEBERG_INSERT_SETTINGS}
    """)

    locations_engine = f"IcebergS3({_engine_args(caps, 'tracehouse/iceberg_nyc_taxi/locations/')})"

    print("Creating iceberg_nyc_taxi.locations (IcebergS3)...")
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS iceberg_nyc_taxi.locations
        (
            location_id Int32,
            borough String,
            zone String,
            service_zone String
        )
        ENGINE = {locations_engine}
        SETTINGS {_ICEBERG_INSERT_SETTINGS}
    """)

    _BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"]
    _SERVICE_ZONES = ["Yellow Zone", "Boro Zone", "Airports", "EWR"]

    wait_for_table(client, "iceberg_nyc_taxi.locations")
    existing = client.execute("SELECT count() FROM iceberg_nyc_taxi.locations")[0][0]
    if existing > 0:
        print(f"  iceberg_nyc_taxi.locations already has {existing} rows, skipping")
    else:
        boroughs_arr = "['" + "','".join(_BOROUGHS) + "']"
        zones_arr = "['" + "','".join(_SERVICE_ZONES) + "']"
        print("  Inserting iceberg_nyc_taxi.locations (265 zones)...")
        client.execute(f"""
            INSERT INTO iceberg_nyc_taxi.locations
            SETTINGS {_ICEBERG_INSERT_SETTINGS}
            SELECT
                toInt32(number + 1) AS location_id,
                arrayElement({boroughs_arr}, (number % {len(_BOROUGHS)}) + 1) AS borough,
                concat('Zone-', toString(number + 1)) AS zone,
                arrayElement({zones_arr}, (number % {len(_SERVICE_ZONES)}) + 1) AS service_zone
            FROM numbers(265)
        """)

    _try_register_catalog(caps)


def _try_register_catalog(caps: Capabilities) -> None:
    """Optionally register the Iceberg table in the REST catalog."""
    if not caps.iceberg_catalog_url:
        return

    catalog_url = caps.iceberg_catalog_url.rstrip("/")
    try:
        config_url = f"{catalog_url}/v1/config?warehouse=tracehouse"
        req = urllib.request.Request(config_url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            config = json.loads(resp.read())
        prefix = config.get("defaults", {}).get("prefix", "")
        if not prefix:
            log.warning("Catalog returned no prefix, skipping registration")
            return
    except Exception as e:
        log.warning("Could not reach catalog at %s: %s", catalog_url, e)
        return

    # Create namespace
    ns_url = f"{catalog_url}/v1/{prefix}/namespaces"
    try:
        body = json.dumps({"namespace": ["default"]}).encode()
        req = urllib.request.Request(ns_url, data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as e:
        if e.code != 409:
            log.warning("Failed to create namespace: %s", e)

    # Register table
    metadata_url = f"s3://{caps.iceberg_warehouse_bucket}/tracehouse/iceberg_nyc_taxi/trips/metadata/"
    # Find latest metadata file — we need the actual filename
    # The register API needs the exact metadata-location, but we don't have
    # mc/aws CLI here. Skip registration if we can't determine the path.
    log.info("Catalog registration: table created on S3, register manually or via init script")


def insert_iceberg_nyc_taxi(
    client: Client, rows: int, partitions: int, batch_size: int,
    mode: InsertMode = InsertMode.RESUME,
    tracker: ProgressTracker | None = None,
    throttle_min: float = 0.0, throttle_max: float = 0.0,
) -> None:
    remaining = check_existing_rows(client, "iceberg_nyc_taxi.trips", rows, mode)
    if remaining is None:
        if tracker:
            tracker.register("iceberg_nyc_taxi", rows)
            tracker.skip("iceberg_nyc_taxi")
        return

    months = generate_month_list(partitions)

    def build_sql(month_start: str, batch: int, bs: int, current_batch: int, month_rows: int, partition_offset: int) -> str:
        return f"""
            INSERT INTO iceberg_nyc_taxi.trips
            SETTINGS {_ICEBERG_INSERT_SETTINGS}
            SELECT
                {partition_offset} + {batch * bs} + number as trip_id,
                toDateTime('{month_start}') + toIntervalSecond(
                    ({batch} * {bs} + number) * 86400 * 28 / {month_rows}
                    + rand() % 3600
                ) as pickup_datetime,
                pickup_datetime + toIntervalSecond(300 + rand() % 3600) as dropoff_datetime,
                toDate(pickup_datetime) as pickup_date,
                toInt32(1 + rand() % 6) as passenger_count,
                0.5 + (rand() % 2000) / 100.0 as trip_distance,
                toInt32(1 + rand() % 265) as pickup_location_id,
                toInt32(1 + rand() % 265) as dropoff_location_id,
                arrayElement(['Credit card', 'Cash', 'No charge', 'Dispute'], (rand() % 4) + 1) as payment_type,
                toFloat64(2.5 + (rand() % 5000) / 100.0) as fare_amount,
                toFloat64(if(rand() % 3 = 0, (rand() % 2000) / 100.0, 0)) as tip_amount,
                fare_amount + tip_amount as total_amount,
                arrayElement(['Yellow Cab', 'Green Cab', 'Uber', 'Lyft', 'Via'], (rand() % 5) + 1) as vendor_name,
                toInt64(dateDiff('second', pickup_datetime, dropoff_datetime)) as trip_duration_seconds,
                arrayElement(['Standard', 'JFK', 'Newark', 'Nassau', 'Negotiated', 'Group'], (rand() % 6) + 1) as rate_code
            FROM numbers({current_batch})
        """

    run_batched_insert(
        client, "iceberg_nyc_taxi.trips", remaining, months, batch_size,
        build_sql, tracker=tracker,
        throttle_min=throttle_min, throttle_max=throttle_max,
    )


# ── Query generators ──────────────────────────────────────────────


def _rand_days_ago() -> int:
    return random.randint(1, 30)


def _rand_pickup_location() -> int:
    return random.choice([132, 138, 161, 186, 237, 48, 79, 230, 170, 162])


def _rand_pickup_locations(n: int = 5) -> str:
    locs = random.sample([132, 138, 161, 186, 237, 48, 79, 230, 170, 162, 100, 234, 249, 113, 114], min(n, 15))
    return ", ".join(str(loc) for loc in locs)


def _rand_max_threads() -> int:
    return random.choice([1, 2, 4, 8, 16])


# ── Plugin class ──────────────────────────────────────────────────


class IcebergNycTaxi:
    """Dataset implementation for iceberg_nyc_taxi."""

    name = "iceberg_nyc_taxi"
    flag = "iceberg_taxi_only"

    def __init__(self, caps: Capabilities | None = None, ttl_interval: int = 0):
        self._caps = caps
        self._ttl_interval = ttl_interval

    def drop(self, client: Client) -> None:
        drop_iceberg_nyc_taxi(client, caps=self._caps)

    def create(self, client: Client) -> None:
        create_iceberg_nyc_taxi(client, caps=self._caps)

    def insert(
        self,
        client: Client,
        config: InsertConfig,
        tracker: ProgressTracker | None = None,
    ) -> None:
        if not self._caps or not self._caps.has_iceberg_insert:
            if tracker:
                tracker.register(self.name, config.rows)
                tracker.skip(self.name, "no Iceberg support")
            else:
                print(f"  Skipping {self.name}: Iceberg insert not supported")
            return
        insert_iceberg_nyc_taxi(
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
        avg(trip_distance) as avg_distance
    FROM iceberg_nyc_taxi.trips
    GROUP BY pickup_location_id
    ORDER BY trips DESC
    SETTINGS use_query_cache = 0
    """,
                """
    SELECT
        toHour(pickup_datetime) as hour,
        toDayOfWeek(pickup_datetime) as dow,
        count() as trips,
        avg(total_amount) as avg_fare,
        sum(tip_amount) as total_tips
    FROM iceberg_nyc_taxi.trips
    GROUP BY hour, dow
    ORDER BY trips DESC
    """,
            ],
            fast=[
                "SELECT count() FROM iceberg_nyc_taxi.trips",
                "SELECT min(pickup_date), max(pickup_date) FROM iceberg_nyc_taxi.trips",
                "SELECT payment_type, count() FROM iceberg_nyc_taxi.trips WHERE pickup_date = today() GROUP BY payment_type",
                "SELECT count() FROM iceberg_nyc_taxi.trips WHERE pickup_date = today() AND pickup_location_id = 100",
            ],
            pk_generators=[
                lambda: f"""
    SELECT count(), avg(total_amount), avg(trip_distance)
    FROM iceberg_nyc_taxi.trips
    WHERE pickup_date = today() - {_rand_days_ago()}
      AND pickup_location_id = {_rand_pickup_location()}
    SETTINGS use_query_cache = 0
    """,
                lambda: f"""
    SELECT payment_type, count() AS trips, sum(total_amount) AS revenue
    FROM iceberg_nyc_taxi.trips
    WHERE pickup_date = today() - {_rand_days_ago()}
    GROUP BY payment_type
    ORDER BY trips DESC
    SETTINGS use_query_cache = 0
    """,
                lambda: f"""
    SELECT toHour(pickup_datetime) AS hr, count(), avg(fare_amount)
    FROM iceberg_nyc_taxi.trips
    WHERE pickup_date BETWEEN today() - {random.randint(3, 14)} AND today()
      AND pickup_location_id IN ({_rand_pickup_locations()})
    GROUP BY hr
    ORDER BY hr
    SETTINGS use_query_cache = 0
    """,
                lambda: f"""
    SELECT pickup_date, count(), avg(total_amount)
    FROM iceberg_nyc_taxi.trips
    WHERE pickup_location_id = {_rand_pickup_location()}
    GROUP BY pickup_date
    ORDER BY pickup_date
    SETTINGS use_query_cache = 0
    """,
                lambda: """
    SELECT toStartOfHour(pickup_datetime) AS hour, count()
    FROM iceberg_nyc_taxi.trips
    WHERE vendor_name = 'Yellow Cab' AND payment_type = 'Credit card'
    GROUP BY hour
    ORDER BY hour
    SETTINGS use_query_cache = 0
    """,
            ],
            join_generators=[
                lambda: f"""
    SELECT
        l.borough,
        count() AS trips,
        avg(t.total_amount) AS avg_fare,
        avg(t.trip_distance) AS avg_dist
    FROM iceberg_nyc_taxi.trips AS t
    INNER JOIN iceberg_nyc_taxi.locations AS l
        ON t.pickup_location_id = l.location_id
    WHERE t.pickup_date >= today() - {random.randint(7, 30)}
    GROUP BY l.borough
    ORDER BY trips DESC
    SETTINGS use_query_cache = 0
    """,
                lambda: f"""
    SELECT
        p.borough AS pickup_borough,
        d.borough AS dropoff_borough,
        count() AS trips,
        avg(t.total_amount) AS avg_fare
    FROM iceberg_nyc_taxi.trips AS t
    INNER JOIN iceberg_nyc_taxi.locations AS p ON t.pickup_location_id = p.location_id
    INNER JOIN iceberg_nyc_taxi.locations AS d ON t.dropoff_location_id = d.location_id
    WHERE t.pickup_date >= today() - {random.randint(3, 14)}
    GROUP BY pickup_borough, dropoff_borough
    ORDER BY trips DESC
    LIMIT 30
    SETTINGS use_query_cache = 0
    """,
            ],
            settings_generators=[
                lambda: f"""
    SELECT
        toHour(pickup_datetime) AS hour,
        count() AS trips,
        avg(total_amount) AS avg_fare,
        avg(trip_distance) AS avg_distance,
        sum(tip_amount) AS total_tips
    FROM iceberg_nyc_taxi.trips
    WHERE pickup_date >= today() - {_rand_days_ago()}
    GROUP BY hour
    ORDER BY hour
    SETTINGS max_threads = {_rand_max_threads()}, use_query_cache = 0
    """,
            ],
        )


if TYPE_CHECKING:
    _: type[Dataset] = IcebergNycTaxi
