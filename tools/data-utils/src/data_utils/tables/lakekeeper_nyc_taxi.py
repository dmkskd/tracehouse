"""lakekeeper_nyc_taxi - NYC taxi Iceberg tables accessed through Lakekeeper.

This dataset intentionally differs from ``iceberg_nyc_taxi``:

* ``iceberg_nyc_taxi`` uses path-based ``IcebergS3`` tables directly.
* ``lakekeeper_nyc_taxi`` bootstraps Iceberg metadata once, registers the
  tables in Lakekeeper, then writes through ClickHouse ``DataLakeCatalog``.
"""

from __future__ import annotations

import json
import logging
import random
import urllib.error
import urllib.request
from typing import TYPE_CHECKING

from clickhouse_driver import Client

from .helpers import generate_month_list, run_batched_insert, wait_for_table
from .protocol import InsertMode, QuerySet
from data_utils.capabilities import Capabilities

if TYPE_CHECKING:
    from .helpers import ProgressTracker
    from .protocol import Dataset, InsertConfig

log = logging.getLogger(__name__)

_CATALOG_DB = "lakekeeper_iceberg"
_BOOTSTRAP_DB = "lakekeeper_nyc_taxi_bootstrap"
_NAMESPACE = "default"
_TRIPS_TABLE = f"{_CATALOG_DB}.`{_NAMESPACE}.trips`"
_LOCATIONS_TABLE = f"{_CATALOG_DB}.`{_NAMESPACE}.locations`"
_PREFIX = "tracehouse/lakekeeper_catalog_nyc_taxi_v2"
_ICEBERG_INSERT_SETTINGS = (
    "allow_insert_into_iceberg = 1, "
    "write_full_path_in_iceberg_metadata = 1"
)


def _s3_url(caps: Capabilities, path: str) -> str:
    endpoint = caps.iceberg_s3_endpoint.rstrip("/")
    bucket = caps.iceberg_warehouse_bucket
    return f"{endpoint}/{bucket}/{path}"


def _engine_args(caps: Capabilities, path: str) -> str:
    return f"'{_s3_url(caps, path)}', '{caps.iceberg_s3_key}', '{caps.iceberg_s3_secret}'"


def _catalog_url(caps: Capabilities) -> str:
    return caps.iceberg_catalog_url.rstrip("/")


def _catalog_config(caps: Capabilities) -> tuple[str, str]:
    catalog_url = caps.iceberg_catalog_client_url.rstrip("/")
    url = f"{catalog_url}/v1/config?warehouse=tracehouse"
    with urllib.request.urlopen(url, timeout=10) as resp:
        config = json.loads(resp.read())
    prefix = config.get("defaults", {}).get("prefix", "")
    if not prefix:
        raise RuntimeError("Lakekeeper catalog returned no prefix")
    return catalog_url, prefix


def _request_json(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = resp.read()
            return resp.status, json.loads(payload) if payload else {}
    except urllib.error.HTTPError as e:
        payload = e.read()
        try:
            parsed = json.loads(payload) if payload else {}
        except Exception:
            parsed = {"error": payload.decode(errors="replace")}
        return e.code, parsed


def _ensure_namespace(caps: Capabilities) -> None:
    catalog_url, prefix = _catalog_config(caps)
    status, payload = _request_json(
        "POST",
        f"{catalog_url}/v1/{prefix}/namespaces",
        {"namespace": [_NAMESPACE]},
    )
    if status not in (200, 201, 409):
        raise RuntimeError(f"Failed to create Lakekeeper namespace: {status} {payload}")


def _catalog_table_exists(caps: Capabilities, table: str) -> bool:
    catalog_url, prefix = _catalog_config(caps)
    req = urllib.request.Request(
        f"{catalog_url}/v1/{prefix}/namespaces/{_NAMESPACE}/tables/{table}",
        method="HEAD",
    )
    try:
        urllib.request.urlopen(req, timeout=10).close()
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def _register_table(caps: Capabilities, table: str, metadata_path: str, version: int = 2) -> None:
    if _catalog_table_exists(caps, table):
        return
    catalog_url, prefix = _catalog_config(caps)
    metadata_location = f"s3://{caps.iceberg_warehouse_bucket}/{metadata_path}/metadata/v{version}.metadata.json"
    status, payload = _request_json(
        "POST",
        f"{catalog_url}/v1/{prefix}/namespaces/{_NAMESPACE}/register",
        {"name": table, "metadata-location": metadata_location},
    )
    if status not in (200, 201, 409):
        raise RuntimeError(f"Failed to register Lakekeeper table {table}: {status} {payload}")


def _latest_metadata_version(client: Client, caps: Capabilities, metadata_path: str) -> int:
    rows = client.execute(f"""
        SELECT DISTINCT _path
        FROM s3(
            '{_s3_url(caps, f'{metadata_path}/metadata/v*.metadata.json')}',
            '{caps.iceberg_s3_key}',
            '{caps.iceberg_s3_secret}',
            'LineAsString',
            'line String'
        )
    """)
    versions: list[int] = []
    for (path,) in rows:
        filename = path.rsplit("/", 1)[-1]
        if filename.startswith("v") and filename.endswith(".metadata.json"):
            versions.append(int(filename[1:-len(".metadata.json")]))
    if not versions:
        raise RuntimeError(f"No Iceberg metadata files found under {metadata_path}")
    return max(versions)


def _drop_catalog_table(caps: Capabilities, table: str) -> None:
    catalog_url, prefix = _catalog_config(caps)
    status, payload = _request_json(
        "DELETE",
        f"{catalog_url}/v1/{prefix}/namespaces/{_NAMESPACE}/tables/{table}",
    )
    if status not in (200, 204, 404):
        raise RuntimeError(f"Failed to drop Lakekeeper table {table}: {status} {payload}")


def _ensure_catalog_database(client: Client, caps: Capabilities) -> None:
    client.execute("SET allow_database_iceberg = 1")
    client.execute(f"""
        CREATE DATABASE IF NOT EXISTS {_CATALOG_DB}
        ENGINE = DataLakeCatalog('{_catalog_url(caps)}')
        SETTINGS catalog_type = 'rest', warehouse = 'tracehouse'
    """)


def _drop_bootstrap_database(client: Client) -> None:
    client.execute(f"DROP TABLE IF EXISTS {_BOOTSTRAP_DB}.trips SYNC")
    client.execute(f"DROP TABLE IF EXISTS {_BOOTSTRAP_DB}.locations SYNC")
    client.execute(f"DROP DATABASE IF EXISTS {_BOOTSTRAP_DB} SYNC")


def drop_lakekeeper_nyc_taxi(client: Client, caps: Capabilities | None = None) -> None:
    print("Dropping lakekeeper_nyc_taxi...")
    if caps and caps.iceberg_catalog_url:
        try:
            _drop_catalog_table(caps, "trips")
            _drop_catalog_table(caps, "locations")
        except Exception as e:
            log.warning("Failed to drop Lakekeeper catalog tables: %s", e)
    client.execute("SET allow_database_iceberg = 1")
    client.execute(f"DROP DATABASE IF EXISTS {_CATALOG_DB} SYNC")
    _drop_bootstrap_database(client)


def create_lakekeeper_nyc_taxi(client: Client, caps: Capabilities | None = None) -> bool:
    if not caps or not caps.has_iceberg_insert or not caps.iceberg_catalog_url:
        print("  Skipping lakekeeper_nyc_taxi: Iceberg insert or Lakekeeper URL unavailable")
        return False

    try:
        _ensure_namespace(caps)
    except Exception as e:
        print(f"  Skipping lakekeeper_nyc_taxi: Lakekeeper unavailable ({e})")
        return False

    print("Creating Lakekeeper catalog database...")
    _ensure_catalog_database(client, caps)

    locations_exists = _catalog_table_exists(caps, "locations")
    trips_exists = _catalog_table_exists(caps, "trips")
    if locations_exists and trips_exists:
        _drop_bootstrap_database(client)
        return True

    print("Creating Lakekeeper bootstrap Iceberg tables...")
    client.execute(f"CREATE DATABASE IF NOT EXISTS {_BOOTSTRAP_DB}")
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {_BOOTSTRAP_DB}.trips
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
        ENGINE = IcebergS3({_engine_args(caps, f'{_PREFIX}/trips/')})
        SETTINGS {_ICEBERG_INSERT_SETTINGS}
    """)
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {_BOOTSTRAP_DB}.locations
        (
            location_id Int32,
            borough String,
            zone String,
            service_zone String
        )
        ENGINE = IcebergS3({_engine_args(caps, f'{_PREFIX}/locations/')})
        SETTINGS {_ICEBERG_INSERT_SETTINGS}
    """)

    client.execute("SET allow_database_iceberg = 1")
    client.execute(f"DROP DATABASE IF EXISTS {_CATALOG_DB} SYNC")
    _ensure_catalog_database(client, caps)

    if locations_exists:
        _bootstrap_trips_if_needed(client, caps)
        _drop_bootstrap_database(client)
        return True

    boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"]
    service_zones = ["Yellow Zone", "Boro Zone", "Airports", "EWR"]
    boroughs_arr = "['" + "','".join(boroughs) + "']"
    zones_arr = "['" + "','".join(service_zones) + "']"
    print("  Bootstrapping Lakekeeper locations (265 zones)...")
    client.execute(f"""
        INSERT INTO {_BOOTSTRAP_DB}.locations
        SETTINGS {_ICEBERG_INSERT_SETTINGS}
        SELECT
            toInt32(number + 1) AS location_id,
            arrayElement({boroughs_arr}, (number % {len(boroughs)}) + 1) AS borough,
            concat('Zone-', toString(number + 1)) AS zone,
            arrayElement({zones_arr}, (number % {len(service_zones)}) + 1) AS service_zone
        FROM numbers(265)
    """)
    _register_table(
        caps,
        "locations",
        f"{_PREFIX}/locations",
        version=_latest_metadata_version(client, caps, f"{_PREFIX}/locations"),
    )
    _bootstrap_trips_if_needed(client, caps)
    _drop_bootstrap_database(client)
    return True


def _bootstrap_trips_if_needed(client: Client, caps: Capabilities) -> int:
    if _catalog_table_exists(caps, "trips"):
        return 0
    print("  Bootstrapping Lakekeeper trips with one initial row...")
    client.execute(f"""
        INSERT INTO {_BOOTSTRAP_DB}.trips
        SETTINGS {_ICEBERG_INSERT_SETTINGS}
        SELECT
            toInt64(0) AS trip_id,
            toDateTime('2026-02-01 00:00:00') AS pickup_datetime,
            pickup_datetime + toIntervalSecond(600) AS dropoff_datetime,
            toDate(pickup_datetime) AS pickup_date,
            toInt32(1) AS passenger_count,
            toFloat32(1.0) AS trip_distance,
            toInt32(1) AS pickup_location_id,
            toInt32(2) AS dropoff_location_id,
            'Credit card' AS payment_type,
            toFloat64(10.0) AS fare_amount,
            toFloat64(2.0) AS tip_amount,
            fare_amount + tip_amount AS total_amount,
            'Yellow Cab' AS vendor_name,
            toInt64(dateDiff('second', pickup_datetime, dropoff_datetime)) AS trip_duration_seconds,
            'Standard' AS rate_code
    """)
    _register_table(
        caps,
        "trips",
        f"{_PREFIX}/trips",
        version=_latest_metadata_version(client, caps, f"{_PREFIX}/trips"),
    )
    client.execute("SET allow_database_iceberg = 1")
    client.execute(f"DROP DATABASE IF EXISTS {_CATALOG_DB} SYNC")
    _ensure_catalog_database(client, caps)
    wait_for_table(client, _TRIPS_TABLE)
    wait_for_table(client, _LOCATIONS_TABLE)
    return 1


def insert_lakekeeper_nyc_taxi(
    client: Client,
    caps: Capabilities,
    rows: int,
    partitions: int,
    batch_size: int,
    mode: InsertMode = InsertMode.RESUME,
    tracker: ProgressTracker | None = None,
    throttle_min: float = 0.0,
    throttle_max: float = 0.0,
) -> None:
    client.execute("SET allow_database_iceberg = 1")

    current_count = client.execute(f"SELECT count() FROM {_TRIPS_TABLE}")[0][0]
    if mode is InsertMode.APPEND:
        remaining = rows
    elif current_count > rows and mode is not InsertMode.DROP:
        log.info("[%s] has %s rows, exceeds target %s - use --drop to reset", _TRIPS_TABLE, f"{current_count:,}", f"{rows:,}")
        remaining = None
    elif current_count >= rows * 0.9 and mode is not InsertMode.DROP:
        log.info("[%s] already has %s rows (target: %s), skipping", _TRIPS_TABLE, f"{current_count:,}", f"{rows:,}")
        remaining = None
    else:
        remaining = max(rows - current_count, 0)

    if not remaining:
        if tracker:
            tracker.register("lakekeeper_nyc_taxi", rows)
            tracker.skip("lakekeeper_nyc_taxi")
        return

    months = generate_month_list(partitions)
    base_offset = current_count

    def build_sql(month_start: str, batch: int, bs: int, current_batch: int, month_rows: int, partition_offset: int) -> str:
        return f"""
            INSERT INTO {_TRIPS_TABLE}
            SETTINGS allow_insert_into_iceberg = 1
            SELECT
                {base_offset} + {partition_offset} + {batch * bs} + number as trip_id,
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
        client, _TRIPS_TABLE, remaining, months, batch_size,
        build_sql, tracker=tracker, tracker_name="lakekeeper_nyc_taxi",
        throttle_min=throttle_min,
        throttle_max=throttle_max,
    )


def _rand_days_ago() -> int:
    return random.randint(1, 30)


def _rand_pickup_location() -> int:
    return random.choice([132, 138, 161, 186, 237, 48, 79, 230, 170, 162])


class LakekeeperNycTaxi:
    """Dataset implementation for Lakekeeper-catalog Iceberg NYC taxi data."""

    name = "lakekeeper_nyc_taxi"
    flag = "lakekeeper_taxi_only"

    def __init__(self, caps: Capabilities | None = None, ttl_interval: int = 0):
        self._caps = caps
        self._ttl_interval = ttl_interval
        self._enabled = False

    def drop(self, client: Client) -> None:
        drop_lakekeeper_nyc_taxi(client, caps=self._caps)

    def create(self, client: Client) -> None:
        self._enabled = create_lakekeeper_nyc_taxi(client, caps=self._caps)

    def insert(
        self,
        client: Client,
        config: InsertConfig,
        tracker: ProgressTracker | None = None,
    ) -> None:
        if not self._enabled or not self._caps or not self._caps.has_iceberg_insert or not self._caps.iceberg_catalog_url:
            if tracker:
                tracker.register(self.name, config.rows)
                tracker.skip(self.name, "no Lakekeeper support")
            else:
                print(f"  Skipping {self.name}: Iceberg insert or Lakekeeper URL unavailable")
            return
        insert_lakekeeper_nyc_taxi(
            client, self._caps, config.rows, config.partitions, config.batch_size,
            mode=config.mode, tracker=tracker,
            throttle_min=config.throttle_min, throttle_max=config.throttle_max,
        )

    @property
    def queries(self) -> QuerySet:
        return QuerySet(
            fast=[
                f"SELECT count() FROM {_TRIPS_TABLE}",
                f"SELECT min(pickup_date), max(pickup_date) FROM {_TRIPS_TABLE}",
            ],
            pk_generators=[
                lambda: f"""
    SELECT count(), avg(total_amount), avg(trip_distance)
    FROM {_TRIPS_TABLE}
    WHERE pickup_date = today() - {_rand_days_ago()}
      AND pickup_location_id = {_rand_pickup_location()}
    SETTINGS use_query_cache = 0
    """,
                lambda: f"""
    SELECT toStartOfHour(pickup_datetime) AS hour, count()
    FROM {_TRIPS_TABLE}
    WHERE vendor_name = 'Yellow Cab' AND payment_type = 'Credit card'
    GROUP BY hour
    ORDER BY hour
    SETTINGS use_query_cache = 0
    """,
            ],
        )


if TYPE_CHECKING:
    _: type[Dataset] = LakekeeperNycTaxi
