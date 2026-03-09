"""nyc_taxi.trips — NYC taxi trip data with optional S3 tiered storage."""

from __future__ import annotations

from clickhouse_driver import Client
from ._helpers import (
    engine_clause, retry_on_drop_race, create_database,
    generate_month_list, check_existing_rows, run_batched_insert,
)

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ch_capabilities import Capabilities


def drop_nyc_taxi(client: Client) -> None:
    print("Dropping nyc_taxi...")
    client.execute("DROP TABLE IF EXISTS nyc_taxi.trips SYNC")
    client.execute("DROP DATABASE IF EXISTS nyc_taxi SYNC")


def create_nyc_taxi(
    client: Client, replicated: bool, caps: Capabilities | None = None,
) -> None:
    print("Creating nyc_taxi database...")
    create_database(client, "nyc_taxi", replicated)

    engine = engine_clause(replicated)

    use_s3 = caps.has_s3_storage_policy if caps else False
    if use_s3:
        ttl_clause = "TTL inserted_at + INTERVAL 5 MINUTE TO VOLUME 's3cached'"
        settings_clause = "SETTINGS old_parts_lifetime = 60, storage_policy = 's3tiered', merge_with_ttl_timeout = 60"
        print(f"Creating nyc_taxi.trips table (engine: {engine.split('(')[0]}, storage: s3tiered)...")
    else:
        ttl_clause = ""
        settings_clause = "SETTINGS old_parts_lifetime = 60"
        print(f"Creating nyc_taxi.trips table (engine: {engine.split('(')[0]}, storage: local — s3tiered not available)...")

    retry_on_drop_race(lambda: client.execute(f"""
        CREATE TABLE IF NOT EXISTS nyc_taxi.trips
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
            inserted_at DateTime DEFAULT now()
        )
        ENGINE = {engine}
        PARTITION BY toYYYYMM(pickup_date)
        ORDER BY (pickup_date, pickup_location_id, pickup_datetime)
        {ttl_clause}
        {settings_clause}
    """))


def insert_nyc_taxi(
    client: Client, rows: int, partitions: int, batch_size: int, drop: bool = False,
    tracker=None, throttle_min: float = 0.0, throttle_max: float = 0.0,
) -> None:
    remaining = check_existing_rows(client, "nyc_taxi.trips", rows, drop)
    if remaining is None:
        if tracker:
            tracker.register("nyc_taxi", rows)
            tracker.skip("nyc_taxi")
        return

    months = generate_month_list(partitions)

    def build_sql(month_start, batch, bs, current_batch, month_rows, partition_offset):
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
                now() as inserted_at
            FROM numbers({current_batch})
        """

    run_batched_insert(client, "nyc_taxi.trips", remaining, months, batch_size, build_sql, tracker=tracker, throttle_min=throttle_min, throttle_max=throttle_max)
