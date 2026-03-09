"""synthetic_data.events — synthetic clickstream data."""

from __future__ import annotations

from clickhouse_driver import Client
from ._helpers import (
    engine_clause, retry_on_drop_race, create_database,
    generate_month_list, check_existing_rows, run_batched_insert,
)


def drop_synthetic_data(client: Client) -> None:
    print("Dropping synthetic_data...")
    client.execute("DROP TABLE IF EXISTS synthetic_data.events SYNC")
    client.execute("DROP DATABASE IF EXISTS synthetic_data SYNC")


def create_synthetic_data(client: Client, replicated: bool) -> None:
    print("Creating synthetic_data database...")
    create_database(client, "synthetic_data", replicated)

    engine = engine_clause(replicated)
    print(f"Creating synthetic_data.events table (engine: {engine.split('(')[0]})...")
    retry_on_drop_race(lambda: client.execute(f"""
        CREATE TABLE IF NOT EXISTS synthetic_data.events
        (
            event_id UUID DEFAULT generateUUIDv4(),
            event_time DateTime DEFAULT now(),
            event_date Date DEFAULT toDate(event_time),
            user_id UInt64,
            session_id String,
            event_type LowCardinality(String),
            page_url String,
            country_code LowCardinality(String),
            device_type LowCardinality(String),
            browser LowCardinality(String),
            duration_ms UInt32,
            revenue Decimal64(2)
        )
        ENGINE = {engine}
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, user_id, event_time)
        SETTINGS old_parts_lifetime = 60,
            enable_block_number_column = 1,
            enable_block_offset_column = 1
    """))


def insert_synthetic_data(
    client: Client, rows: int, partitions: int, batch_size: int, drop: bool = False,
    tracker=None, throttle_min: float = 0.0, throttle_max: float = 0.0,
) -> None:
    remaining = check_existing_rows(client, "synthetic_data.events", rows, drop)
    if remaining is None:
        if tracker:
            tracker.register("synthetic_data", rows)
            tracker.skip("synthetic_data")
        return

    months = generate_month_list(partitions)

    def build_sql(month_start, batch, bs, current_batch, month_rows, _offset):
        return f"""
            INSERT INTO synthetic_data.events
            SELECT
                generateUUIDv4() as event_id,
                toDateTime('{month_start}') + toIntervalSecond(
                    ({batch} * {bs} + number) * 86400 * 28 / {month_rows}
                    + rand() % 3600
                ) as event_time,
                toDate(event_time) as event_date,
                rand() % 1000000 as user_id,
                toString(rand() % 100000) as session_id,
                arrayElement(['pageview', 'click', 'scroll', 'purchase', 'signup', 'login'], (rand() % 6) + 1) as event_type,
                concat('https://example.com/', arrayElement(['home', 'products', 'about', 'blog', 'pricing'], (rand() % 5) + 1)) as page_url,
                arrayElement(['US', 'GB', 'DE', 'FR', 'JP', 'CN', 'BR', 'IN', 'CA', 'AU'], (rand() % 10) + 1) as country_code,
                arrayElement(['desktop', 'mobile', 'tablet'], (rand() % 3) + 1) as device_type,
                arrayElement(['Chrome', 'Firefox', 'Safari', 'Edge'], (rand() % 4) + 1) as browser,
                rand() % 300000 as duration_ms,
                if(rand() % 20 = 0, toDecimal64(rand() % 50000 / 100, 2), 0) as revenue
            FROM numbers({current_batch})
        """

    run_batched_insert(client, "synthetic_data.events", remaining, months, batch_size, build_sql, tracker=tracker, throttle_min=throttle_min, throttle_max=throttle_max)
