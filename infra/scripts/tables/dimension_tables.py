"""Dimension / lookup tables for JOIN query testing.

Creates small reference tables that can be joined against the existing
fact tables (nyc_taxi.trips, synthetic_data.events, web_analytics.pageviews).

Tables:
  - nyc_taxi.locations        — taxi zone lookup (265 rows)
  - synthetic_data.user_tiers — user segment lookup (~1000 rows)
"""

from __future__ import annotations

from clickhouse_driver import Client
from ._helpers import engine_clause, retry_on_drop_race, wait_for_table


# ── NYC taxi zone lookup ────────────────────────────────────────────

_BOROUGHS = [
    "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR",
]

_SERVICE_ZONES = ["Yellow Zone", "Boro Zone", "Airports", "EWR"]


def drop_dimension_tables(client: Client, *, has_taxi: bool = True, has_synthetic: bool = True) -> None:
    print("Dropping dimension tables...")
    if has_taxi:
        client.execute("DROP TABLE IF EXISTS nyc_taxi.locations SYNC")
    if has_synthetic:
        client.execute("DROP TABLE IF EXISTS synthetic_data.user_tiers SYNC")


def create_dimension_tables(client: Client, replicated: bool, *, has_taxi: bool = True, has_synthetic: bool = True) -> None:
    engine = engine_clause(replicated)

    # ── nyc_taxi.locations ──
    if has_taxi:
        print(f"Creating nyc_taxi.locations (engine: {engine.split('(')[0]})...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS nyc_taxi.locations
            (
                location_id UInt16,
                borough LowCardinality(String),
                zone String,
                service_zone LowCardinality(String)
            )
            ENGINE = {engine}
            ORDER BY location_id
        """))

    # ── synthetic_data.user_tiers ──
    if has_synthetic:
        print(f"Creating synthetic_data.user_tiers (engine: {engine.split('(')[0]})...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS synthetic_data.user_tiers
            (
                user_id UInt64,
                tier LowCardinality(String),
                signup_date Date,
                lifetime_value Decimal64(2)
            )
            ENGINE = {engine}
            ORDER BY user_id
        """))


def insert_dimension_tables(client: Client, *, has_taxi: bool = True, has_synthetic: bool = True) -> None:
    """Populate dimension tables with deterministic reference data."""

    # ── locations: 265 taxi zones ──
    if has_taxi:
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

    # ── user_tiers: 1000 user segments ──
    if has_synthetic:
        wait_for_table(client, "synthetic_data.user_tiers")
        existing = client.execute("SELECT count() FROM synthetic_data.user_tiers")[0][0]
        if existing > 0:
            print(f"  synthetic_data.user_tiers already has {existing} rows, skipping")
        else:
            print("  Inserting synthetic_data.user_tiers (1000 users)...")
            client.execute("""
                INSERT INTO synthetic_data.user_tiers
                SELECT
                    number AS user_id,
                    arrayElement(
                        ['free', 'starter', 'pro', 'enterprise'],
                        (number % 4) + 1
                    ) AS tier,
                    toDate('2020-01-01') + toIntervalDay(number % 1800) AS signup_date,
                    toDecimal64(
                        if(number % 4 = 3, 5000 + rand() % 50000,
                        if(number % 4 = 2, 500 + rand() % 5000,
                        if(number % 4 = 1, 50 + rand() % 500,
                        0))),
                        2
                    ) AS lifetime_value
                FROM numbers(1000)
            """)

    print("  Dimension tables ready.")


def _sql_array(items: list[str]) -> str:
    """Build a ClickHouse array literal from a Python list of strings."""
    return "['" + "','".join(items) + "']"
