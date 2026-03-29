"""synthetic_data — synthetic clickstream data with user tier lookups."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from clickhouse_driver import Client
from .helpers import (
    retry_on_drop_race, create_database, drop_database,
    generate_month_list, check_existing_rows, run_batched_insert,
    wait_for_table, ttl_clause, ttl_settings, is_sharded,
)
from data_utils.capabilities import Capabilities
from .protocol import InsertMode, QuerySet

if TYPE_CHECKING:
    from .helpers import ProgressTracker
    from .protocol import InsertConfig, Dataset

_SCHEMA = """
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
        revenue Decimal64(2),
        _inserted_at DateTime DEFAULT now()
    )
"""

def _order_and_settings(ttl_hours: int = 0) -> str:
    ttl = ttl_clause(ttl_hours)
    ttl_s = ttl_settings(ttl_hours)
    extra = f", {ttl_s}" if ttl_s else ""
    return f"""
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (event_date, user_id, event_time)
    {ttl}
    SETTINGS old_parts_lifetime = 60{extra},
        enable_block_number_column = 1,
        enable_block_offset_column = 1
"""


def drop_synthetic_data(client: Client, caps: Capabilities | None = None) -> None:
    print("Dropping synthetic_data...")
    sharded, cluster = is_sharded(caps)
    cluster_name = cluster or (caps.cluster_name if caps and caps.has_cluster else "")
    client.execute("DROP TABLE IF EXISTS synthetic_data.user_tiers SYNC")
    if sharded:
        client.execute("DROP TABLE IF EXISTS synthetic_data.user_tiers_local SYNC")
    client.execute("DROP TABLE IF EXISTS synthetic_data.events SYNC")
    if sharded:
        client.execute("DROP TABLE IF EXISTS synthetic_data.events_local SYNC")
    drop_database(client, "synthetic_data", cluster=cluster_name)


def create_synthetic_data(client: Client, caps: Capabilities | None = None, ttl_hours: int = 0) -> None:
    sharded, cluster = is_sharded(caps)  # cluster is only set when multi-shard
    oas = _order_and_settings(ttl_hours)

    if sharded:
        print(f"Creating synthetic_data database (Replicated, cluster={cluster})...")
        create_database(client, "synthetic_data", replicated=True, cluster=cluster)

        print("Creating synthetic_data.events_local (ReplicatedMergeTree, per-shard)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS synthetic_data.events_local
            {_SCHEMA}
            ENGINE = ReplicatedMergeTree()
            {oas}
        """))
        print("Creating synthetic_data.events (Distributed, sharded by user_id)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS synthetic_data.events
            AS synthetic_data.events_local
            ENGINE = Distributed('{cluster}', synthetic_data, events_local, sipHash64(user_id))
        """))
    else:
        replicated = caps.has_keeper if caps else False
        cluster_name = caps.cluster_name if caps else ""
        if replicated:
            print(f"Creating synthetic_data database (Replicated, cluster={cluster_name})...")
            create_database(client, "synthetic_data", replicated=True, cluster=cluster_name)
            engine = "ReplicatedMergeTree()"
        else:
            print("Creating synthetic_data database...")
            create_database(client, "synthetic_data", replicated=False)
            engine = "MergeTree()"

        print(f"Creating synthetic_data.events ({engine.split('(')[0]})...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS synthetic_data.events
            {_SCHEMA}
            ENGINE = {engine}
            {oas}
        """))

    # ── Dimension: user_tiers (1000 user segments) ──
    # Small dimension table replicated to every shard so local JOINs work.
    # In sharded mode we create a _local + Distributed pair (same as fact tables)
    # so the INSERT fans out to all shards automatically.
    _DIM_SCHEMA = """
        (
            user_id UInt64,
            tier LowCardinality(String),
            signup_date Date,
            lifetime_value Decimal64(2)
        )
    """
    if sharded:
        print("Creating synthetic_data.user_tiers_local (ReplicatedMergeTree, per-shard)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS synthetic_data.user_tiers_local
            {_DIM_SCHEMA}
            ENGINE = ReplicatedMergeTree()
            ORDER BY user_id
        """))
        print("Creating synthetic_data.user_tiers (Distributed, all shards)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS synthetic_data.user_tiers
            AS synthetic_data.user_tiers_local
            ENGINE = Distributed('{cluster}', synthetic_data, user_tiers_local, rand())
        """))
    else:
        replicated = caps.has_keeper if caps else False
        dim_engine = "ReplicatedMergeTree()" if replicated else "MergeTree()"
        print(f"Creating synthetic_data.user_tiers (engine: {dim_engine.split('(')[0]})...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS synthetic_data.user_tiers
            {_DIM_SCHEMA}
            ENGINE = {dim_engine}
            ORDER BY user_id
        """))

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


def insert_synthetic_data(
    client: Client, rows: int, partitions: int, batch_size: int,
    mode: InsertMode = InsertMode.RESUME,
    tracker: ProgressTracker | None = None, throttle_min: float = 0.0, throttle_max: float = 0.0,
) -> None:
    remaining = check_existing_rows(client, "synthetic_data.events", rows, mode)
    if remaining is None:
        if tracker:
            tracker.register("synthetic_data", rows)
            tracker.skip("synthetic_data")
        return

    months = generate_month_list(partitions)

    def build_sql(month_start: str, batch: int, bs: int, current_batch: int, month_rows: int, _offset: int) -> str:
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
                if(rand() % 20 = 0, toDecimal64(rand() % 50000 / 100, 2), 0) as revenue,
                now() as _inserted_at
            FROM numbers({current_batch})
        """

    run_batched_insert(client, "synthetic_data.events", remaining, months, batch_size, build_sql, tracker=tracker, throttle_min=throttle_min, throttle_max=throttle_max)


# ── Query helpers ─────────────────────────────────────────────────


def _rand_days_ago() -> int:
    return random.randint(1, 30)


def _rand_user_id() -> int:
    return random.randint(1, 50000)


def _rand_user_ids(n: int = 5) -> str:
    return ', '.join(str(random.randint(1, 50000)) for _ in range(n))


def _rand_country() -> str:
    return random.choice(['US', 'GB', 'DE', 'FR', 'JP', 'BR', 'IN', 'CA', 'AU', 'MX'])


def _rand_device() -> str:
    return random.choice(['mobile', 'desktop', 'tablet'])


# ── Plugin class ───────────────────────────────────────────────────


class SyntheticData:
    """Dataset implementation for synthetic_data."""

    name = "synthetic_data"
    flag = "synthetic_only"

    def __init__(self, caps: Capabilities | None = None, ttl_hours: int = 0):
        self._caps = caps
        self._ttl_hours = ttl_hours

    def drop(self, client: Client) -> None:
        drop_synthetic_data(client, caps=self._caps)

    def create(self, client: Client) -> None:
        create_synthetic_data(client, caps=self._caps, ttl_hours=self._ttl_hours)

    def insert(
        self,
        client: Client,
        config: InsertConfig,
        tracker: ProgressTracker | None = None,
    ) -> None:
        insert_synthetic_data(
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
        country_code,
        count() as events,
        uniqExact(user_id) as unique_users,
        sum(revenue) as total_revenue,
        sleepEachRow(0.0001) as _delay
    FROM synthetic_data.events
    GROUP BY country_code
    ORDER BY events DESC
    SETTINGS max_threads = 1, use_uncompressed_cache = 0, use_query_cache = 0
    """,
                """
    SELECT
        toStartOfHour(event_time) as hour,
        count() as events,
        uniqExact(user_id) as users,
        avg(duration_ms) as avg_duration
    FROM synthetic_data.events
    GROUP BY hour
    ORDER BY hour
    """,
                """
    SELECT
        device_type,
        browser,
        count() as events,
        uniqExact(user_id) as unique_users,
        uniqExact(session_id) as unique_sessions,
        quantilesExact(0.5, 0.9, 0.99)(duration_ms) as duration_pcts
    FROM synthetic_data.events
    GROUP BY device_type, browser
    ORDER BY events DESC
    """,
                """
    SELECT
        session_id,
        count() as events_in_session,
        min(event_time) as session_start,
        max(event_time) as session_end,
        sum(revenue) as session_revenue,
        sleepEachRow(0.0001) as _delay
    FROM synthetic_data.events
    GROUP BY session_id
    HAVING events_in_session >= 2
    ORDER BY session_revenue DESC
    LIMIT 100000
    SETTINGS max_threads = 1, use_uncompressed_cache = 0, use_query_cache = 0
    """,
                """
    SELECT
        a.country_code,
        b.device_type,
        count() as combinations
    FROM synthetic_data.events a
    CROSS JOIN (SELECT DISTINCT device_type FROM synthetic_data.events LIMIT 10) b
    GROUP BY a.country_code, b.device_type
    ORDER BY combinations DESC
    """,
                """
    SELECT
        event_type,
        country_code,
        device_type,
        count() as cnt,
        uniqExact(user_id) as users,
        uniqExact(session_id) as sessions,
        sum(duration_ms) as total_duration,
        sum(revenue) as total_revenue
    FROM synthetic_data.events
    GROUP BY event_type, country_code, device_type
    ORDER BY cnt DESC
    """,
                """
    SELECT
        user_id,
        event_time,
        revenue,
        sum(revenue) OVER (PARTITION BY user_id ORDER BY event_time) as cumulative_revenue
    FROM synthetic_data.events
    WHERE revenue > 0
    ORDER BY user_id, event_time
    LIMIT 50000
    """,
                """
    SELECT *
    FROM synthetic_data.events
    ORDER BY duration_ms DESC, revenue DESC, event_time
    LIMIT 100000
    """,
            ],
            fast=[
                "SELECT count() FROM synthetic_data.events",
                "SELECT min(event_date), max(event_date) FROM synthetic_data.events",
                "SELECT event_type, count() FROM synthetic_data.events WHERE event_date = today() GROUP BY event_type",
                "SELECT count() FROM synthetic_data.events WHERE event_date = today() AND user_id = 42",
            ],
            pk_generators=[
                # Full key match — all 3 ORDER BY columns in WHERE
                lambda: f"""
    SELECT count(), avg(duration_ms), sum(revenue)
    FROM synthetic_data.events
    WHERE event_date = today() - {_rand_days_ago()}
      AND user_id = {_rand_user_id()}
      AND event_time >= now() - INTERVAL {random.randint(1, 7)} DAY
    SETTINGS use_query_cache = 0
    """,
                # Partial key (1/3) — leftmost only
                lambda: f"""
    SELECT event_type, count() AS cnt, uniq(user_id) AS users
    FROM synthetic_data.events
    WHERE event_date = today() - {_rand_days_ago()}
    GROUP BY event_type
    ORDER BY cnt DESC
    SETTINGS use_query_cache = 0
    """,
                # Partial key (2/3) — leftmost + second
                lambda: f"""
    SELECT count(), sum(revenue), avg(duration_ms)
    FROM synthetic_data.events
    WHERE event_date BETWEEN today() - {random.randint(7, 30)} AND today()
      AND user_id IN ({_rand_user_ids()})
    SETTINGS use_query_cache = 0
    """,
                # Skips leftmost key — filters on user_id only (2nd key column)
                lambda: f"""
    SELECT event_date, count(), sum(revenue)
    FROM synthetic_data.events
    WHERE user_id = {_rand_user_id()}
    GROUP BY event_date
    ORDER BY event_date
    SETTINGS use_query_cache = 0
    """,
                # No key match — WHERE on non-key columns (country_code, device_type)
                lambda: f"""
    SELECT event_type, count(), avg(duration_ms)
    FROM synthetic_data.events
    WHERE country_code = '{_rand_country()}' AND device_type = '{_rand_device()}'
    GROUP BY event_type
    ORDER BY count() DESC
    SETTINGS use_query_cache = 0
    """,
            ],
            join_generators=[
                # INNER JOIN: events × user_tiers
                lambda: f"""
    SELECT
        u.tier,
        count() AS events,
        uniq(e.session_id) AS sessions,
        sum(e.revenue) AS revenue
    FROM synthetic_data.events AS e
    INNER JOIN synthetic_data.user_tiers AS u
        ON e.user_id = u.user_id
    WHERE e.event_date >= today() - {random.randint(3, 14)}
    GROUP BY u.tier
    ORDER BY revenue DESC
    SETTINGS use_query_cache = 0
    """,
                # LEFT JOIN: user_tiers × events (find inactive users)
                lambda: f"""
    SELECT
        u.tier,
        count() AS total_users,
        countIf(e.user_id = 0) AS inactive_users
    FROM synthetic_data.user_tiers AS u
    LEFT JOIN (
        SELECT DISTINCT user_id
        FROM synthetic_data.events
        WHERE event_date >= today() - {random.randint(7, 30)}
    ) AS e ON u.user_id = e.user_id
    GROUP BY u.tier
    ORDER BY total_users DESC
    SETTINGS use_query_cache = 0
    """,
                # LEFT SEMI JOIN: events from users who purchased
                lambda: f"""
    SELECT
        e.country_code,
        count() AS events_from_buyers,
        uniq(e.user_id) AS buyer_count
    FROM synthetic_data.events AS e
    LEFT SEMI JOIN (
        SELECT DISTINCT user_id
        FROM synthetic_data.events
        WHERE event_type = 'purchase'
          AND event_date >= today() - {random.randint(7, 30)}
    ) AS buyers ON e.user_id = buyers.user_id
    WHERE e.event_date >= today() - {random.randint(7, 30)}
    GROUP BY e.country_code
    ORDER BY events_from_buyers DESC
    SETTINGS use_query_cache = 0
    """,
                # LEFT ANTI JOIN: events from users who never purchased
                lambda: f"""
    SELECT
        e.device_type,
        count() AS events_from_non_buyers,
        uniq(e.user_id) AS non_buyer_count
    FROM synthetic_data.events AS e
    LEFT ANTI JOIN (
        SELECT DISTINCT user_id
        FROM synthetic_data.events
        WHERE event_type = 'purchase'
    ) AS buyers ON e.user_id = buyers.user_id
    WHERE e.event_date >= today() - {random.randint(3, 14)}
    GROUP BY e.device_type
    ORDER BY events_from_non_buyers DESC
    SETTINGS use_query_cache = 0
    """,
                # JOIN with window function: user tier revenue ranking
                lambda: f"""
    SELECT
        u.tier,
        e.user_id,
        e.total_revenue,
        row_number() OVER (PARTITION BY u.tier ORDER BY e.total_revenue DESC) AS rank_in_tier
    FROM (
        SELECT user_id, sum(revenue) AS total_revenue
        FROM synthetic_data.events
        WHERE event_date >= today() - {random.randint(7, 30)}
          AND revenue > 0
        GROUP BY user_id
    ) AS e
    INNER JOIN synthetic_data.user_tiers AS u ON e.user_id = u.user_id
    ORDER BY u.tier, rank_in_tier
    LIMIT 100
    SETTINGS use_query_cache = 0
    """,
                # ASOF JOIN: match events to closest user signup
                lambda: f"""
    SELECT
        u.tier,
        count() AS events,
        avg(e.duration_ms) AS avg_duration
    FROM synthetic_data.events AS e
    ASOF LEFT JOIN synthetic_data.user_tiers AS u
        ON e.user_id = u.user_id AND e.event_date >= u.signup_date
    WHERE e.event_date >= today() - {random.randint(3, 14)}
    GROUP BY u.tier
    ORDER BY events DESC
    SETTINGS use_query_cache = 0
    """,
            ],
            settings_generators=[
                lambda: f"""
    SELECT
        country_code,
        device_type,
        count() AS events,
        uniqExact(user_id) AS unique_users,
        sum(revenue) AS total_revenue,
        avg(duration_ms) AS avg_duration
    FROM synthetic_data.events
    WHERE event_date >= today() - {_rand_days_ago()}
    GROUP BY country_code, device_type
    ORDER BY events DESC
    SETTINGS max_threads = {random.choice([1, 2, 4, 8, 16])}, use_query_cache = 0
    """,
            ],
        )


if TYPE_CHECKING:
    _: type[Dataset] = SyntheticData  # satisfies Dataset
