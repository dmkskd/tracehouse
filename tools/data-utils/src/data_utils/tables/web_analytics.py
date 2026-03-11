"""web_analytics.pageviews — web analytics with optional Distributed sharding."""

from __future__ import annotations

import time
import random
from typing import TYPE_CHECKING

from clickhouse_driver import Client
from .helpers import (
    retry_on_drop_race, generate_month_list,
    check_existing_rows, run_batched_insert,
)
from data_utils.capabilities import Capabilities
from .protocol import QuerySet

if TYPE_CHECKING:
    from .helpers import ProgressTracker
    from .protocol import InsertConfig, Dataset

# ── Reference data ──────────────────────────────────────────────────

DOMAINS = [
    "example.com", "shop.example.com", "blog.example.com",
    "docs.example.com", "api.example.com", "app.example.com",
    "news.example.com", "forum.example.com", "wiki.example.com",
    "status.example.com",
]

PATHS = [
    "/", "/about", "/pricing", "/docs", "/blog",
    "/login", "/signup", "/dashboard", "/settings", "/contact",
    "/products", "/cart", "/checkout", "/faq", "/support",
]

_SCHEMA = """
    (
        event_time DateTime,
        event_date Date DEFAULT toDate(event_time),
        domain LowCardinality(String),
        path String,
        user_id UInt64,
        session_id String,
        country_code LowCardinality(String),
        device_type LowCardinality(String),
        browser LowCardinality(String),
        referrer String,
        duration_ms UInt32,
        is_bounce UInt8
    )
"""

_ORDER_AND_SETTINGS = """
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (domain, event_date, user_id)
    SETTINGS old_parts_lifetime = 60
"""


def _is_sharded(caps: Capabilities | None) -> tuple[bool, str | None]:
    if caps and caps.has_cluster and caps.has_keeper and caps.shard_count > 1:
        return True, caps.cluster_name
    return False, None


def drop_web_analytics(client: Client, caps: Capabilities | None = None) -> None:
    print("Dropping web_analytics...")
    use_sharded, cluster = _is_sharded(caps)
    client.execute("DROP TABLE IF EXISTS web_analytics.pageviews SYNC")
    if use_sharded:
        client.execute("DROP TABLE IF EXISTS web_analytics.pageviews_local SYNC")
    client.execute("DROP DATABASE IF EXISTS web_analytics SYNC")


def create_web_analytics(client: Client, caps: Capabilities | None = None) -> None:
    use_sharded, cluster = _is_sharded(caps)

    if use_sharded:
        print(f"Creating web_analytics database (Replicated, cluster={cluster})...")
        retry_on_drop_race(lambda: client.execute(
            "CREATE DATABASE IF NOT EXISTS web_analytics "
            "ENGINE = Replicated('/clickhouse/databases/web_analytics', '{shard}', '{replica}')"
        ))
    else:
        print("Creating web_analytics database...")
        retry_on_drop_race(lambda: client.execute("CREATE DATABASE IF NOT EXISTS web_analytics"))

    if use_sharded:
        print("Creating web_analytics.pageviews_local (ReplicatedMergeTree, per-shard)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS web_analytics.pageviews_local
            {_SCHEMA}
            ENGINE = ReplicatedMergeTree()
            {_ORDER_AND_SETTINGS}
        """))
        print("Creating web_analytics.pageviews (Distributed, sharded by domain)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS web_analytics.pageviews
            AS web_analytics.pageviews_local
            ENGINE = Distributed('{cluster}', web_analytics, pageviews_local, sipHash64(domain))
        """))
    else:
        print("Creating web_analytics.pageviews (MergeTree)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS web_analytics.pageviews
            {_SCHEMA}
            ENGINE = MergeTree()
            {_ORDER_AND_SETTINGS}
        """))


def insert_web_analytics(
    client: Client,
    rows: int,
    partitions: int,
    batch_size: int,
    drop: bool = False,
    caps: Capabilities | None = None,
    tracker=None,
    throttle_min: float = 0.0,
    throttle_max: float = 0.0,
) -> None:
    remaining = check_existing_rows(client, "web_analytics.pageviews", rows, drop)
    if remaining is None:
        if tracker:
            tracker.register("web_analytics", rows)
            tracker.skip("web_analytics")
        return

    months = generate_month_list(partitions)
    domains_sql = "['" + "','".join(DOMAINS) + "']"
    paths_sql = "['" + "','".join(PATHS) + "']"

    use_sharded, cluster = _is_sharded(caps)

    def build_sql(month_start, batch, bs, current_batch, month_rows, _offset):
        return f"""
            INSERT INTO web_analytics.pageviews
            SELECT
                toDateTime('{month_start}') + toIntervalSecond(
                    ({batch} * {bs} + number) * 86400 * 28 / {month_rows}
                    + rand() % 3600
                ) as event_time,
                toDate(event_time) as event_date,
                arrayElement({domains_sql}, (rand() % {len(DOMAINS)}) + 1) as domain,
                arrayElement({paths_sql}, (rand() % {len(PATHS)}) + 1) as path,
                rand() % 500000 as user_id,
                toString(rand() % 100000) as session_id,
                arrayElement(['US','GB','DE','FR','JP','CN','BR','IN','CA','AU'], (rand() % 10) + 1) as country_code,
                arrayElement(['desktop','mobile','tablet'], (rand() % 3) + 1) as device_type,
                arrayElement(['Chrome','Firefox','Safari','Edge'], (rand() % 4) + 1) as browser,
                arrayElement(['https://google.com','https://twitter.com','','https://reddit.com','https://hn.com'], (rand() % 5) + 1) as referrer,
                rand() % 120000 as duration_ms,
                if(rand() % 4 = 0, 1, 0) as is_bounce
            FROM numbers({current_batch})
        """

    if tracker:
        # Parallel mode — use shared batched insert with tracker
        run_batched_insert(client, "web_analytics.pageviews", remaining, months, batch_size, build_sql, tracker=tracker, throttle_min=throttle_min, throttle_max=throttle_max)
    else:
        # Sequential mode — custom header with mode label
        mode_label = "Distributed" if use_sharded else "MergeTree"
        rows_per_partition = remaining // partitions
        print(f"\nInserting {remaining:,} total rows into web_analytics.pageviews ({mode_label})")
        print(f"  Partitions: {partitions}")
        print(f"  Rows per partition: {rows_per_partition:,}")
        print(f"  Batch size: {batch_size:,}")
        print()

        partition_offset = 0
        for partition_key, partition_label in months:
            batches = (rows_per_partition + batch_size - 1) // batch_size
            print(f"  [{partition_label}] {rows_per_partition:,} rows in {batches} batches")

            for batch_idx in range(batches):
                current_batch = min(batch_size, rows_per_partition - batch_idx * batch_size)
                if current_batch <= 0:
                    break
                sql = build_sql(partition_key, batch_idx, batch_size, current_batch, rows_per_partition, partition_offset)
                client.execute(sql)
                pct = (batch_idx + 1) * 100 // batches
                print(f"    Batch {batch_idx + 1}/{batches} ({pct}%)", end="\r")
                if throttle_max > 0:
                    time.sleep(random.uniform(throttle_min, throttle_max))

            partition_offset += rows_per_partition
            print(f"    \u2713 {partition_label} complete" + " " * 20)

    # Shard distribution (cluster only)
    if use_sharded and cluster:
        print("\n  Shard distribution:")
        rows_per_shard = client.execute(
            f"SELECT hostName(), count() FROM clusterAllReplicas('{cluster}', web_analytics.pageviews_local) GROUP BY 1"
        )
        for host, cnt in rows_per_shard:
            print(f"    {host}: {cnt:,} rows")

    if not tracker:
        print("  Done!")


# ── Query helpers ──────────────────────────────────────────────────

def _rand_days_ago() -> int:
    return random.randint(1, 30)

def _rand_user_id() -> int:
    return random.randint(1, 50000)

def _rand_country() -> str:
    return random.choice(['US', 'GB', 'DE', 'FR', 'JP', 'BR', 'IN', 'CA', 'AU', 'MX'])

def _rand_domain() -> str:
    return random.choice([
        'example.com', 'shop.example.com', 'blog.example.com',
        'docs.example.com', 'api.example.com', 'app.example.com',
        'news.example.com', 'forum.example.com', 'wiki.example.com',
        'status.example.com',
    ])

def _rand_domains(n: int = 4) -> str:
    domains = random.sample([
        'example.com', 'shop.example.com', 'blog.example.com',
        'docs.example.com', 'api.example.com', 'app.example.com',
        'news.example.com', 'forum.example.com', 'wiki.example.com',
        'status.example.com',
    ], min(n, 10))
    return ', '.join(f"'{d}'" for d in domains)

def _rand_limit() -> int:
    return random.choice([10, 20, 30, 50, 100])

def _rand_max_threads() -> int:
    return random.choice([1, 2, 4, 8, 16, 32])


# ── Plugin class ───────────────────────────────────────────────────


class WebAnalytics:
    """Dataset implementation for web_analytics."""

    name = "web_analytics"
    flag = "web_only"

    def __init__(self, caps: Capabilities | None = None):
        self._caps = caps

    def drop(self, client: Client) -> None:
        drop_web_analytics(client, self._caps)

    def create(self, client: Client) -> None:
        create_web_analytics(client, self._caps)

    def insert(
        self,
        client: Client,
        config: InsertConfig,
        tracker: ProgressTracker | None = None,
    ) -> None:
        insert_web_analytics(
            client, config.rows, config.partitions, config.batch_size,
            config.drop, self._caps, tracker=tracker,
            throttle_min=config.throttle_min, throttle_max=config.throttle_max,
        )

    @property
    def queries(self) -> QuerySet:
        return QuerySet(
            slow=[
                """
                SELECT
                    domain,
                    count() as pageviews,
                    uniqExact(user_id) as unique_visitors,
                    avg(duration_ms) as avg_duration,
                    sleepEachRow(0.0001) as _delay
                FROM web_analytics.pageviews
                GROUP BY domain
                ORDER BY pageviews DESC
                SETTINGS max_threads = 1, use_uncompressed_cache = 0, use_query_cache = 0
                """,
                """
                SELECT
                    toStartOfHour(event_time) as hour,
                    domain,
                    count() as hits,
                    uniqExact(session_id) as sessions,
                    countIf(is_bounce = 1) as bounces
                FROM web_analytics.pageviews
                GROUP BY hour, domain
                ORDER BY hour, hits DESC
                """,
                """
                SELECT
                    user_id,
                    groupArray(path) as page_sequence,
                    count() as pages_visited,
                    min(event_time) as first_seen,
                    max(event_time) as last_seen,
                    sleepEachRow(0.0001) as _delay
                FROM web_analytics.pageviews
                GROUP BY user_id
                HAVING pages_visited >= 3
                ORDER BY pages_visited DESC
                LIMIT 50000
                SETTINGS max_threads = 1, use_uncompressed_cache = 0, use_query_cache = 0
                """,
                """
                SELECT
                    referrer,
                    domain,
                    count() as visits,
                    uniqExact(user_id) as unique_users,
                    avg(duration_ms) as avg_duration,
                    countIf(is_bounce = 1) * 100.0 / count() as bounce_rate
                FROM web_analytics.pageviews
                WHERE referrer != ''
                GROUP BY referrer, domain
                ORDER BY visits DESC
                """,
            ],
            fast=[
                "SELECT count() FROM web_analytics.pageviews",
                "SELECT min(event_date), max(event_date) FROM web_analytics.pageviews",
                "SELECT domain, count() FROM web_analytics.pageviews WHERE event_date = today() GROUP BY domain",
                "SELECT count() FROM web_analytics.pageviews WHERE domain = 'example.com' AND event_date = today()",
            ],
            pk_generators=[
                # Full key match — all 3 ORDER BY columns
                lambda: f"""
                SELECT count(), avg(duration_ms), countIf(is_bounce = 1)
                FROM web_analytics.pageviews
                WHERE domain = '{_rand_domain()}'
                  AND event_date = today() - {_rand_days_ago()}
                  AND user_id = {_rand_user_id()}
                SETTINGS use_query_cache = 0
                """,
                # Partial key (1/3) — leftmost only (domain)
                lambda: f"""
                SELECT path, count() AS hits, uniq(user_id) AS visitors
                FROM web_analytics.pageviews
                WHERE domain = '{_rand_domain()}'
                GROUP BY path
                ORDER BY hits DESC
                SETTINGS use_query_cache = 0
                """,
                # Partial key (2/3) — domain + event_date
                lambda: f"""
                SELECT count(), avg(duration_ms), countIf(is_bounce = 1) * 100.0 / count() AS bounce_rate
                FROM web_analytics.pageviews
                WHERE domain IN ({_rand_domains()})
                  AND event_date BETWEEN today() - {random.randint(7, 30)} AND today()
                SETTINGS use_query_cache = 0
                """,
                # Skips leftmost key — filters on event_date only (2nd key column)
                lambda: f"""
                SELECT domain, count(), uniq(session_id)
                FROM web_analytics.pageviews
                WHERE event_date = today() - {_rand_days_ago()}
                GROUP BY domain
                ORDER BY count() DESC
                SETTINGS use_query_cache = 0
                """,
                # No key match — WHERE on non-key columns (browser, country_code)
                lambda: f"""
                SELECT path, count(), avg(duration_ms)
                FROM web_analytics.pageviews
                WHERE browser = '{random.choice(["Chrome", "Firefox", "Safari", "Edge"])}'
                  AND country_code = '{_rand_country()}'
                GROUP BY path
                ORDER BY count() DESC
                LIMIT {_rand_limit()}
                SETTINGS use_query_cache = 0
                """,
            ],
            join_generators=[
                # Cross-table JOIN: top taxi zones × web analytics domains
                lambda: f"""
                SELECT
                    t.borough,
                    w.domain,
                    t.trip_count,
                    w.pageview_count
                FROM (
                    SELECT l.borough, count() AS trip_count
                    FROM nyc_taxi.trips AS tr
                    INNER JOIN nyc_taxi.locations AS l ON tr.pickup_location_id = l.location_id
                    WHERE tr.pickup_date >= today() - {random.randint(7, 30)}
                    GROUP BY l.borough
                    ORDER BY trip_count DESC
                    LIMIT 5
                ) AS t
                CROSS JOIN (
                    SELECT domain, count() AS pageview_count
                    FROM web_analytics.pageviews
                    WHERE event_date >= today() - {random.randint(7, 30)}
                    GROUP BY domain
                    ORDER BY pageview_count DESC
                    LIMIT 5
                ) AS w
                ORDER BY t.trip_count DESC, w.pageview_count DESC
                SETTINGS use_query_cache = 0
                """,
            ],
            settings_generators=[
                lambda: f"""
                SELECT
                    domain,
                    path,
                    count() AS pageviews,
                    uniqExact(user_id) AS unique_visitors,
                    countIf(is_bounce = 1) * 100.0 / count() AS bounce_rate
                FROM web_analytics.pageviews
                WHERE event_date >= today() - {_rand_days_ago()}
                GROUP BY domain, path
                ORDER BY pageviews DESC
                LIMIT 200
                SETTINGS max_threads = {_rand_max_threads()}, use_query_cache = 0
                """,
            ],
        )


if TYPE_CHECKING:
    _: type[Dataset] = WebAnalytics  # satisfies Dataset
