"""web_analytics.pageviews — web analytics with optional Distributed sharding."""

from __future__ import annotations

import sys
import os
import time
import random
from clickhouse_driver import Client
from ._helpers import (
    retry_on_drop_race, generate_month_list,
    check_existing_rows, run_batched_insert,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ch_capabilities import Capabilities

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
            print(f"    ✓ {partition_label} complete" + " " * 20)

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
