"""replacing_merge — ReplacingMergeTree dataset for observing dedup/replacement behaviour.

Inserts the same product_id keys many times with increasing version numbers.
This lets you observe:
  - How long it takes for merges to collapse duplicate rows (query with vs without FINAL)
  - The ratio of "stale" rows still visible before a merge
  - Part-level deduplication progress over time
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from clickhouse_driver import Client
from .helpers import (
    retry_on_drop_race, create_database,
    generate_month_list, check_existing_rows, run_batched_insert,
)
from .protocol import QuerySet

if TYPE_CHECKING:
    from .helpers import ProgressTracker
    from .protocol import InsertConfig, Dataset


NUM_PRODUCTS = 3_000_000  # distinct product_id values
# With 10M rows and 3M products × 3 currencies (9M unique keys),
# ~10% of rows are duplicates. After full merge ~90% of rows
# survive — enough to keep the table meaningfully sized while
# still showing replacement behaviour.


def _engine(replicated: bool) -> str:
    if replicated:
        return "ReplicatedReplacingMergeTree(version)"
    return "ReplacingMergeTree(version)"


def drop_replacing_merge(client: Client) -> None:
    print("Dropping replacing_test...")
    client.execute("DROP TABLE IF EXISTS replacing_test.product_prices SYNC")
    client.execute("DROP DATABASE IF EXISTS replacing_test SYNC")


def create_replacing_merge(client: Client, replicated: bool, cluster: str = "") -> None:
    print("Creating replacing_test database...")
    create_database(client, "replacing_test", replicated, cluster=cluster)

    engine = _engine(replicated)
    print(f"Creating replacing_test.product_prices (engine: {engine.split('(')[0]})...")
    retry_on_drop_race(lambda: client.execute(f"""
        CREATE TABLE IF NOT EXISTS replacing_test.product_prices
        (
            product_id  UInt64,
            updated_at  DateTime DEFAULT now(),
            version     UInt64,
            price       Decimal64(2),
            currency    LowCardinality(String),
            category    LowCardinality(String),
            supplier_id UInt32,
            in_stock    UInt8
        )
        ENGINE = {engine}
        PARTITION BY toYYYYMM(updated_at)
        ORDER BY (product_id, currency)
        SETTINGS old_parts_lifetime = 60,
            enable_block_number_column = 1,
            enable_block_offset_column = 1
    """))


def insert_replacing_merge(
    client: Client, rows: int, partitions: int, batch_size: int, drop: bool = False,
    tracker: ProgressTracker | None = None, throttle_min: float = 0.0, throttle_max: float = 0.0,
) -> None:
    remaining = check_existing_rows(client, "replacing_test.product_prices", rows, drop)
    if remaining is None:
        if tracker:
            tracker.register("replacing_test", rows)
            tracker.skip("replacing_test")
        return

    months = generate_month_list(partitions)

    def build_sql(month_start: str, batch: int, bs: int, current_batch: int, month_rows: int, _offset: int) -> str:
        # Each row picks a random product_id from the fixed pool.
        # version = toUnixTimestamp of the generated updated_at, so later inserts
        # naturally produce higher versions for the same product_id.
        return f"""
            INSERT INTO replacing_test.product_prices
            SELECT
                rand() % {NUM_PRODUCTS} AS product_id,
                toDateTime('{month_start}') + toIntervalSecond(
                    ({batch} * {bs} + number) * 86400 * 28 / {month_rows}
                    + rand() % 3600
                ) AS updated_at,
                toUnixTimestamp(updated_at) AS version,
                toDecimal64(10 + (rand() % 100000) / 100, 2) AS price,
                arrayElement(['USD', 'EUR', 'GBP'], (rand() % 3) + 1) AS currency,
                arrayElement(
                    ['electronics', 'clothing', 'food', 'books', 'home', 'sports', 'toys', 'health'],
                    (rand() % 8) + 1
                ) AS category,
                rand() % 1000 AS supplier_id,
                rand() % 2 AS in_stock
            FROM numbers({current_batch})
        """

    run_batched_insert(
        client, "replacing_test.product_prices", remaining, months, batch_size, build_sql,
        tracker=tracker, throttle_min=throttle_min, throttle_max=throttle_max,
    )


# ── Query helpers ─────────────────────────────────────────────────


def _rand_category() -> str:
    return random.choice(['electronics', 'clothing', 'food', 'books', 'home', 'sports', 'toys', 'health'])


def _rand_currency() -> str:
    return random.choice(['USD', 'EUR', 'GBP'])


# ── Plugin class ───────────────────────────────────────────────────


class ReplacingMerge:
    """Dataset implementation for replacing_test (ReplacingMergeTree)."""

    name = "replacing_test"
    flag = "replacing_only"

    def __init__(self, replicated: bool, cluster: str = ""):
        self._replicated = replicated
        self._cluster = cluster

    def drop(self, client: Client) -> None:
        drop_replacing_merge(client)

    def create(self, client: Client) -> None:
        create_replacing_merge(client, self._replicated, cluster=self._cluster)

    def insert(
        self,
        client: Client,
        config: InsertConfig,
        tracker: ProgressTracker | None = None,
    ) -> None:
        insert_replacing_merge(
            client, config.rows, config.partitions, config.batch_size,
            config.drop, tracker=tracker,
            throttle_min=config.throttle_min, throttle_max=config.throttle_max,
        )

    @property
    def queries(self) -> QuerySet:
        return QuerySet(
            slow=[
                # Replacement lag: compare row count with vs without FINAL
                """
    SELECT
        'without_final' AS mode,
        count() AS rows,
        uniq(product_id, currency) AS unique_keys
    FROM replacing_test.product_prices
    UNION ALL
    SELECT
        'with_final' AS mode,
        count() AS rows,
        uniq(product_id, currency) AS unique_keys
    FROM replacing_test.product_prices FINAL
    """,
                # Stale row ratio per partition
                """
    SELECT
        toYYYYMM(updated_at) AS partition,
        count() AS total_rows,
        uniq(product_id, currency) AS unique_keys,
        count() - uniq(product_id, currency) AS stale_rows,
        round(1 - uniq(product_id, currency) / count(), 4) AS stale_ratio
    FROM replacing_test.product_prices
    GROUP BY partition
    ORDER BY partition
    """,
                # Category-level aggregation with FINAL (deduplicated)
                """
    SELECT
        category,
        currency,
        count() AS products,
        avg(price) AS avg_price,
        min(price) AS min_price,
        max(price) AS max_price,
        sum(in_stock) AS in_stock_count
    FROM replacing_test.product_prices FINAL
    GROUP BY category, currency
    ORDER BY category, currency
    """,
                # Latest version per product (window function approach vs FINAL)
                """
    SELECT
        product_id,
        currency,
        price,
        version,
        updated_at
    FROM replacing_test.product_prices FINAL
    ORDER BY price DESC
    LIMIT 100
    """,
            ],
            fast=[
                "SELECT count() FROM replacing_test.product_prices",
                "SELECT count() FROM replacing_test.product_prices FINAL",
                "SELECT uniq(product_id, currency) FROM replacing_test.product_prices",
                # Quick stale ratio check
                """SELECT
                    round(1 - (SELECT count() FROM replacing_test.product_prices FINAL)
                            / (SELECT count() FROM replacing_test.product_prices), 4)
                    AS stale_ratio""",
            ],
            pk_generators=[
                # Lookup specific product with FINAL
                lambda: f"""
    SELECT product_id, currency, price, version, updated_at
    FROM replacing_test.product_prices FINAL
    WHERE product_id = {random.randint(0, NUM_PRODUCTS - 1)}
    SETTINGS use_query_cache = 0
    """,
                # Category filter with FINAL
                lambda: f"""
    SELECT count(), avg(price), sum(in_stock)
    FROM replacing_test.product_prices FINAL
    WHERE category = '{_rand_category()}' AND currency = '{_rand_currency()}'
    SETTINGS use_query_cache = 0
    """,

                # ────────────────────────────────────────────────────────
                # [DEDUP-COMPARE] category_agg — Normal vs FINAL vs argMax
                # Compare these three by searching for "DEDUP-COMPARE category_agg"
                # in query logs / system.query_log to compare resource usage.
                #
                # Each query scans the full table single-threaded with heavy
                # aggregates (uniqExact, quantilesExact) and a numbers(3) cross
                # join to amplify work so queries run ~30s on 10M rows.
                # ────────────────────────────────────────────────────────

                # [DEDUP-COMPARE category_agg] 1/3: Normal (no dedup — reads all rows including stale)
                lambda: f"""
    SELECT /* DEDUP-COMPARE category_agg: normal */
        category,
        count() AS products,
        avg(price) AS avg_price,
        sum(in_stock) AS in_stock_count,
        uniqExact(product_id) AS unique_products,
        quantilesExact(0.5, 0.95)(price) AS price_pcts
    FROM replacing_test.product_prices
    CROSS JOIN numbers(3) AS n
    GROUP BY category
    ORDER BY products DESC
    SETTINGS max_threads = 1, use_query_cache = 0, use_uncompressed_cache = 0
    """,
                # [DEDUP-COMPARE category_agg] 2/3: FINAL (engine-level dedup)
                lambda: f"""
    SELECT /* DEDUP-COMPARE category_agg: FINAL */
        category,
        count() AS products,
        avg(price) AS avg_price,
        sum(in_stock) AS in_stock_count,
        uniqExact(product_id) AS unique_products,
        quantilesExact(0.5, 0.95)(price) AS price_pcts
    FROM replacing_test.product_prices FINAL
    CROSS JOIN numbers(3) AS n
    GROUP BY category
    ORDER BY products DESC
    SETTINGS max_threads = 1, use_query_cache = 0, use_uncompressed_cache = 0
    """,
                # [DEDUP-COMPARE category_agg] 3/3: argMax (manual dedup — latest version per key)
                lambda: f"""
    SELECT /* DEDUP-COMPARE category_agg: argMax */
        category,
        count() AS products,
        avg(latest_price) AS avg_price,
        sum(latest_in_stock) AS in_stock_count,
        uniqExact(product_id) AS unique_products,
        quantilesExact(0.5, 0.95)(latest_price) AS price_pcts
    FROM (
        SELECT
            product_id,
            argMax(category, version) AS category,
            argMax(price, version) AS latest_price,
            argMax(in_stock, version) AS latest_in_stock
        FROM replacing_test.product_prices
        CROSS JOIN numbers(3) AS n
        GROUP BY product_id, currency
    )
    GROUP BY category
    ORDER BY products DESC
    SETTINGS max_threads = 1, use_query_cache = 0, use_uncompressed_cache = 0
    """,

                # ────────────────────────────────────────────────────────
                # [DEDUP-COMPARE supplier_topk] — Normal vs FINAL vs argMax
                # Compare these three by searching for "DEDUP-COMPARE supplier_topk"
                # ────────────────────────────────────────────────────────

                # [DEDUP-COMPARE supplier_topk] 1/3: Normal (no dedup)
                lambda: f"""
    SELECT /* DEDUP-COMPARE supplier_topk: normal */
        supplier_id,
        count() AS product_count,
        avg(price) AS avg_price,
        sum(in_stock) AS in_stock_count,
        uniqExact(product_id) AS unique_products
    FROM replacing_test.product_prices
    CROSS JOIN numbers(3) AS n
    GROUP BY supplier_id
    ORDER BY product_count DESC
    LIMIT 50
    SETTINGS max_threads = 1, use_query_cache = 0, use_uncompressed_cache = 0
    """,
                # [DEDUP-COMPARE supplier_topk] 2/3: FINAL (engine-level dedup)
                lambda: f"""
    SELECT /* DEDUP-COMPARE supplier_topk: FINAL */
        supplier_id,
        count() AS product_count,
        avg(price) AS avg_price,
        sum(in_stock) AS in_stock_count,
        uniqExact(product_id) AS unique_products
    FROM replacing_test.product_prices FINAL
    CROSS JOIN numbers(3) AS n
    GROUP BY supplier_id
    ORDER BY product_count DESC
    LIMIT 50
    SETTINGS max_threads = 1, use_query_cache = 0, use_uncompressed_cache = 0
    """,
                # [DEDUP-COMPARE supplier_topk] 3/3: argMax (manual dedup)
                lambda: f"""
    SELECT /* DEDUP-COMPARE supplier_topk: argMax */
        supplier_id,
        count() AS product_count,
        avg(latest_price) AS avg_price,
        sum(latest_in_stock) AS in_stock_count,
        uniqExact(product_id) AS unique_products
    FROM (
        SELECT
            product_id,
            currency,
            argMax(supplier_id, version) AS supplier_id,
            argMax(price, version) AS latest_price,
            argMax(in_stock, version) AS latest_in_stock
        FROM replacing_test.product_prices
        CROSS JOIN numbers(3) AS n
        GROUP BY product_id, currency
    )
    GROUP BY supplier_id
    ORDER BY product_count DESC
    LIMIT 50
    SETTINGS max_threads = 1, use_query_cache = 0, use_uncompressed_cache = 0
    """,

                # ────────────────────────────────────────────────────────
                # [DEDUP-COMPARE price_stats] — Normal vs FINAL vs argMax
                # Compare these three by searching for "DEDUP-COMPARE price_stats"
                # ────────────────────────────────────────────────────────

                # [DEDUP-COMPARE price_stats] 1/3: Normal (no dedup)
                lambda: f"""
    SELECT /* DEDUP-COMPARE price_stats: normal */
        count() AS total,
        avg(price) AS avg_price,
        quantilesExact(0.5, 0.9, 0.99)(price) AS price_pcts,
        sum(in_stock) AS in_stock,
        uniqExact(product_id) AS unique_products
    FROM replacing_test.product_prices
    CROSS JOIN numbers(3) AS n
    SETTINGS max_threads = 1, use_query_cache = 0, use_uncompressed_cache = 0
    """,
                # [DEDUP-COMPARE price_stats] 2/3: FINAL (engine-level dedup)
                lambda: f"""
    SELECT /* DEDUP-COMPARE price_stats: FINAL */
        count() AS total,
        avg(price) AS avg_price,
        quantilesExact(0.5, 0.9, 0.99)(price) AS price_pcts,
        sum(in_stock) AS in_stock,
        uniqExact(product_id) AS unique_products
    FROM replacing_test.product_prices FINAL
    CROSS JOIN numbers(3) AS n
    SETTINGS max_threads = 1, use_query_cache = 0, use_uncompressed_cache = 0
    """,
                # [DEDUP-COMPARE price_stats] 3/3: argMax (manual dedup)
                lambda: f"""
    SELECT /* DEDUP-COMPARE price_stats: argMax */
        count() AS total,
        avg(latest_price) AS avg_price,
        quantilesExact(0.5, 0.9, 0.99)(latest_price) AS price_pcts,
        sum(latest_in_stock) AS in_stock,
        uniqExact(product_id) AS unique_products
    FROM (
        SELECT
            product_id,
            currency,
            argMax(price, version) AS latest_price,
            argMax(in_stock, version) AS latest_in_stock
        FROM replacing_test.product_prices
        CROSS JOIN numbers(3) AS n
        GROUP BY product_id, currency
    )
    SETTINGS max_threads = 1, use_query_cache = 0, use_uncompressed_cache = 0
    """,
            ],
        )


if TYPE_CHECKING:
    _: type[Dataset] = ReplacingMerge  # satisfies Dataset
