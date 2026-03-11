"""Continuous Query Runner for TraceHouse Testing

Runs queries of varying types (slow, fast, PK-pattern, JOIN, S3, settings-variation)
to generate activity for the monitoring dashboard.

Queries are collected from table plugins — each table defines its own workload
queries via the ``queries`` property.  S3 queries are standalone (no local table).

Usage:
    tracehouse-queries [options]
"""

import argparse
import io
import logging
import os
import random
import re
import sys
import threading
import time
import warnings
from collections.abc import Callable
from datetime import datetime

from clickhouse_driver import Client

from data_utils.capabilities import probe, Capabilities
from data_utils.env import env_int, pre_parse_env_file, print_connection, add_connection_args, make_client
from data_utils.tables import SyntheticData, NycTaxi, UkHousePrices, WebAnalytics

# Suppress noisy "Error on socket shutdown" messages from clickhouse-driver
# when TLS connections are closed by the server (common on managed services).
logging.getLogger('clickhouse_driver.connection').setLevel(logging.CRITICAL)
warnings.filterwarnings('ignore', message='.*socket.*')


class _SocketErrorFilter(io.TextIOBase):
    """Wraps stderr to suppress clickhouse-driver socket shutdown noise."""
    def __init__(self, stream):
        self._stream = stream
    def write(self, s):
        if 'Error on socket shutdown' in s:
            return len(s)
        return self._stream.write(s)
    def flush(self):
        self._stream.flush()
    def fileno(self):
        return self._stream.fileno()

sys.stderr = _SocketErrorFilter(sys.stderr)


# ── S3 Parquet queries (no local table, query public datasets) ─────

S3_PARQUET_QUERIES = [
    """
    SELECT
        product_category,
        count() as review_count,
        avg(star_rating) as avg_rating,
        sum(helpful_votes) as total_helpful
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
    GROUP BY product_category
    ORDER BY review_count DESC
    LIMIT 20
    """,
    """
    SELECT
        toYear(CreationDate) as year,
        count() as posts,
        avg(Score) as avg_score,
        sum(ViewCount) as total_views
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2023.parquet')
    GROUP BY year
    """,
    """
    SELECT
        Id,
        Title,
        Score,
        ViewCount
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2023.parquet')
    WHERE Title IS NOT NULL AND position(lower(Title), 'clickhouse') > 0
    ORDER BY Score DESC
    LIMIT 50
    """,
    """
    SELECT
        product_id,
        any(product_title) as title,
        avg(star_rating) as avg_rating,
        count() as mention_count
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
    WHERE position(review_body, 'awesome') > 0
    GROUP BY product_id
    ORDER BY mention_count DESC
    LIMIT 30
    """,
    """
    SELECT
        toYear(toDate(date)) as yr,
        count() as transactions,
        avg(price) as avg_price,
        max(price) as max_price
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/uk-house-prices/parquet/house_prices_*.parquet')
    GROUP BY yr
    ORDER BY yr DESC
    LIMIT 20
    """,
    """
    SELECT
        Location,
        count() as user_count,
        avg(toInt64OrZero(toString(Reputation))) as avg_reputation,
        max(toInt64OrZero(toString(Reputation))) as max_reputation
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet')
    WHERE Location != ''
    GROUP BY Location
    ORDER BY user_count DESC
    LIMIT 30
    """,
    """
    SELECT
        marketplace,
        product_category,
        count() as reviews,
        avg(star_rating) as avg_stars,
        countIf(verified_purchase) as verified_count
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
    GROUP BY marketplace, product_category
    ORDER BY reviews DESC
    LIMIT 50
    """,
    """
    SELECT
        VoteTypeId,
        toYear(toDateTime64OrZero(toString(CreationDate), 3)) as year,
        count() as vote_count
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/votes.parquet')
    WHERE year > 2000
    GROUP BY VoteTypeId, year
    ORDER BY year DESC, vote_count DESC
    LIMIT 50
    """,
]


# ── Query execution infrastructure ────────────────────────────────

def _extract_table(query: str) -> str:
    """Extract the primary table name (db.table) from a SQL query."""
    m = re.search(r'\bFROM\s+(s3\s*\(|(\w+\.\w+))', query, re.IGNORECASE)
    if m:
        if m.group(2):
            return m.group(2)
        return "s3(...)"
    return "?"


def _make_client(host: str, port: int, **conn_kwargs) -> Client:
    """Create a clickhouse-driver Client, suppressing socket shutdown noise."""
    return Client(host=host, port=port, **conn_kwargs)


def _is_connection_error(e: Exception) -> bool:
    """Check if an exception is a connection/socket error that warrants reconnect."""
    msg = str(e).lower()
    return any(tok in msg for tok in [
        'socket', 'connection reset', 'broken pipe', 'eof',
        'errno 57', 'errno 54', 'errno 104', 'connection refused',
        'unexpected eof', 'connection was lost',
    ])


def _is_routing_error(e: Exception) -> bool:
    """Check if an exception looks like a cluster routing issue."""
    msg = str(e)
    return 'Code: 81' in msg or 'Code: 60' in msg


# Track which settings are restricted so we only try them once
_restricted_settings_detected = False

# Global stop event — set on Ctrl+C so workers exit their loops
_stop_event = threading.Event()


def run_query(client_holder: list, host: str, port: int, conn_kwargs: dict,
              query: str, query_type: str) -> None:
    """Execute a single query, reconnecting on socket errors.

    client_holder is a 1-element list so we can swap the client on reconnect.
    """
    if _stop_event.is_set():
        return
    global _restricted_settings_detected
    table = _extract_table(query)
    tagged_query = f"/* run-queries type:{query_type.lower()} table:{table} */ {query}"
    start = time.time()

    extra_settings: dict | None = None if _restricted_settings_detected else {
        'opentelemetry_start_trace_probability': 0.01,
        'opentelemetry_trace_processors': 0,
        'memory_profiler_sample_probability': 1,
        'max_untracked_memory': 1,
        'log_query_threads': 1,
    }

    for attempt in range(4):
        try:
            if extra_settings:
                result = client_holder[0].execute(tagged_query, settings=extra_settings)
            else:
                result = client_holder[0].execute(tagged_query)
            elapsed = time.time() - start
            rows = len(result) if result else 0
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {query_type:5} {table:30} {elapsed:.2f}s ({rows} rows)")
            return
        except Exception as e:
            err_str = str(e)
            if 'Code: 452' in err_str or 'should not be changed' in err_str:
                _restricted_settings_detected = True
                extra_settings = None
                continue
            if (_is_connection_error(e) or _is_routing_error(e)) and attempt < 3:
                try:
                    client_holder[0].disconnect()
                except Exception:
                    pass
                try:
                    client_holder[0] = _make_client(host, port, **conn_kwargs)
                except Exception:
                    pass
                continue
            elapsed = time.time() - start
            error_msg = err_str[:80]
            query_preview = query.replace('\n', ' ')[:100]
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {query_type:5} {table:30} FAILED {elapsed:.2f}s: {error_msg}")
            print(f"    SQL: {query_preview}...")
            return


# ── Generic worker ─────────────────────────────────────────────────

def _query_worker(
    host: str, port: int, interval: float, label: str,
    queries: list[str] | None = None,
    generators: list[Callable[[], str]] | None = None,
    jitter: float = 0.5,
    **conn_kwargs,
) -> None:
    """Generic worker loop. Supply either static queries or generators."""
    client_holder = [_make_client(host, port, **conn_kwargs)]
    while not _stop_event.is_set():
        if generators:
            query = random.choice(generators)()
        elif queries:
            query = random.choice(queries)
        else:
            return
        run_query(client_holder, host, port, conn_kwargs, query, label)
        _stop_event.wait(interval + random.uniform(0, interval * jitter))


# ── Table detection ────────────────────────────────────────────────

def _detect_available_tables(client: Client) -> set[str]:
    """Query system.tables to find which test databases/tables exist."""
    try:
        rows = client.execute(
            "SELECT database || '.' || name FROM system.tables "
            "WHERE database IN ('synthetic_data', 'nyc_taxi', 'uk_price_paid', 'web_analytics')"
        )
        return {r[0].lower() for r in rows}
    except Exception as e:
        print(f"  Warning: Failed to detect tables: {e}")
        return set()


def _collect_queries(
    caps: Capabilities,
    available_tables: set[str],
) -> dict[str, list]:
    """Instantiate table plugins and collect their queries.

    Only includes queries from tables that actually exist in the database.
    """
    # Map table name prefix → plugin instance
    plugins = {
        'synthetic_data': SyntheticData(replicated=False),
        'nyc_taxi': NycTaxi(replicated=False),
        'uk_price_paid': UkHousePrices(replicated=False),
        'web_analytics': WebAnalytics(caps=caps),
    }

    slow: list[str] = []
    fast: list[str] = []
    pk_generators: list[Callable[[], str]] = []
    join_generators: list[Callable[[], str]] = []
    settings_generators: list[Callable[[], str]] = []

    for db_prefix, plugin in plugins.items():
        # Check if any table from this database exists
        if not any(t.startswith(db_prefix + '.') for t in available_tables):
            continue
        qs = plugin.queries
        slow.extend(qs.slow)
        fast.extend(qs.fast)
        pk_generators.extend(qs.pk_generators)
        join_generators.extend(qs.join_generators)
        settings_generators.extend(qs.settings_generators)

    # S3 queries are independent of local tables
    s3 = S3_PARQUET_QUERIES if caps.has_s3_function else []

    return {
        'slow': slow,
        'fast': fast,
        'pk_generators': pk_generators,
        'join_generators': join_generators,
        'settings_generators': settings_generators,
        's3': s3,
    }


def main():
    env_path = pre_parse_env_file()

    parser = argparse.ArgumentParser(description="Run continuous queries for TraceHouse testing")
    add_connection_args(parser)
    parser.add_argument("--slow-interval", type=float, default=float(os.environ.get("CH_QUERY_SLOW_INTERVAL", "1.0")), help="Interval between slow queries (default: $CH_QUERY_SLOW_INTERVAL or 1)")
    parser.add_argument("--fast-interval", type=float, default=float(os.environ.get("CH_QUERY_FAST_INTERVAL", "10.0")), help="Interval between fast queries (default: $CH_QUERY_FAST_INTERVAL or 10)")
    parser.add_argument("--s3-interval", type=float, default=float(os.environ.get("CH_QUERY_S3_INTERVAL", "30.0")), help="Interval between S3 parquet queries (default: $CH_QUERY_S3_INTERVAL or 30)")
    parser.add_argument("--pk-interval", type=float, default=float(os.environ.get("CH_QUERY_PK_INTERVAL", "5.0")), help="Interval between PK pattern queries (default: $CH_QUERY_PK_INTERVAL or 5)")
    parser.add_argument("--slow-workers", type=int, default=env_int("CH_QUERY_SLOW_WORKERS", "5"), help="Number of slow query workers (default: $CH_QUERY_SLOW_WORKERS or 5)")
    parser.add_argument("--fast-workers", type=int, default=env_int("CH_QUERY_FAST_WORKERS", "1"), help="Number of fast query workers (default: $CH_QUERY_FAST_WORKERS or 1)")
    parser.add_argument("--s3-workers", type=int, default=env_int("CH_QUERY_S3_WORKERS", "2"), help="Number of S3 parquet query workers (default: $CH_QUERY_S3_WORKERS or 2)")
    parser.add_argument("--pk-workers", type=int, default=env_int("CH_QUERY_PK_WORKERS", "2"), help="Number of PK pattern query workers (default: $CH_QUERY_PK_WORKERS or 2)")
    parser.add_argument("--join-interval", type=float, default=float(os.environ.get("CH_QUERY_JOIN_INTERVAL", "3.0")), help="Interval between JOIN queries (default: $CH_QUERY_JOIN_INTERVAL or 3)")
    parser.add_argument("--join-workers", type=int, default=env_int("CH_QUERY_JOIN_WORKERS", "2"), help="Number of JOIN query workers (default: $CH_QUERY_JOIN_WORKERS or 2)")
    parser.add_argument("--settings-interval", type=float, default=float(os.environ.get("CH_QUERY_SETTINGS_INTERVAL", "5.0")), help="Interval between settings-variation queries (default: $CH_QUERY_SETTINGS_INTERVAL or 5)")
    parser.add_argument("--settings-workers", type=int, default=env_int("CH_QUERY_SETTINGS_WORKERS", "1"), help="Number of settings-variation query workers (default: $CH_QUERY_SETTINGS_WORKERS or 1)")
    args = parser.parse_args()

    conn_kwargs = dict(user=args.user, password=args.password, secure=args.secure)

    # ── Probe capabilities ──────────────────────────────────────────
    print_connection(args, env_path)
    print(f"\nConnecting to {args.host}:{args.port}...")
    probe_client = make_client(args)

    print("Probing server capabilities...")
    caps = probe(probe_client)
    print(caps.summary())

    available_tables = _detect_available_tables(probe_client)
    print(f"\n  Available tables:      {', '.join(sorted(available_tables)) or '(none found)'}")
    probe_client.disconnect()

    # ── Collect queries from table plugins ─────────────────────────
    pools = _collect_queries(caps, available_tables)

    print(f"\n  Slow queries:          {len(pools['slow'])}")
    print(f"  Fast queries:          {len(pools['fast'])}")
    print(f"  PK query generators:   {len(pools['pk_generators'])}")
    print(f"  JOIN query generators: {len(pools['join_generators'])}")
    print(f"  S3 parquet queries:    {len(pools['s3'])}")
    print(f"  Settings-variation:    {len(pools['settings_generators'])}")

    if not any(pools.values()):
        print("\nNo queries available — have you loaded test data? (just load-data)")
        return

    # ── Start workers ───────────────────────────────────────────────
    worker_configs = [
        ("SLOW",  pools['slow'],              None,                         args.slow_workers,     args.slow_interval,     0.5),
        ("FAST",  pools['fast'],              None,                         args.fast_workers,     args.fast_interval,     0.3),
        ("S3",    pools['s3'],                None,                         args.s3_workers,       args.s3_interval,       0.5),
        ("PK",    None,                       pools['pk_generators'],       args.pk_workers,       args.pk_interval,       0.3),
        ("JOIN",  None,                       pools['join_generators'],     args.join_workers,     args.join_interval,     0.3),
        ("SVAR",  None,                       pools['settings_generators'], args.settings_workers, args.settings_interval, 0.3),
    ]

    print(f"\nStarting workers:")
    threads = []

    for label, queries, generators, num_workers, interval, jitter in worker_configs:
        pool = queries or generators
        if not pool:
            print(f"  {label:5} skipped (no queries)")
            continue
        actual = min(num_workers, len(pool)) if queries else num_workers
        print(f"  {label:5} {actual} workers (interval: {interval}s)")
        for i in range(actual):
            t = threading.Thread(
                target=_query_worker,
                args=(args.host, args.port, interval, label),
                kwargs={**conn_kwargs, 'queries': queries, 'generators': generators, 'jitter': jitter},
                daemon=True,
                name=f"{label.lower()}-worker-{i}",
            )
            t.start()
            threads.append(t)

    print(f"\n{len(threads)} workers running. Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping query runner...")
        _stop_event.set()

        try:
            kill_client = Client(host=args.host, port=args.port, **conn_kwargs)
            killed = kill_client.execute(
                "KILL QUERY WHERE query LIKE '%/* run-queries %' AND user = currentUser() ASYNC"
            )
            if killed:
                print(f"  Cancelled {len(killed)} in-flight ClickHouse queries")
            kill_client.disconnect()
        except Exception:
            pass

        for t in threads:
            t.join(timeout=2)

        print("Stopped")


if __name__ == "__main__":
    main()
