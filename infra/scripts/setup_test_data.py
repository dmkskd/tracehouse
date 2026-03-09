#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["clickhouse-driver[lz4]", "python-dotenv"]
# ///
"""
TraceHouse Test Data Setup

Creates databases and tables with synthetic data for testing.
Data is loaded partition-by-partition to simulate real-world ingestion.

Usage:
    uv run scripts/setup_test_data.py [options]

Examples:
    # Quick test (1M rows, 3 partitions, 100K batches)
    uv run scripts/setup_test_data.py --rows 1000000 --partitions 3 --batch-size 100000

    # Medium load (10M rows, 4 partitions, 500K batches)
    uv run scripts/setup_test_data.py --rows 10000000 --partitions 4 --batch-size 500000

    # Heavy load (100M rows, default settings)
    uv run scripts/setup_test_data.py --rows 100000000

    # Reset and reload
    uv run scripts/setup_test_data.py --drop --rows 5000000

    # Replicated tables (for clustered setups with Keeper)
    uv run scripts/setup_test_data.py --replicated --rows 5000000
"""

import argparse
import os
import sys
import concurrent.futures
from clickhouse_driver import Client

# Allow importing sibling modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ch_capabilities import probe
from tables import (
    drop_synthetic_data, create_synthetic_data, insert_synthetic_data,
    drop_nyc_taxi, create_nyc_taxi, insert_nyc_taxi,
    drop_uk_house_prices, create_uk_house_prices, insert_uk_house_prices,
    drop_web_analytics, create_web_analytics, insert_web_analytics,
    drop_dimension_tables, create_dimension_tables, insert_dimension_tables,
    ProgressTracker,
)


# ── Env / config helpers ────────────────────────────────────────────


def _env_int(key: str, default: str) -> int:
    """Read an int from env, allowing underscores (e.g. 100_000_000)."""
    return int(os.environ.get(key, default).replace("_", ""))


def _load_env_file(env_file: str | None = None) -> str | None:
    """Load .env file into os.environ.

    When the file is explicitly requested (via --env-file or $CH_ENV_FILE),
    its values override any existing environment variables so the user's
    intent is always honoured.  When falling back to the default repo-root
    .env, existing env vars take precedence (override=False).

    Resolution order:
      1. Explicit --env-file path
      2. $CH_ENV_FILE environment variable
      3. .env in the repo root (infra/scripts/../../.env)

    Returns the resolved path if a file was loaded, else None.
    """
    from dotenv import load_dotenv

    explicit = False
    if env_file:
        path = env_file
        explicit = True
    elif os.environ.get("CH_ENV_FILE"):
        path = os.environ["CH_ENV_FILE"]
        explicit = True
    else:
        path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")

    path = os.path.abspath(path)
    if os.path.isfile(path):
        load_dotenv(path, override=explicit)
        return path
    return None


def _obfuscate(password: str) -> str:
    """Return an obfuscated password for display."""
    if not password:
        return "(empty)"
    if len(password) <= 4:
        return "****"
    return password[:2] + "*" * (len(password) - 4) + password[-2:]


def _print_resolved_config(args: argparse.Namespace, env_path: str | None) -> None:
    """Print which .env was loaded and the final resolved connection variables."""
    print("─" * 50)
    if env_path:
        print(f"  Env file:  {env_path}")
    else:
        print("  Env file:  (none found)")
    print(f"  CH_HOST:     {args.host}")
    print(f"  CH_PORT:     {args.port}")
    print(f"  CH_USER:     {args.user}")
    print(f"  CH_PASSWORD: {_obfuscate(args.password)}")
    print(f"  CH_SECURE:   {args.secure}")
    print("─" * 50)


# ── CLI ─────────────────────────────────────────────────────────────


def main():
    # Pre-parse --env-file so .env is loaded before argparse reads defaults
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--env-file", default=None, help="Path to .env file (default: $CH_ENV_FILE or <repo>/.env)")
    pre_args, _ = pre.parse_known_args()
    env_path = _load_env_file(pre_args.env_file)

    parser = argparse.ArgumentParser(
        description="Setup test data for TraceHouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Quick test:   uv run scripts/setup_test_data.py --rows 1000000 --batch-size 100000
  Medium load:  uv run scripts/setup_test_data.py --rows 10000000 --partitions 4
  Reset data:   uv run scripts/setup_test_data.py --drop --rows 5000000
  UK only:      uv run scripts/setup_test_data.py --uk-only --rows 5000000

Server capabilities (S3, cluster, Keeper) are auto-detected.
Tables are created with the best engine available for the target server.
        """,
    )
    parser.add_argument("--env-file", default=None, help="Path to .env file (default: $CH_ENV_FILE or <repo>/.env)")
    parser.add_argument("--host", default=os.environ.get("CH_HOST", "localhost"), help="ClickHouse host (default: $CH_HOST or localhost)")
    parser.add_argument("--port", type=int, default=_env_int("CH_PORT", "9000"), help="ClickHouse native port (default: $CH_PORT or 9000)")
    parser.add_argument("--user", default=os.environ.get("CH_USER", "default"), help="ClickHouse user (default: $CH_USER or default)")
    parser.add_argument("--password", default=os.environ.get("CH_PASSWORD", ""), help="ClickHouse password (default: $CH_PASSWORD or empty)")
    parser.add_argument("--secure", action="store_true", default=os.environ.get("CH_SECURE", "").lower() in ("1", "true", "yes"), help="Use TLS (default: $CH_SECURE or false)")
    parser.add_argument("--rows", type=int, default=_env_int("CH_LOAD_ROWS", "10000000"), help="Total rows per table (default: $CH_LOAD_ROWS or 10M)")
    parser.add_argument("--partitions", type=int, default=_env_int("CH_LOAD_PARTITIONS", "3"), help="Number of partitions/months (default: $CH_LOAD_PARTITIONS or 3)")
    parser.add_argument("--batch-size", type=int, default=_env_int("CH_LOAD_BATCH_SIZE", "500000"), help="Rows per INSERT batch (default: $CH_LOAD_BATCH_SIZE or 500K)")
    parser.add_argument("--drop", action="store_true", default=os.environ.get("CH_LOAD_DROP", "").lower() in ("1", "true", "yes"), help="Drop existing tables before creating (default: $CH_LOAD_DROP or false)")
    parser.add_argument("--parallelism", type=int, default=_env_int("CH_LOAD_PARALLELISM", "0"), help="Max tables to load concurrently (0 = all, default: $CH_LOAD_PARALLELISM or 0)")
    parser.add_argument("--throttle-min", type=float, default=float(os.environ.get("CH_LOAD_THROTTLE_MIN", "0")), help="Min delay in seconds between batches (default: $CH_LOAD_THROTTLE_MIN or 0)")
    parser.add_argument("--throttle-max", type=float, default=float(os.environ.get("CH_LOAD_THROTTLE_MAX", "0")), help="Max delay in seconds between batches (default: $CH_LOAD_THROTTLE_MAX or 0)")
    parser.add_argument("--synthetic-only", action="store_true", help="Only create synthetic_data table")
    parser.add_argument("--taxi-only", action="store_true", help="Only create nyc_taxi table")
    parser.add_argument("--uk-only", action="store_true", help="Only create uk_price_paid table")
    parser.add_argument("--web-only", action="store_true", help="Only create web_analytics table")
    parser.add_argument("--dataset", default=os.environ.get("CH_LOAD_DATASET", ""),
                        help="Dataset to load: synthetic, taxi, uk, web, or blank for all (default: $CH_LOAD_DATASET)")
    args = parser.parse_args()

    # Map --dataset to the *-only flags
    _ds = args.dataset.strip().lower()
    if _ds == "synthetic": args.synthetic_only = True
    elif _ds == "taxi":    args.taxi_only = True
    elif _ds == "uk":      args.uk_only = True
    elif _ds == "web":     args.web_only = True

    _print_resolved_config(args, env_path)
    print(f"\nConnecting to ClickHouse at {args.host}:{args.port}...")
    client = Client(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        secure=args.secure,
        compression='lz4',
    )

    # ── Probe capabilities ──────────────────────────────────────────
    print("\nProbing server capabilities...")
    caps = probe(client)
    print(caps.summary())

    use_replicated = caps.has_keeper

    print()
    print(f"Configuration:")
    print(f"  Total rows per table: {args.rows:,}")
    print(f"  Partitions: {args.partitions}")
    print(f"  Batch size: {args.batch_size:,}")
    print(f"  Batches per partition: {(args.rows // args.partitions + args.batch_size - 1) // args.batch_size}")
    print(f"  Engine mode:           {'ReplicatedMergeTree' if use_replicated else 'MergeTree'} (auto-detected)")
    print(f"  Parallelism:           {'all tables' if args.parallelism == 0 else f'{args.parallelism} concurrent'}")
    if args.throttle_max > 0:
        print(f"  Throttle:              {args.throttle_min:.1f}s – {args.throttle_max:.1f}s between batches")
    print()

    create_all = not (args.synthetic_only or args.taxi_only or args.uk_only or args.web_only)

    # ── Build table task list ───────────────────────────────────────

    def _make_client():
        return Client(
            host=args.host, port=args.port,
            user=args.user, password=args.password,
            secure=args.secure, compression='lz4',
        )

    table_defs = []
    if create_all or args.synthetic_only:
        table_defs.append(("synthetic_data",
            lambda c: (drop_synthetic_data(c) if args.drop else None, create_synthetic_data(c, use_replicated)),
            lambda c, tr: insert_synthetic_data(c, args.rows, args.partitions, args.batch_size, args.drop, tracker=tr, throttle_min=args.throttle_min, throttle_max=args.throttle_max),
        ))
    if create_all or args.taxi_only:
        table_defs.append(("nyc_taxi",
            lambda c: (drop_nyc_taxi(c) if args.drop else None, create_nyc_taxi(c, use_replicated, caps)),
            lambda c, tr: insert_nyc_taxi(c, args.rows, args.partitions, args.batch_size, args.drop, tracker=tr, throttle_min=args.throttle_min, throttle_max=args.throttle_max),
        ))
    if create_all or args.uk_only:
        table_defs.append(("uk_price_paid",
            lambda c: (drop_uk_house_prices(c) if args.drop else None, create_uk_house_prices(c, use_replicated)),
            lambda c, tr: insert_uk_house_prices(c, args.rows, args.partitions, args.batch_size, args.drop, tracker=tr, throttle_min=args.throttle_min, throttle_max=args.throttle_max),
        ))
    if create_all or args.web_only:
        table_defs.append(("web_analytics",
            lambda c: (drop_web_analytics(c, caps) if args.drop else None, create_web_analytics(c, caps)),
            lambda c, tr: insert_web_analytics(c, args.rows, args.partitions, args.batch_size, args.drop, caps, tracker=tr, throttle_min=args.throttle_min, throttle_max=args.throttle_max),
        ))

    # ── Execute (parallel or sequential) ────────────────────────────
    max_workers = len(table_defs) if args.parallelism == 0 else min(args.parallelism, len(table_defs))

    # Helper: set up dimension tables right after databases/tables are created
    # but before the bulk insert phase, so JOINs work while data is still loading.
    has_synthetic = create_all or args.synthetic_only
    has_taxi = create_all or args.taxi_only

    def _setup_dimension_tables(reuse_client: Client):
        if has_synthetic or has_taxi:
            print("\nSetting up dimension tables for JOIN queries...")
            if args.drop:
                drop_dimension_tables(reuse_client, has_taxi=has_taxi, has_synthetic=has_synthetic)
            create_dimension_tables(reuse_client, use_replicated, has_taxi=has_taxi, has_synthetic=has_synthetic)
            insert_dimension_tables(reuse_client, has_taxi=has_taxi, has_synthetic=has_synthetic)

    if max_workers <= 1:
        # Sequential — create all tables first, then dimension tables, then inserts
        seq_clients = {}
        for name, setup_fn, _ in table_defs:
            print(f"\n── Preparing {name} ──")
            c = _make_client()
            setup_fn(c)
            seq_clients[name] = c

        # Reuse a client that already created a database on this node
        _dim_client = seq_clients.get("nyc_taxi") or seq_clients.get("synthetic_data") or _make_client()
        _setup_dimension_tables(_dim_client)

        for name, _, insert_fn in table_defs:
            print(f"\n── Loading {name} ──")
            insert_fn(seq_clients[name], None)
    else:
        # Parallel — create all tables first (prints normally), then insert with progress bars
        print(f"\nPreparing {len(table_defs)} tables...")
        clients = {}
        for name, setup_fn, _ in table_defs:
            c = _make_client()
            setup_fn(c)
            clients[name] = c

        # Reuse a client that already created a database on this node
        _dim_client = clients.get("nyc_taxi") or clients.get("synthetic_data") or _make_client()
        _setup_dimension_tables(_dim_client)

        tracker = ProgressTracker()
        for name, _, _ in table_defs:
            tracker.register(name, args.rows)

        print(f"\nLoading with {max_workers} concurrent workers...\n")
        tracker.start()

        def _run_insert(name, insert_fn):
            try:
                insert_fn(clients[name], tracker)
            except Exception:
                if not tracker.cancelled.is_set():
                    raise

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        try:
            futures = {pool.submit(_run_insert, name, insert_fn): name for name, _, insert_fn in table_defs}
            for future in concurrent.futures.as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    if not tracker.cancelled.is_set():
                        tracker.skip(name, f"failed: {e}")
        except KeyboardInterrupt:
            print("\n\nInterrupted — stopping workers...")
            tracker.cancelled.set()
            pool.shutdown(wait=False, cancel_futures=True)
            # Kill any in-flight ClickHouse INSERT queries from our load
            try:
                kill_client = _make_client()
                kill_client.execute(
                    "KILL QUERY WHERE query LIKE 'INSERT INTO %' AND user = currentUser() ASYNC"
                )
            except Exception:
                pass
        else:
            pool.shutdown(wait=True)
        tracker.stop()

        if tracker.cancelled.is_set():
            print("\n✗ Load cancelled by user (partial data may exist)")
            sys.exit(1)

    print("\n" + "=" * 60)
    print("✓ Test data setup complete!")
    print("=" * 60)

    print("\nVerify with:")
    print("  SELECT database, table, partition,")
    print("         formatReadableSize(sum(bytes_on_disk)) as size,")
    print("         sum(rows) as rows, count() as parts")
    print("  FROM system.parts")
    print("  WHERE active AND database IN ('synthetic_data', 'nyc_taxi', 'uk_price_paid', 'web_analytics')")
    print("  GROUP BY database, table, partition")
    print("  ORDER BY database, table, partition")


if __name__ == "__main__":
    main()
