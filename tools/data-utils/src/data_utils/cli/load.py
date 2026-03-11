"""
TraceHouse Test Data Setup

Creates databases and tables with synthetic data for testing.
Data is loaded partition-by-partition to simulate real-world ingestion.

Usage:
    tracehouse-load [options]

Examples:
    # Quick test (1M rows, 3 partitions, 100K batches)
    tracehouse-load --rows 1000000 --partitions 3 --batch-size 100000

    # Medium load (10M rows, 4 partitions, 500K batches)
    tracehouse-load --rows 10000000 --partitions 4 --batch-size 500000

    # Heavy load (100M rows, default settings)
    tracehouse-load --rows 100000000

    # Reset and reload
    tracehouse-load --drop --rows 5000000

    # Replicated tables (for clustered setups with Keeper)
    tracehouse-load --replicated --rows 5000000
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import sys

from clickhouse_driver import Client

from data_utils.capabilities import Capabilities, probe
from data_utils.env import (
    env_int, print_connection,
    add_connection_args, make_client, pre_parse_env_file, confirm_or_exit,
)
from data_utils.tables import (
    SyntheticData, NycTaxi, UkHousePrices, WebAnalytics, InsertConfig,
    ProgressTracker,
)


# ── CLI ─────────────────────────────────────────────────────────────


def _parse_args() -> tuple[argparse.Namespace, str | None]:
    """Parse CLI arguments and load .env."""
    # Pre-parse --env-file so .env is loaded before argparse reads defaults
    env_path = pre_parse_env_file()

    parser = argparse.ArgumentParser(
        description="Setup test data for TraceHouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Quick test:   tracehouse-load --rows 1000000 --batch-size 100000
  Medium load:  tracehouse-load --rows 10000000 --partitions 4
  Reset data:   tracehouse-load --drop --rows 5000000
  UK only:      tracehouse-load --uk-only --rows 5000000

Server capabilities (S3, cluster, Keeper) are auto-detected.
Tables are created with the best engine available for the target server.
        """,
    )
    add_connection_args(parser)
    parser.add_argument("--rows", type=int, default=env_int("CH_LOAD_ROWS", "10000000"), help="Total rows per table (default: $CH_LOAD_ROWS or 10M)")
    parser.add_argument("--partitions", type=int, default=env_int("CH_LOAD_PARTITIONS", "3"), help="Number of partitions/months (default: $CH_LOAD_PARTITIONS or 3)")
    parser.add_argument("--batch-size", type=int, default=env_int("CH_LOAD_BATCH_SIZE", "500000"), help="Rows per INSERT batch (default: $CH_LOAD_BATCH_SIZE or 500K)")
    parser.add_argument("--drop", action="store_true", default=os.environ.get("CH_LOAD_DROP", "").lower() in ("1", "true", "yes"), help="Drop existing tables before creating (default: $CH_LOAD_DROP or false)")
    parser.add_argument("--parallelism", type=int, default=env_int("CH_LOAD_PARALLELISM", "0"), help="Max tables to load concurrently (0 = all, default: $CH_LOAD_PARALLELISM or 0)")
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

    return args, env_path


# ── Dataset selection ──────────────────────────────────────────────


def _build_datasets(args: argparse.Namespace, caps: Capabilities) -> list:
    """Instantiate dataset plugins, filtered by CLI flags."""
    replicated = caps.has_keeper
    all_datasets = [
        SyntheticData(replicated=replicated),
        NycTaxi(replicated=replicated, caps=caps),
        UkHousePrices(replicated=replicated),
        WebAnalytics(caps=caps),
    ]
    create_all = not any(getattr(args, ds.flag) for ds in all_datasets)
    return [ds for ds in all_datasets if create_all or getattr(args, ds.flag)]


# ── Prepare (create databases + tables) ───────────────────────────


def _prepare_datasets(datasets: list, config: InsertConfig, args: argparse.Namespace) -> dict[str, Client]:
    """Drop (if requested), create each dataset's database and tables, return per-dataset clients."""
    clients: dict[str, Client] = {}
    for ds in datasets:
        print(f"\n── Preparing {ds.name} ──")
        c = make_client(args)
        if config.drop:
            ds.drop(c)
        ds.create(c)
        clients[ds.name] = c
    return clients


# ── Insert orchestration ────────────────────────────────────────────


def _run_sequential(datasets: list, config: InsertConfig, clients: dict[str, Client]) -> None:
    for ds in datasets:
        print(f"\n── Loading {ds.name} ──")
        ds.insert(clients[ds.name], config)


def _run_parallel(
    datasets: list,
    config: InsertConfig,
    clients: dict[str, Client],
    max_workers: int,
    args: argparse.Namespace,
) -> None:
    tracker = ProgressTracker()
    for ds in datasets:
        tracker.register(ds.name, config.rows)

    print(f"\nLoading with {max_workers} concurrent workers...\n")
    tracker.start()

    def _run_insert(ds):
        try:
            ds.insert(clients[ds.name], config, tracker=tracker)
        except Exception:
            if not tracker.cancelled.is_set():
                raise

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {pool.submit(_run_insert, ds): ds.name for ds in datasets}
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
        try:
            kill_client = make_client(args)
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


# ── Printing helpers ────────────────────────────────────────────────


def _print_config(config: InsertConfig, caps: Capabilities, parallelism: int, datasets: list) -> None:
    engine = 'ReplicatedMergeTree' if caps.has_keeper else 'MergeTree'
    batches = (config.rows // config.partitions + config.batch_size - 1) // config.batch_size
    par_label = 'all datasets' if parallelism == 0 else f'{parallelism} concurrent'

    print()
    print("Configuration:")
    print(f"  Total rows per table: {config.rows:,}")
    print(f"  Partitions: {config.partitions}")
    print(f"  Batch size: {config.batch_size:,}")
    print(f"  Batches per partition: {batches}")
    print(f"  Engine mode:           {engine} (auto-detected)")
    print(f"  Parallelism:           {par_label}")
    if config.throttle_max > 0:
        print(f"  Throttle:              {config.throttle_min:.1f}s – {config.throttle_max:.1f}s between batches")
    print(f"  Datasets:              {', '.join(ds.name for ds in datasets)}")
    if config.drop:
        print(f"  Drop existing:         yes")
    print()


def _print_verify_query() -> None:
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


# ── Main ────────────────────────────────────────────────────────────


def main():
    args, env_path = _parse_args()
    print_connection(args, env_path)

    # Connect and probe
    print(f"\nConnecting to ClickHouse at {args.host}:{args.port}...")
    client = make_client(args)
    print("\nProbing server capabilities...")
    caps = probe(client)
    print(caps.summary())

    config = InsertConfig(
        rows=args.rows,
        partitions=args.partitions,
        batch_size=args.batch_size,
        drop=args.drop,
        throttle_min=args.throttle_min,
        throttle_max=args.throttle_max,
    )

    datasets = _build_datasets(args, caps)
    _print_config(config, caps, args.parallelism, datasets)
    confirm_or_exit(args)

    # Phase 1: Create databases and tables
    clients = _prepare_datasets(datasets, config, args)

    # Phase 2: Insert data
    max_workers = len(datasets) if args.parallelism == 0 else min(args.parallelism, len(datasets))

    if max_workers <= 1:
        _run_sequential(datasets, config, clients)
    else:
        print(f"\nLoading {len(datasets)} datasets...")
        _run_parallel(datasets, config, clients, max_workers, args)

    _print_verify_query()


if __name__ == "__main__":
    main()
