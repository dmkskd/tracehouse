"""
TraceHouse Test Data Generator

Creates databases and tables with synthetic data for testing.
Data is generated partition-by-partition to simulate real-world ingestion.

Usage:
    tracehouse-generate [options]

Examples:
    # Quick test (1M rows, 3 partitions, 100K batches)
    tracehouse-generate --rows 1000000 --partitions 3 --batch-size 100000

    # Medium load (10M rows, 4 partitions, 500K batches)
    tracehouse-generate --rows 10000000 --partitions 4 --batch-size 500000

    # Heavy load (100M rows, default settings)
    tracehouse-generate --rows 100000000

    # Reset and regenerate
    tracehouse-generate --drop --rows 5000000

    # Replicated tables (for clustered setups with Keeper)
    tracehouse-generate --replicated --rows 5000000
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
    Dataset, InsertConfig, InsertMode, ProgressTracker, build_all_datasets,
    DATASET_ALIASES, list_datasets,
)
from data_utils.users import (
    create_test_users, load_test_users_from_env, lock_test_users,
    make_user_client, get_user_for_index, print_test_users,
    verify_test_user, TestUser,
)


# ── CLI ─────────────────────────────────────────────────────────────


def _parse_args() -> tuple[argparse.Namespace, str | None]:
    """Parse CLI arguments and load .env."""
    # Pre-parse --env-file so .env is loaded before argparse reads defaults
    env_path = pre_parse_env_file()

    parser = argparse.ArgumentParser(
        description="Generate test data for TraceHouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Quick test:   tracehouse-generate --rows 1000000 --batch-size 100000
  Medium load:  tracehouse-generate --rows 10000000 --partitions 4
  Reset data:   tracehouse-generate --drop --rows 5000000
  UK only:      tracehouse-generate --uk-only --rows 5000000

Server capabilities (S3, cluster, Keeper) are auto-detected.
Tables are created with the best engine available for the target server.
        """,
    )
    add_connection_args(parser)
    parser.add_argument("--rows", type=int, default=env_int("CH_GEN_ROWS", "10000000"), help="Total rows per table (default: $CH_GEN_ROWS or 10M)")
    parser.add_argument("--partitions", type=int, default=env_int("CH_GEN_PARTITIONS", "3"), help="Number of partitions/months (default: $CH_GEN_PARTITIONS or 3)")
    parser.add_argument("--batch-size", type=int, default=env_int("CH_GEN_BATCH_SIZE", "500000"), help="Rows per INSERT batch (default: $CH_GEN_BATCH_SIZE or 500K)")
    parser.add_argument("--mode", choices=["resume", "drop", "append"],
                        default=os.environ.get("CH_GEN_MODE", "resume").lower(),
                        help="Insert mode: resume (fill to target), drop (recreate), append (always insert) (default: $CH_GEN_MODE or resume)")
    parser.add_argument("--parallelism", type=int, default=env_int("CH_GEN_PARALLELISM", "0"), help="Max tables to generate concurrently (0 = all, default: $CH_GEN_PARALLELISM or 0)")
    parser.add_argument("--throttle-min", type=float, default=float(os.environ.get("CH_GEN_THROTTLE_MIN", "0")), help="Min delay in seconds between batches (default: $CH_GEN_THROTTLE_MIN or 0)")
    parser.add_argument("--throttle-max", type=float, default=float(os.environ.get("CH_GEN_THROTTLE_MAX", "0")), help="Max delay in seconds between batches (default: $CH_GEN_THROTTLE_MAX or 0)")
    parser.add_argument("--synthetic-only", action="store_true", help="Only create synthetic_data table")
    parser.add_argument("--taxi-only", action="store_true", help="Only create nyc_taxi table")
    parser.add_argument("--uk-only", action="store_true", help="Only create uk_price_paid table")
    parser.add_argument("--web-only", action="store_true", help="Only create web_analytics table")
    parser.add_argument("--replacing-only", action="store_true", help="Only create replacing_test table (ReplacingMergeTree)")
    parser.add_argument("--dataset", default=os.environ.get("CH_GEN_DATASET", ""),
                        help="Dataset to generate: synthetic, taxi, uk, web, replacing, or blank for all (default: $CH_GEN_DATASET)")
    parser.add_argument("--list-datasets", action="store_true", help="List available datasets and exit")
    args = parser.parse_args()

    if args.list_datasets:
        list_datasets()
        return args, env_path

    # Map --dataset to the *-only flags
    _ds = args.dataset.strip().lower()
    if _ds:
        target_flag = DATASET_ALIASES.get(_ds)
        if target_flag:
            setattr(args, target_flag, True)
        else:
            print(f"Unknown dataset '{args.dataset}'. Valid: {', '.join(DATASET_ALIASES)}")
            sys.exit(1)

    return args, env_path


# ── Dataset selection ──────────────────────────────────────────────


def _build_datasets(args: argparse.Namespace, caps: Capabilities) -> list[Dataset]:
    """Instantiate dataset plugins, filtered by CLI flags."""
    replicated = caps.has_keeper
    cluster = caps.cluster_name if caps.has_cluster else ""
    all_datasets = build_all_datasets(replicated=replicated, cluster=cluster, caps=caps)
    create_all = not any(getattr(args, ds.flag) for ds in all_datasets)
    return [ds for ds in all_datasets if create_all or getattr(args, ds.flag)]


# ── Prepare (create databases + tables) ───────────────────────────


def _prepare_datasets(
    datasets: list[Dataset],
    config: InsertConfig,
    args: argparse.Namespace,
    test_users: list[TestUser] | None = None,
) -> dict[str, Client]:
    """Drop (if requested), create each dataset's database and tables, return per-dataset clients.

    When *test_users* is provided, each dataset gets a client connected as a
    different test user (round-robin).  Schema DDL is always run as the admin
    (default) user — test users only do the inserts.
    """
    clients: dict[str, Client] = {}
    admin = make_client(args)
    for i, ds in enumerate(datasets):
        print(f"\n── Preparing {ds.name} ──")
        if config.mode is InsertMode.DROP:
            ds.drop(admin)
        ds.create(admin)
        if test_users:
            user = get_user_for_index(test_users, i)
            clients[ds.name] = make_user_client(args, user)
            print(f"   (insert as {user.name})")
        else:
            clients[ds.name] = make_client(args)
    return clients


# ── Insert orchestration ────────────────────────────────────────────


def _run_sequential(datasets: list[Dataset], config: InsertConfig, clients: dict[str, Client]) -> None:
    for ds in datasets:
        print(f"\n── Loading {ds.name} ──")
        ds.insert(clients[ds.name], config)


def _run_parallel(
    datasets: list[Dataset],
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

    def _run_insert(ds: Dataset) -> None:
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


def _print_config(config: InsertConfig, caps: Capabilities, parallelism: int, datasets: list[Dataset]) -> None:
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
    if config.mode is not InsertMode.RESUME:
        print(f"  Mode:                  {config.mode.value}")
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
    print("  WHERE active AND database IN ('synthetic_data', 'nyc_taxi', 'uk_price_paid', 'web_analytics', 'replacing_test')")
    print("  GROUP BY database, table, partition")
    print("  ORDER BY database, table, partition")


# ── Main ────────────────────────────────────────────────────────────


def main() -> None:
    args, env_path = _parse_args()
    if args.list_datasets:
        return
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
        mode=InsertMode(args.mode),
        throttle_min=args.throttle_min,
        throttle_max=args.throttle_max,
    )

    datasets = _build_datasets(args, caps)
    _print_config(config, caps, args.parallelism, datasets)

    # Create test users if requested (env var takes precedence)
    test_users: list[TestUser] | None = load_test_users_from_env()
    users_from_env = test_users is not None
    if test_users:
        print(f"Using {len(test_users)} test users from TRACEHOUSE_TEST_USERS")
        print_test_users(test_users, skew=args.user_skew)
    elif args.users > 0:
        print(f"Creating {args.users} test users...")
        test_users = create_test_users(client, args.users)
        print_test_users(test_users, skew=args.user_skew)
        verify_test_user(args, test_users[0])

    confirm_or_exit(args)

    try:
        # Phase 1: Create databases and tables
        clients = _prepare_datasets(datasets, config, args, test_users)

        # Phase 2: Insert data
        max_workers = len(datasets) if args.parallelism == 0 else min(args.parallelism, len(datasets))

        if max_workers <= 1:
            _run_sequential(datasets, config, clients)
        else:
            print(f"\nLoading {len(datasets)} datasets...")
            _run_parallel(datasets, config, clients, max_workers, args)

        _print_verify_query()
    finally:
        if test_users and not users_from_env:
            print("\nLocking test users...")
            lock_test_users(client, test_users)
            print("  ✓ All test users locked (HOST NONE)")


if __name__ == "__main__":
    main()
