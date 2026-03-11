"""
ClickHouse Mutation Load Test

Generates mutations to test monitoring capabilities:
- Heavy delete:       ALTER TABLE DELETE (rewrites parts, rows removed)
- Heavy update:       ALTER TABLE UPDATE (rewrites parts, rows preserved)
- Lightweight delete: DELETE FROM (masks rows via _row_exists, no part rewrite)
- Lightweight update: UPDATE SET (patch-based) [beta, unstable on CH 26.1]

Usage:
    tracehouse-mutations [options]

Examples:
    # Run default mix of mutations
    tracehouse-mutations

    # Lightweight deletes only
    tracehouse-mutations --type lightweight_delete

    # Heavy mutations only (ALTER TABLE UPDATE/DELETE)
    tracehouse-mutations --type heavy

    # Custom intervals
    tracehouse-mutations --interval 5 --count 20

Mutation types and how they appear in part_log / UI:
  heavy_delete:       MutatePart + NotAMerge, rows_diff < 0 -> Mutation badge
  heavy_update:       MutatePart + NotAMerge, rows_diff = 0 -> Mutation badge
  lightweight_delete: MutatePart (mask phase) -> Mutation badge
                      then MergeParts cleanup  -> LightweightDelete badge (rows_diff < 0)
  lightweight_update: Creates patch-* parts, materialized during next merge.
                      Requires enable_block_number_column=1 and
                      enable_block_offset_column=1 on the target table.
                      BETA: consecutive updates before patch materialization may
                      fail with internal column resolution errors (CH 26.1 bug).

Environment variables (all optional, CLI flags override):
  CH_MUTATION_COUNT     Number of mutations (default: 10)
  CH_MUTATION_INTERVAL  Seconds between mutations (default: 3.0)
  CH_MUTATION_DATABASE  Target database (default: synthetic_data)
  CH_MUTATION_TABLE     Target table (default: events)
  CH_MUTATION_TYPE      Filter: blank=all, or one of:
                        lightweight_delete, lightweight_update,
                        heavy_delete, heavy_update,
                        lightweight, heavy, delete, update
"""

import argparse
import os
import random
import time
from datetime import datetime
from clickhouse_driver import Client

from data_utils.env import env_int, pre_parse_env_file, print_connection, add_connection_args, make_client


def get_random_partition(client: Client, database: str, table: str) -> str | None:
    """Get a random YYYYMM partition from the table.

    Filters to 6-digit numeric partition values so that patch parts
    (which carry hash-based partition IDs like '1e5b2ee238fbe84a863b7581944851ce')
    are excluded.  All our tables use ``PARTITION BY toYYYYMM(...)`` so valid
    partitions are always plain integers such as ``202602``.
    """
    result = client.execute(f"""
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = '{database}' AND table = '{table}' AND active
          AND match(partition, '^\\\\d{{6}}$')
    """)
    if result:
        return random.choice(result)[0]
    return None


def run_heavy_update(client: Client, database: str, table: str, mutations_sync: int = 0) -> dict:
    """Run ALTER TABLE UPDATE (heavy mutation - rewrites parts)."""
    partition = get_random_partition(client, database, table)
    if not partition:
        return {"type": "heavy_update", "status": "skipped", "reason": "no partitions"}

    # Pick a random column and value to update
    if table == "events":
        updates = [
            ("duration_ms", f"duration_ms + {random.randint(1, 100)}"),
            ("device_type", f"'{random.choice(['desktop', 'mobile', 'tablet'])}'"),
        ]
    else:  # trips
        updates = [
            ("trip_distance", f"trip_distance + {random.uniform(0.1, 1.0):.2f}"),
            ("passenger_count", f"{random.randint(1, 4)}"),
        ]

    col, val = random.choice(updates)
    where_pct = random.randint(5, 20)  # Update 5-20% of rows

    # Use sipHash64 on a row identifier instead of rand() — replicated tables
    # require deterministic functions so every replica produces the same result.
    row_hash = "sipHash64(event_id)" if table == "events" else "sipHash64(trip_id)"

    query = f"""
        ALTER TABLE {database}.{table}
        UPDATE {col} = {val}
        WHERE {row_hash} % 100 < {where_pct}
          AND toYYYYMM({"event_date" if table == "events" else "pickup_date"}) = {partition}
        SETTINGS mutations_sync = {mutations_sync}
    """

    start = time.time()
    client.execute(f"/* run-mutations type:heavy_update table:{database}.{table} */ " + query)
    elapsed = time.time() - start

    return {
        "type": "heavy_update",
        "database": database,
        "table": table,
        "partition": partition,
        "column": col,
        "where_pct": where_pct,
        "elapsed": elapsed,
    }


def run_heavy_delete(client: Client, database: str, table: str, mutations_sync: int = 0) -> dict:
    """Run ALTER TABLE DELETE (heavy mutation - rewrites parts)."""
    partition = get_random_partition(client, database, table)
    if not partition:
        return {"type": "heavy_delete", "status": "skipped", "reason": "no partitions"}

    # Delete a small percentage of rows
    delete_pct = random.randint(1, 5)
    date_col = "event_date" if table == "events" else "pickup_date"

    row_hash = "sipHash64(event_id)" if table == "events" else "sipHash64(trip_id)"

    query = f"""
        ALTER TABLE {database}.{table}
        DELETE WHERE {row_hash} % 100 < {delete_pct}
          AND toYYYYMM({date_col}) = {partition}
        SETTINGS mutations_sync = {mutations_sync}
    """

    start = time.time()
    client.execute(f"/* run-mutations type:heavy_delete table:{database}.{table} */ " + query)
    elapsed = time.time() - start

    return {
        "type": "heavy_delete",
        "database": database,
        "table": table,
        "partition": partition,
        "delete_pct": delete_pct,
        "elapsed": elapsed,
    }


def run_lightweight_update(client: Client, database: str, table: str, mutations_sync: int = 0) -> dict:
    """Run lightweight UPDATE (patch-based, doesn't rewrite entire parts).

    Uses the UPDATE ... SET ... WHERE syntax (not ALTER TABLE UPDATE).
    Creates patch parts containing only the updated columns/rows.
    Updated values are immediately visible in SELECT queries.
    Physical materialization happens during subsequent merges.

    BETA: Requires enable_block_number_column=1 and enable_block_offset_column=1
    on the target table. Consecutive updates before patch materialization may
    fail with internal column resolution errors (CH bug as of 26.1).
    """
    partition = get_random_partition(client, database, table)
    if not partition:
        return {"type": "lightweight_update", "status": "skipped", "reason": "no partitions"}

    date_col = "event_date" if table == "events" else "pickup_date"

    if table == "events":
        col, val = "duration_ms", f"duration_ms + {random.randint(1, 50)}"
    else:
        col, val = "tip_amount", f"tip_amount + {random.uniform(0.5, 2.0):.2f}"

    # UPDATE table SET ... WHERE ... — lightweight path using patch parts.
    query = f"""
        UPDATE {database}.{table}
        SET {col} = {val}
        WHERE toYYYYMM({date_col}) = {partition}
        SETTINGS mutations_sync = {mutations_sync}
    """

    start = time.time()
    try:
        client.execute(f"/* run-mutations type:lightweight_update table:{database}.{table} */ " + query)
        elapsed = time.time() - start
        return {
            "type": "lightweight_update",
            "database": database,
            "table": table,
            "partition": partition,
            "column": col,
            "elapsed": elapsed,
        }
    except Exception as e:
        return {
            "type": "lightweight_update",
            "status": "error",
            "error": str(e),
        }


def run_lightweight_delete(client: Client, database: str, table: str, mutations_sync: int = 0) -> dict:
    """Run lightweight DELETE FROM (marks rows via _row_exists mask, no part rewrite)."""
    partition = get_random_partition(client, database, table)
    if not partition:
        return {"type": "lightweight_delete", "status": "skipped", "reason": "no partitions"}

    date_col = "event_date" if table == "events" else "pickup_date"
    delete_pct = random.randint(1, 3)

    row_hash = "sipHash64(event_id)" if table == "events" else "sipHash64(trip_id)"

    # DELETE FROM (not ALTER TABLE DELETE) — this is the lightweight path.
    # It sets _row_exists = 0 on matching rows without rewriting parts.
    # Rows are physically removed during the next merge.
    query = f"""
        DELETE FROM {database}.{table}
        WHERE {row_hash} % 1000 < {delete_pct * 10}
          AND toYYYYMM({date_col}) = {partition}
        SETTINGS mutations_sync = {mutations_sync}
    """

    start = time.time()
    try:
        client.execute(f"/* run-mutations type:lightweight_delete table:{database}.{table} */ " + query)
        elapsed = time.time() - start
        return {
            "type": "lightweight_delete",
            "database": database,
            "table": table,
            "partition": partition,
            "delete_pct": delete_pct,
            "elapsed": elapsed,
        }
    except Exception as e:
        return {
            "type": "lightweight_delete",
            "status": "error",
            "error": str(e),
        }


def get_mutation_status(client: Client) -> list[dict]:
    """Get current mutation status."""
    result = client.execute("""
        SELECT
            database,
            table,
            mutation_id,
            command,
            create_time,
            is_done,
            parts_to_do,
            latest_fail_reason
        FROM system.mutations
        WHERE NOT is_done
        ORDER BY create_time DESC
        LIMIT 10
    """)
    return [
        {
            "database": r[0],
            "table": r[1],
            "mutation_id": r[2],
            "command": r[3][:50] + "..." if len(r[3]) > 50 else r[3],
            "create_time": r[4],
            "is_done": r[5],
            "parts_to_do": r[6],
            "fail_reason": r[7],
        }
        for r in result
    ]


def main():
    env_path = pre_parse_env_file()

    parser = argparse.ArgumentParser(
        description="Run mutation load tests for TraceHouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_connection_args(parser)
    parser.add_argument("--count", type=int, default=env_int("CH_MUTATION_COUNT", "10"), help="Number of mutations to run (default: $CH_MUTATION_COUNT or 10)")
    parser.add_argument("--interval", type=float, default=float(os.environ.get("CH_MUTATION_INTERVAL", "3.0")), help="Seconds between mutations (default: $CH_MUTATION_INTERVAL or 3)")
    parser.add_argument("--type", default=os.environ.get("CH_MUTATION_TYPE", ""),
                        help="Mutation type filter: lightweight_delete, lightweight_update, heavy_delete, heavy_update, "
                             "lightweight, heavy, delete, update, or blank for all (default: $CH_MUTATION_TYPE)")
    parser.add_argument("--database", default=os.environ.get("CH_MUTATION_DATABASE", "synthetic_data"), help="Target database (default: $CH_MUTATION_DATABASE or synthetic_data)")
    parser.add_argument("--table", default=os.environ.get("CH_MUTATION_TABLE", "events"), help="Target table (default: $CH_MUTATION_TABLE or events)")
    parser.add_argument("--sync", default=os.environ.get("CH_MUTATION_SYNC", "async"),
                        choices=["async", "sync"],
                        help="async = fire-and-forget (mutations_sync=0), sync = wait for completion (mutations_sync=1). "
                             "Default: $CH_MUTATION_SYNC or async")
    args = parser.parse_args()

    print_connection(args, env_path)

    print(f"\nConnecting to ClickHouse at {args.host}:{args.port}...")
    client = make_client(args)

    # Verify table exists
    result = client.execute(f"""
        SELECT count() FROM system.tables
        WHERE database = '{args.database}' AND name = '{args.table}'
    """)
    if result[0][0] == 0:
        print(f"Error: Table {args.database}.{args.table} does not exist")
        print("Run 'just load-data' first to create test data")
        return

    # Build mutation list based on --type filter
    all_mutations = {
        'heavy_update':       run_heavy_update,
        'heavy_delete':       run_heavy_delete,
        'lightweight_update': run_lightweight_update,
        'lightweight_delete': run_lightweight_delete,
    }
    type_filter = args.type.strip().lower()
    if type_filter and type_filter in all_mutations:
        # Exact match: e.g. "lightweight_delete"
        mutations = [all_mutations[type_filter]]
    elif type_filter:
        # Partial match: e.g. "lightweight" or "delete"
        mutations = [fn for key, fn in all_mutations.items() if type_filter in key]
    else:
        mutations = list(all_mutations.values())

    if not mutations:
        print(f"Error: No mutation types match '{type_filter}'")
        print(f"  Valid: {', '.join(all_mutations.keys())}, lightweight, heavy, delete, update")
        return

    mutations_sync = 0 if args.sync == "async" else 1

    print(f"\nMutation Load Test")
    print(f"  Target: {args.database}.{args.table}")
    print(f"  Count: {args.count}")
    print(f"  Interval: {args.interval}s")
    print(f"  Types: {', '.join(m.__name__ for m in mutations)}")
    print(f"  Mode: {args.sync} (mutations_sync={mutations_sync})")
    print()

    for i in range(args.count):
        mutation_fn = random.choice(mutations)
        print(f"[{i+1}/{args.count}] Running {mutation_fn.__name__}...", end=" ")

        try:
            result = mutation_fn(client, args.database, args.table, mutations_sync=mutations_sync)
        except KeyboardInterrupt:
            print("INTERRUPTED")
            break

        if result.get("status") == "skipped":
            print(f"SKIPPED ({result.get('reason')})")
        elif result.get("status") == "error":
            print(f"ERROR: {result.get('error')}")
        else:
            print(f"OK ({result.get('elapsed', 0):.2f}s)")
            if "partition" in result:
                tbl = f"{result.get('database', args.database)}.{result.get('table', args.table)}"
                print(f"       {tbl}  partition={result['partition']}", end="")
                if "column" in result:
                    print(f", column={result['column']}", end="")
                if "where_pct" in result:
                    print(f", affected~{result['where_pct']}%", end="")
                if "delete_pct" in result:
                    print(f", deleted~{result['delete_pct']}%", end="")
                print()

        if i < args.count - 1:
            try:
                time.sleep(args.interval)
            except KeyboardInterrupt:
                print("\nInterrupted during wait")
                break
    else:
        # Only show status if we completed all mutations (no break)
        # Show final mutation status
        print("\n" + "=" * 60)
        print("Active Mutations:")
        print("=" * 60)
        status = get_mutation_status(client)
        if status:
            for m in status:
                print(f"  {m['database']}.{m['table']}: {m['command']}")
                print(f"    parts_to_do={m['parts_to_do']}, created={m['create_time']}")
                if m['fail_reason']:
                    print(f"    FAILED: {m['fail_reason']}")
        else:
            print("  No active mutations (all completed)")

        print("\n✓ Mutation load test complete")
        return

    # Interrupted — kill in-flight mutations from our session
    print("\nCancelling in-flight mutations...")
    try:
        client.execute(
            "KILL QUERY WHERE query LIKE '%ALTER TABLE%' AND user = currentUser() ASYNC"
        )
    except Exception:
        pass
    print("✗ Mutation load test interrupted")


if __name__ == "__main__":
    main()
