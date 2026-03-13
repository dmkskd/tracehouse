"""
ClickHouse Merge Type Trigger & Classifier

Triggers all known merge types so you can observe them in system.merges
and system.part_log. Useful for integration testing and understanding
what's happening inside your ClickHouse instance.

Merge types triggered:
  1. Regular       — background merge from many small inserts
  2. TTLDelete     — merge triggered by TTL DELETE expiry
  3. TTLRecompress — merge triggered by TTL RECOMPRESS expiry
  4. TTLMove       — part relocated to different volume/disk (NOT a merge!)
  5. Mutation      — ALTER TABLE UPDATE/DELETE applied to parts
  6. OPTIMIZE      — user-initiated forced merge

Usage:
    tracehouse-merge-triggers [options]

Examples:
    # Trigger all merge types (default)
    tracehouse-merge-triggers

    # Only trigger TTL merges
    tracehouse-merge-triggers --types ttl_delete,ttl_recompress

    # Trigger regular + optimize, then watch for 60s
    tracehouse-merge-triggers --types regular,optimize --watch 60
"""

import argparse
import sys
import time
from datetime import datetime
from clickhouse_driver import Client

from data_utils.capabilities import probe
from data_utils.env import (
    env_int,
    pre_parse_env_file,
    print_connection,
    add_connection_args,
    make_client,
)
from data_utils.users import (
    create_test_users, load_test_users_from_env, lock_test_users,
    make_user_client, pick_random_user, print_test_users, TestUser,
)


# ── Dedicated test database & tables ────────────────────────────────

DB = "merge_type_test"

# Table with TTL DELETE — rows older than 5s get deleted
TABLE_TTL_DELETE = "ttl_delete_test"

# Table with TTL RECOMPRESS — data older than 5s gets recompressed
TABLE_TTL_RECOMPRESS = "ttl_recompress_test"

# Table with TTL MOVE — parts move from local to S3 volume
TABLE_TTL_MOVE = "ttl_move_test"

# Plain table for regular merges, mutations, and OPTIMIZE
TABLE_REGULAR = "regular_merge_test"


def _has_s3tiered(client: Client) -> bool:
    """Check if the s3tiered storage policy is available."""
    try:
        rows = client.execute(
            "SELECT policy_name FROM system.storage_policies WHERE policy_name = 's3tiered'"
        )
        return len(rows) > 0
    except Exception:
        return False


def setup_test_tables(client: Client) -> bool:
    """Create the dedicated merge-test database and tables.

    Returns True if s3tiered storage is available (TTL MOVE table created).
    """
    print(f"Setting up {DB} database...")
    client.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")
    has_s3 = _has_s3tiered(client)

    # Regular merge table — no TTL, just a plain MergeTree
    client.execute(f"DROP TABLE IF EXISTS {DB}.{TABLE_REGULAR} SYNC")
    client.execute(f"""
        CREATE TABLE {DB}.{TABLE_REGULAR}
        (
            id UInt64,
            ts DateTime DEFAULT now(),
            value Float64,
            category LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY (category, ts)
        SETTINGS
            old_parts_lifetime = 5,
            min_bytes_for_wide_part = 0
    """)
    print(f"  ✓ {DB}.{TABLE_REGULAR}")

    # TTL DELETE table — rows expire 5 seconds after insertion
    client.execute(f"DROP TABLE IF EXISTS {DB}.{TABLE_TTL_DELETE} SYNC")
    client.execute(f"""
        CREATE TABLE {DB}.{TABLE_TTL_DELETE}
        (
            id UInt64,
            ts DateTime DEFAULT now(),
            value Float64,
            category LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY (category, ts)
        TTL ts + INTERVAL 5 SECOND DELETE
        SETTINGS
            old_parts_lifetime = 5,
            merge_with_ttl_timeout = 1,
            min_bytes_for_wide_part = 0
    """)
    print(f"  ✓ {DB}.{TABLE_TTL_DELETE} (TTL DELETE after 5s)")

    # TTL RECOMPRESS table — data gets recompressed after 5 seconds
    client.execute(f"DROP TABLE IF EXISTS {DB}.{TABLE_TTL_RECOMPRESS} SYNC")
    client.execute(f"""
        CREATE TABLE {DB}.{TABLE_TTL_RECOMPRESS}
        (
            id UInt64,
            ts DateTime DEFAULT now(),
            value Float64,
            category LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY (category, ts)
        TTL ts + INTERVAL 5 SECOND RECOMPRESS CODEC(ZSTD(15))
        SETTINGS
            old_parts_lifetime = 5,
            merge_with_recompression_ttl_timeout = 1,
            min_bytes_for_wide_part = 0
    """)
    print(f"  ✓ {DB}.{TABLE_TTL_RECOMPRESS} (TTL RECOMPRESS after 5s)")

    # TTL MOVE table — parts move from default volume to s3cached after 5 seconds
    # This requires the s3tiered storage policy (Docker setup with MinIO)
    client.execute(f"DROP TABLE IF EXISTS {DB}.{TABLE_TTL_MOVE} SYNC")
    if has_s3:
        client.execute(f"""
            CREATE TABLE {DB}.{TABLE_TTL_MOVE}
            (
                id UInt64,
                ts DateTime DEFAULT now(),
                value Float64,
                category LowCardinality(String)
            )
            ENGINE = MergeTree()
            ORDER BY (category, ts)
            TTL ts + INTERVAL 5 SECOND TO VOLUME 's3cached'
            SETTINGS
                old_parts_lifetime = 5,
                merge_with_ttl_timeout = 1,
                storage_policy = 's3tiered',
                min_bytes_for_wide_part = 0
        """)
        print(f"  ✓ {DB}.{TABLE_TTL_MOVE} (TTL MOVE to s3cached after 5s)")
    else:
        print(f"  ⊘ {DB}.{TABLE_TTL_MOVE} skipped (s3tiered storage policy not available)")

    return has_s3


def cleanup_test_tables(client: Client) -> None:
    """Drop the merge-test database."""
    print(f"\nCleaning up {DB} database...")
    client.execute(f"DROP DATABASE IF EXISTS {DB} SYNC")
    print("  ✓ Cleaned up")


# ── Merge triggers ──────────────────────────────────────────────────

CATEGORIES = ["alpha", "bravo", "charlie", "delta", "echo"]


def _small_insert(client: Client, table: str, rows: int = 100) -> None:
    """Insert a small batch to create a Level-0 part."""
    cats = ", ".join(f"('{c}')" for c in CATEGORIES)
    client.execute(f"""
        INSERT INTO {DB}.{table}
        SELECT
            rand64() AS id,
            now() - toIntervalSecond(rand() % 10) AS ts,
            rand() / 4294967295.0 * 1000 AS value,
            arrayElement([{', '.join(f"'{c}'" for c in CATEGORIES)}], (number % {len(CATEGORIES)}) + 1) AS category
        FROM numbers({rows})
    """)


def trigger_regular_merge(client: Client) -> dict:
    """Create many small parts to trigger background merge.

    Strategy: do many tiny inserts so the merge scheduler picks them up.
    """
    print("  Inserting 20 small batches to create L0 parts...")
    for i in range(20):
        _small_insert(client, TABLE_REGULAR, rows=50)
        time.sleep(0.05)  # tiny gap so each INSERT creates a separate part

    # Count parts before
    parts_before = client.execute(f"""
        SELECT count() FROM system.parts
        WHERE database = '{DB}' AND table = '{TABLE_REGULAR}' AND active
    """)[0][0]

    print(f"  Active parts: {parts_before} — waiting for background merge...")
    return {"type": "regular", "parts_created": 20, "active_parts": parts_before}


def trigger_ttl_delete(client: Client) -> dict:
    """Insert data that will expire via TTL DELETE.

    The merge scheduler's TTLMergeSelector will notice expired rows
    and schedule a merge specifically to delete them.
    """
    # Insert rows with ts = now() - 10s so they're already expired
    print("  Inserting 500 rows with already-expired TTL...")
    client.execute(f"""
        INSERT INTO {DB}.{TABLE_TTL_DELETE}
        SELECT
            rand64() AS id,
            now() - toIntervalSecond(10 + rand() % 5) AS ts,
            rand() / 4294967295.0 * 1000 AS value,
            arrayElement([{', '.join(f"'{c}'" for c in CATEGORIES)}], (number % {len(CATEGORIES)}) + 1) AS category
        FROM numbers(500)
    """)

    rows_before = client.execute(f"SELECT count() FROM {DB}.{TABLE_TTL_DELETE}")[0][0]
    print(f"  Rows inserted: {rows_before} (all with expired TTL)")
    print("  TTL merge should fire within ~1s (merge_with_ttl_timeout=1)...")
    return {"type": "ttl_delete", "rows_inserted": rows_before}


def trigger_ttl_recompress(client: Client) -> dict:
    """Insert data that will be recompressed via TTL RECOMPRESS.

    Similar to TTL DELETE but the merge changes the codec instead of
    removing rows.
    """
    print("  Inserting 500 rows with already-expired recompression TTL...")
    client.execute(f"""
        INSERT INTO {DB}.{TABLE_TTL_RECOMPRESS}
        SELECT
            rand64() AS id,
            now() - toIntervalSecond(10 + rand() % 5) AS ts,
            rand() / 4294967295.0 * 1000 AS value,
            arrayElement([{', '.join(f"'{c}'" for c in CATEGORIES)}], (number % {len(CATEGORIES)}) + 1) AS category
        FROM numbers(500)
    """)

    rows = client.execute(f"SELECT count() FROM {DB}.{TABLE_TTL_RECOMPRESS}")[0][0]
    print(f"  Rows inserted: {rows} (all with expired recompression TTL)")
    print("  TTL recompress merge should fire within ~1s...")
    return {"type": "ttl_recompress", "rows_inserted": rows}


def trigger_ttl_move(client: Client) -> dict:
    """Insert data that will be moved to a different volume via TTL MOVE.

    Unlike TTL DELETE/RECOMPRESS, this is NOT a merge — it's a part move.
    ClickHouse relocates the part file from the default volume to s3cached
    without rewriting data. Shows up as 'MovePart' in system.part_log.
    """
    print("  Inserting 500 rows with already-expired move TTL...")
    client.execute(f"""
        INSERT INTO {DB}.{TABLE_TTL_MOVE}
        SELECT
            rand64() AS id,
            now() - toIntervalSecond(10 + rand() % 5) AS ts,
            rand() / 4294967295.0 * 1000 AS value,
            arrayElement([{', '.join(f"'{c}'" for c in CATEGORIES)}], (number % {len(CATEGORIES)}) + 1) AS category
        FROM numbers(500)
    """)

    rows = client.execute(f"SELECT count() FROM {DB}.{TABLE_TTL_MOVE}")[0][0]

    # Check which disk the part is on right now
    disk_info = client.execute(f"""
        SELECT name, disk_name FROM system.parts
        WHERE database = '{DB}' AND table = '{TABLE_TTL_MOVE}' AND active
    """)
    disks = [(r[0], r[1]) for r in disk_info] if disk_info else []
    print(f"  Rows inserted: {rows}")
    if disks:
        print(f"  Current parts: {', '.join(f'{p}@{d}' for p, d in disks)}")
    print("  TTL move should relocate parts to s3cached volume within ~1s...")
    return {"type": "ttl_move", "rows_inserted": rows, "initial_disks": disks}


def trigger_mutation(client: Client) -> dict:
    """Run an ALTER TABLE UPDATE to create a mutation merge."""
    # Make sure there's data
    _small_insert(client, TABLE_REGULAR, rows=200)
    time.sleep(0.5)

    print("  Running ALTER TABLE UPDATE (mutation)...")
    client.execute(f"""
        ALTER TABLE {DB}.{TABLE_REGULAR}
        UPDATE value = value + 1
        WHERE sipHash64(id) % 100 < 30
    """)
    return {"type": "mutation", "affected_pct": "~30%"}


def trigger_optimize(client: Client) -> dict:
    """Run OPTIMIZE TABLE FINAL to force a merge."""
    # Insert a few parts first
    for _ in range(5):
        _small_insert(client, TABLE_REGULAR, rows=100)
        time.sleep(0.05)

    parts_before = client.execute(f"""
        SELECT count() FROM system.parts
        WHERE database = '{DB}' AND table = '{TABLE_REGULAR}' AND active
    """)[0][0]

    print(f"  Parts before OPTIMIZE: {parts_before}")
    print("  Running OPTIMIZE TABLE FINAL...")
    client.execute(f"OPTIMIZE TABLE {DB}.{TABLE_REGULAR} FINAL")

    parts_after = client.execute(f"""
        SELECT count() FROM system.parts
        WHERE database = '{DB}' AND table = '{TABLE_REGULAR}' AND active
    """)[0][0]

    print(f"  Parts after OPTIMIZE: {parts_after}")
    return {"type": "optimize", "parts_before": parts_before, "parts_after": parts_after}


# ── Merge observer ──────────────────────────────────────────────────


def snapshot_merges(client: Client) -> list[dict]:
    """Snapshot current in-flight merges for our test database."""
    rows = client.execute(f"""
        SELECT
            database,
            table,
            merge_type,
            is_mutation,
            num_parts,
            result_part_name,
            progress,
            total_size_bytes_compressed,
            rows_read,
            rows_written
        FROM system.merges
        WHERE database = '{DB}'
        ORDER BY progress ASC
    """)
    return [
        {
            "database": r[0], "table": r[1], "merge_type": r[2],
            "is_mutation": bool(r[3]), "num_parts": r[4],
            "result_part": r[5], "progress": f"{r[6]:.1%}",
            "size_compressed": r[7], "rows_read": r[8], "rows_written": r[9],
        }
        for r in rows
    ]


def query_part_log(client: Client, since: datetime) -> list[dict]:
    """Query system.part_log for merge events since a given time."""
    rows = client.execute(f"""
        SELECT
            event_time,
            event_type,
            merge_reason,
            merge_algorithm,
            database,
            table,
            part_name,
            rows,
            size_in_bytes,
            duration_ms
        FROM system.part_log
        WHERE database = '{DB}'
          AND event_time >= %(since)s
          AND event_type IN ('MergeParts', 'MutatePart', 'MovePart')
        ORDER BY event_time ASC
    """, {"since": since})
    return [
        {
            "time": r[0].strftime("%H:%M:%S"),
            "event": r[1], "merge_reason": r[2], "algorithm": r[3],
            "table": f"{r[4]}.{r[5]}", "part": r[6],
            "rows": r[7], "size": r[8], "duration_ms": r[9],
        }
        for r in rows
    ]


# ── Pretty printing ─────────────────────────────────────────────────

MERGE_TYPE_LABELS = {
    # system.merges.merge_type values
    "Regular":              "🔄 Regular        — background size-based merge",
    "TTLDelete":            "🗑️  TTL Delete     — merge to remove expired rows",
    "TTLRecompress":        "🗜️  TTL Recompress — merge to change codec on aged data",
    # system.part_log.merge_reason values (different naming!)
    "RegularMerge":         "🔄 Regular        — background size-based merge",
    "TTLDropMerge":         "🗑️  TTL Delete     — merge to remove expired rows",
    "TTLDeleteMerge":       "🗑️  TTL Delete     — merge to remove expired rows",
    "TTLRecompressMerge":   "🗜️  TTL Recompress — merge to change codec on aged data",
    "":                     "❓ Unknown        — merge_reason not reported",
}


def print_part_log_summary(events: list[dict]) -> None:
    """Print a categorized summary of merge events."""
    if not events:
        print("\n  (no merge events captured in part_log — try increasing --watch)")
        return

    # Group by merge_reason
    by_reason: dict[str, list[dict]] = {}
    mutations = []
    moves = []
    for e in events:
        if e["event"] == "MutatePart":
            mutations.append(e)
        elif e["event"] == "MovePart":
            moves.append(e)
        else:
            reason = e["merge_reason"] or "(empty)"
            by_reason.setdefault(reason, []).append(e)

    print(f"\n{'─' * 70}")
    print("  MERGE EVENT CLASSIFICATION")
    print(f"{'─' * 70}")

    for reason, evts in sorted(by_reason.items()):
        label = MERGE_TYPE_LABELS.get(reason, f"❓ {reason}")
        print(f"\n  {label}")
        print(f"  {'.' * 60}")
        for e in evts:
            print(f"    [{e['time']}] {e['table']} → {e['part']}"
                  f"  ({e['rows']} rows, {e['duration_ms']}ms, algo={e['algorithm']})")

    if mutations:
        print(f"\n  ✏️  Mutations — ALTER TABLE UPDATE/DELETE applied to parts")
        print(f"  {'.' * 60}")
        for e in mutations:
            print(f"    [{e['time']}] {e['table']} → {e['part']}"
                  f"  ({e['rows']} rows, {e['duration_ms']}ms)")

    if moves:
        print(f"\n  📦 TTL Move — parts relocated to different volume/disk (NOT a merge)")
        print(f"  {'.' * 60}")
        for e in moves:
            print(f"    [{e['time']}] {e['table']} → {e['part']}"
                  f"  ({e['rows']} rows, {e['duration_ms']}ms)")

    total = len(events)
    print(f"\n  Total merge/mutate events: {total}")
    print(f"{'─' * 70}")


# ── Main ────────────────────────────────────────────────────────────

TRIGGER_MAP = {
    "regular":        trigger_regular_merge,
    "ttl_delete":     trigger_ttl_delete,
    "ttl_recompress": trigger_ttl_recompress,
    "ttl_move":       trigger_ttl_move,
    "mutation":       trigger_mutation,
    "optimize":       trigger_optimize,
}

ALL_TYPES = list(TRIGGER_MAP.keys())


def main():
    env_path = pre_parse_env_file()

    parser = argparse.ArgumentParser(
        description="Trigger & classify all ClickHouse merge types",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Merge types you can trigger:
  regular        Many small inserts → background merge scheduler picks them up
  ttl_delete     Rows with expired TTL → TTLMergeSelector schedules a delete merge
  ttl_recompress Rows past recompression TTL → merge to apply new codec
  ttl_move       Rows past move TTL → part relocated to different volume (NOT a merge)
  mutation       ALTER TABLE UPDATE → mutation applied to parts
  optimize       OPTIMIZE TABLE FINAL → forced merge of all parts
        """,
    )
    add_connection_args(parser)
    parser.add_argument("--types", default=",".join(ALL_TYPES),
                        help=f"Comma-separated merge types to trigger (default: all). "
                             f"Options: {', '.join(ALL_TYPES)}")
    parser.add_argument("--watch", type=int, default=15,
                        help="Seconds to watch for merge activity after triggers (default: 15)")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="Don't drop test tables after run (useful for manual inspection)")
    parser.add_argument("--no-setup", action="store_true",
                        help="Skip table creation (reuse from previous run with --no-cleanup)")
    args = parser.parse_args()

    requested_types = [t.strip() for t in args.types.split(",")]
    for t in requested_types:
        if t not in TRIGGER_MAP:
            print(f"Error: unknown merge type '{t}'. Options: {', '.join(ALL_TYPES)}")
            sys.exit(1)

    # Connect
    print_connection(args, env_path)
    print(f"Connecting to ClickHouse at {args.host}:{args.port}...")
    admin_client = make_client(args)
    version = admin_client.execute("SELECT version()")[0][0]
    print(f"  ClickHouse {version}\n")

    # Create test users if requested (env var takes precedence)
    test_users: list[TestUser] | None = load_test_users_from_env()
    users_from_env = test_users is not None
    if test_users:
        print(f"Using {len(test_users)} test users from TRACEHOUSE_TEST_USERS")
        print_test_users(test_users, skew=args.user_skew)
    elif args.users > 0:
        print(f"Creating {args.users} test users...")
        test_users = create_test_users(admin_client, args.users)
        print_test_users(test_users, skew=args.user_skew)

    client = admin_client

    # Check part_log availability
    has_part_log = False
    try:
        client.execute("SELECT 1 FROM system.part_log LIMIT 0")
        has_part_log = True
    except Exception:
        print("  ⚠ system.part_log not available — merge classification will be limited")
        print("    Enable it in config: <part_log><database>system</database><table>part_log</table></part_log>\n")

    # Setup
    has_s3 = False
    if not args.no_setup:
        has_s3 = setup_test_tables(client)

    # Skip ttl_move if s3tiered not available
    if "ttl_move" in requested_types and not has_s3:
        print("  ⊘ Skipping ttl_move (s3tiered storage policy not available)")
        requested_types = [t for t in requested_types if t != "ttl_move"]

    print()

    # Record start time for part_log query
    start_time = datetime.now()

    # Trigger each requested merge type
    print("=" * 60)
    print("  TRIGGERING MERGE TYPES")
    print("=" * 60)

    results = []
    for i, merge_type in enumerate(requested_types):
        if test_users:
            user = pick_random_user(test_users, skew=args.user_skew)
            trigger_client = make_user_client(args, user)
            user_label = f" (as {user.name})"
        else:
            trigger_client = client
            user_label = ""

        print(f"\n▸ {merge_type.upper()}{user_label}")
        fn = TRIGGER_MAP[merge_type]
        try:
            result = fn(trigger_client)
            results.append(result)
            print(f"  ✓ Triggered")
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            results.append({"type": merge_type, "error": str(e)})

    # Watch phase — poll system.merges and wait for activity
    print(f"\n{'=' * 60}")
    print(f"  WATCHING FOR MERGES ({args.watch}s)")
    print(f"{'=' * 60}")

    seen_merges = set()
    try:
        for elapsed in range(args.watch):
            merges = snapshot_merges(client)
            for m in merges:
                key = (m["table"], m["result_part"], m["merge_type"], m["is_mutation"])
                if key not in seen_merges:
                    seen_merges.add(key)
                    mt = m["merge_type"]
                    label = "MUTATION" if m["is_mutation"] else mt
                    print(f"  [{elapsed:2d}s] {label:20s} | {m['table']:30s} | "
                          f"parts={m['num_parts']} → {m['result_part']}")

            time.sleep(1)
    except KeyboardInterrupt:
        print("\n  Watch interrupted")

    # Flush part_log and query results
    if has_part_log:
        print(f"\n{'=' * 60}")
        print("  PART LOG ANALYSIS")
        print(f"{'=' * 60}")

        try:
            client.execute("SYSTEM FLUSH LOGS")
        except Exception:
            pass  # may not have permission on managed services

        time.sleep(1)
        events = query_part_log(client, start_time)
        print_part_log_summary(events)
    else:
        print("\n  (part_log not available — enable it for full merge classification)")

    # Show final parts state
    print(f"\n{'=' * 60}")
    print("  FINAL PARTS STATE")
    print(f"{'=' * 60}")
    for table in [TABLE_REGULAR, TABLE_TTL_DELETE, TABLE_TTL_RECOMPRESS, TABLE_TTL_MOVE]:
        try:
            parts = client.execute(f"""
                SELECT count(), sum(rows), sum(bytes_on_disk)
                FROM system.parts
                WHERE database = '{DB}' AND table = '{table}' AND active
            """)
            if parts:
                p, r, b = parts[0]
                extra = ""
                if table == TABLE_TTL_MOVE and has_s3:
                    disks = client.execute(f"""
                        SELECT DISTINCT disk_name FROM system.parts
                        WHERE database = '{DB}' AND table = '{table}' AND active
                    """)
                    if disks:
                        extra = f", disk={','.join(d[0] for d in disks)}"
                print(f"  {DB}.{table:30s} | parts={p}, rows={r}, bytes={b}{extra}")
        except Exception:
            pass

    # Cleanup
    if not args.no_cleanup:
        cleanup_test_tables(client)

    if test_users and not users_from_env:
        lock_test_users(admin_client, test_users)
        print("  ✓ Test users locked (HOST NONE)")

    print("\n✓ Merge trigger test complete")


if __name__ == "__main__":
    main()
