"""Shared helpers for dataset setup modules."""

from __future__ import annotations

import logging
import sys
import threading
import time
import random
from collections.abc import Callable
from datetime import datetime, timedelta
import re

log = logging.getLogger("tracehouse.insert")

from clickhouse_driver import Client

from data_utils.tables.protocol import InsertMode

_SAFE_CLUSTER_RE = re.compile(r'^[a-zA-Z0-9_.\-]+$')

# Callback signature for build_insert_sql functions used by run_batched_insert.
# Args: (partition_key, batch_index, batch_size, current_batch, partition_rows, partition_offset)
BuildInsertSQL = Callable[[str, int, int, int, int, int], str]


# ── Progress tracker for parallel table loading ─────────────────────


class ProgressTracker:
    """Thread-safe multi-table progress display.

    Each table registers itself, then reports progress. A background
    thread redraws all lines in-place using ANSI escape codes.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._tables: dict[str, dict] = {}  # name -> {rows_done, rows_total, status, partition}
        self._order: list[str] = []
        self._stop = threading.Event()
        self.cancelled = threading.Event()  # shared cancellation signal for workers
        self._thread: threading.Thread | None = None
        self._started = False

    def register(self, name: str, total_rows: int) -> None:
        with self._lock:
            self._tables[name] = {
                "rows_done": 0,
                "rows_total": total_rows,
                "status": "starting",
                "partition": "",
            }
            if name not in self._order:
                self._order.append(name)

    def update(self, name: str, rows_done: int, partition: str = "", status: str = "loading") -> None:
        with self._lock:
            if name in self._tables:
                self._tables[name]["rows_done"] = rows_done
                self._tables[name]["partition"] = partition
                self._tables[name]["status"] = status

    def complete(self, name: str) -> None:
        with self._lock:
            if name in self._tables:
                t = self._tables[name]
                t["rows_done"] = t["rows_total"]
                t["status"] = "done"

    def skip(self, name: str, reason: str = "skipped") -> None:
        with self._lock:
            if name in self._tables:
                self._tables[name]["status"] = reason

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        # Print initial blank lines to reserve space
        with self._lock:
            for _ in self._order:
                sys.stdout.write("\n")
            sys.stdout.flush()
        self._thread = threading.Thread(target=self._render_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        self._render_once()  # final render

    def _render_loop(self) -> None:
        while not self._stop.is_set():
            self._render_once()
            self._stop.wait(0.25)

    def _render_once(self) -> None:
        with self._lock:
            n = len(self._order)
            if n == 0:
                return
            # Move cursor up N lines
            sys.stdout.write(f"\033[{n}A")
            for name in self._order:
                t = self._tables[name]
                total = t["rows_total"]
                done = t["rows_done"]
                status = t["status"]
                partition = t["partition"]

                if status == "done":
                    pct = 100
                    bar = "\u2588" * 20
                    line = f"  \u2713 {name:<20s} [{bar}] 100%  {done:>14,} rows"
                elif status == "cancelled":
                    filled = int(done * 20 / total) if total else 0
                    bar = "\u2588" * filled + "\u2591" * (20 - filled)
                    line = f"  \u2717 {name:<20s} [{bar}] cancelled at {done:,} rows"
                elif status in ("skipped", "exceeds target"):
                    bar = "\u2500" * 20
                    line = f"  \u2298 {name:<20s} [{bar}] {status}"
                elif total > 0:
                    pct = min(int(done * 100 / total), 99) if total else 0
                    filled = int(done * 20 / total) if total else 0
                    bar = "\u2588" * filled + "\u2591" * (20 - filled)
                    part_str = f"  {partition}" if partition else ""
                    line = f"  \u231b {name:<20s} [{bar}] {pct:>3d}%  {done:>14,} / {total:,} rows{part_str}"
                else:
                    bar = "\u2591" * 20
                    line = f"  \u231b {name:<20s} [{bar}]   0%  {status}"

                # Pad to clear previous content, then carriage return
                sys.stdout.write(f"\r{line:<100s}\n")
            sys.stdout.flush()


def is_sharded(caps) -> tuple[bool, str | None]:
    """Return (True, cluster_name) when the target has multiple shards."""
    if caps and caps.has_cluster and caps.has_keeper and caps.shard_count > 1:
        return True, caps.cluster_name
    return False, None


def engine_clause(replicated: bool) -> str:
    """Return the ENGINE clause.

    When the database uses ENGINE = Replicated, ClickHouse manages
    ZooKeeper paths automatically — explicit args are not allowed.
    So we use bare ReplicatedMergeTree() with no arguments.
    """
    if replicated:
        return "ReplicatedMergeTree()"
    return "MergeTree()"


def retry_on_drop_race(fn: Callable[[], None], max_retries: int = 5) -> None:
    """Retry a callable when ClickHouse reports a transient DDL state.

    Handles two cases:
      1. Database is still being dropped/renamed (explicit message).
      2. Database does not exist yet on this node because Replicated
         database DDL hasn't propagated from another replica/shard.
    """
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            msg = str(e)
            transient = (
                "currently dropped or renamed" in msg
                or ("Code: 81" in msg and "does not exist" in msg)
            )
            if transient and attempt < max_retries - 1:
                wait = 2 * (attempt + 1)
                print(f"  Database not ready (replication lag?), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def on_cluster_clause(cluster: str) -> str:
    """Return ``ON CLUSTER 'name'`` when *cluster* is set, otherwise ``""``."""
    if not cluster:
        return ""
    if not _SAFE_CLUSTER_RE.match(cluster):
        raise ValueError(f"Unsafe cluster name: {cluster!r}")
    return f"ON CLUSTER '{cluster}'"


_TTL_SUFFIXES: dict[str, str] = {
    "hours": "HOUR", "hour": "HOUR", "hr": "HOUR", "h": "HOUR",
    "days": "DAY", "day": "DAY", "d": "DAY",
    "minutes": "MINUTE", "minute": "MINUTE", "min": "MINUTE", "m": "MINUTE",
}


def parse_ttl(value: str) -> str:
    """Parse a human TTL string into a ClickHouse INTERVAL expression.

    Accepts ``12h``, ``2d``, ``30m``, ``12`` (bare integer → hours for
    backward compat), or ``0`` / empty to disable.

    Returns e.g. ``"12 HOUR"``, ``"2 DAY"``, ``"30 MINUTE"``, or ``""``.
    """
    value = value.strip().lower() if value else ""
    if not value or value == "0":
        return ""
    # Try suffixed form — longest suffix first to avoid 'h' matching before 'hour'
    for suffix, unit in sorted(_TTL_SUFFIXES.items(), key=lambda x: -len(x[0])):
        if value.endswith(suffix):
            num = value[: -len(suffix)].strip()
            if num.isdigit() and int(num) > 0:
                return f"{int(num)} {unit}"
            break
    # Bare integer → hours (backward compat with CH_GEN_TTL_HOURS)
    if value.isdigit():
        n = int(value)
        return f"{n} HOUR" if n > 0 else ""
    raise ValueError(f"Invalid TTL format: {value!r} (use e.g. '12h', '2d', '30m')")


def ttl_clause(interval: str) -> str:
    """Return ``TTL _inserted_at + INTERVAL ...`` when *interval* is set.

    *interval* should be a ClickHouse interval expression such as
    ``"12 HOUR"`` or ``"30 MINUTE"`` (as returned by ``parse_ttl``).

    Uses ``_inserted_at`` (a DEFAULT now() column) so the TTL counts from
    real insertion time, not from the business-date column. This avoids
    rows being born already expired when generating historical data.
    """
    if not interval:
        return ""
    return f"TTL _inserted_at + INTERVAL {interval}"


def ttl_settings(interval: str) -> str:
    """Return TTL-related settings when TTL is active, otherwise ``""``.

    ``ttl_only_drop_parts = 1`` tells ClickHouse to drop whole parts once
    every row has expired, instead of rewriting parts to filter out expired
    rows.  Combined with hourly partitioning (see ``partition_clause``),
    this makes TTL cleanup a metadata-only operation with zero I/O.
    """
    if not interval:
        return ""
    return "merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1"


def partition_clause(interval: str, default: str) -> str:
    """Return the PARTITION BY clause.

    When TTL is active, partitions by insertion time so that all rows in a
    partition expire together — enabling ``ttl_only_drop_parts`` to drop
    whole parts with zero I/O instead of rewriting them.

    The granularity matches the TTL unit:
      - MINUTE → ``toStartOfMinute`` (so a 1m TTL actually drops in ~1-2m)
      - HOUR / DAY → ``toStartOfHour`` (12h TTL drops in ~12-13h)

    When TTL is off, uses the provided *default* business-date expression
    for optimal query partition-pruning.
    """
    if interval:
        if "MINUTE" in interval:
            return "PARTITION BY toStartOfMinute(_inserted_at)"
        if "DAY" in interval:
            return "PARTITION BY toStartOfDay(_inserted_at)"
        return "PARTITION BY toStartOfHour(_inserted_at)"
    return f"PARTITION BY {default}"


def drop_database(client: Client, name: str, cluster: str = "") -> None:
    """Drop a database, using ON CLUSTER when *cluster* is set.

    For Replicated databases the DDL log propagates table drops
    automatically, but ON CLUSTER on the DROP DATABASE itself is
    still needed to remove the database entry on every node.
    """
    on_cluster = on_cluster_clause(cluster)
    client.execute(f"DROP DATABASE IF EXISTS {name} {on_cluster} SYNC")


def create_database(client: Client, name: str, replicated: bool, cluster: str = "") -> None:
    """Create a database, using the Replicated engine when replicated=True.

    When *cluster* is provided, uses ON CLUSTER to propagate the DDL to
    every shard and replica so the database exists cluster-wide.
    """
    on_cluster = on_cluster_clause(cluster)
    def _create():
        if replicated:
            client.execute(
                f"CREATE DATABASE IF NOT EXISTS {name} {on_cluster} "
                f"ENGINE = Replicated('/clickhouse/databases/{name}', '{{shard}}', '{{replica}}')"
            )
        else:
            client.execute(f"CREATE DATABASE IF NOT EXISTS {name} {on_cluster}")
    retry_on_drop_race(_create)


def wait_for_table(client: Client, table: str, max_retries: int = 10) -> None:
    """Block until *table* (``db.table``) is queryable on this node.

    Replicated databases propagate DDL asynchronously, so a table that was
    just created on one replica may not be visible on another for a few
    seconds.  This helper retries a lightweight ``EXISTS`` check with
    exponential back-off so that subsequent DML doesn't blow up with
    "Database \u2026 does not exist" / "Table \u2026 doesn't exist".
    """
    for attempt in range(max_retries):
        try:
            client.execute(f"SELECT 1 FROM {table} LIMIT 0")
            return
        except Exception as e:
            msg = str(e)
            transient = (
                "does not exist" in msg
                or "currently dropped or renamed" in msg
            )
            if transient and attempt < max_retries - 1:
                wait = 2 * (attempt + 1)
                print(f"  Waiting for {table} to appear (replication lag?), retry in {wait}s...")
                time.sleep(wait)
            else:
                raise


def generate_month_list(num_partitions: int) -> list[tuple[str, str]]:
    """Generate a list of (month_start, month_name) tuples going backwards from current month."""
    months = []
    base = datetime(2026, 2, 1)
    for i in range(num_partitions):
        month_date = base - timedelta(days=30 * i)
        month_start = month_date.replace(day=1).strftime("%Y-%m-%d")
        month_name = month_date.strftime("%B %Y")
        months.append((month_start, month_name))
    return months


def check_existing_rows(client: Client, table: str, target_rows: int, mode: InsertMode) -> int | None:
    """Check existing row count and return adjusted rows to insert, or None to skip.

    Returns:
        int — number of rows still needed
        None — table already at target, skip insert
    """
    if mode is InsertMode.APPEND:
        log.info("[%s] append mode — skipping row check", table)
        return target_rows
    log.info("[%s] connecting...", table)
    client.execute("SELECT 1")
    log.info("[%s] counting existing rows...", table)
    t0 = time.monotonic()
    count = client.execute(f"SELECT count() FROM {table}")[0][0]
    elapsed = time.monotonic() - t0
    log.info("[%s] existing rows: %d (%.1fs)", table, count, elapsed)
    if count == 0:
        log.info("[%s] empty, inserting %s rows...", table, f"{target_rows:,}")
    if count > 0 and mode is not InsertMode.DROP:
        if count > target_rows:
            log.info("[%s] has %s rows, exceeds target %s — use --drop to reset", table, f"{count:,}", f"{target_rows:,}")
            return None
        elif count >= target_rows * 0.9:
            log.info("[%s] already has %s rows (target: %s), skipping", table, f"{count:,}", f"{target_rows:,}")
            return None
        else:
            remaining = target_rows - count
            log.info("[%s] has %s rows, inserting %s more...", table, f"{count:,}", f"{remaining:,}")
            return remaining
    return target_rows


def run_batched_insert(
    client: Client,
    table: str,
    rows: int,
    partitions: list[tuple[str, str]],
    batch_size: int,
    build_insert_sql: BuildInsertSQL,
    tracker: ProgressTracker | None = None,
    throttle_min: float = 0.0,
    throttle_max: float = 0.0,
) -> None:
    """Generic batched insert loop.

    Args:
        build_insert_sql: callable(month_start, batch_index, batch_size, current_batch, month_rows, partition_offset)
            that returns the INSERT SQL string.
        partitions: list of (partition_key, partition_label) tuples.
        tracker: optional ProgressTracker for parallel progress display.
        throttle_min: minimum delay in seconds between batches (0 = no throttle).
        throttle_max: maximum delay in seconds between batches (0 = no throttle).
    """
    rows_per_partition = rows // len(partitions)
    # Extract short table name (e.g. "synthetic_data" from "synthetic_data.events")
    short_name = table.split(".")[0] if "." in table else table

    if tracker:
        # Only register if not already registered (parallel mode pre-registers)
        with tracker._lock:
            if short_name not in tracker._tables:
                tracker.register(short_name, rows)
            else:
                tracker._tables[short_name]["rows_total"] = rows
    else:
        print(f"\nInserting {rows:,} total rows into {table}")
        print(f"  Partitions: {len(partitions)}")
        print(f"  Rows per partition: {rows_per_partition:,}")
        print(f"  Batch size: {batch_size:,}")
        print()

    total_done = 0
    partition_offset = 0
    for partition_key, partition_label in partitions:
        batches = (rows_per_partition + batch_size - 1) // batch_size

        if not tracker:
            print(f"  [{partition_label}] {rows_per_partition:,} rows in {batches} batches")

        for batch in range(batches):
            # Check for cancellation between batches
            if tracker and tracker.cancelled.is_set():
                tracker.update(short_name, total_done, status="cancelled")
                return

            current_batch = min(batch_size, rows_per_partition - batch * batch_size)
            if current_batch <= 0:
                break

            sql = build_insert_sql(
                partition_key, batch, batch_size, current_batch,
                rows_per_partition, partition_offset,
            )
            log.debug("[%s] batch %d/%d (%d rows) — sending to server...", short_name, batch + 1, batches, current_batch)
            t0 = time.monotonic()
            try:
                client.execute(sql)
            except Exception:
                if tracker and tracker.cancelled.is_set():
                    tracker.update(short_name, total_done, status="cancelled")
                    return
                raise
            elapsed = time.monotonic() - t0
            total_done += current_batch
            log.debug("[%s] batch %d/%d done in %.1fs", short_name, batch + 1, batches, elapsed)

            if tracker:
                tracker.update(short_name, total_done, partition=partition_label)
            else:
                pct = (batch + 1) * 100 // batches
                print(f"    Batch {batch + 1}/{batches} ({pct}%)", end="\r")

            # Throttle between batches
            if throttle_max > 0:
                delay = random.uniform(throttle_min, throttle_max)
                # Sleep in small increments so cancellation is responsive
                end_time = time.monotonic() + delay
                while time.monotonic() < end_time:
                    if tracker and tracker.cancelled.is_set():
                        tracker.update(short_name, total_done, status="cancelled")
                        return
                    time.sleep(min(0.25, end_time - time.monotonic()))

        partition_offset += rows_per_partition
        if not tracker:
            print(f"    \u2713 {partition_label} complete" + " " * 20)

    if tracker:
        tracker.complete(short_name)
    else:
        print("  Done!")
