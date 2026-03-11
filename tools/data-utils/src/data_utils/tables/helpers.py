"""Shared helpers for table setup modules."""

from __future__ import annotations

import sys
import threading
import time
import random
from datetime import datetime, timedelta
from clickhouse_driver import Client


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


def engine_clause(replicated: bool) -> str:
    """Return the ENGINE clause.

    When the database uses ENGINE = Replicated, ClickHouse manages
    ZooKeeper paths automatically — explicit args are not allowed.
    So we use bare ReplicatedMergeTree() with no arguments.
    """
    if replicated:
        return "ReplicatedMergeTree()"
    return "MergeTree()"


def retry_on_drop_race(fn, max_retries: int = 5):
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


def create_database(client: Client, name: str, replicated: bool) -> None:
    """Create a database, using the Replicated engine when replicated=True.

    Note: ON CLUSTER is not used here — the Replicated engine propagates
    DDL automatically.  Using ON CLUSTER with a Replicated database raises
    INCORRECT_QUERY (Code 80) on ClickHouse 26.x+.
    """
    def _create():
        if replicated:
            client.execute(
                f"CREATE DATABASE IF NOT EXISTS {name} "
                f"ENGINE = Replicated('/clickhouse/databases/{name}', '{{shard}}', '{{replica}}')"
            )
        else:
            client.execute(f"CREATE DATABASE IF NOT EXISTS {name}")
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


def check_existing_rows(client: Client, table: str, target_rows: int, drop: bool) -> int | None:
    """Check existing row count and return adjusted rows to insert, or None to skip.

    Returns:
        int — number of rows still needed
        None — table already at target, skip insert
    """
    count = client.execute(f"SELECT count() FROM {table}")[0][0]
    if count > 0 and not drop:
        if count > target_rows:
            print(f"  Table has {count:,} rows which exceeds target {target_rows:,} — use --drop to reset")
            return None
        elif count >= target_rows * 0.9:
            print(f"  Table already has {count:,} rows (target: {target_rows:,}), skipping insert (use --drop to reset)")
            return None
        else:
            remaining = target_rows - count
            print(f"  Table has {count:,} rows, inserting {remaining:,} more to reach target...")
            return remaining
    return target_rows


def run_batched_insert(
    client: Client,
    table: str,
    rows: int,
    partitions: list[tuple[str, str]],
    batch_size: int,
    build_insert_sql,
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
            try:
                client.execute(sql)
            except Exception:
                if tracker and tracker.cancelled.is_set():
                    tracker.update(short_name, total_done, status="cancelled")
                    return
                raise
            total_done += current_batch

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
