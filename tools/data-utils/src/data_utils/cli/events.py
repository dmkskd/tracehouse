"""Generate safe, observable ClickHouse events for TraceHouse demos.

The workload deliberately produces query-scoped failures and disposable DDL.
It never crashes, restarts, fills, or corrupts a ClickHouse server.

Usage:
    tracehouse-events --once
    tracehouse-events --types ddl,oom,timeout
"""

from __future__ import annotations

import argparse
import os
import re
import threading
import time
from collections.abc import Callable
from datetime import datetime

from clickhouse_driver import Client

from data_utils.capabilities import probe_system_log_tables
from data_utils.env import (
    add_connection_args,
    confirm_or_exit,
    make_client,
    pre_parse_env_file,
    print_connection,
)


EVENT_TYPES = ("ddl", "oom", "timeout")
EVENT_TAG = "tracehouse-demo-event"
DEFAULT_DATABASE = "tracehouse_event_demo"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_stop_event = threading.Event()

OOM_QUERY = f"""
/* {EVENT_TAG} kind:query_oom */
SELECT uniqExact(toString(number))
FROM numbers_mt(10000000)
SETTINGS
    max_memory_usage = 1000000,
    max_bytes_before_external_group_by = 0,
    max_execution_time = 5
"""

TIMEOUT_QUERY = f"""
/* {EVENT_TAG} kind:query_timeout */
SELECT sum(sipHash64(number))
FROM numbers_mt(1000000000000)
SETTINGS
    max_execution_time = 0.05,
    timeout_overflow_mode = 'throw'
"""


def parse_event_types(value: str) -> tuple[str, ...]:
    """Parse and validate a comma-separated event type list."""
    requested = tuple(dict.fromkeys(part.strip().lower() for part in value.split(",") if part.strip()))
    unknown = sorted(set(requested) - set(EVENT_TYPES))
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown event type(s): {', '.join(unknown)}; choose from {', '.join(EVENT_TYPES)}"
        )
    if not requested:
        raise argparse.ArgumentTypeError("at least one event type is required")
    return requested


def quote_identifier(value: str) -> str:
    """Validate and quote a ClickHouse identifier."""
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"invalid identifier {value!r}; use letters, digits, and underscores, "
            "starting with a letter or underscore"
        )
    return f"`{value}`"


def exception_code(error: Exception) -> int | None:
    """Extract a ClickHouse error code without depending on one exception class."""
    code = getattr(error, "code", None)
    if isinstance(code, int):
        return code
    match = re.search(r"\bCode:\s*(\d+)", str(error))
    return int(match.group(1)) if match else None


def execute_expected_failure(
    client: Client,
    query: str,
    expected_code: int,
    label: str,
) -> bool:
    """Execute a query that should fail with a specific ClickHouse code."""
    started = time.monotonic()
    try:
        client.execute(query)
    except Exception as error:
        elapsed = time.monotonic() - started
        actual_code = exception_code(error)
        if actual_code == expected_code:
            print(
                f"[{datetime.now():%H:%M:%S}] generated {label} "
                f"(code {actual_code}, {elapsed:.2f}s)"
            )
            return True
        print(
            f"[{datetime.now():%H:%M:%S}] could not generate {label}: "
            f"expected code {expected_code}, got {actual_code or 'unknown'}: "
            f"{str(error).splitlines()[0][:180]}"
        )
        return False

    print(
        f"[{datetime.now():%H:%M:%S}] could not generate {label}: "
        "query unexpectedly succeeded"
    )
    return False


def generate_oom(client: Client, database: str) -> bool:
    """Generate a query-scoped MEMORY_LIMIT_EXCEEDED event."""
    del database
    return execute_expected_failure(client, OOM_QUERY, 241, "query OOM")


def generate_timeout(client: Client, database: str) -> bool:
    """Generate a query-scoped TIMEOUT_EXCEEDED event."""
    del database
    return execute_expected_failure(client, TIMEOUT_QUERY, 159, "query timeout")


def ddl_statements(database: str) -> list[str]:
    """Build one self-cleaning DDL cycle in a dedicated database."""
    db = quote_identifier(database)
    table_a = f"{db}.`tracehouse_event_a`"
    table_b = f"{db}.`tracehouse_event_b`"
    return [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"DROP TABLE IF EXISTS {table_b} SYNC",
        f"DROP TABLE IF EXISTS {table_a} SYNC",
        (
            f"CREATE TABLE {table_a} "
            "(event_time DateTime64(3), payload String) "
            "ENGINE = MergeTree ORDER BY event_time"
        ),
        f"ALTER TABLE {table_a} ADD COLUMN source LowCardinality(String) DEFAULT 'demo'",
        f"ALTER TABLE {table_a} RENAME COLUMN payload TO message",
        f"RENAME TABLE {table_a} TO {table_b}",
        f"TRUNCATE TABLE {table_b}",
        f"DROP TABLE {table_b} SYNC",
    ]


def generate_ddl(client: Client, database: str) -> bool:
    """Generate successful DDL events and leave no tables behind."""
    try:
        for index, statement in enumerate(ddl_statements(database), start=1):
            tagged = f"/* {EVENT_TAG} kind:ddl step:{index} */ {statement}"
            client.execute(tagged)
        print(
            f"[{datetime.now():%H:%M:%S}] generated DDL cycle "
            f"(database {database}, 9 statements)"
        )
        return True
    except Exception as error:
        print(
            f"[{datetime.now():%H:%M:%S}] could not generate DDL cycle: "
            f"{str(error).splitlines()[0][:180]}"
        )
        return False


GENERATORS: dict[str, Callable[[Client, str], bool]] = {
    "ddl": generate_ddl,
    "oom": generate_oom,
    "timeout": generate_timeout,
}


def flush_logs(client: Client) -> None:
    """Best-effort flush so one-shot events become visible immediately."""
    try:
        client.execute("SYSTEM FLUSH LOGS")
        print(f"[{datetime.now():%H:%M:%S}] flushed ClickHouse system logs")
    except Exception as error:
        print(
            "Warning: could not flush system logs; events will appear after the "
            f"normal flush interval ({str(error).splitlines()[0][:120]})"
        )


def run_schedule(
    client: Client,
    event_types: tuple[str, ...],
    database: str,
    intervals: dict[str, float],
    once: bool,
    duration: float | None,
) -> int:
    """Run selected event generators once or on independent cadences."""
    generated = 0
    if once:
        for event_type in event_types:
            generated += int(GENERATORS[event_type](client, database))
        flush_logs(client)
        return generated

    started = time.monotonic()
    due = {event_type: started for event_type in event_types}
    while not _stop_event.is_set():
        now = time.monotonic()
        if duration is not None and now - started >= duration:
            break

        for event_type in event_types:
            if now >= due[event_type]:
                generated += int(GENERATORS[event_type](client, database))
                due[event_type] = time.monotonic() + intervals[event_type]

        next_due = min(due.values())
        wait_for = max(0.05, min(0.5, next_due - time.monotonic()))
        _stop_event.wait(wait_for)

    flush_logs(client)
    return generated


def _parse_args() -> tuple[argparse.Namespace, str | None]:
    env_path = pre_parse_env_file()
    parser = argparse.ArgumentParser(
        description=(
            "Generate safe Time Travel events: disposable DDL, query-scoped "
            "OOMs, and query timeouts"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_connection_args(parser)
    parser.add_argument(
        "--types",
        type=parse_event_types,
        default=parse_event_types(os.environ.get("CH_EVENT_TYPES", "ddl,oom,timeout")),
        help="Comma-separated event types: ddl, oom, timeout ($CH_EVENT_TYPES)",
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("CH_EVENT_DATABASE", DEFAULT_DATABASE),
        help="Disposable database used by DDL events ($CH_EVENT_DATABASE)",
    )
    parser.add_argument("--once", action="store_true", help="Generate each selected type once and exit")
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Stop a continuous run after this many seconds",
    )
    parser.add_argument(
        "--ddl-interval",
        type=float,
        default=float(os.environ.get("CH_EVENT_DDL_INTERVAL", "300")),
        help="Seconds between DDL cycles ($CH_EVENT_DDL_INTERVAL)",
    )
    parser.add_argument(
        "--oom-interval",
        type=float,
        default=float(os.environ.get("CH_EVENT_OOM_INTERVAL", "900")),
        help="Seconds between query OOMs ($CH_EVENT_OOM_INTERVAL)",
    )
    parser.add_argument(
        "--timeout-interval",
        type=float,
        default=float(os.environ.get("CH_EVENT_TIMEOUT_INTERVAL", "600")),
        help="Seconds between query timeouts ($CH_EVENT_TIMEOUT_INTERVAL)",
    )
    args = parser.parse_args()

    try:
        quote_identifier(args.database)
    except ValueError as error:
        parser.error(str(error))
    for name in EVENT_TYPES:
        interval = getattr(args, f"{name}_interval")
        if interval <= 0:
            parser.error(f"--{name}-interval must be greater than zero")
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration must be greater than zero")
    return args, env_path


def main() -> None:
    args, env_path = _parse_args()
    print_connection(args, env_path)

    print("\nThis workload intentionally records failed queries and disposable DDL.")
    print("It does not restart, crash, fill, or corrupt the ClickHouse server.")
    print(f"  Types:    {', '.join(args.types)}")
    print(f"  Database: {args.database} (DDL only)")
    if not args.once:
        print(
            "  Cadence:  "
            + ", ".join(
                f"{kind} every {getattr(args, f'{kind}_interval'):g}s"
                for kind in args.types
            )
        )
    confirm_or_exit(args)

    client = make_client(args)
    try:
        available_logs = probe_system_log_tables(client)
        print(
            "\nEvent source capabilities: "
            + (", ".join(sorted(available_logs)) if available_logs else "none visible")
        )
        if "query_log" not in available_logs:
            raise SystemExit(
                "Cannot generate observable events: system.query_log is not "
                "available to this ClickHouse user."
            )

        intervals = {
            "ddl": args.ddl_interval,
            "oom": args.oom_interval,
            "timeout": args.timeout_interval,
        }
        print("\nGenerating events. Press Ctrl+C to stop.\n")
        try:
            generated = run_schedule(
                client,
                args.types,
                args.database,
                intervals,
                args.once,
                args.duration,
            )
        except KeyboardInterrupt:
            _stop_event.set()
            print("\nStopping event workload...")
            flush_logs(client)
            generated = 0
        print(f"\nFinished: {generated} event generator runs succeeded.")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
