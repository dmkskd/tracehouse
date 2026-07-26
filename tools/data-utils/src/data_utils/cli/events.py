"""Generate safe, observable ClickHouse events for TraceHouse demos.

The workload deliberately produces query-scoped failures and disposable DDL.
It never crashes, restarts, fills, or corrupts a ClickHouse server.

Usage:
    tracehouse-events --once
    tracehouse-events --types ddl,oom,timeout,rejected,resource,coordination,network
"""

from __future__ import annotations

import argparse
import os
import re
import threading
import time
from collections.abc import Callable, Collection
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


EVENT_TYPES = (
    "ddl",
    "oom",
    "timeout",
    "rejected",
    "resource",
    "coordination",
    "network",
)
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

RESOURCE_LIMIT_QUERY = f"""
/* {EVENT_TAG} kind:query_resource_limit */
SELECT number
FROM numbers(1000000)
ORDER BY sipHash64(number)
SETTINGS
    max_bytes_before_external_sort = 100000,
    max_bytes_ratio_before_external_sort = 0,
    min_free_disk_space_for_temporary_data = 1000000000000000,
    max_memory_usage = 100000000,
    max_execution_time = 5
FORMAT Null
"""

NETWORK_QUERY = f"""
/* {EVENT_TAG} kind:operational_network_error */
SELECT *
FROM remote('127.0.0.1:1', system, one)
SETTINGS
    connect_timeout = 1,
    connect_timeout_with_failover_ms = 100,
    connections_with_failover_max_tries = 1
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
    expected_code: int | Collection[int],
    label: str,
) -> bool:
    """Execute a query that should fail with one of the expected ClickHouse codes."""
    expected_codes = (
        {expected_code}
        if isinstance(expected_code, int)
        else set(expected_code)
    )
    started = time.monotonic()
    try:
        client.execute(query)
    except Exception as error:
        elapsed = time.monotonic() - started
        actual_code = exception_code(error)
        if actual_code in expected_codes:
            print(
                f"[{datetime.now():%H:%M:%S}] generated {label} "
                f"(code {actual_code}, {elapsed:.2f}s)"
            )
            return True
        expected_label = "/".join(str(code) for code in sorted(expected_codes))
        print(
            f"[{datetime.now():%H:%M:%S}] could not generate {label}: "
            f"expected code {expected_label}, got {actual_code or 'unknown'}: "
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


def generate_resource_limit(client: Client, database: str) -> bool:
    """Generate query-scoped NOT_ENOUGH_SPACE without consuming the disk."""
    del database
    return execute_expected_failure(
        client,
        RESOURCE_LIMIT_QUERY,
        243,
        "query resource limit",
    )


def _quiet_execute(client: Client, query: str) -> None:
    """Run setup/cleanup SQL without adding it to the generated event set."""
    client.execute(query, settings={"log_queries": 0})


def _best_effort_quiet_execute(client: Client, query: str) -> None:
    try:
        _quiet_execute(client, query)
    except Exception:
        pass


def generate_rejected(client: Client, database: str) -> bool:
    """Generate a bounded TOO_MANY_PARTS query rejection."""
    db = quote_identifier(database)
    table = f"{db}.`tracehouse_event_rejected`"
    merges_stopped = False
    try:
        _quiet_execute(client, f"CREATE DATABASE IF NOT EXISTS {db}")
        _quiet_execute(client, f"DROP TABLE IF EXISTS {table} SYNC")
        _quiet_execute(
            client,
            (
                f"CREATE TABLE {table} (value UInt64) "
                "ENGINE = MergeTree ORDER BY value "
                "SETTINGS parts_to_delay_insert = 1, parts_to_throw_insert = 1"
            ),
        )
        _quiet_execute(client, f"SYSTEM STOP MERGES {table}")
        merges_stopped = True
        _quiet_execute(client, f"INSERT INTO {table} VALUES (1)")
        query = (
            f"/* {EVENT_TAG} kind:query_rejected */ "
            f"INSERT INTO {table} VALUES (2)"
        )
        return execute_expected_failure(client, query, 252, "query rejection")
    except Exception as error:
        print(
            f"[{datetime.now():%H:%M:%S}] could not prepare query rejection: "
            f"{str(error).splitlines()[0][:180]}"
        )
        return False
    finally:
        if merges_stopped:
            _best_effort_quiet_execute(client, f"SYSTEM START MERGES {table}")
        _best_effort_quiet_execute(client, f"DROP TABLE IF EXISTS {table} SYNC")


def generate_coordination(client: Client, database: str) -> bool:
    """Generate NO_ZOOKEEPER when Keeper is unavailable, cleaning up on success."""
    db = quote_identifier(database)
    table = f"{db}.`tracehouse_event_no_keeper`"
    try:
        client.execute(
            "SELECT 1 FROM system.zookeeper WHERE path = '/' LIMIT 1",
            settings={"log_queries": 0},
        )
        print(
            f"[{datetime.now():%H:%M:%S}] could not generate coordination "
            "error: Keeper is available"
        )
        return False
    except Exception:
        pass

    try:
        _quiet_execute(client, f"CREATE DATABASE IF NOT EXISTS {db}")
        _quiet_execute(client, f"DROP TABLE IF EXISTS {table} SYNC")
        query = (
            f"/* {EVENT_TAG} kind:operational_coordination_error */ "
            f"CREATE TABLE {table} (value UInt64) "
            "ENGINE = ReplicatedMergeTree("
            f"'/tracehouse/events/{database}/no-keeper', 'event-generator') "
            "ORDER BY value"
        )
        return execute_expected_failure(client, query, 225, "coordination error")
    except Exception as error:
        print(
            f"[{datetime.now():%H:%M:%S}] could not prepare coordination error: "
            f"{str(error).splitlines()[0][:180]}"
        )
        return False
    finally:
        # If Keeper is configured, CREATE can succeed instead of producing
        # NO_ZOOKEEPER. Remove the isolated table and its replica metadata.
        _best_effort_quiet_execute(client, f"DROP TABLE IF EXISTS {table} SYNC")


def generate_network(client: Client, database: str) -> bool:
    """Generate an ALL_CONNECTION_TRIES_FAILED operational error locally."""
    del database
    # Some versions wrap code 279 in NO_REMOTE_SHARD_AVAILABLE (519).
    return execute_expected_failure(
        client,
        NETWORK_QUERY,
        (279, 519),
        "operational network error",
    )


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
        f"OPTIMIZE TABLE {table_b} FINAL",
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
            f"(database {database}, {len(ddl_statements(database))} statements)"
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
    "rejected": generate_rejected,
    "resource": generate_resource_limit,
    "coordination": generate_coordination,
    "network": generate_network,
}

EVENT_TYPE_SOURCE = {
    "ddl": "query_log",
    "oom": "query_log",
    "timeout": "query_log",
    "rejected": "query_log",
    "resource": "query_log",
    "coordination": "error_log",
    "network": "error_log",
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
            "Generate safe Time Travel events across query, change, "
            "coordination, and maintenance categories"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_connection_args(parser)
    parser.add_argument(
        "--types",
        type=parse_event_types,
        default=parse_event_types(
            os.environ.get(
                "CH_EVENT_TYPES",
                "ddl,oom,timeout,rejected,resource,coordination,network",
            )
        ),
        help=f"Comma-separated event types: {', '.join(EVENT_TYPES)} ($CH_EVENT_TYPES)",
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("CH_EVENT_DATABASE", DEFAULT_DATABASE),
        help="Disposable database used by table-backed events ($CH_EVENT_DATABASE)",
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
    parser.add_argument(
        "--rejected-interval",
        type=float,
        default=float(os.environ.get("CH_EVENT_REJECTED_INTERVAL", "1200")),
        help="Seconds between query rejections ($CH_EVENT_REJECTED_INTERVAL)",
    )
    parser.add_argument(
        "--resource-interval",
        type=float,
        default=float(os.environ.get("CH_EVENT_RESOURCE_INTERVAL", "1800")),
        help="Seconds between query resource limits ($CH_EVENT_RESOURCE_INTERVAL)",
    )
    parser.add_argument(
        "--coordination-interval",
        type=float,
        default=float(os.environ.get("CH_EVENT_COORDINATION_INTERVAL", "1800")),
        help="Seconds between coordination probes ($CH_EVENT_COORDINATION_INTERVAL)",
    )
    parser.add_argument(
        "--network-interval",
        type=float,
        default=float(os.environ.get("CH_EVENT_NETWORK_INTERVAL", "1200")),
        help="Seconds between network failure probes ($CH_EVENT_NETWORK_INTERVAL)",
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
    print(f"  Database: {args.database} (disposable event tables)")
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
        runnable_types = tuple(
            event_type
            for event_type in args.types
            if EVENT_TYPE_SOURCE[event_type] in available_logs
        )
        skipped_types = tuple(
            event_type for event_type in args.types if event_type not in runnable_types
        )
        if skipped_types:
            print(
                "Skipping event types whose source log is unavailable: "
                + ", ".join(
                    f"{event_type} ({EVENT_TYPE_SOURCE[event_type]})"
                    for event_type in skipped_types
                )
            )
        if not runnable_types:
            raise SystemExit(
                "Cannot generate observable events: none of the selected "
                "types has a visible source system log."
            )

        intervals = {
            "ddl": args.ddl_interval,
            "oom": args.oom_interval,
            "timeout": args.timeout_interval,
            "rejected": args.rejected_interval,
            "resource": args.resource_interval,
            "coordination": args.coordination_interval,
            "network": args.network_interval,
        }
        print("\nGenerating events. Press Ctrl+C to stop.\n")
        try:
            generated = run_schedule(
                client,
                runnable_types,
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
