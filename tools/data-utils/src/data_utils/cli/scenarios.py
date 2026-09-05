"""Fault-injection scenarios for evaluating LLM investigations.

Each scenario composes the existing generators (queries, mutations, events,
merge triggers) on a timeline and writes two files:

  <run>.question.md   The prompt handed to the model. Symptom and time window
                      only, never the cause.
  <run>.answer.json   The answer key: what was actually injected, when, and
                      which TraceHouse primitives should surface it.

Keep the two apart. A model that sees the answer key is not being evaluated.

Usage:
    tracehouse-scenarios --list
    tracehouse-scenarios mutation-storm --dry-run
    tracehouse-scenarios mutation-storm --warmup 120 --settle 180

Scenarios:
    mutation-storm   Heavy async mutations behind a live query workload
    memory-pressure  Repeated MEMORY_LIMIT_EXCEEDED under query concurrency
    pk-scan          Queries that skip the leading ORDER BY column
    failed-merge     One failing MutatePart among healthy merges
    control          Workload only, nothing injected (false-positive check)

Environment variables (all optional, CLI flags override):
  CH_SCENARIO_DATABASE  Target database (default: synthetic_data)
  CH_SCENARIO_TABLE     Target table (default: events)
  CH_SCENARIO_OUT       Output directory (default: ./scenario-runs)
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from clickhouse_driver import Client

from data_utils.cli.events import (
    flush_logs,
    generate_merge_failure,
    generate_oom,
)
from data_utils.cli.merge_triggers import (
    cleanup_test_tables,
    setup_test_tables,
    trigger_regular_merge,
)
from data_utils.cli.mutations import run_heavy_delete, run_heavy_update
from data_utils.env import (
    add_connection_args,
    confirm_or_exit,
    env_int,
    make_client,
    pre_parse_env_file,
    print_connection,
)

SCENARIO_TAG = "tracehouse-scenario"

# The recorded window must contain the fault and nothing the harness itself did.
# Starting the workload fires capability probes (S3 access, system.zookeeper)
# that fail on servers which do not allow them; stopping it cancels in-flight
# queries. Both would otherwise look like symptoms to whoever investigates.
WORKLOAD_START_MARGIN = 8.0
WORKLOAD_STOP_MARGIN = 6.0

# Failures the harness produces by design, in every scenario including the
# control. Recorded in the answer key so grading does not credit or punish a
# model for noticing them.
KNOWN_WORKLOAD_NOISE = (
    {"code": 394, "name": "QUERY_WAS_CANCELLED",
     "source": "workload teardown cancelling in-flight queries"},
    {"code": 497, "name": "S3 server-managed credentials denied",
     "source": "capability probe at workload startup"},
    {"code": 60, "name": "system.zookeeper unknown",
     "source": "capability probe at workload startup on a non-replicated server"},
    {"code": 160, "name": "max sleep time exceeded",
     "source": "a slow workload query using sleep() beyond the server limit"},
)


def utc_now() -> str:
    """Timestamp in the form investigations are scoped by."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class Context:
    """Everything a scenario body needs, plus the timeline recorder."""

    client: Client
    args: argparse.Namespace
    timeline: list[dict] = field(default_factory=list)

    @property
    def database(self) -> str:
        return self.args.database

    @property
    def table(self) -> str:
        return self.args.table

    def mark(self, label: str, **detail: object) -> None:
        """Record a timeline entry. This is what the answer key is built from."""
        entry = {"at": utc_now(), "label": label, **detail}
        self.timeline.append(entry)
        detail_text = " ".join(f"{k}={v}" for k, v in detail.items() if k != "sql")
        print(f"[{datetime.now():%H:%M:%S}] {label}" + (f"  {detail_text}" if detail_text else ""))

    def sleep(self, seconds: float, label: str) -> None:
        """Sleep in a way that stays interruptible and shows progress."""
        if seconds <= 0:
            return
        print(f"[{datetime.now():%H:%M:%S}] {label} for {seconds:.0f}s ...")
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            time.sleep(min(5.0, deadline - time.monotonic()))


def probe_preexisting_faults(client: Client, lookback_minutes: int = 30) -> dict:
    """Snapshot faults that already exist before anything is injected.

    Without this the control scenario is a lie: an environment with a full
    object store or a stuck mutation will hand the model a real problem to
    find, and grading cannot tell that from a fabrication.
    """
    probe: dict[str, object] = {"lookback_minutes": lookback_minutes}
    try:
        probe["query_exceptions"] = [
            {"code": code, "count": count, "sample": sample}
            for code, count, sample in client.execute(f"""
                SELECT exception_code, count(), any(substring(exception, 1, 160))
                FROM system.query_log
                WHERE event_time >= now() - INTERVAL {lookback_minutes} MINUTE
                  AND type IN ('ExceptionWhileProcessing', 'ExceptionBeforeStart')
                GROUP BY exception_code
                ORDER BY 2 DESC
            """)
        ]
        probe["failed_part_operations"] = [
            {"event_type": str(event_type), "table": f"{database}.{table}",
             "error": error, "count": count, "sample": sample}
            for event_type, database, table, error, count, sample in client.execute(f"""
                SELECT event_type, database, table, error, count(),
                       any(substring(exception, 1, 160))
                FROM system.part_log
                WHERE event_time >= now() - INTERVAL {lookback_minutes} MINUTE
                  AND error != 0
                GROUP BY event_type, database, table, error
                ORDER BY 5 DESC
            """)
        ]
        probe["unfinished_mutations"] = client.execute(
            "SELECT count() FROM system.mutations WHERE NOT is_done"
        )[0][0]
        probe["readonly_replicas"] = client.execute(
            "SELECT count() FROM system.replicas WHERE is_readonly"
        )[0][0]
    except Exception as error:
        probe["error"] = str(error).splitlines()[0][:200]
    return probe


@dataclass(frozen=True)
class Scenario:
    """A fault injection plus the ground truth needed to grade an answer."""

    key: str
    title: str
    symptom: str
    """What the model is told. Must not name the cause."""
    mechanism: str
    """What actually happened. The answer key."""
    expected_primitives: tuple[str, ...]
    expected_system_tables: tuple[str, ...]
    run: Callable[[Context], None]
    workload: bool = True
    difficulty: str = ""


# ── Background workload ─────────────────────────────────────────────


class Workload:
    """The query runner as a subprocess, so a scenario has realistic noise.

    Invoked as ``python -m data_utils.cli.queries`` rather than through the
    console script, which may not be on PATH in every install.
    """

    def __init__(self, args: argparse.Namespace, extra: list[str] | None = None):
        self.args = args
        self.extra = extra or []
        self.process: subprocess.Popen | None = None

    def command(self) -> list[str]:
        cmd = [
            sys.executable, "-m", "data_utils.cli.queries",
            "--host", self.args.host,
            "--port", str(self.args.port),
            "--user", self.args.user,
        ]
        if self.args.password:
            cmd += ["--password", self.args.password]
        if self.args.secure:
            cmd += ["--secure"]
        return cmd + self.extra

    def start(self, ctx: Context) -> None:
        cmd = self.command()
        # Keep the child in its own process group so our Ctrl-C does not race it.
        self.process = subprocess.Popen(cmd, start_new_session=True)
        ctx.mark("workload started", pid=self.process.pid, args=" ".join(self.extra) or "defaults")

    def stop(self, ctx: Context) -> None:
        if not self.process or self.process.poll() is not None:
            return
        # SIGINT first: queries.py handles KeyboardInterrupt and locks test users.
        os.killpg(os.getpgid(self.process.pid), signal.SIGINT)
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
            self.process.wait(timeout=10)
        ctx.mark("workload stopped")


# ── Scenario bodies ─────────────────────────────────────────────────


def run_mutation_storm(ctx: Context) -> None:
    """Fire heavy async mutations that rewrite parts behind a live workload.

    The symptom (slow queries, merge backlog) shows up minutes after the cause,
    and the cause is invisible unless you look at system.mutations.
    """
    count = ctx.args.intensity
    ctx.mark("injection start", kind="heavy mutations", count=count)
    for index in range(count):
        runner = run_heavy_update if index % 2 == 0 else run_heavy_delete
        result = runner(ctx.client, ctx.database, ctx.table, mutations_sync=0)
        if result.get("status") == "skipped":
            ctx.mark("mutation skipped", reason=result.get("reason", ""))
            continue
        ctx.mark(
            "mutation issued",
            type=result.get("type", ""),
            partition=result.get("partition", ""),
        )
        time.sleep(2.0)
    ctx.mark("injection end", kind="heavy mutations")


def run_memory_pressure(ctx: Context) -> None:
    """Drive repeated MEMORY_LIMIT_EXCEEDED while the cluster is already busy.

    The failures are easy to spot. Attributing the memory to a query class is
    the part that needs the workload breakdown rather than the error list.
    """
    count = ctx.args.intensity
    ctx.mark("injection start", kind="query OOM", count=count)
    for _ in range(count):
        generate_oom(ctx.client, ctx.database)
        ctx.mark("oom generated", code=241)
        time.sleep(3.0)
    ctx.mark("injection end", kind="query OOM")


PK_SCAN_QUERIES = (
    # ORDER BY is (event_date, user_id, event_time). Filtering on user_id alone
    # skips the leading column, so the primary key cannot narrow the scan.
    """
    SELECT device_type, count() AS hits, avg(duration_ms) AS avg_ms
    FROM {db}.{table}
    WHERE user_id = {user_id}
    GROUP BY device_type
    ORDER BY hits DESC
    SETTINGS use_query_cache = 0
    """,
    # No key column at all in the WHERE clause.
    """
    SELECT toStartOfHour(event_time) AS hour, count() AS hits
    FROM {db}.{table}
    WHERE device_type = '{device_type}'
    GROUP BY hour
    ORDER BY hour
    SETTINGS use_query_cache = 0
    """,
)


def run_pk_scan(ctx: Context) -> None:
    """Run queries that read far more rows than they return.

    High read_rows against a small result is the signature. The workload runs
    legitimately heavy queries too, so volume alone does not identify these.
    """
    count = ctx.args.intensity
    ctx.mark("injection start", kind="primary key skipped", count=count, tag=SCENARIO_TAG)
    for index in range(count):
        template = PK_SCAN_QUERIES[index % len(PK_SCAN_QUERIES)]
        query = template.format(
            db=ctx.database,
            table=ctx.table,
            user_id=random.randint(1, 10_000),
            device_type=random.choice(["desktop", "mobile", "tablet"]),
        )
        tagged = f"/* {SCENARIO_TAG} key:pk-scan */ " + query
        started = time.monotonic()
        try:
            ctx.client.execute(tagged)
            ctx.mark("scan query ran", elapsed=f"{time.monotonic() - started:.2f}s")
        except Exception as error:
            ctx.mark("scan query failed", error=str(error).splitlines()[0][:160])
        time.sleep(2.0)
    ctx.mark("injection end", kind="primary key skipped")


def run_failed_merge(ctx: Context) -> None:
    """Produce healthy merges, then exactly one failing MutatePart among them.

    Uses the merge-triggers test tables so the healthy merges are real rather
    than incidental, then drops them again.
    """
    ctx.mark("injection start", kind="failed merge")
    created = False
    try:
        setup_test_tables(ctx.client)
        created = True
        for _ in range(max(1, ctx.args.intensity // 2)):
            result = trigger_regular_merge(ctx.client)
            ctx.mark("healthy merge triggered", parts=result.get("active_parts", ""))
            time.sleep(3.0)

        ok = generate_merge_failure(ctx.client, ctx.database)
        ctx.mark("merge failure generated", succeeded=ok)
    finally:
        if created and not ctx.args.keep:
            cleanup_test_tables(ctx.client)
            ctx.mark("merge test tables dropped")
    ctx.mark("injection end", kind="failed merge")


def run_control(ctx: Context) -> None:
    """Inject nothing. The workload alone is the whole scenario.

    A model that reports a root cause here is inventing one.
    """
    ctx.mark("injection start", kind="none (control)")
    ctx.sleep(ctx.args.intensity * 2.0, "running clean")
    ctx.mark("injection end", kind="none (control)")


SCENARIOS: dict[str, Scenario] = {
    "mutation-storm": Scenario(
        key="mutation-storm",
        title="Mutation storm starves merges",
        symptom=(
            "Queries against {db}.{table} got slower and the part count started "
            "climbing. Nothing was deployed. What happened?"
        ),
        mechanism=(
            "Heavy ALTER TABLE UPDATE/DELETE mutations were issued asynchronously "
            "(mutations_sync=0) against {db}.{table}. Each rewrites whole parts, "
            "competing with background merges for the same pool. The mutations are "
            "visible in system.mutations with is_done=0; the merge backlog they "
            "cause is the symptom, not the cause."
        ),
        expected_primitives=("mutations-monitoring", "merge-monitoring", "storage-parts"),
        expected_system_tables=("system.mutations", "system.part_log", "system.parts"),
        run=run_mutation_storm,
        difficulty=(
            "Cause and symptom are minutes apart. Merge dashboards show the "
            "backlog but not why. Also a regression check: mutations were once "
            "hidden behind a memory filter in TraceHouse."
        ),
    ),
    "memory-pressure": Scenario(
        key="memory-pressure",
        title="Memory limit exceeded under concurrency",
        symptom=(
            "Some queries started failing with MEMORY_LIMIT_EXCEEDED. The cluster "
            "was busy but not obviously overloaded. Which workload is responsible?"
        ),
        mechanism=(
            "Queries with a deliberately low max_memory_usage were run against "
            "numbers_mt while the standard query workload was active. The failures "
            "are code 241 in system.query_log. The interesting answer is which "
            "query class actually held memory, which needs per-query attribution "
            "rather than the error list."
        ),
        expected_primitives=("memory-monitoring", "workload-breakdown", "select-perf"),
        expected_system_tables=("system.query_log", "system.metric_log"),
        run=run_memory_pressure,
        difficulty=(
            "Naming the error is trivial and worth no credit. Attributing memory "
            "to a query class is the actual task."
        ),
    ),
    "pk-scan": Scenario(
        key="pk-scan",
        title="Queries skip the leading ORDER BY column",
        symptom=(
            "A few query shapes against {db}.{table} are much slower than their "
            "result size suggests. The table has not changed. Why?"
        ),
        mechanism=(
            "{db}.{table} is ordered by (event_date, user_id, event_time). The "
            "injected queries filter on user_id alone, or on device_type only, so "
            "the primary key cannot narrow the scan. The signature is high "
            "read_rows against a small result. Injected queries carry the comment "
            "tag '" + SCENARIO_TAG + " key:pk-scan' in system.query_log."
        ),
        expected_primitives=("select-perf", "xray", "query detail"),
        expected_system_tables=("system.query_log",),
        run=run_pk_scan,
        difficulty=(
            "The workload runs legitimately heavy queries at the same time, so "
            "raw duration does not separate them. The ratio does."
        ),
    ),
    "failed-merge": Scenario(
        key="failed-merge",
        title="One failed merge in a healthy cluster",
        symptom=(
            "Merge activity looks normal overall. Is anything actually wrong with "
            "merges right now?"
        ),
        mechanism=(
            "A deliberately failing mutation was isolated in a disposable MergeTree "
            "table, producing one failed MutatePart in system.part_log, while "
            "successful merges were triggered on the merge-test tables at the same "
            "time. The failure is a single row among many healthy ones."
        ),
        expected_primitives=("merge-monitoring", "Merge Tracker", "events"),
        expected_system_tables=("system.part_log", "system.mutations"),
        run=run_failed_merge,
        difficulty=(
            "Aggregate merge health looks fine. Only a per-event view surfaces "
            "the single failure."
        ),
    ),
    "control": Scenario(
        key="control",
        title="Nothing is wrong (control)",
        symptom=(
            "Someone reported the cluster felt slow around this window. Was "
            "anything actually wrong?"
        ),
        mechanism=(
            "Nothing was injected. Only the standard query workload ran. The "
            "correct answer is that no fault is present. Any confidently named "
            "root cause is a fabrication."
        ),
        expected_primitives=(),
        expected_system_tables=("system.query_log",),
        run=run_control,
        difficulty=(
            "The most valuable run in the set. Models that pattern-match a "
            "narrative onto noise fail here."
        ),
    ),
}


# ── Output ──────────────────────────────────────────────────────────


def format_symptom(scenario: Scenario, args: argparse.Namespace) -> str:
    return scenario.symptom.format(db=args.database, table=args.table)


def format_mechanism(scenario: Scenario, args: argparse.Namespace) -> str:
    return scenario.mechanism.format(db=args.database, table=args.table)


def write_question(path: str, scenario: Scenario, args: argparse.Namespace,
                   started_at: str, ended_at: str) -> None:
    """The prompt for the model. Symptom and window only."""
    body = f"""# Investigation request: {scenario.key}

{format_symptom(scenario, args)}

## Scope

- Window: {started_at} to {ended_at}
- Host: {args.host}
- Database: {args.database}

## What to produce

A TraceHouse notebook that answers the question above. Ground every claim in
evidence you actually queried, link to the TraceHouse view that shows it, and
separate what you observed from what you inferred. If the evidence does not
support a root cause, say so rather than choosing one.
"""
    with open(path, "w") as handle:
        handle.write(body)


def write_answer(path: str, scenario: Scenario, args: argparse.Namespace,
                 ctx: Context, started_at: str, ended_at: str,
                 preexisting: dict) -> None:
    """The answer key. Do not give this to the model."""
    answer = {
        "_warning": "Answer key. Do not include in any prompt given to a model.",
        "scenario": scenario.key,
        "title": scenario.title,
        "started_at": started_at,
        "ended_at": ended_at,
        "connection": {"host": args.host, "port": args.port},
        "targets": {"database": args.database, "table": args.table},
        "workload": scenario.workload,
        "intensity": args.intensity,
        "ground_truth": {
            "mechanism": format_mechanism(scenario, args),
            "expected_primitives": list(scenario.expected_primitives),
            "expected_system_tables": list(scenario.expected_system_tables),
            "difficulty": scenario.difficulty,
        },
        "preexisting_faults": preexisting,
        "known_workload_noise": list(KNOWN_WORKLOAD_NOISE),
        "timeline": ctx.timeline,
        "grading": {
            "found_it": [
                "Correct time window for the injection",
                "Correct database and table (or correctly says host-wide)",
                "Correct mechanism, not just the symptom",
                "Anything in preexisting_faults is NOT a fabrication: the model "
                "found something that was already broken. Score it separately.",
            ],
            "described_it": [
                "Each claim points at a primitive that actually shows it",
                "Observed and inferred are separated",
                "Deep links resolve to the right view",
                "Would you forward it unedited",
            ],
        },
    }
    with open(path, "w") as handle:
        json.dump(answer, handle, indent=2)


# ── Entry point ─────────────────────────────────────────────────────


SIDE_EFFECTS = {
    "mutation-storm": "It issues heavy ALTER TABLE UPDATE/DELETE mutations.",
    "memory-pressure": "It runs queries that deliberately fail on memory limits.",
    "pk-scan": "It runs deliberately unselective SELECT queries.",
    "failed-merge": "It creates and drops merge-test tables and forces a mutation to fail.",
    "control": "It runs the read-only query workload and injects nothing.",
}


def report_preexisting(probe: dict) -> None:
    """Warn before the run if the cluster is already unhealthy."""
    if probe.get("error"):
        print(f"\n⚠  Could not probe pre-existing faults: {probe['error']}\n")
        return

    exceptions = probe.get("query_exceptions") or []
    failures = probe.get("failed_part_operations") or []
    stuck = probe.get("unfinished_mutations") or 0
    readonly = probe.get("readonly_replicas") or 0
    if not (exceptions or failures or stuck or readonly):
        print("\n  Pre-run check: clean.\n")
        return

    print("\n⚠  Pre-existing faults on this cluster before injection:")
    for item in exceptions:
        print(f"     query exception code {item['code']} x{item['count']}")
    for item in failures:
        print(f"     {item['event_type']} on {item['table']} error {item['error']} x{item['count']}")
    if stuck:
        print(f"     {stuck} unfinished mutation(s)")
    if readonly:
        print(f"     {readonly} readonly replica(s)")
    print("   These are recorded in the answer key. A model that reports them")
    print("   is right, not hallucinating.\n")


def print_plan(scenario: Scenario, args: argparse.Namespace) -> None:
    print()
    print("=" * 62)
    print(f"  Scenario: {scenario.key}  ({scenario.title})")
    print("=" * 62)
    print(f"  Target:     {args.database}.{args.table} on {args.host}:{args.port}")
    print(f"  Workload:   {'yes' if scenario.workload else 'no'}")
    print(f"  Warmup:     {args.warmup}s")
    print(f"  Intensity:  {args.intensity}")
    print(f"  Settle:     {args.settle}s")
    print(f"  Output:     {args.out}")
    print()
    print(f"  Question given to the model:")
    print(f"    {format_symptom(scenario, args)}")
    print()
    print(f"  Answer key (withheld):")
    print(f"    {format_mechanism(scenario, args)}")
    print("=" * 62)
    print()


def main() -> None:
    env_path = pre_parse_env_file()

    parser = argparse.ArgumentParser(
        description="Run fault-injection scenarios and record the answer key",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_connection_args(parser)
    parser.add_argument("scenario", nargs="?", help="Scenario key (see --list)")
    parser.add_argument("--list", action="store_true", help="List scenarios and exit")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without connecting or injecting")
    parser.add_argument("--database", default=os.environ.get("CH_SCENARIO_DATABASE", "synthetic_data"), help="Target database (default: $CH_SCENARIO_DATABASE or synthetic_data)")
    parser.add_argument("--table", default=os.environ.get("CH_SCENARIO_TABLE", "events"), help="Target table (default: $CH_SCENARIO_TABLE or events)")
    parser.add_argument("--warmup", type=float, default=120.0, help="Seconds of workload-only traffic before injection (default: 120)")
    parser.add_argument("--settle", type=float, default=180.0, help="Seconds of workload after injection so the symptom develops (default: 180)")
    parser.add_argument("--intensity", type=int, default=env_int("CH_SCENARIO_INTENSITY", "10"), help="How many injections to perform (default: $CH_SCENARIO_INTENSITY or 10)")
    parser.add_argument("--no-workload", action="store_true", help="Skip the background query workload")
    parser.add_argument("--keep", action="store_true", help="Keep any scenario-created tables for inspection")
    parser.add_argument("--out", default=os.environ.get("CH_SCENARIO_OUT", "scenario-runs"), help="Output directory (default: $CH_SCENARIO_OUT or ./scenario-runs)")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable scenarios:\n")
        for scenario in SCENARIOS.values():
            print(f"  {scenario.key:<16} {scenario.title}")
            print(f"  {'':<16} {scenario.difficulty}")
            print()
        return

    if not args.scenario:
        parser.error("a scenario key is required (see --list)")
    if args.scenario not in SCENARIOS:
        parser.error(f"unknown scenario '{args.scenario}'. Valid: {', '.join(SCENARIOS)}")

    scenario = SCENARIOS[args.scenario]
    if args.no_workload:
        scenario = Scenario(**{**scenario.__dict__, "workload": False})

    print_connection(args, env_path)
    print_plan(scenario, args)

    if args.dry_run:
        print("Dry run: nothing was executed.\n")
        return

    print(f"This runs against a real cluster. {SIDE_EFFECTS[scenario.key]}")
    confirm_or_exit(args)

    client = make_client(args)
    ctx = Context(client=client, args=args)

    preexisting = probe_preexisting_faults(client)
    report_preexisting(preexisting)

    workload = Workload(args) if scenario.workload else None
    if workload:
        # Start before the window opens so the capability probes it fires do
        # not look like part of the incident.
        workload.start(ctx)
        time.sleep(WORKLOAD_START_MARGIN)

    started_at = utc_now()
    ended_at = started_at
    try:
        if workload:
            ctx.sleep(args.warmup, "warming up")

        scenario.run(ctx)

        if workload:
            ctx.sleep(args.settle, "settling")
        ended_at = utc_now()
        if workload:
            # Close the window before teardown so the cancellations it causes
            # are logged outside it.
            time.sleep(WORKLOAD_STOP_MARGIN)
    except KeyboardInterrupt:
        ended_at = utc_now()
        ctx.mark("interrupted")
        print("\nInterrupted. Writing what was recorded so far.")
    except Exception as error:
        # A run that dies partway through still produced load and still needs a
        # record: losing the timeline means losing the only account of what the
        # cluster was subjected to.
        ended_at = utc_now()
        ctx.mark("aborted", error=str(error).splitlines()[0][:200])
        print(f"\nRun aborted: {error}")
        print("Writing what was recorded so far.")
    finally:
        if workload:
            workload.stop(ctx)
        flush_logs(client)

    os.makedirs(args.out, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = os.path.join(args.out, f"{scenario.key}-{stamp}")
    write_question(f"{base}.question.md", scenario, args, started_at, ended_at)
    write_answer(f"{base}.answer.json", scenario, args, ctx, started_at, ended_at, preexisting)

    print()
    print(f"  Prompt for the model:  {base}.question.md")
    print(f"  Answer key (withhold): {base}.answer.json")
    print()


if __name__ == "__main__":
    main()
