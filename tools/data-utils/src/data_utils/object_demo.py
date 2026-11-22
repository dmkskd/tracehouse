"""Persistent, namespaced object catalog and cascading-MV demo.

DDL belongs to this data-tools scenario. Demo principals have HOST NONE and are
separate from the credential-bearing workload users. Setup is additive and
repeatable; cleanup removes only this scenario's fixed namespace.

``run_object_demo_activity`` lets the demo users log in for one run, with random
passwords, runs the scenario's queries as each of them and locks them again.
"""
from __future__ import annotations

import json
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from importlib.resources import files

from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException

from data_utils.tables.helpers import on_cluster_clause

DATABASE = "tracehouse_object_demo"


def scenario() -> dict:
    return json.loads(files("data_utils").joinpath("sql/demo-objects.json").read_text())


def setup_object_demo(client: Client, cluster: str = "") -> None:
    statements = scenario()
    clause = on_cluster_clause(cluster)
    for sql in statements["setup"]:
        client.execute(sql.replace("__ON_CLUSTER__", clause))
    # Seed only the connected host. The demo uses local Atomic/MergeTree tables,
    # deliberately independent of replicated workload datasets and Keeper.
    client.execute(statements["seed"])
    verify_object_demo(client)


def verify_object_demo(client: Client) -> list[tuple]:
    statements = scenario()
    window = client.execute(statements["clock"])[0][0]
    raw, minute, hourly = [client.execute(sql, {"window": window}) for sql in statements["verify"]]
    if not raw or raw != minute or raw != hourly:
        raise RuntimeError(f"Cascading MV totals differ: source={raw}, minute={minute}, hourly={hourly}")
    return raw


def append_object_demo(client: Client) -> None:
    client.execute(scenario()["append"])


def drop_object_demo(client: Client, cluster: str = "") -> None:
    clause = on_cluster_clause(cluster)
    for sql in scenario()["teardown"]:
        client.execute(sql.replace("__ON_CLUSTER__", clause))


# Tags the activity in query_log, next to the user it ran as.
ACTIVITY_COMMENT = "tracehouse:object-demo"


@dataclass
class ActivityResult:
    user: str
    sql: str
    # ``ok`` or a ClickHouse error code (497 is ACCESS_DENIED).
    expected: str | int
    actual: str | int
    why: str

    @property
    def matched(self) -> bool:
        return self.expected == self.actual


def run_object_demo_activity(admin: Client, connect: Callable[[str, str], Client], cluster: str = "") -> list[ActivityResult]:
    """Run the scenario's activity as the demo users, one session per user in script order.

    ``connect(user, password)`` opens a session as that user. A step's outcome is ``ok``
    or the server's error code; the caller compares it with the expected outcome.
    """
    statements = scenario()
    clause = on_cluster_clause(cluster)
    users = list(dict.fromkeys(step["user"] for step in statements["activity"]))
    passwords = {user: secrets.token_hex(16) for user in users}
    sessions: dict[str, Client] = {}
    results: list[ActivityResult] = []
    try:
        for user in users:
            admin.execute(statements["login"].format(user=user, password=passwords[user]).replace("__ON_CLUSTER__", clause))
        for step in statements["activity"]:
            user = step["user"]
            session = sessions.get(user) or sessions.setdefault(user, connect(user, passwords[user]))
            try:
                session.execute(step["sql"], settings={"log_comment": ACTIVITY_COMMENT})
                actual: str | int = "ok"
            except ServerException as error:
                actual = error.code
            results.append(ActivityResult(user, step["sql"], step["expect"], actual, step["why"]))
    finally:
        for session in sessions.values():
            session.disconnect()
        for user in users:
            admin.execute(statements["lock"].format(user=user).replace("__ON_CLUSTER__", clause))
    return results
