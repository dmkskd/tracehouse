"""Persistent, namespaced object catalog and cascading-MV demo.

DDL belongs to this data-tools scenario. Demo principals have HOST NONE and are
separate from the credential-bearing workload users. Setup is additive and
repeatable; cleanup removes only this scenario's fixed namespace.
"""
from __future__ import annotations

import json
from importlib.resources import files

from clickhouse_driver import Client

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
