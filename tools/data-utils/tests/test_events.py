"""Tests for the safe Time Travel event workload."""

from __future__ import annotations

import argparse

import pytest

from data_utils.cli.events import (
    EVENT_TAG,
    NETWORK_QUERY,
    OOM_QUERY,
    TIMEOUT_QUERY,
    ddl_statements,
    exception_code,
    execute_expected_failure,
    generate_coordination,
    generate_rejected,
    parse_event_types,
    quote_identifier,
)


class FakeClickHouseError(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(f"Code: {code}. generated test failure")
        self.code = code


class FailingClient:
    def __init__(self, code: int) -> None:
        self.code = code
        self.queries: list[str] = []

    def execute(self, query: str):
        self.queries.append(query)
        raise FakeClickHouseError(self.code)


class EventClient:
    def __init__(self, failure_marker: str, code: int) -> None:
        self.failure_marker = failure_marker
        self.code = code
        self.queries: list[tuple[str, dict[str, int] | None]] = []

    def execute(self, query: str, settings: dict[str, int] | None = None):
        self.queries.append((query, settings))
        if self.failure_marker in query:
            raise FakeClickHouseError(self.code)
        return []


def test_parse_event_types_deduplicates_and_preserves_order() -> None:
    assert parse_event_types("timeout, ddl,timeout,oom") == ("timeout", "ddl", "oom")


def test_parse_event_types_rejects_unknown_type() -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="unknown event type"):
        parse_event_types("ddl,crash")


@pytest.mark.parametrize("value", ["prod-db", "db.name", "db`name", ""])
def test_quote_identifier_rejects_unsafe_names(value: str) -> None:
    with pytest.raises(ValueError, match="invalid identifier"):
        quote_identifier(value)


def test_failure_queries_are_tagged_and_bounded() -> None:
    assert EVENT_TAG in OOM_QUERY
    assert "max_memory_usage = 1000000" in OOM_QUERY
    assert "max_execution_time = 5" in OOM_QUERY
    assert EVENT_TAG in TIMEOUT_QUERY
    assert "max_execution_time = 0.05" in TIMEOUT_QUERY
    assert "timeout_overflow_mode = 'throw'" in TIMEOUT_QUERY
    assert EVENT_TAG in NETWORK_QUERY
    assert "127.0.0.1:1" in NETWORK_QUERY
    assert "connect_timeout = 1" in NETWORK_QUERY


def test_expected_failure_accepts_only_expected_code() -> None:
    assert execute_expected_failure(FailingClient(241), OOM_QUERY, 241, "OOM")
    assert not execute_expected_failure(FailingClient(159), OOM_QUERY, 241, "OOM")
    assert execute_expected_failure(FailingClient(519), NETWORK_QUERY, (279, 519), "network")


def test_query_rejection_is_bounded_and_restores_merges() -> None:
    client = EventClient("kind:query_rejected", 252)

    assert generate_rejected(client, "tracehouse_event_demo")

    queries = [query for query, _ in client.queries]
    assert any("parts_to_throw_insert = 1" in query for query in queries)
    assert any("SYSTEM STOP MERGES" in query for query in queries)
    assert any("SYSTEM START MERGES" in query for query in queries)
    assert queries[-1].startswith("DROP TABLE IF EXISTS")
    event_calls = [
        (query, settings)
        for query, settings in client.queries
        if "kind:query_rejected" in query
    ]
    assert len(event_calls) == 1
    assert event_calls[0][1] is None
    assert all(
        settings == {"log_queries": 0}
        for query, settings in client.queries
        if "kind:query_rejected" not in query
    )


def test_coordination_failure_is_tagged_and_cleanup_is_quiet() -> None:
    client = EventClient("kind:operational_coordination_error", 225)

    assert generate_coordination(client, "tracehouse_event_demo")

    queries = [query for query, _ in client.queries]
    assert any("ReplicatedMergeTree" in query for query in queries)
    assert queries[-1].startswith("DROP TABLE IF EXISTS")
    assert client.queries[-1][1] == {"log_queries": 0}


def test_exception_code_falls_back_to_message() -> None:
    assert exception_code(Exception("DB::Exception: Code: 159. timeout")) == 159
    assert exception_code(Exception("no ClickHouse code")) is None


def test_ddl_cycle_is_scoped_and_self_cleaning() -> None:
    statements = ddl_statements("tracehouse_event_demo")
    assert len(statements) == 10
    assert all("tracehouse_event_demo" in statement for statement in statements)
    assert any(statement.startswith("CREATE TABLE") for statement in statements)
    assert any(statement.startswith("OPTIMIZE TABLE") for statement in statements)
    assert statements[-1].startswith("DROP TABLE")
    assert all("DROP DATABASE" not in statement for statement in statements)
