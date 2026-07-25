"""Tests for the safe Time Travel event workload."""

from __future__ import annotations

import argparse

import pytest

from data_utils.cli.events import (
    EVENT_TAG,
    OOM_QUERY,
    TIMEOUT_QUERY,
    ddl_statements,
    exception_code,
    execute_expected_failure,
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


def test_expected_failure_accepts_only_expected_code() -> None:
    assert execute_expected_failure(FailingClient(241), OOM_QUERY, 241, "OOM")
    assert not execute_expected_failure(FailingClient(159), OOM_QUERY, 241, "OOM")


def test_exception_code_falls_back_to_message() -> None:
    assert exception_code(Exception("DB::Exception: Code: 159. timeout")) == 159
    assert exception_code(Exception("no ClickHouse code")) is None


def test_ddl_cycle_is_scoped_and_self_cleaning() -> None:
    statements = ddl_statements("tracehouse_event_demo")
    assert len(statements) == 9
    assert all("tracehouse_event_demo" in statement for statement in statements)
    assert any(statement.startswith("CREATE TABLE") for statement in statements)
    assert statements[-1].startswith("DROP TABLE")
    assert all("DROP DATABASE" not in statement for statement in statements)
