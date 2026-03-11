"""Shared fixtures for dataset integration tests.

Uses testcontainers to spin up a real ClickHouse instance — same image
the TypeScript integration tests use.

Fixtures:
    clickhouse  — session-scoped ClickHouse container (started once, shared by all tests)
    client      — clickhouse-driver Client connected to that container
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from clickhouse_driver import Client
from testcontainers.clickhouse import ClickHouseContainer

CH_IMAGE = "clickhouse/clickhouse-server:26.1-alpine"


@pytest.fixture(scope="session")
def clickhouse() -> Generator[ClickHouseContainer]:
    """Start a ClickHouse container once for the whole test session."""
    with ClickHouseContainer(CH_IMAGE) as ch:
        yield ch


@pytest.fixture(scope="session")
def client(clickhouse: ClickHouseContainer) -> Client:
    """Return a clickhouse-driver Client connected to the container."""
    c = Client.from_url(clickhouse.get_connection_url())
    assert c.execute("SELECT 1") == [(1,)]
    return c
