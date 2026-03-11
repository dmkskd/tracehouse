"""Shared fixtures for table-plugin integration tests.

Uses testcontainers to spin up a real ClickHouse instance — same image
the TypeScript integration tests use.
"""

from __future__ import annotations

import pytest
from clickhouse_driver import Client
from testcontainers.clickhouse import ClickHouseContainer

CH_IMAGE = "clickhouse/clickhouse-server:26.1-alpine"


@pytest.fixture(scope="session")
def clickhouse():
    """Start a ClickHouse container once for the whole test session."""
    with ClickHouseContainer(CH_IMAGE) as ch:
        yield ch


@pytest.fixture(scope="session")
def client(clickhouse):
    """Return a clickhouse-driver Client connected to the container."""
    c = Client.from_url(clickhouse.get_connection_url())
    assert c.execute("SELECT 1") == [(1,)]
    return c
