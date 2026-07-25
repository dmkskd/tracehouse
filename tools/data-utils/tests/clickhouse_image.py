"""ClickHouse images supplied by the test environment."""

from __future__ import annotations

import os

CLICKHOUSE_IMAGE = os.environ.get(
    "CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:latest"
)
