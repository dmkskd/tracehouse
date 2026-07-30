"""ClickHouse images supplied by the test environment."""

from __future__ import annotations

import os
import re

CLICKHOUSE_IMAGE = os.environ.get(
    "CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:latest"
)


def configured_clickhouse_is_before(
    minimum_major: int,
    minimum_minor: int,
    minimum_patch: int = 0,
) -> bool:
    """Compare a numeric pinned image tag; unknown tags such as latest run."""
    tag = CLICKHOUSE_IMAGE.rsplit(":", 1)[-1]
    match = re.match(r"^(\d+)\.(\d+)(?:\.(\d+))?", tag)
    if match is None:
        return False

    configured = (
        int(match.group(1)),
        int(match.group(2)),
        int(match.group(3) or 0),
    )
    return configured < (minimum_major, minimum_minor, minimum_patch)
