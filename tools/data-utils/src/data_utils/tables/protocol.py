"""Contract and shared config for dataset plugins."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from clickhouse_driver import Client
    from .helpers import ProgressTracker

# A query generator is a callable that returns a fresh SQL string with
# randomized literal values on each call — useful for producing different
# parameters under the same normalized_query_hash.
QueryGenerator = Callable[[], str]


@dataclass(frozen=True)
class InsertConfig:
    """Shared insert parameters passed to every table plugin."""

    rows: int
    partitions: int
    batch_size: int
    drop: bool = False
    throttle_min: float = 0.0
    throttle_max: float = 0.0


@dataclass(frozen=True)
class QuerySet:
    """Collection of queries a table plugin provides for workload generation.

    Each field is optional — a table only needs to supply the categories
    it supports.  Static SQL goes into the ``list[str]`` fields; queries
    that need randomized literals on every run go into the ``generators``
    fields as callables returning SQL strings.
    """

    slow: list[str] = field(default_factory=list)
    fast: list[str] = field(default_factory=list)
    pk_generators: list[QueryGenerator] = field(default_factory=list)
    join_generators: list[QueryGenerator] = field(default_factory=list)
    settings_generators: list[QueryGenerator] = field(default_factory=list)


@runtime_checkable
class Dataset(Protocol):
    """Interface every dataset module must satisfy.

    A dataset owns an entire ClickHouse database — including its fact
    table(s) and any dimension/lookup tables.

    Each concrete class stores its own dependencies (``replicated``,
    ``caps``, etc.) as constructor arguments so that the orchestration
    layer can call every dataset through the same uniform API.
    """

    name: str
    flag: str  # argparse flag, e.g. "synthetic_only"

    def drop(self, client: Client) -> None: ...

    def create(self, client: Client) -> None: ...

    def insert(
        self,
        client: Client,
        config: InsertConfig,
        tracker: ProgressTracker | None = None,
    ) -> None: ...

    @property
    def queries(self) -> QuerySet:
        """Return the workload queries this table provides."""
        ...
