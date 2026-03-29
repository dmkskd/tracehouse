"""Integration tests for dataset plugins.

Verifies that each plugin satisfies the Dataset protocol and can
create databases + insert data into a real ClickHouse instance.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from clickhouse_driver import Client

from data_utils.tables import (
    Dataset, InsertConfig, InsertMode, QuerySet,
    SyntheticData, NycTaxi, UkHousePrices, WebAnalytics,
)


# ── Protocol conformance ───────────────────────────────────────────


# Instantiate with minimal config (no caps = plain MergeTree)
ALL_DATASETS: list[Dataset] = [
    SyntheticData(),
    NycTaxi(),
    UkHousePrices(),
    WebAnalytics(),
]


@pytest.mark.parametrize("dataset", ALL_DATASETS, ids=lambda ds: type(ds).__name__)
def test_satisfies_protocol(dataset: Dataset) -> None:
    """Each plugin instance must satisfy the runtime-checkable Dataset."""
    assert isinstance(dataset, Dataset)


@pytest.mark.parametrize("dataset", ALL_DATASETS, ids=lambda ds: type(ds).__name__)
def test_has_required_attrs(dataset: Dataset) -> None:
    """Each plugin must declare name and flag."""
    assert isinstance(dataset.name, str) and dataset.name
    assert isinstance(dataset.flag, str) and dataset.flag


# ── Integration tests (one per dataset) ───────────────────────────

SMALL_CONFIG = InsertConfig(rows=1000, partitions=1, batch_size=500)


class TestSyntheticData:
    @pytest.fixture(autouse=True)
    def setup(self, client: Client) -> Generator[None]:
        self.dataset = SyntheticData()
        self.client = client
        yield
        client.execute("DROP TABLE IF EXISTS synthetic_data.user_tiers SYNC")
        client.execute("DROP TABLE IF EXISTS synthetic_data.events SYNC")
        client.execute("DROP DATABASE IF EXISTS synthetic_data SYNC")

    def test_create_and_insert(self) -> None:
        self.dataset.create(self.client)
        self.dataset.insert(self.client, SMALL_CONFIG)
        rows: int = self.client.execute("SELECT count() FROM synthetic_data.events")[0][0]
        assert rows == SMALL_CONFIG.rows

    def test_drop(self) -> None:
        self.dataset.create(self.client)
        self.dataset.drop(self.client)
        dbs: list[str] = [r[0] for r in self.client.execute("SHOW DATABASES")]
        assert "synthetic_data" not in dbs


class TestNycTaxi:
    @pytest.fixture(autouse=True)
    def setup(self, client: Client) -> Generator[None]:
        self.dataset = NycTaxi()
        self.client = client
        yield
        client.execute("DROP TABLE IF EXISTS nyc_taxi.locations SYNC")
        client.execute("DROP TABLE IF EXISTS nyc_taxi.trips SYNC")
        client.execute("DROP DATABASE IF EXISTS nyc_taxi SYNC")

    def test_create_and_insert(self) -> None:
        self.dataset.create(self.client)
        self.dataset.insert(self.client, SMALL_CONFIG)
        rows: int = self.client.execute("SELECT count() FROM nyc_taxi.trips")[0][0]
        assert rows == SMALL_CONFIG.rows


class TestUkHousePrices:
    @pytest.fixture(autouse=True)
    def setup(self, client: Client) -> Generator[None]:
        self.dataset = UkHousePrices()
        self.client = client
        yield
        client.execute("DROP TABLE IF EXISTS uk_price_paid.uk_price_paid SYNC")
        client.execute("DROP DATABASE IF EXISTS uk_price_paid SYNC")

    def test_create_and_insert(self) -> None:
        self.dataset.create(self.client)
        self.dataset.insert(self.client, SMALL_CONFIG)
        rows: int = self.client.execute("SELECT count() FROM uk_price_paid.uk_price_paid")[0][0]
        assert rows == SMALL_CONFIG.rows


class TestWebAnalytics:
    @pytest.fixture(autouse=True)
    def setup(self, client: Client) -> Generator[None]:
        self.dataset = WebAnalytics()
        self.client = client
        yield
        client.execute("DROP TABLE IF EXISTS web_analytics.pageviews SYNC")
        client.execute("DROP DATABASE IF EXISTS web_analytics SYNC")

    def test_create_and_insert(self) -> None:
        self.dataset.create(self.client)
        self.dataset.insert(self.client, SMALL_CONFIG)
        rows: int = self.client.execute("SELECT count() FROM web_analytics.pageviews")[0][0]
        assert rows == SMALL_CONFIG.rows


# ── InsertConfig defaults ──────────────────────────────────────────


def test_insert_config_defaults() -> None:
    cfg = InsertConfig(rows=100, partitions=1, batch_size=50)
    assert cfg.mode is InsertMode.RESUME
    assert cfg.throttle_min == 0.0
    assert cfg.throttle_max == 0.0


def test_insert_config_frozen() -> None:
    cfg = InsertConfig(rows=100, partitions=1, batch_size=50)
    with pytest.raises(AttributeError):
        cfg.rows = 200  # type: ignore[misc]


# ── QuerySet tests ─────────────────────────────────────────────────


@pytest.mark.parametrize("dataset", ALL_DATASETS, ids=lambda ds: type(ds).__name__)
def test_queries_returns_queryset(dataset: Dataset) -> None:
    """Each dataset's queries property must return a QuerySet."""
    qs: QuerySet = dataset.queries
    assert isinstance(qs, QuerySet)


@pytest.mark.parametrize("dataset", ALL_DATASETS, ids=lambda ds: type(ds).__name__)
def test_queries_not_empty(dataset: Dataset) -> None:
    """Each dataset should provide at least some queries."""
    qs: QuerySet = dataset.queries
    total = len(qs.slow) + len(qs.fast) + len(qs.pk_generators) + len(qs.join_generators) + len(qs.settings_generators)
    assert total > 0, f"{type(dataset).__name__} has no queries"


@pytest.mark.parametrize("dataset", ALL_DATASETS, ids=lambda ds: type(ds).__name__)
def test_generators_return_sql(dataset: Dataset) -> None:
    """All query generators must return non-empty SQL strings."""
    qs: QuerySet = dataset.queries
    for gen in qs.pk_generators + qs.join_generators + qs.settings_generators:
        sql: str = gen()
        assert isinstance(sql, str) and sql.strip()
