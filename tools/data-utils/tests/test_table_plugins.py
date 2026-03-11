"""Integration tests for table plugins.

Verifies that each plugin satisfies the Dataset protocol and can
create tables + insert data into a real ClickHouse instance.
"""

from __future__ import annotations

import pytest
from clickhouse_driver import Client

from data_utils.tables import (
    Dataset, InsertConfig, QuerySet,
    SyntheticData, NycTaxi, UkHousePrices, WebAnalytics,
)


# ── Protocol conformance ───────────────────────────────────────────


# Instantiate with minimal config (replicated=False, no caps)
ALL_PLUGINS = [
    SyntheticData(replicated=False),
    NycTaxi(replicated=False),
    UkHousePrices(replicated=False),
    WebAnalytics(caps=None),
]


@pytest.mark.parametrize("plugin", ALL_PLUGINS, ids=lambda p: type(p).__name__)
def test_satisfies_protocol(plugin):
    """Each plugin instance must satisfy the runtime-checkable Dataset."""
    assert isinstance(plugin, Dataset)


@pytest.mark.parametrize("plugin", ALL_PLUGINS, ids=lambda p: type(p).__name__)
def test_has_required_attrs(plugin):
    """Each plugin must declare name and flag."""
    assert isinstance(plugin.name, str) and plugin.name
    assert isinstance(plugin.flag, str) and plugin.flag


# ── Integration tests (one per table) ──────────────────────────────

SMALL_CONFIG = InsertConfig(rows=1000, partitions=1, batch_size=500)


class TestSyntheticData:
    @pytest.fixture(autouse=True)
    def setup(self, client: Client):
        self.plugin = SyntheticData(replicated=False)
        self.client = client
        yield
        client.execute("DROP TABLE IF EXISTS synthetic_data.events SYNC")
        client.execute("DROP DATABASE IF EXISTS synthetic_data SYNC")

    def test_create_and_insert(self):
        self.plugin.create(self.client)
        self.plugin.insert(self.client, SMALL_CONFIG)
        rows = self.client.execute("SELECT count() FROM synthetic_data.events")[0][0]
        assert rows == SMALL_CONFIG.rows

    def test_drop(self):
        self.plugin.create(self.client)
        self.plugin.drop(self.client)
        dbs = [r[0] for r in self.client.execute("SHOW DATABASES")]
        assert "synthetic_data" not in dbs


class TestNycTaxi:
    @pytest.fixture(autouse=True)
    def setup(self, client: Client):
        self.plugin = NycTaxi(replicated=False)
        self.client = client
        yield
        client.execute("DROP TABLE IF EXISTS nyc_taxi.trips SYNC")
        client.execute("DROP DATABASE IF EXISTS nyc_taxi SYNC")

    def test_create_and_insert(self):
        self.plugin.create(self.client)
        self.plugin.insert(self.client, SMALL_CONFIG)
        rows = self.client.execute("SELECT count() FROM nyc_taxi.trips")[0][0]
        assert rows == SMALL_CONFIG.rows


class TestUkHousePrices:
    @pytest.fixture(autouse=True)
    def setup(self, client: Client):
        self.plugin = UkHousePrices(replicated=False)
        self.client = client
        yield
        client.execute("DROP TABLE IF EXISTS uk_price_paid.uk_price_paid SYNC")
        client.execute("DROP DATABASE IF EXISTS uk_price_paid SYNC")

    def test_create_and_insert(self):
        self.plugin.create(self.client)
        self.plugin.insert(self.client, SMALL_CONFIG)
        rows = self.client.execute("SELECT count() FROM uk_price_paid.uk_price_paid")[0][0]
        assert rows == SMALL_CONFIG.rows


class TestWebAnalytics:
    @pytest.fixture(autouse=True)
    def setup(self, client: Client):
        self.plugin = WebAnalytics(caps=None)
        self.client = client
        yield
        client.execute("DROP TABLE IF EXISTS web_analytics.pageviews SYNC")
        client.execute("DROP DATABASE IF EXISTS web_analytics SYNC")

    def test_create_and_insert(self):
        self.plugin.create(self.client)
        self.plugin.insert(self.client, SMALL_CONFIG)
        rows = self.client.execute("SELECT count() FROM web_analytics.pageviews")[0][0]
        assert rows == SMALL_CONFIG.rows


# ── InsertConfig defaults ──────────────────────────────────────────


def test_insert_config_defaults():
    cfg = InsertConfig(rows=100, partitions=1, batch_size=50)
    assert cfg.drop is False
    assert cfg.throttle_min == 0.0
    assert cfg.throttle_max == 0.0


def test_insert_config_frozen():
    cfg = InsertConfig(rows=100, partitions=1, batch_size=50)
    with pytest.raises(AttributeError):
        cfg.rows = 200  # type: ignore[misc]


# ── QuerySet tests ─────────────────────────────────────────────────


@pytest.mark.parametrize("plugin", ALL_PLUGINS, ids=lambda p: type(p).__name__)
def test_queries_returns_queryset(plugin):
    """Each plugin's queries property must return a QuerySet."""
    qs = plugin.queries
    assert isinstance(qs, QuerySet)


@pytest.mark.parametrize("plugin", ALL_PLUGINS, ids=lambda p: type(p).__name__)
def test_queries_not_empty(plugin):
    """Each plugin should provide at least some queries."""
    qs = plugin.queries
    total = len(qs.slow) + len(qs.fast) + len(qs.pk_generators) + len(qs.join_generators) + len(qs.settings_generators)
    assert total > 0, f"{type(plugin).__name__} has no queries"


@pytest.mark.parametrize("plugin", ALL_PLUGINS, ids=lambda p: type(p).__name__)
def test_generators_return_sql(plugin):
    """All query generators must return non-empty SQL strings."""
    qs = plugin.queries
    for gen in qs.pk_generators + qs.join_generators + qs.settings_generators:
        sql = gen()
        assert isinstance(sql, str) and sql.strip()
