"""Integration tests: ON CLUSTER database + table DDL propagation.

Verifies that CREATE DATABASE ON CLUSTER + CREATE TABLE inside a
Replicated database propagates to all shards automatically.

Requires Docker. Uses a 2-shard × 1-replica cluster.

Reference: https://clickhouse.com/docs/engines/database-engines/replicated
  - ON CLUSTER for CREATE DATABASE propagates the database to all nodes
  - CREATE TABLE inside a Replicated database is forbidden with ON CLUSTER
    (Code 80) but propagates automatically via the shared DDL log in ZK
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from clickhouse_driver import Client

from cluster_fixture import ClusterContext, start_cluster, stop_cluster
from data_utils.tables.helpers import create_database, on_cluster_clause


@pytest.fixture(scope="module")
def cluster() -> Generator[ClusterContext]:
    ctx = start_cluster()
    yield ctx
    stop_cluster(ctx)


@pytest.fixture()
def client1(cluster: ClusterContext) -> Client:
    return cluster.client1


@pytest.fixture()
def client2(cluster: ClusterContext) -> Client:
    return cluster.client2


CLUSTER = "dev"


class TestOnClusterClause:
    def test_empty_cluster(self) -> None:
        assert on_cluster_clause("") == ""

    def test_valid_cluster(self) -> None:
        assert on_cluster_clause("dev") == "ON CLUSTER 'dev'"

    def test_unsafe_cluster_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsafe"):
            on_cluster_clause("dev; DROP TABLE x")


class TestCreateDatabaseOnCluster:
    """CREATE DATABASE ON CLUSTER should make the database exist on all shards."""

    def test_replicated_database_on_all_shards(
        self, client1: Client, client2: Client,
    ) -> None:
        db = "test_replicated_db"
        try:
            create_database(client1, db, replicated=True, cluster=CLUSTER)

            # Database should exist on shard 1 (where we ran the DDL)
            dbs1 = [r[0] for r in client1.execute("SHOW DATABASES")]
            assert db in dbs1

            # Database should also exist on shard 2 (propagated via ON CLUSTER)
            dbs2 = [r[0] for r in client2.execute("SHOW DATABASES")]
            assert db in dbs2
        finally:
            client1.execute(f"DROP DATABASE IF EXISTS {db} SYNC")

    def test_atomic_database_on_all_shards(
        self, client1: Client, client2: Client,
    ) -> None:
        db = "test_atomic_db"
        try:
            create_database(client1, db, replicated=False, cluster=CLUSTER)

            dbs1 = [r[0] for r in client1.execute("SHOW DATABASES")]
            assert db in dbs1

            dbs2 = [r[0] for r in client2.execute("SHOW DATABASES")]
            assert db in dbs2
        finally:
            client1.execute(f"DROP DATABASE IF EXISTS {db} ON CLUSTER '{CLUSTER}' SYNC")


class TestTableDDLPropagation:
    """Tables created inside a Replicated database should propagate to all shards."""

    @pytest.fixture(autouse=True)
    def setup_db(self, client1: Client, client2: Client) -> Generator[None]:
        self.db = "test_table_prop"
        create_database(client1, self.db, replicated=True, cluster=CLUSTER)
        yield
        client1.execute(f"DROP DATABASE IF EXISTS {self.db} SYNC")
        # Other shards may take a moment to process the drop
        try:
            client2.execute(f"DROP DATABASE IF EXISTS {self.db} SYNC")
        except Exception:
            pass

    def test_create_table_propagates_across_shards(
        self, client1: Client, client2: Client,
    ) -> None:
        """CREATE TABLE on shard 1 should appear on shard 2 via Replicated DDL log."""
        client1.execute(f"""
            CREATE TABLE {self.db}.test_table (id UInt64, name String)
            ENGINE = ReplicatedMergeTree()
            ORDER BY id
        """)

        # Table should exist on shard 1
        tables1 = [r[0] for r in client1.execute(
            f"SELECT name FROM system.tables WHERE database = '{self.db}'"
        )]
        assert "test_table" in tables1

        # Table should also exist on shard 2 (propagated by Replicated DB DDL log)
        _wait_for_table(client2, f"{self.db}.test_table")
        tables2 = [r[0] for r in client2.execute(
            f"SELECT name FROM system.tables WHERE database = '{self.db}'"
        )]
        assert "test_table" in tables2

    def test_on_cluster_rejected_for_table_in_replicated_db(
        self, client1: Client,
    ) -> None:
        """ON CLUSTER on CREATE TABLE inside a Replicated database must fail (Code 80)."""
        with pytest.raises(Exception, match="Code: 80"):
            client1.execute(f"""
                CREATE TABLE {self.db}.bad_table ON CLUSTER '{CLUSTER}'
                (id UInt64) ENGINE = ReplicatedMergeTree() ORDER BY id
            """)

    def test_multiple_tables_propagate(
        self, client1: Client, client2: Client,
    ) -> None:
        for name in ["events", "users", "sessions"]:
            client1.execute(f"""
                CREATE TABLE {self.db}.{name} (id UInt64)
                ENGINE = ReplicatedMergeTree() ORDER BY id
            """)

        _wait_for_table(client2, f"{self.db}.sessions")  # wait for last one

        tables2 = {r[0] for r in client2.execute(
            f"SELECT name FROM system.tables WHERE database = '{self.db}'"
        )}
        assert {"events", "users", "sessions"} <= tables2


class TestDatasetCreateOnCluster:
    """End-to-end: dataset create() with cluster= should work on all shards."""

    @pytest.fixture(autouse=True)
    def cleanup(self, client1: Client, client2: Client) -> Generator[None]:
        yield
        for db in ["synthetic_data", "nyc_taxi", "uk_price_paid"]:
            for c in [client1, client2]:
                try:
                    c.execute(f"DROP DATABASE IF EXISTS {db} SYNC")
                except Exception:
                    pass

    def test_synthetic_data(self, client1: Client, client2: Client) -> None:
        from data_utils.tables.synthetic_data import create_synthetic_data

        create_synthetic_data(client1, replicated=True, cluster=CLUSTER)

        _wait_for_table(client2, "synthetic_data.events")
        tables = {r[0] for r in client2.execute(
            "SELECT name FROM system.tables WHERE database = 'synthetic_data'"
        )}
        assert "events" in tables
        assert "user_tiers" in tables

    def test_nyc_taxi(self, client1: Client, client2: Client) -> None:
        from data_utils.tables.nyc_taxi import create_nyc_taxi

        create_nyc_taxi(client1, replicated=True, cluster=CLUSTER)

        _wait_for_table(client2, "nyc_taxi.trips")
        tables = {r[0] for r in client2.execute(
            "SELECT name FROM system.tables WHERE database = 'nyc_taxi'"
        )}
        assert "trips" in tables
        assert "locations" in tables

    def test_uk_house_prices(self, client1: Client, client2: Client) -> None:
        from data_utils.tables.uk_house_prices import create_uk_house_prices

        create_uk_house_prices(client1, replicated=True, cluster=CLUSTER)

        _wait_for_table(client2, "uk_price_paid.uk_price_paid")
        tables = {r[0] for r in client2.execute(
            "SELECT name FROM system.tables WHERE database = 'uk_price_paid'"
        )}
        assert "uk_price_paid" in tables


def _wait_for_table(client: Client, table: str, timeout: float = 30) -> None:
    """Wait for a table to appear on a node (Replicated DDL is async)."""
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            client.execute(f"SELECT 1 FROM {table} LIMIT 0")
            return
        except Exception:
            time.sleep(1)
    raise TimeoutError(f"Table {table} did not appear within {timeout}s")
