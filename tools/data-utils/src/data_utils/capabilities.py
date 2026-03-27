"""ClickHouse capability probing.

Connects to a ClickHouse instance and detects which features are available.
Used by the CLI tools to gracefully skip features that aren't supported
(e.g. S3 on Aiven, Distributed on single-node).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from clickhouse_driver import Client

log = logging.getLogger(__name__)


@dataclass
class Capabilities:
    """What this ClickHouse instance supports."""
    version: str = ""
    # Storage
    has_s3_storage_policy: bool = False      # s3tiered storage policy exists
    has_s3_function: bool = False            # s3() table function works
    # Cluster
    has_cluster: bool = False                # multi-node cluster detected
    cluster_name: str = ""
    shard_count: int = 0
    replica_count: int = 0
    has_keeper: bool = False                 # ZooKeeper/Keeper is available
    # Settings
    restricted_settings: list[str] = field(default_factory=list)  # settings that can't be changed
    # Databases that already exist
    existing_databases: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"  ClickHouse version:    {self.version}",
            f"  S3 storage policy:     {'✓' if self.has_s3_storage_policy else '✗ (nyc_taxi will use plain MergeTree)'}",
            f"  s3() table function:   {'✓' if self.has_s3_function else '✗ (S3 parquet queries will be skipped)'}",
            f"  Cluster:               {'✓ ' + self.cluster_name + f' ({self.shard_count}s/{self.replica_count}r)' if self.has_cluster else '✗ (web_analytics sharded table will be skipped)'}",
            f"  Keeper/ZooKeeper:      {'✓' if self.has_keeper else '✗ (Replicated engines unavailable)'}",
        ]
        if self.restricted_settings:
            lines.append(f"  Restricted settings:   {', '.join(self.restricted_settings)}")
        return "\n".join(lines)


def probe(client: Client) -> Capabilities:
    """Probe a ClickHouse instance for available capabilities."""
    caps = Capabilities()

    log.info("probing version")
    try:
        rows = client.execute("SELECT version()")
        caps.version = rows[0][0] if rows else "unknown"
    except Exception:
        caps.version = "unknown"

    log.info("probing S3 storage policy")
    try:
        rows = client.execute(
            "SELECT policy_name FROM system.storage_policies WHERE policy_name = 's3tiered'"
        )
        caps.has_s3_storage_policy = len(rows) > 0
    except Exception:
        caps.has_s3_storage_policy = False

    log.info("probing s3() table function (5s timeout)")
    try:
        client.execute(
            "SELECT 1 FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/"
            "amazon_reviews/amazon_reviews_2015.snappy.parquet') LIMIT 0",
            settings={"max_execution_time": 5},
        )
        caps.has_s3_function = True
    except Exception:
        caps.has_s3_function = False

    log.info("probing cluster topology")
    override = os.environ.get("CH_CLUSTER", "").strip()
    try:
        if override:
            rows = client.execute(
                "SELECT cluster, count() AS replicas, uniq(shard_num) AS shards "
                "FROM system.clusters "
                "WHERE cluster = %(name)s "
                "GROUP BY cluster",
                {"name": override},
            )
        else:
            rows = client.execute(
                "SELECT cluster, count() AS replicas, uniq(shard_num) AS shards "
                "FROM system.clusters "
                "WHERE cluster NOT IN ('test_shard_localhost', 'test_cluster_one_shard_three_replicas_localhost', "
                "  'test_cluster_two_shards_localhost', 'test_cluster_two_shards', "
                "  'test_unavailable_shard', 'test_shard_localhost_secure') "
                "GROUP BY cluster "
                "ORDER BY replicas DESC "
                "LIMIT 1"
            )
        if rows and rows[0][1] > 1:
            caps.has_cluster = True
            caps.cluster_name = rows[0][0]
            caps.replica_count = rows[0][1]
            caps.shard_count = rows[0][2]
    except Exception:
        pass

    log.info("probing Keeper/ZooKeeper")
    try:
        rows = client.execute("SELECT count() FROM system.zookeeper WHERE path = '/'")
        caps.has_keeper = rows[0][0] > 0 if rows else False
    except Exception:
        caps.has_keeper = False

    log.info("probing restricted settings")
    test_settings = [
        ('memory_profiler_sample_probability', 1),
        ('log_query_threads', 1),
        ('opentelemetry_start_trace_probability', 0.01),
    ]
    for setting_name, setting_value in test_settings:
        try:
            client.execute(
                f"SELECT 1 SETTINGS {setting_name} = {setting_value}"
            )
        except Exception as e:
            if 'Code: 452' in str(e) or 'should not be changed' in str(e):
                caps.restricted_settings.append(setting_name)

    log.info("probing existing databases")
    try:
        rows = client.execute(
            "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA', 'default')"
        )
        caps.existing_databases = [r[0] for r in rows]
    except Exception:
        pass

    return caps
