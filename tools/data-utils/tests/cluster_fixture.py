"""Multi-node ClickHouse cluster fixture for integration tests.

Starts a 2-shard × 1-replica cluster + ClickHouse Keeper using
testcontainers, matching the Altinity operator topology.
"""

from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass
from clickhouse_driver import Client
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

CH_IMAGE = "clickhouse/clickhouse-server:26.1-alpine"

# Override default-password.xml to allow passwordless login.
# Recent CH images auto-generate a random password; this disables that.
USERS_CONFIG = """\
<?xml version="1.0"?>
<clickhouse>
  <users>
    <default>
      <password></password>
      <access_management>1</access_management>
    </default>
  </users>
</clickhouse>"""

KEEPER_CONFIG = """\
<?xml version="1.0"?>
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
  <keeper_server>
    <tcp_port>9181</tcp_port>
    <server_id>1</server_id>
    <raft_configuration>
      <server><id>1</id><hostname>keeper</hostname><port>9234</port></server>
    </raft_configuration>
  </keeper_server>
</clickhouse>"""


def _ch_config(hostname: str, shard: str, replica: str) -> str:
    return f"""\
<?xml version="1.0"?>
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
  <interserver_http_host>{hostname}</interserver_http_host>
  <remote_servers>
    <dev>
      <secret>cluster_secret</secret>
      <shard>
        <internal_replication>true</internal_replication>
        <replica><host>ch1</host><port>9000</port></replica>
      </shard>
      <shard>
        <internal_replication>true</internal_replication>
        <replica><host>ch2</host><port>9000</port></replica>
      </shard>
    </dev>
  </remote_servers>
  <zookeeper>
    <node><host>keeper</host><port>9181</port></node>
  </zookeeper>
  <macros>
    <shard>{shard}</shard>
    <replica>{replica}</replica>
    <cluster>dev</cluster>
  </macros>
</clickhouse>"""


@dataclass
class ClusterContext:
    """Running 2-shard cluster with clients for each shard."""
    network: Network
    keeper: DockerContainer
    ch1: DockerContainer  # shard 01
    ch2: DockerContainer  # shard 02
    client1: Client       # connected to ch1
    client2: Client       # connected to ch2
    cluster_name: str = "dev"
    shard_count: int = 2


def _start_container(
    image: str, network: Network, alias: str, config: str,
    *,
    config_filename: str = "cluster.xml",
    exposed_ports: tuple[int, ...] = (8123, 9000),
) -> DockerContainer:
    """Start a ClickHouse container with XML config injected via a bind mount."""
    # Write config to a temp file and bind-mount it into the container.
    # This avoids fragile shell escaping with echo.
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".xml", prefix=f"ch_{alias}_", delete=False,
    )
    tmp.write(config)
    tmp.flush()
    tmp.close()

    config_path = f"/etc/clickhouse-server/config.d/{config_filename}"

    # Users config: allow passwordless default user
    users_tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".xml", prefix=f"ch_{alias}_users_", delete=False,
    )
    users_tmp.write(USERS_CONFIG)
    users_tmp.flush()
    users_tmp.close()

    c = DockerContainer(image)
    c.with_network(network)
    c.with_network_aliases(alias)
    c.with_exposed_ports(*exposed_ports)
    c.with_volume_mapping(tmp.name, config_path, "ro")
    c.with_volume_mapping(users_tmp.name, "/etc/clickhouse-server/users.d/default-password.xml", "ro")
    c.start()
    return c


def start_cluster() -> ClusterContext:
    """Start a 2-shard × 1-replica ClickHouse cluster with Keeper."""
    network = Network()
    network.create()

    keeper = _start_container(
        CH_IMAGE, network, "keeper", KEEPER_CONFIG,
        config_filename="keeper.xml",
        exposed_ports=(8123, 9181),
    )

    ch1 = _start_container(CH_IMAGE, network, "ch1", _ch_config("ch1", "01", "ch1"))
    ch2 = _start_container(CH_IMAGE, network, "ch2", _ch_config("ch2", "02", "ch2"))

    client1 = Client(host=ch1.get_container_host_ip(), port=int(ch1.get_exposed_port(9000)))
    client2 = Client(host=ch2.get_container_host_ip(), port=int(ch2.get_exposed_port(9000)))

    _wait_for_cluster(client1, expected_nodes=2, timeout=60)

    return ClusterContext(
        network=network, keeper=keeper,
        ch1=ch1, ch2=ch2,
        client1=client1, client2=client2,
    )


def stop_cluster(ctx: ClusterContext) -> None:
    ctx.client1.disconnect()
    ctx.client2.disconnect()
    ctx.ch1.stop()
    ctx.ch2.stop()
    ctx.keeper.stop()
    ctx.network.remove()


def _wait_for_cluster(client: Client, expected_nodes: int, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            rows = client.execute("SELECT count() FROM system.clusters WHERE cluster = 'dev'")
            if rows and rows[0][0] >= expected_nodes:
                return
        except Exception as exc:
            last_err = exc
        time.sleep(2)
    detail = f" (last error: {last_err})" if last_err else ""
    raise TimeoutError(f"Cluster did not reach {expected_nodes} nodes within {timeout}s{detail}")
