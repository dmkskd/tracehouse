"""Integration coverage for the persistent demo's two insert-trigger stages."""
import pytest
from clickhouse_driver import Client
from testcontainers.clickhouse import ClickHouseContainer

from clickhouse_image import CLICKHOUSE_IMAGE
from data_utils.object_demo import append_object_demo, drop_object_demo, setup_object_demo, verify_object_demo


@pytest.fixture
def object_demo_client():
    # Access-management privileges are needed only by this scenario. Keep the
    # existing shared dataset container's user and grants unchanged.
    with ClickHouseContainer(CLICKHOUSE_IMAGE).with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1") as container:
        connection = Client.from_url(container.get_connection_url())
        try:
            yield connection
        finally:
            connection.disconnect()


def test_demo_cascade_and_repeatable_setup(object_demo_client):
    client = object_demo_client
    try:
        setup_object_demo(client)
        initial = verify_object_demo(client)
        assert sum(row[2] for row in initial) == 6
        setup_object_demo(client)
        assert verify_object_demo(client) == initial
        append_object_demo(client)
        appended = verify_object_demo(client)
        assert sum(row[2] for row in appended) == 106
        assert sum(row[1] for row in appended) == 760
        assert client.execute("SELECT count() FROM system.tables WHERE database = 'tracehouse_object_demo' AND engine = 'MaterializedView'") == [(2,)]
        # Source policies do not establish or propagate access to derived targets.
        assert client.execute("SELECT count() FROM system.row_policies WHERE database = 'tracehouse_object_demo'") == [(2,)]
        assert client.execute("SELECT count() FROM system.role_grants WHERE user_name = 'th_ob_bob' AND granted_role_is_default = 0") == [(1,)]
        assert client.execute("SELECT count() FROM system.grants WHERE role_name = 'th_ob_analyst' AND is_partial_revoke = 1") == [(1,)]
    finally:
        drop_object_demo(client)
    assert client.execute("SELECT count() FROM system.databases WHERE name = 'tracehouse_object_demo'") == [(0,)]
    assert client.execute("SELECT count() FROM system.users WHERE name IN ('th_ob_alice','th_ob_bob','th_ob_carol','th_ob_denied')") == [(0,)]
