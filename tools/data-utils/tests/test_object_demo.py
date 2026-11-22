"""Integration coverage for the persistent demo's two insert-trigger stages."""
import pytest
from clickhouse_driver import Client
from testcontainers.clickhouse import ClickHouseContainer

from clickhouse_image import CLICKHOUSE_IMAGE
from data_utils.object_demo import ACTIVITY_COMMENT, append_object_demo, drop_object_demo, run_object_demo_activity, setup_object_demo, verify_object_demo


@pytest.fixture
def object_demo_container():
    # Access-management privileges are needed only by this scenario. Keep the
    # existing shared dataset container's user and grants unchanged.
    with ClickHouseContainer(CLICKHOUSE_IMAGE).with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1") as container:
        yield container


@pytest.fixture
def object_demo_client(object_demo_container):
    connection = Client.from_url(object_demo_container.get_connection_url())
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


def test_activity_runs_as_each_demo_user_and_locks_them_again(object_demo_container, object_demo_client):
    client = object_demo_client
    host, port = object_demo_container.get_container_host_ip(), int(object_demo_container.get_exposed_port(9000))
    try:
        setup_object_demo(client)
        results = run_object_demo_activity(client, lambda user, password: Client(host=host, port=port, user=user, password=password))
        assert [(r.user, r.actual) for r in results if not r.matched] == []
        # The activity reads only: the cascade totals are unchanged.
        assert sum(row[2] for row in verify_object_demo(client)) == 6
        client.execute("SYSTEM FLUSH LOGS")
        by_user = dict(client.execute(
            "SELECT user, groupUniqArray(exception_code) FROM system.query_log WHERE log_comment = %(comment)s"
            " AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing') GROUP BY user", {"comment": ACTIVITY_COMMENT}))
        assert sorted(by_user) == ["th_ob_alice", "th_ob_bob", "th_ob_carol", "th_ob_denied"]
        assert sorted(by_user["th_ob_bob"]) == [0, 497]
        assert by_user["th_ob_denied"] == [497]
        assert by_user["th_ob_carol"] == [497]
        # Locked again: no host may connect.
        assert client.execute("SELECT DISTINCT host_ip, host_names, host_names_regexp, host_names_like FROM system.users WHERE name LIKE 'th_ob_%'") == [([], [], [], [])]
    finally:
        drop_object_demo(client)
