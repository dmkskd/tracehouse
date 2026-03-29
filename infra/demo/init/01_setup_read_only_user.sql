-- Read-only user setup for the demo environment.
--
-- Single source of truth — used by both the demo docker-compose and
-- integration tests (readonly-sandbox.integration.test.ts).
--
-- This file is loaded via docker-entrypoint-initdb.d (demo) or executed
-- directly by the test harness. The password placeholder CHANGEME_RO_PASSWORD
-- is replaced at load time with the real password.

-- ============================================================
-- Settings profile
-- ============================================================
CREATE SETTINGS PROFILE IF NOT EXISTS readonly_profile
  SETTINGS
    readonly = 2,
    allow_introspection_functions = 1,
    log_query_threads = 1,
    log_profile_events = 1,
    log_processors_profiles = 1;

-- ============================================================
-- User
-- ============================================================
CREATE USER IF NOT EXISTS read_only
  IDENTIFIED BY 'CHANGEME_RO_PASSWORD'
  SETTINGS PROFILE readonly_profile;

-- ============================================================
-- Grants — user databases
-- ============================================================
GRANT SELECT ON default.* TO read_only;
GRANT SELECT ON tracehouse.* TO read_only;
-- Databases created by the workload generator
GRANT SELECT ON synthetic_data.* TO read_only;
GRANT SELECT ON nyc_taxi.* TO read_only;
GRANT SELECT ON uk_price_paid.* TO read_only;
GRANT SELECT ON web_analytics.* TO read_only;
GRANT SELECT ON replacing_test.* TO read_only;

-- ============================================================
-- Grants — system tables the app needs
-- ============================================================
GRANT SELECT ON system.asynchronous_metric_log TO read_only;
GRANT SELECT ON system.asynchronous_metrics TO read_only;
GRANT SELECT ON system.build_options TO read_only;
GRANT SELECT ON system.clusters TO read_only;
GRANT SELECT ON system.columns TO read_only;
GRANT SELECT ON system.databases TO read_only;
GRANT SELECT ON system.dictionaries TO read_only;
GRANT SELECT ON system.disks TO read_only;
GRANT SELECT ON system.events TO read_only;
GRANT SELECT ON system.merges TO read_only;
GRANT SELECT ON system.metric_log TO read_only;
GRANT SELECT ON system.metrics TO read_only;
GRANT SELECT ON system.mutations TO read_only;
GRANT SELECT ON system.one TO read_only;
GRANT SELECT ON system.opentelemetry_span_log TO read_only;
GRANT SELECT ON system.part_log TO read_only;
GRANT SELECT ON system.parts TO read_only;
GRANT SELECT ON system.parts_columns TO read_only;
GRANT SELECT ON system.processes TO read_only;
GRANT SELECT ON system.processors_profile_log TO read_only;
GRANT SELECT ON system.query_log TO read_only;
GRANT SELECT ON system.query_thread_log TO read_only;
GRANT SELECT ON system.replicas TO read_only;
GRANT SELECT ON system.replication_queue TO read_only;
GRANT SELECT ON system.server_settings TO read_only;
GRANT SELECT ON system.settings TO read_only;
GRANT SELECT ON system.storage_policies TO read_only;
GRANT SELECT ON system.tables TO read_only;
GRANT SELECT ON system.text_log TO read_only;
GRANT SELECT ON system.trace_log TO read_only;
GRANT SELECT ON system.view_refreshes TO read_only;
GRANT SELECT ON system.zookeeper TO read_only;
GRANT SELECT ON system.functions TO read_only;
GRANT SELECT ON system.zookeeper_connection TO read_only;
GRANT SELECT ON system.zookeeper_log TO read_only;
GRANT SELECT ON system.query_views_log TO read_only;
GRANT SELECT ON system.crash_log TO read_only;
GRANT SELECT ON system.session_log TO read_only;

-- ============================================================
-- Grants — schema browsing
-- ============================================================
GRANT SHOW DATABASES ON *.* TO read_only;
GRANT SHOW TABLES ON *.* TO read_only;
GRANT SHOW COLUMNS ON *.* TO read_only;

-- ============================================================
-- Grants — introspection (flamegraphs)
-- ============================================================
GRANT dictGet ON *.* TO read_only;
GRANT INTROSPECTION ON *.* TO read_only;

-- ============================================================
-- Grants — remote ZooKeeper/Keeper reads (clustered setup)
-- ============================================================
GRANT REMOTE ON *.* TO read_only;

-- ============================================================
-- NOTE on KILL QUERY: readonly=2 bypasses RBAC and allows
-- KILL QUERY regardless of grants/revokes. This is a ClickHouse
-- limitation (legacy behaviour predating the RBAC system).
-- REVOKE KILL QUERY has no effect under readonly=2.
-- Mitigation: the demo is single-tenant, so this is acceptable.
-- ============================================================
