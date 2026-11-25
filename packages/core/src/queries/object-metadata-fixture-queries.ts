/** Disposable integration fixture and extraction probes, not persistent demo setup.
 * Probes prove server metadata availability; they are not production collectors. */
import { accessFixtureQueries } from './access-fixture-queries.js';

export function objectMetadataFixtureQueries(prefix: string) {
  const access = accessFixtureQueries(prefix);
  // Shaped like the demo read_only account: readonly = 2, SELECT on what the object
  // browser reads, and no CREATE TEMPORARY TABLE.
  const catalog = `${prefix}_catalog`;
  // wild: wildcard and column scopes, to prove how system.grants records them.
  const wild = `${prefix}_wild`;
  const bob = `${prefix}_bob`, admin = `${prefix}_admin`, profile = `${prefix}_profile`, baseProfile = `${prefix}_base_profile`, quota = `${prefix}_quota`;
  const db = access.db;
  return {
    ...access, bob, admin, wild, catalog, profile, baseProfile, quota,
    setup: [...access.setup,
      `CREATE USER ${bob}`,
      `CREATE USER ${catalog} SETTINGS readonly = 2`,
      `GRANT SELECT ON ${db}.* TO ${catalog}`,
      ...['one', 'columns', 'tables', 'users', 'roles', 'role_grants', 'grants', 'row_policies', 'privileges', 'storage_policies', 'disks']
        .map(table => `GRANT SELECT ON system.${table} TO ${catalog}`),
      `GRANT ${access.parent} TO ${bob}`,
      // system.grants keeps ALL ON *.* as one ALL row; ALL ON db.* is split into one row per privilege.
      `CREATE USER ${admin}`,
      `GRANT ALL ON *.* TO ${admin}`,
      `SET DEFAULT ROLE NONE TO ${bob}`,
      `CREATE TABLE ${db}.events (id UInt64, tenant_id UInt64, ts DateTime DEFAULT now(), amount UInt64,
        note String DEFAULT 'sample' CODEC(ZSTD(1))) ENGINE = MergeTree
        ORDER BY (tenant_id, ts, id) TTL ts + INTERVAL 30 DAY DELETE SETTINGS index_granularity = 4096`,
      `CREATE TABLE ${db}.minute (tenant_id UInt64, minute DateTime, amount UInt64, events UInt64)
        ENGINE = SummingMergeTree ORDER BY (tenant_id, minute)`,
      `CREATE TABLE ${db}.hourly (tenant_id UInt64, hour DateTime, amount UInt64, events UInt64)
        ENGINE = SummingMergeTree ORDER BY (tenant_id, hour)`,
      `CREATE MATERIALIZED VIEW ${db}.minute_to_hourly TO ${db}.hourly AS
        SELECT tenant_id, toStartOfHour(minute) AS hour, sum(amount) AS amount, sum(events) AS events
        FROM ${db}.minute GROUP BY tenant_id, hour`,
      `CREATE MATERIALIZED VIEW ${db}.events_to_minute TO ${db}.minute AS
        SELECT tenant_id, toStartOfMinute(ts) AS minute, sum(amount) AS amount, count() AS events
        FROM ${db}.events GROUP BY tenant_id, minute`,
      `CREATE VIEW ${db}.public_events AS SELECT id, tenant_id FROM ${db}.events`,
      // References the native fields do not expose: a view with a CTE and a join, a
      // refreshable MV reading a system table, and engine-argument destinations.
      `CREATE VIEW ${db}.tenant_report AS WITH recent AS (SELECT tenant_id, amount FROM ${db}.events)
        SELECT r.tenant_id, sum(r.amount) AS amount, any(m.events) AS events
        FROM recent AS r LEFT JOIN ${db}.minute AS m USING tenant_id GROUP BY r.tenant_id`,
      // Analysis would make the server act: url() without columns is asked for them over
      // HTTP (the server's own /ping), and a scalar subquery runs. An IN subquery does not run.
      `CREATE VIEW ${db}.ping_feed AS SELECT * FROM url('http://127.0.0.1:8123/ping', TSV)`,
      `CREATE VIEW ${db}.top_amount AS SELECT (SELECT max(amount) FROM ${db}.events) AS top`,
      `CREATE VIEW ${db}.active_events AS SELECT id FROM ${db}.events WHERE tenant_id IN (SELECT tenant_id FROM ${db}.minute)`,
      `CREATE TABLE ${db}.samples (dummy UInt8) ENGINE = MergeTree ORDER BY tuple()`,
      `CREATE MATERIALIZED VIEW ${db}.one_sampler REFRESH EVERY 1 HOUR APPEND TO ${db}.samples AS SELECT dummy FROM system.one`,
      `CREATE TABLE ${db}.events_buffer AS ${db}.events ENGINE = Buffer('${db}', 'events', 1, 1, 10, 1, 100, 1, 1000)`,
      `CREATE TABLE ${db}.events_all AS ${db}.events ENGINE = Distributed('default', '${db}', 'events')
        SETTINGS fsync_after_insert = 0`,
      // Storage locations from definitions only. IcebergLocal tables are created by the
      // server on its own disk; S3 tables point at a host that is never contacted.
      `CREATE TABLE ${db}.lake_trips (id UInt64) ENGINE = IcebergLocal('/var/lib/clickhouse/user_files/${db}/wh/nyc/trips/')`,
      `CREATE TABLE ${db}.lake_locations (id UInt64) ENGINE = IcebergLocal('/var/lib/clickhouse/user_files/${db}/wh/nyc/locations/')`,
      `CREATE TABLE ${db}.raw_events (id UInt64) ENGINE = S3('http://lake.invalid:9000/fixture-bucket/raw/events.parquet', NOSIGN, 'Parquet')`,
      `CREATE TABLE ${db}.raw_events_copy (id UInt64) ENGINE = S3('http://lake.invalid:9000/fixture-bucket/raw/events.parquet', NOSIGN, 'Parquet')`,
      // Needs the storage policy th_fixture_tiered (a local volume, then a cache disk over an S3
      // disk at a host that is never contacted), which the suite mounts as server config.
      `CREATE TABLE ${db}.tiered (ts DateTime, id UInt64) ENGINE = MergeTree ORDER BY id
        TTL ts + INTERVAL 7 DAY TO VOLUME 'cold', ts + INTERVAL 90 DAY DELETE SETTINGS storage_policy = 'th_fixture_tiered'`,
      `CREATE TABLE ${db}.excluded (id UInt64) ENGINE = MergeTree ORDER BY id`,
      `REVOKE SELECT ON ${db}.excluded FROM ${access.reader}`,
      `GRANT SELECT(id) ON ${db}.events TO ${bob} WITH GRANT OPTION`,
      `CREATE USER ${wild}`,
      `GRANT SELECT ON ${db}.* TO ${wild}`,
      `REVOKE SELECT ON ${db}.ho* FROM ${wild}`,
      `GRANT SELECT ON ${db}.hourly TO ${wild}`,
      `REVOKE SELECT ON ${db}.mi* FROM ${wild}`,
      `REVOKE SELECT(note) ON ${db}.events FROM ${wild}`,
      // A database prefix: every fixture database starts with the prefix's first words.
      `GRANT INSERT ON th_object_model*.* TO ${wild}`,
      `CREATE SETTINGS PROFILE ${baseProfile} SETTINGS max_threads = 2`,
      `CREATE SETTINGS PROFILE ${profile} SETTINGS INHERIT ${baseProfile}, max_memory_usage = 268435456 MIN 1048576 MAX 536870912 TO ${access.reader}`,
      `CREATE QUOTA ${quota} KEYED BY user_name FOR INTERVAL 1 HOUR MAX queries = 100 TO ${access.reader}`,
    ],
    teardown: [
      `DROP QUOTA IF EXISTS ${quota}`, `DROP SETTINGS PROFILE IF EXISTS ${profile}`,
      `DROP SETTINGS PROFILE IF EXISTS ${baseProfile}`, `DROP USER IF EXISTS ${bob}`, `DROP USER IF EXISTS ${wild}`, `DROP USER IF EXISTS ${admin}`, `DROP USER IF EXISTS ${catalog}`, ...access.teardown,
    ],
    probes: {
      schema: `SELECT hostName() AS host, table, name, type FROM {{cluster_aware:system.columns}}
        WHERE database = 'system' AND table IN ('tables','columns','databases','users','roles','grants','role_grants',
        'row_policies','settings_profiles','settings_profile_elements','quotas','quota_limits','storage_policies','disks')`,
      tables: `SELECT hostName() AS host, * FROM {{cluster_aware:system.tables}} WHERE database = '${db}' ORDER BY name`,
      grants: `SELECT hostName() AS host, * FROM {{cluster_aware:system.grants}} WHERE role_name = '${access.reader}' OR user_name = '${bob}'`,
      columns: `SELECT hostName() AS host, * FROM {{cluster_aware:system.columns}} WHERE database = '${db}' ORDER BY table, position`,
      databases: `SELECT hostName() AS host, name, engine, uuid FROM {{cluster_aware:system.databases}} WHERE name = '${db}'`,
      profiles: `SELECT hostName() AS host, * FROM {{cluster_aware:system.settings_profiles}} WHERE name IN ('${profile}','${baseProfile}')`,
      elements: `SELECT hostName() AS host, * FROM {{cluster_aware:system.settings_profile_elements}} WHERE profile_name IN ('${profile}','${baseProfile}')`,
      quotas: `SELECT hostName() AS host, * FROM {{cluster_aware:system.quotas}} WHERE name = '${quota}'`,
      limits: `SELECT hostName() AS host, * FROM {{cluster_aware:system.quota_limits}} WHERE quota_name = '${quota}'`,
      policies: `SELECT hostName() AS host, * FROM {{cluster_aware:system.storage_policies}}`,
      disks: `SELECT hostName() AS host, * FROM {{cluster_aware:system.disks}}`,
    },
    seed: `INSERT INTO ${db}.events (id,tenant_id,ts,amount) SELECT number,42,toStartOfHour(now()),number+1 FROM numbers(3)`,
    totals: ['events','minute','hourly'].map(table => `SELECT sum(amount) AS amount, ${table === 'events' ? 'count()' : 'sum(events)'} AS events FROM ${db}.${table}`),
    alter: `ALTER TABLE ${db}.events MODIFY TTL ts + INTERVAL 60 DAY DELETE`,
  };
}
