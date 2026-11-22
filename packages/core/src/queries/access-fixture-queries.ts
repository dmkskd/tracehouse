/** Isolated integration fixture. SQL ownership remains in core, including test setup. */
export function accessFixtureQueries(prefix: string) {
  if (!/^th_object_model_[a-z0-9_]+$/.test(prefix)) throw new Error('Invalid object-model fixture prefix');
  const db = prefix, alice = `${prefix}_alice`, denied = `${prefix}_denied`, observer = `${prefix}_observer`;
  const reader = `${prefix}_reader`, parent = `${prefix}_parent`;
  return {
    db, alice, denied, observer, reader, parent,
    setup: [
      `CREATE DATABASE ${db}`,
      `CREATE TABLE ${db}.trips (id UInt64, tenant_id UInt64, total_amount Float64) ENGINE = MergeTree ORDER BY id`,
      `INSERT INTO ${db}.trips VALUES (1,42,10),(2,43,20)`,
      `CREATE USER ${alice}`, `CREATE USER ${denied}`, `CREATE USER ${observer}`,
      `CREATE ROLE ${reader}`, `CREATE ROLE ${parent}`,
      `GRANT SELECT ON ${db}.* TO ${reader}`, `GRANT ${reader} TO ${parent}`, `GRANT ${parent} TO ${alice}`,
      `SET DEFAULT ROLE ${parent} TO ${alice}`,
      `CREATE ROW POLICY tenant_filter ON ${db}.trips USING tenant_id = 42 TO ${reader}`,
      `GRANT SHOW USERS ON *.* TO ${observer}`,
    ],
    read: `SELECT id FROM ${db}.trips ORDER BY id`,
    removeReaderGrant: `REVOKE SELECT ON ${db}.* FROM ${reader}`,
    removeAliceAssignment: `REVOKE ${parent} FROM ${alice}`,
    teardown: [
      `DROP USER IF EXISTS ${alice}`, `DROP USER IF EXISTS ${denied}`, `DROP USER IF EXISTS ${observer}`,
      `DROP ROLE IF EXISTS ${parent}`, `DROP ROLE IF EXISTS ${reader}`, `DROP DATABASE IF EXISTS ${db}`,
    ],
  };
}
