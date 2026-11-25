import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { ClickHouseContainer, type StartedClickHouseContainer } from '@testcontainers/clickhouse';
import { createClient, type ClickHouseClient } from '@clickhouse/client';
import { CH_IMAGE, configuredClickHouseIsBefore } from './setup/constants.js';
import { startWithPortRetry } from './setup/start-container.js';
import { TestAdapter } from './setup/clickhouse-container.js';
import { HttpAdapter } from '../../adapters/http-adapter.js';
import { ClusterAwareAdapter } from '../../adapters/cluster-adapter.js';
import { objectMetadataFixtureQueries } from '../../queries/object-metadata-fixture-queries.js';
import { tagQuery } from '../../queries/builder.js';
import { collectObjects } from '../../services/object-browser/collectors/collect.js';
import { coveredOperations, projectCatalogGraph, provenancePaths } from '../../services/object-browser/projections/graph.js';
import { objectKey, type Capture } from '../../services/object-browser/model/types.js';
import { describeDetail, loadDetailStatement, loadTableActivity, loadTablePlacement } from '../../services/object-browser/presentation/details.js';
import { answerFor } from '../../services/object-browser/presentation/links.js';

/** Server extraction proof is deliberately separate from production-model coverage.
 * Run through the normal integration runner with an exact CLICKHOUSE_IMAGE. */
describe('Object browser metadata extraction', { tags: ['security', 'storage'] }, () => {
  const fixture = objectMetadataFixtureQueries(`th_object_model_proof_${Date.now().toString(36)}`);
  let container: StartedClickHouseContainer;
  let client: ClickHouseClient;
  let adapter: ClusterAwareAdapter;
  let capture: Capture;
  let schema: Record<string, unknown>[];
  const http: HttpAdapter[] = [];
  const input = { connection: 'metadata-proof', principal: fixture.alice, database: fixture.db, table: 'trips', operation: 'SELECT' as const };
  const q = (sql: string) => tagQuery(sql, 'TraceHouse:Objects:metadata-proof');
  const read = (sql: string) => adapter.executeQuery(q(sql));
  const hasField = (table: string, name: string) => schema.some(row => row.table === table && row.name === name);
  // A cache disk accepts skip_access_check on 26.9.7 and rejects it on 26.3.62.3, where its startup
  // check would contact the endpoint; older servers get the S3 disk without the cache.
  const cacheDisk = !configuredClickHouseIsBefore(26, 9);
  const coldDisk = cacheDisk ? 'th_s3_cache' : 'th_s3';
  const grantsReadable = () => capture.coverage.find(source => source.source === 'grants')!.state === 'readable';
  function asUser(user: string) {
    const transport = new HttpAdapter({ host: container.getHost(), port: container.getHttpPort(), user,
      password: '', database: 'default', secure: false, connect_timeout: 5, send_receive_timeout: 15 });
    http.push(transport);return new ClusterAwareAdapter(transport);
  }
  beforeAll(async () => {
    // This suite's disposable server needs access-management DDL. Do not broaden
    // the shared dataset-test user or any external deployment's privileges.
    container = await startWithPortRetry(() => new ClickHouseContainer(CH_IMAGE)
      .withEnvironment({ CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: '1' })
      // Granting ALL ON *.* needs the named-collection privileges, which users.xml enables per user.
      .withCopyContentToContainer([{ target: '/etc/clickhouse-server/users.d/named-collections.xml',
        content: '<clickhouse><users><test><named_collection_control>1</named_collection_control><show_named_collections_secrets>1</show_named_collections_secrets></test></users></clickhouse>' },
      // Shaped like the demo's s3_storage.xml. The endpoint host does not resolve and is never contacted; the disks skip the startup access check.
      { target: '/etc/clickhouse-server/config.d/th-storage.xml', content: `<clickhouse><storage_configuration>
        <disks>
          <th_s3><type>s3</type><endpoint>http://lake.invalid:9000/fixture-bucket/data/</endpoint>
            <access_key_id>fixture</access_key_id><secret_access_key>fixture_secret</secret_access_key><skip_access_check>true</skip_access_check></th_s3>
          ${cacheDisk ? '<th_s3_cache><type>cache</type><disk>th_s3</disk><path>th_s3_cache</path><max_size>10Mi</max_size><skip_access_check>true</skip_access_check></th_s3_cache>' : ''}
        </disks>
        <policies><th_fixture_tiered><volumes>
          <hot><disk>default</disk></hot><cold><disk>${coldDisk}</disk></cold>
        </volumes></th_fixture_tiered></policies>
      </storage_configuration></clickhouse>` }]));
    client = createClient(container.getClientOptions());
    adapter = new ClusterAwareAdapter(new TestAdapter(client));
    // Lets the fixture create IcebergLocal tables; other statements ignore it.
    for (const sql of fixture.setup) await client.command({ query: sql, clickhouse_settings: { allow_experimental_insert_into_iceberg: 1 } });
    capture = await collectObjects(adapter, input, { mode: 'catalog' });
    schema = await read(fixture.probes.schema);
    console.info('Object metadata proof:', JSON.stringify({ image: CH_IMAGE, hosts: capture.hosts, sources: capture.coverage,
      nativeMvTarget: hasField('tables','target_table'), wildcardFlag: hasField('grants','is_wildcard') }));
  }, 120_000);
  afterAll(async () => {
    try {
      if (client) for (const sql of fixture.teardown) await client.command({ query: sql });
    } finally {
      for (const transport of http) await transport.close();
      await client?.close();await container?.stop();
    }
  }, 60_000);

  it('collects supported production sources and explicitly rejects unsupported grant schemas', () => {
    expect(capture.coverage).toHaveLength(6);
    for (const source of capture.coverage) {
      if (source.source === 'grants' && !hasField('grants','is_wildcard')) {
        expect(source.state).toBe('schema-unavailable');expect(source.reason).toContain('is_wildcard');
      } else expect(source.state).toBe('readable');
    }
    expect(capture.observations.every(record => capture.hosts.some(host => host.host === record.host))).toBe(true);
    expect(capture.verdict.kind).toBe('not-checked');
    for (const kind of ['user','role','table','row-policy']) expect(capture.observations.some(record => record.kind === kind)).toBe(true);
    expect(capture.observations.some(record => record.kind === 'grant-scope')).toBe(grantsReadable());
  });
  it('reads assignment defaults, column delegation and partial revokes without inventing unavailable collector data', async () => {
    expect(capture.relationships.some(edge => edge.kind === 'has-role' && edge.evidence.user_name === fixture.bob && Number(edge.evidence.granted_role_is_default) === 0)).toBe(true);
    expect(capture.relationships.some(edge => edge.kind === 'has-role' && edge.evidence.role_name === fixture.parent && edge.evidence.granted_role_name === fixture.reader)).toBe(true);
    const grants = grantsReadable() ? capture.relationships.filter(edge => edge.kind === 'grant').map(edge => edge.evidence) : await read(fixture.probes.grants);
    expect(grants.some(row => row.column === 'id' && Number(row.grant_option) === 1)).toBe(true);
    expect(grants.some(row => row.table === 'excluded' && Number(row.is_partial_revoke) === 1)).toBe(true);
    // The revoke is its own privilege object, named as a revoke, apart from the grant it narrows.
    if (grantsReadable()) {
      const revoke = capture.observations.find(record => record.kind === 'grant-scope' && record.attributes.table === 'excluded')!;
      expect(revoke.name).toBe(`REVOKE SELECT ON ${fixture.db}.excluded`);
      expect(revoke.attributes.partial_revoke).toBe(1);
    }
    const policy = capture.observations.find(record => record.kind === 'row-policy' && record.attributes.short_name === 'tenant_filter')!;
    expect(policy.attributes.select_filter).toBe('tenant_id = 42');
    expect(policy.attributes.apply_to_list).toEqual([fixture.reader]);
  });
  it('retains both cascade views and all source/target tables in the collected inventory', () => {
    const records = capture.observations.filter(record => record.kind === 'table' && record.attributes.database === fixture.db);
    expect(records.filter(record => record.attributes.engine === 'MaterializedView').map(record => record.attributes.name).sort()).toEqual(['events_to_minute','minute_to_hourly','one_sampler']);
    for (const name of ['events','minute','hourly','public_events']) expect(records.some(record => record.attributes.name === name)).toBe(true);
    // Native links only; definition references are asserted separately below.
    const endpoints = (kind: 'insert-trigger' | 'writes-to') => capture.relationships.filter(edge => edge.kind === kind && edge.evidence.method === 'native')
      .map(edge => [JSON.parse(edge.from).at(-1), JSON.parse(edge.to).at(-1)]);
    expect(endpoints('insert-trigger')).toEqual(expect.arrayContaining([['events','events_to_minute'], ['minute','minute_to_hourly']]));
    if (hasField('tables','target_table')) expect(endpoints('writes-to')).toEqual(expect.arrayContaining([['events_to_minute','minute'], ['minute_to_hourly','hourly']]));
    else {
      expect(endpoints('writes-to')).toEqual([]);
      expect(capture.coverage.find(source => source.source === 'tables')!.reason).toContain('target_table');
    }
  });
  it('builds storage locations from definitions without contacting storage', async () => {
    const name = (key: string) => (JSON.parse(key) as string[]).slice(2).join('.');
    const host = capture.hosts[0].host;
    const storedOn = capture.relationships.filter(edge => edge.kind === 'points-to')
      .map(edge => [name(edge.from), name(edge.to), edge.evidence.method, edge.evidence.location]);
    const local = `/var/lib/clickhouse/user_files/${fixture.db}/wh/nyc`;
    expect(storedOn).toEqual(expect.arrayContaining([
      // Both IcebergLocal tables are stored on this ClickHouse host's disk: one node.
      [`${fixture.db}.lake_trips`, `local disk · ${host}`, 'engine-location', `${local}/trips/`],
      [`${fixture.db}.lake_locations`, `local disk · ${host}`, 'engine-location', `${local}/locations/`],
      // Both S3 tables point at one server: one node.
      [`${fixture.db}.raw_events`, 'http://lake.invalid:9000', 'engine-location', 'http://lake.invalid:9000/fixture-bucket/raw/events.parquet'],
      [`${fixture.db}.raw_events_copy`, 'http://lake.invalid:9000', 'engine-location', 'http://lake.invalid:9000/fixture-bucket/raw/events.parquet'],
    ]));
    const locations = capture.observations.filter(record => record.kind === 'location');
    expect(locations.map(record => [record.name, record.attributes.backend]).sort()).toEqual([['http://lake.invalid:9000', 'S3'], [`local disk · ${host}`, 'Local']]);
    // No object-browser query made ClickHouse read from S3.
    await client.command({ query: 'SYSTEM FLUSH LOGS' });
    const [usage] = await read(`SELECT count() AS queries, sum(ProfileEvents['S3GetObject'] + ProfileEvents['S3ListObjects'] + ProfileEvents['S3HeadObject']) AS s3
      FROM system.query_log WHERE type = 'QueryFinish' AND query LIKE '%source:TraceHouse:Objects:%' AND query NOT LIKE '%system.query_log%'`);
    expect(Number(usage.queries)).toBeGreaterThan(0);
    expect(Number(usage.s3)).toBe(0);
  });
  it('labels grant scopes with the server privilege group and level', () => {
    if (!grantsReadable()) return expect(capture.observations.some(record => record.kind === 'grant-scope')).toBe(false);
    const scope = (type: string) => capture.observations.find(record => record.kind === 'grant-scope' && record.attributes.access_type === type)!;
    expect(scope('SELECT').attributes).toMatchObject({ privilege_group: 'SELECT', privilege_level: 'COLUMN' });
    // SHOW USERS sits under SHOW ACCESS, which ClickHouse files under ACCESS MANAGEMENT.
    expect(scope('SHOW USERS').attributes).toMatchObject({ privilege_group: 'ACCESS MANAGEMENT', privilege_level: 'GLOBAL' });
    expect(capture.coverage.find(source => source.source === 'grants')!.reason ?? '').not.toContain('Privilege groups unavailable');
  });
  it('links a privilege to the tables in its scope for each table operation it includes, such as ALL', () => {
    if (!grantsReadable()) return expect(capture.observations.some(record => record.kind === 'grant-scope')).toBe(false);
    const all = capture.observations.find(record => record.kind === 'grant-scope' && record.attributes.access_type === 'ALL'
      && record.attributes.database == null)!;
    // The server's privilege tree: ALL includes every table-level operation.
    expect(all.attributes.table_operations).toEqual(expect.arrayContaining(['SELECT', 'INSERT', 'DROP TABLE', 'ALTER DELETE']));
    const reader = capture.observations.find(record => record.kind === 'grant-scope' && record.attributes.access_type === 'SELECT'
      && record.attributes.database === fixture.db && record.attributes.table == null)!;
    expect(reader.attributes.table_operations).toEqual(['SELECT']);
    const linked = (scope: string, focus?: string, operations?: string[]) => projectCatalogGraph(capture, focus, 100, operations)
      .edges.filter(e => e.kind === 'candidate-scope' && e.from === scope).map(e => e.to);
    const admin = objectKey(input.connection, 'user', fixture.admin), events = objectKey(input.connection, 'table', fixture.db, 'events');
    // A global scope links only under a focus object, like a global SELECT.
    expect(linked(all.key)).toEqual([]);
    expect(linked(all.key, admin)).toContain(events);
    expect(linked(all.key, admin, ['DROP TABLE'])).toContain(events);
    // A database-wide SELECT links for SELECT, the default, and not for INSERT. Two holders share
    // this scope (the reader role and the catalog user), so it links once per grant.
    expect(new Set(linked(reader.key, events))).toEqual(new Set([events]));
    expect(linked(reader.key, events, ['INSERT'])).toEqual([]);
    // Several access types link a privilege that includes any of them.
    expect(new Set(linked(reader.key, events, ['INSERT', 'SELECT']))).toEqual(new Set([events]));
    // ALL is not named after either type: its tag says which selected types it includes.
    const tags = coveredOperations(projectCatalogGraph(capture, admin, 100, ['SELECT', 'INSERT']), ['SELECT', 'INSERT']);
    expect(tags.get(all.key)).toEqual(['SELECT', 'INSERT']);
    expect(tags.has(reader.key)).toBe(false);
  });
  it('links view sources through the analyzer and engine destinations through server-parsed arguments', () => {
    const name = (key: string) => (JSON.parse(key) as string[]).slice(2).join('.');
    const links = capture.relationships.filter(edge => edge.evidence.method !== undefined && edge.evidence.method !== 'native')
      .map(edge => [name(edge.from), edge.kind, edge.label, name(edge.to), edge.evidence.method]);
    const q = (table: string) => `${fixture.db}.${table}`;
    expect(links).toEqual(expect.arrayContaining([
      // CTE `recent` resolves to events; the joined table is linked too.
      [q('events'), 'reads-from', 'read by', q('tenant_report'), 'analyzer'],
      [q('minute'), 'reads-from', 'read by', q('tenant_report'), 'analyzer'],
      [q('events'), 'reads-from', 'read by', q('public_events'), 'analyzer'],
      // Refreshable MV: no insert trigger exists, so the analyzer supplies its source.
      ['system.one', 'reads-from', 'read by', q('one_sampler'), 'analyzer'],
      [q('events_buffer'), 'writes-to', 'flushes to', q('events'), 'engine-arguments'],
      [q('events_all'), 'writes-to', 'forwards to', q('events'), 'engine-arguments'],
    ]));
    expect(links.some(link => link[0] === q('recent') || link[0] === 'recent')).toBe(false);
    // Insert-triggered MV sources stay native; the analyzer does not duplicate them.
    expect(links.some(link => link[3] === q('events_to_minute'))).toBe(false);
    const system = capture.observations.find(record => record.key === objectKey(input.connection, 'table', 'system', 'one'))!;
    // Read by name although the catalog skips the system database.
    expect(system.attributes).toMatchObject({ database: 'system', name: 'one', engine: 'SystemOne' });
    expect(system.attributes.unresolvedReference).toBeUndefined();
    expect(capture.coverage.find(source => source.source === 'tables')!.reason ?? '').not.toContain('failed');
  });
  it('does not analyze view queries whose analysis contacts a source or runs a subquery, and analyzes them on request', async () => {
    const q = (table: string) => `${fixture.db}.${table}`;
    const record = (table: string) => capture.observations.find(o => o.key === objectKey(input.connection, 'table', fixture.db, table))!;
    expect(record('ping_feed').attributes.analysisEffects).toEqual(['url() table function']);
    expect(record('top_amount').attributes.analysisEffects).toEqual(['scalar subquery']);
    // An IN subquery is not run by the analyzer, so this view is analyzed and linked.
    expect(record('active_events').attributes.analysisEffects).toBeUndefined();
    const name = (key: string) => (JSON.parse(key) as string[]).slice(2).join('.');
    const read = capture.relationships.filter(e => e.evidence.method === 'analyzer' && name(e.to) === q('active_events')).map(e => name(e.from)).sort();
    expect(read).toEqual([q('events'), q('minute')]);
    const reason = capture.coverage.find(source => source.source === 'tables')!.reason ?? '';
    expect(reason).toContain(`${q('ping_feed')}: not analyzed (url() table function)`);
    expect(reason).toContain(`${q('top_amount')}: not analyzed (scalar subquery)`);
    // Collection sent no HTTP request and ran no scalar subquery.
    await client.command({ query: 'SYSTEM FLUSH LOGS' });
    const effects = async (tag: string) => (await adapter.executeQuery(q2(`SELECT count() AS queries,
      sum(ProfileEvents['ReadWriteBufferFromHTTPRequestsSent']) AS http, sum(ProfileEvents['ScalarSubqueriesCacheMiss']) AS scalars
      FROM system.query_log WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing') AND query LIKE '%source:${tag}%' AND query NOT LIKE '%system.query_log%'`)))[0];
    const q2 = (sql: string) => tagQuery(sql, 'TraceHouse:Objects:metadata-proof');
    const collected = await effects('TraceHouse:Objects:references');
    expect(Number(collected.queries)).toBeGreaterThan(0);
    expect([Number(collected.http), Number(collected.scalars)]).toEqual([0, 0]);
    // On request, the detail runs the analysis its label describes.
    const graph = projectCatalogGraph(capture);
    for (const [table, expected] of [['ping_feed', 'url()'], ['top_amount', `${q('events')}`]] as const) {
      const node = graph.nodes.find(n => n.key === objectKey(input.connection, 'table', fixture.db, table))!;
      const statement = describeDetail(node, graph).sections.flatMap(s => s.kind === 'statement' ? [s.statement] : []).find(s => s.title === 'Tables read')!;
      expect(statement.external).toBeTruthy();
      expect((await loadDetailStatement(adapter, statement)).split('\n')).toContain(expected);
    }
    await client.command({ query: 'SYSTEM FLUSH LOGS' });
    const requested = await effects('TraceHouse:Objects:detail');
    expect(Number(requested.http)).toBeGreaterThan(0);
    expect(Number(requested.scalars)).toBeGreaterThan(0);
  });
  it('collects every table link for a read-only account without CREATE TEMPORARY TABLE', async () => {
    const restricted = asUser(fixture.catalog);
    expect(await restricted.executeRawQuery!(q('CHECK GRANT CREATE TEMPORARY TABLE ON *.*'))).toEqual(['0']);
    const scoped = await collectObjects(restricted, input, { mode: 'catalog' });
    const links = (c: Capture) => c.relationships.filter(e => e.evidence.method !== undefined)
      .map(e => JSON.stringify([e.from, e.to, e.kind, e.evidence.method, e.evidence.location ?? null])).sort();
    expect(links(scoped)).toEqual(links(capture));
    expect(scoped.coverage.find(source => source.source === 'tables')!.reason ?? '').not.toContain('failed');
  });
  it('returns EXPLAIN text over HTTP as ClickHouse produced it', async () => {
    // Quotes, a backslash and a line break inside one row, compared with the JSON rows of the same EXPLAIN.
    // Explicit FROM: inside SELECT … FROM (EXPLAIN …) ClickHouse adds FROM system.one to a query without one.
    for (const sql of [`EXPLAIN AST SELECT 'it''s', 'a\\\\b' FROM system.one`, `EXPLAIN SYNTAX SELECT 'it''s' FROM system.one`]) {
      const json = await (await client.query({ query: `SELECT explain FROM (${sql})`, format: 'JSONEachRow' })).json<{ explain: string }>();
      expect(await asUser(fixture.catalog).executeRawQuery!(q(sql))).toEqual(json.map(row => row.explain));
    }
  });
  it('reads MV dependency pairs and verifies structured target availability or absence', async () => {
    const tables = await read(fixture.probes.tables);
    const byName = (name: string) => tables.find(row => row.name === name)!;
    const dependents = (name: string) => (byName(name).dependencies_table as string[]).map((table, i) => [(byName(name).dependencies_database as string[])[i], table]);
    expect(dependents('events')).toContainEqual([fixture.db,'events_to_minute']);
    expect(dependents('minute')).toContainEqual([fixture.db,'minute_to_hourly']);
    for (const [view, target] of [['events_to_minute','minute'],['minute_to_hourly','hourly']]) {
      if (hasField('tables','target_table')) expect(byName(view)).toMatchObject({ target_database: fixture.db, target_table: target });
      else {
        expect(byName(view)).not.toHaveProperty('target_table');
        expect(String(byName(view).create_table_query)).toContain(`TO ${fixture.db}.${target}`);
        // Definition text is available; a target parser/fallback is NOT implemented.
      }
    }
    expect(String(byName('public_events').as_select)).toContain('events');
    expect(String(byName('minute_to_hourly').as_select)).toContain('sum(events)');
  });
  it('proves the two insert-trigger stages propagate multiple batches before merges', async () => {
    await client.command({ query: fixture.seed });await client.command({ query: fixture.seed });
    const totals = await Promise.all(fixture.totals.map(read));
    for (const rows of totals) expect(rows.map(row => [Number(row.amount), Number(row.events)])).toEqual([[12,6]]);
  });
  it('links a table to the remote disk its storage policy moves parts to, with the TTL rule, from configuration only', () => {
    const tiered = objectKey(input.connection, 'table', fixture.db, 'tiered'), disk = objectKey(input.connection, 'disk', 'S3 disk · th_s3');
    expect(capture.relationships.filter(edge => edge.from === tiered && edge.kind === 'points-to').map(edge => [edge.to, edge.label, edge.evidence])).toEqual([
      [disk, 'moves after TTL ts + toIntervalDay(7)', { method: 'storage-policy', policy: 'th_fixture_tiered', volume: 'cold', disk: coldDisk, ttl: ['ts + toIntervalDay(7)'] }]]);
    expect(capture.observations.find(record => record.key === disk)!.attributes).toMatchObject({ backend: 'S3 disk', disk: 'th_s3', tables: 1,
      ...(cacheDisk ? { cache_disks: ['th_s3_cache'] } : {}) });
    // The rules as the server normalized them in engine_full.
    expect(capture.observations.find(record => record.key === tiered)!.attributes.ttl_rules).toEqual([
      { expression: 'ts + toIntervalDay(7)', action: 'move', destination: { type: 'volume', name: 'cold', ifExists: false } },
      { expression: 'ts + toIntervalDay(90)', action: 'delete' }]);
    // No link for tables whose policy has only local disks.
    expect(capture.relationships.some(edge => edge.from === objectKey(input.connection, 'table', fixture.db, 'events') && edge.evidence.method === 'storage-policy')).toBe(false);
  });
  it('reads where a table\'s parts are per disk, with the next move TTL', async () => {
    await client.command({ query: `INSERT INTO ${fixture.db}.tiered SELECT now(), number FROM numbers(3)` });
    const graph = projectCatalogGraph(capture);
    const node = graph.nodes.find(n => n.key === objectKey(input.connection, 'table', fixture.db, 'tiered'))!;
    const section = describeDetail(node, graph).sections.find(s => s.kind === 'lifecycle');
    if (section?.kind !== 'lifecycle') throw new Error('tiered has no lifecycle section');
    // A fresh part stays on the first volume until its move TTL, 7 days out.
    expect(await loadTablePlacement(adapter, section.statement)).toMatchObject([{ disk: 'default', storage: 'Local', parts: 1, rows: 3, pastMoveTTL: 0, nextMove: 'in 7 days' }]);
  });
  it('reads keys, table settings and TTL definition text, including an ALTER', async () => {
    const before = (await read(fixture.probes.tables)).find(row => row.name === 'events')!;
    expect(before.sorting_key).toBe('tenant_id, ts, id');
    expect(before.storage_policy).toBe('default');
    expect(String(before.engine_full)).toMatch(/index_granularity\s*=\s*4096/);
    expect(String(before.create_table_query)).toContain('toIntervalDay(30)');
    await client.command({ query: fixture.alter });
    const after = (await read(fixture.probes.tables)).find(row => row.name === 'events')!;
    expect(String(after.create_table_query)).toContain('toIntervalDay(60)');
    // This proves definition retrieval, not a general TTL/settings AST parser.
  });
  it('reads column types, defaults, codecs and key membership', async () => {
    const columns = await read(fixture.probes.columns);
    expect(columns.find(row => row.table === 'events' && row.name === 'note')).toMatchObject({ type: 'String', default_kind: 'DEFAULT', default_expression: "'sample'", compression_codec: 'CODEC(ZSTD(1))' });
    expect(Number(columns.find(row => row.table === 'events' && row.name === 'tenant_id')!.is_in_sorting_key)).toBe(1);
    expect((await read(fixture.probes.databases))[0]).toMatchObject({ name: fixture.db, engine: 'Atomic' });
  });
  it('reads profile applicability, inheritance, values and constraints separately', async () => {
    const profiles = await read(fixture.probes.profiles), elements = await read(fixture.probes.elements);
    expect(profiles.find(row => row.name === fixture.profile)!.apply_to_list).toEqual([fixture.reader]);
    expect(elements.some(row => row.profile_name === fixture.profile && row.inherit_profile === fixture.baseProfile)).toBe(true);
    expect(elements.find(row => row.profile_name === fixture.baseProfile && row.setting_name === 'max_threads')!.value).toBe('2');
    expect(elements.find(row => row.profile_name === fixture.profile && row.setting_name === 'max_memory_usage')).toMatchObject({ value: '268435456', min: '1048576', max: '536870912' });
  });
  it('reads quota selectors and interval limits from their separate sources', async () => {
    const quotas = await read(fixture.probes.quotas), limits = await read(fixture.probes.limits);
    expect(quotas[0].apply_to_list).toEqual([fixture.reader]);
    expect(quotas[0].keys).toContain('user_name');
    expect(Number(limits[0].duration)).toBe(3600);expect(Number(limits[0].max_queries)).toBe(100);
  });
  it('resolves the table storage policy through volumes to actual disks', async () => {
    const policies = await read(fixture.probes.policies), disks = await read(fixture.probes.disks);
    const policy = policies.find(row => row.policy_name === 'default')!;
    expect(policy.volume_name).toBeTruthy();
    expect((policy.disks as string[]).length).toBeGreaterThan(0);
    for (const name of policy.disks as string[]) expect(disks.some(row => row.name === name)).toBe(true);
  });
  it('records discovered schema and distinguishes missing fields from empty values', async () => {
    for (const [table, name] of [['tables','dependencies_table'],['columns','default_expression'],['settings_profile_elements','inherit_profile'],['quota_limits','max_queries']]) {
      expect(schema.some(row => row.table === table && row.name === name && row.type)).toBe(true);
    }
    const view = (await read(fixture.probes.tables)).find(row => row.name === 'events_to_minute')!;
    expect('target_table' in view).toBe(hasField('tables','target_table'));
  });
  it('lists the system database alone on request, each table once', async () => {
    // No fixture view reads system.contributors; views do read other system tables.
    const contributors = objectKey(input.connection, 'table', 'system', 'contributors');
    expect(capture.observations.some(o => o.key === contributors)).toBe(false);
    const withSystem = await collectObjects(adapter, input, { mode: 'catalog', systemTables: true });
    expect(withSystem.systemTables).toBe(true);
    expect(withSystem.observations.filter(o => o.key === contributors)).toHaveLength(1);
    // The fixture database and information_schema are left out.
    expect(new Set(withSystem.observations.filter(o => o.kind === 'table').map(o => o.attributes.database))).toEqual(new Set(['system']));
    // Grants follow the tables: on system only with system tables, never on system without them; global grants in both.
    const scopes = (c: Capture) => c.observations.filter(o => o.kind === 'grant-scope').map(o => o.attributes.database);
    expect(scopes(capture)).not.toContain('system');
    expect(scopes(capture)).toContain(fixture.db);
    expect(scopes(withSystem)).toContain('system');
    expect(scopes(withSystem)).not.toContain(fixture.db);
    expect(scopes(capture).some(database => database == null) && scopes(withSystem).some(database => database == null)).toBe(true);
    // Row policies on other databases stay out too, so they bring in no placeholder tables.
    expect(withSystem.observations.some(o => o.kind === 'row-policy')).toBe(false);
    // A system table a view reads from is listed once, not again by name.
    const keys = withSystem.observations.filter(o => o.kind === 'table').map(o => `${o.host}|${o.key}`);
    expect(new Set(keys).size).toBe(keys.length);
  });
  it('links only the grants that include the asked operation, from the server privilege tree', async () => {
    if (!grantsReadable()) return;
    // The operations come from system.privileges: ALTER DELETE sits under ALTER.
    expect(capture.operations).toContainEqual({ name: 'ALTER DELETE', group: 'ALTER' });
    expect(capture.operations).toContainEqual({ name: 'DROP TABLE', group: 'DROP' });
    const trips = objectKey(input.connection, 'table', fixture.db, 'trips');
    const user = (name: string) => objectKey(input.connection, 'user', name);
    // Focus answers from the catalog, or from a read of the user and table by name: both agree.
    const byName = async (principal: string) => collectObjects(adapter, { ...input, principal, operation: 'SELECT' });
    for (const [principal, operation, state] of [[fixture.alice, 'SELECT', 'granted'], [fixture.alice, 'INSERT', 'no-path'],
      [fixture.alice, 'DROP TABLE', 'no-path'], [fixture.admin, 'DROP TABLE', 'granted']] as const) {
      expect(answerFor(capture, user(principal), trips, operation).state).toBe(state);
      expect(answerFor(await byName(principal), user(principal), trips, operation).state).toBe(state);
    }
  });
  it('preserves nested provenance and checks real execution without inferring a verdict', async () => {
    const graph = projectCatalogGraph(capture, objectKey(input.connection,'table',fixture.db,'trips'));
    const paths = provenancePaths(graph, objectKey(input.connection,'user',fixture.alice), objectKey(input.connection,'table',fixture.db,'trips'));
    if (grantsReadable()) expect(paths.length).toBeGreaterThan(0);
    else expect(paths).toEqual([]);
    expect((await asUser(fixture.alice).executeQuery(q(fixture.read))).map(row => Number(row.id))).toEqual([1]);
    await expect(asUser(fixture.denied).executeQuery(q(fixture.read))).rejects.toThrow(/privilege|ACCESS_DENIED/i);
    expect(capture.verdict.kind).toBe('not-checked');
  });
  it('reads each detail statement from the server, without secrets, and surfaces observer errors', async () => {
    const graph = projectCatalogGraph(capture);
    const statementsFor = (key: string) => describeDetail(graph.nodes.find(n => n.key === key)!, graph).sections
      .flatMap(section => section.kind === 'statement' ? [section.statement] : []);
    const policy = capture.observations.find(record => record.kind === 'row-policy' && record.attributes.short_name === 'tenant_filter')!;
    const expected: [string, RegExp[]][] = [
      [objectKey(input.connection, 'user', fixture.alice), [/^CREATE USER /, /^GRANT /m]],
      [objectKey(input.connection, 'role', fixture.reader), [/^CREATE ROLE /, /^GRANT /m]],
      [objectKey(input.connection, 'table', fixture.db, 'trips'), [/^CREATE TABLE /]],
      [policy.key, [/^CREATE ROW POLICY .*tenant_id = 42/]],
    ];
    for (const [key, patterns] of expected) {
      const statements = statementsFor(key);
      expect(statements).toHaveLength(patterns.length);
      for (const [i, statement] of statements.entries()) expect(await loadDetailStatement(adapter, statement)).toMatch(patterns[i]);
    }
    // Table definitions come laid out by ClickHouse's formatter, one clause per line.
    const ddl = statementsFor(objectKey(input.connection, 'table', fixture.db, 'trips'))[0];
    expect(await loadDetailStatement(adapter, ddl)).toMatch(/^CREATE TABLE [^\n]+\n\(\n {4}`/);
    // A server without the formatter returns UNKNOWN_FUNCTION; the stored single-line text is used.
    const withoutFormatter = { ...ddl, sql: ddl.sql.replace('formatQueryOrNull', 'formatQueryMissingInThisServer') };
    expect(await loadDetailStatement(adapter, withoutFormatter)).toMatch(/^CREATE TABLE [^\n]+$/);
    const user = await loadDetailStatement(adapter, statementsFor(objectKey(input.connection, 'user', fixture.alice))[0]);
    expect(user).not.toMatch(/ BY '|HASH|SALT/);
    // The observer fixture holds SHOW USERS; the denied user holds no SHOW privilege.
    await expect(loadDetailStatement(asUser(fixture.denied), statementsFor(objectKey(input.connection, 'user', fixture.alice))[0]))
      .rejects.toThrow(/privilege|ACCESS_DENIED/i);
  });
  it('counts the queries on a table from query_log, per kind and user, and links the latest query', async () => {
    const graph = projectCatalogGraph(capture);
    const activityOf = async (table: string) => {
      const section = describeDetail(graph.nodes.find(n => n.key === objectKey(input.connection, 'table', fixture.db, table))!, graph)
        .sections.find(s => s.kind === 'activity')!;
      return (await loadTableActivity(adapter, section.statement, section.kind === 'activity' ? section.table : ''))!;
    };
    const count = (activity: Awaited<ReturnType<typeof activityOf>>, label: string) => activity.counts.find(c => c.label === label)!.value;
    const alice = asUser(fixture.alice);
    await alice.executeQuery(q(fixture.read));await alice.executeQuery(q(fixture.read));
    await client.command({ query: 'SYSTEM FLUSH LOGS' });
    const trips = await activityOf('trips');
    expect(count(trips, 'SELECT')).toBeGreaterThanOrEqual(2);
    expect(trips.topUsers.find(u => u.user === fixture.alice)!.queries).toBeGreaterThanOrEqual(2);
    expect(trips.lastQueryAgo).toBe('just now');
    // The latest query's id is one of alice's reads on the table.
    const latestId = new URLSearchParams(trips.lastQueryUrl.split('?')[1]).get('qd_id');
    const [latest] = await read(`SELECT user, query FROM system.query_log WHERE query_id = '${latestId}' AND type = 'QueryFinish'`);
    expect(latest).toMatchObject({ user: fixture.alice });
    // The fixture setup inserts into `samples`.
    expect(count(await activityOf('samples'), 'INSERT')).toBeGreaterThanOrEqual(1);
  });
  it('does not report a restricted observer as an empty successful catalog', async () => {
    const restricted = await collectObjects(asUser(fixture.observer), input, { mode: 'catalog' });
    expect(restricted.coverage.some(source => source.state === 'failed' || source.state === 'schema-unavailable')).toBe(true);
    expect(restricted.verdict.kind).toBe('not-checked');
  });
  it('reports actual source truncation rather than complete coverage', async () => {
    const limited = await collectObjects(adapter, input, { mode: 'catalog', limit: 1 });
    expect(limited.coverage.find(source => source.source === 'tables')!.state).toBe('truncated');
    expect(limited.coverage.find(source => source.source === 'grants')!.state).toBe(grantsReadable() ? 'truncated' : 'schema-unavailable');
  });
});
