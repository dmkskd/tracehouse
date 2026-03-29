/**
 * Integration tests for surface queries and AnalyticsService resource lanes.
 *
 * Validates:
 * - buildSurfaceTimeFilter generates correct clauses (minutes conversion, absolute ranges)
 * - resourceLanesSystem filters out _table_function.*, system.*, INFORMATION_SCHEMA.*
 * - resourceLanesSystem includes both Selects and Inserts
 * - resourceLanesSystemTotals aggregates across all tables
 * - resourceLanesTable drill-down groups by normalized_query_hash
 * - AnalyticsService.getSystemResourceLanes / getTableResourceLanes return correct shapes
 * - stressSurfaceQueries / stressSurfaceInserts / stressSurfaceMerges run correctly
 *
 * Uses shadow tables (test_shadow.query_log, test_shadow.part_log) with controlled
 * seed data so results are deterministic.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { ShadowAdapter } from './setup/shadow-adapter.js';
import { createShadowDatabase, dropShadowDatabase, truncateShadowTables } from './setup/shadow-tables.js';
import { AnalyticsService } from '../../services/analytics-service.js';
import { buildSurfaceTimeFilter, resourceLanesSystem, resourceLanesSystemTotals, resourceLanesTable, stressSurfaceQueries, stressSurfaceInserts, stressSurfaceMerges, patternSurface, resourceLanesMerges, resourceLanesMergeTotals, resourceLanesTableMerges } from '../../queries/surface-queries.js';
import { buildQuery } from '../../queries/builder.js';

const CONTAINER_TIMEOUT = 120_000;
const SHADOW_DB = 'test_shadow';

/** Insert a row into shadow query_log with sensible defaults. */
async function seedQueryLog(
  ctx: TestClickHouseContext,
  row: {
    event_time: string;
    type?: string;
    query_kind?: string;
    is_initial_query?: number;
    query_duration_ms?: number;
    read_rows?: number;
    read_bytes?: number;
    written_rows?: number;
    written_bytes?: number;
    memory_usage?: number;
    query?: string;
    normalized_query_hash?: number;
    tables?: string[];
    databases?: string[];
    profileEvents?: Record<string, number>;
  },
): Promise<void> {
  const tables = row.tables ?? [];
  const databases = row.databases ?? [];
  const pe = row.profileEvents ?? {};

  const peEntries = Object.entries(pe).map(([k, v]) => `'${k}', ${v}`).join(', ');
  const peMap = peEntries ? `map(${peEntries})` : 'map()';

  await ctx.client.command({
    query: `
      INSERT INTO ${SHADOW_DB}.query_log (
        event_time, type, query_kind, is_initial_query,
        query_duration_ms, read_rows, read_bytes, written_rows, written_bytes,
        memory_usage, query, normalized_query_hash,
        tables, databases, ProfileEvents
      ) VALUES (
        '${row.event_time}',
        '${row.type ?? 'QueryFinish'}',
        '${row.query_kind ?? 'Select'}',
        ${row.is_initial_query ?? 1},
        ${row.query_duration_ms ?? 100},
        ${row.read_rows ?? 1000},
        ${row.read_bytes ?? 50000},
        ${row.written_rows ?? 0},
        ${row.written_bytes ?? 0},
        ${row.memory_usage ?? 1048576},
        '${(row.query ?? 'SELECT 1').replace(/'/g, "\\'")}',
        ${row.normalized_query_hash ?? 12345},
        [${tables.map(t => `'${t}'`).join(', ')}],
        [${databases.map(d => `'${d}'`).join(', ')}],
        ${peMap}
      )
    `,
  });
}

/** Insert a row into shadow part_log. */
async function seedPartLog(
  ctx: TestClickHouseContext,
  row: {
    event_time: string;
    event_type?: string;
    database?: string;
    table?: string;
    duration_ms?: number;
    read_rows?: number;
    read_bytes?: number;
    peak_memory_usage?: number;
    profileEvents?: Record<string, number>;
  },
): Promise<void> {
  const pe = row.profileEvents ?? {};
  const peEntries = Object.entries(pe).map(([k, v]) => `'${k}', ${v}`).join(', ');
  const peMap = peEntries ? `map(${peEntries})` : 'map()';

  await ctx.client.command({
    query: `
      INSERT INTO ${SHADOW_DB}.part_log (
        event_time, event_type, database, \`table\`, duration_ms,
        read_rows, read_bytes, peak_memory_usage, ProfileEvents
      ) VALUES (
        '${row.event_time}',
        '${row.event_type ?? 'MergeParts'}',
        '${row.database ?? 'default'}',
        '${row.table ?? 'events'}',
        ${row.duration_ms ?? 50},
        ${row.read_rows ?? 0},
        ${row.read_bytes ?? 0},
        ${row.peak_memory_usage ?? 0},
        ${peMap}
      )
    `,
  });
}

describe('Surface queries integration', { tags: ['observability'] }, () => {
  let ctx: TestClickHouseContext;
  let shadowAdapter: ShadowAdapter;
  let service: AnalyticsService;

  // Use absolute times so we can use startTime/endTime instead of now()-based lookback
  const T1 = '2026-03-25 10:00:00';
  const T2 = '2026-03-25 10:01:00';
  const T3 = '2026-03-25 10:02:00';
  const START = '2026-03-25 09:00:00';
  const END = '2026-03-25 11:00:00';

  beforeAll(async () => {
    ctx = await startClickHouse();
    shadowAdapter = new ShadowAdapter(ctx.client);
    service = new AnalyticsService(shadowAdapter);

    await createShadowDatabase(ctx.client);
    await truncateShadowTables(ctx.client);

    // ── Seed query_log data ──────────────────────────────────────────

    // Real user table: default.events — SELECT queries
    for (const t of [T1, T2, T3]) {
      await seedQueryLog(ctx, {
        event_time: t,
        query_kind: 'Select',
        tables: ['default.events'],
        databases: ['default'],
        query: 'SELECT count() FROM default.events',
        normalized_query_hash: 1001,
        read_rows: 5000,
        read_bytes: 100000,
        memory_usage: 2097152,
        query_duration_ms: 200,
        profileEvents: {
          RealTimeMicroseconds: 150000,
          IOWaitMicroseconds: 30000,
          SelectedMarks: 42,
        },
      });
    }

    // Real user table: default.events — INSERT queries
    for (const t of [T1, T2]) {
      await seedQueryLog(ctx, {
        event_time: t,
        query_kind: 'Insert',
        tables: ['default.events'],
        databases: ['default'],
        query: 'INSERT INTO default.events VALUES ...',
        normalized_query_hash: 2001,
        read_rows: 0,
        read_bytes: 0,
        written_rows: 10000,
        written_bytes: 500000,
        memory_usage: 524288,
        query_duration_ms: 50,
        profileEvents: {
          RealTimeMicroseconds: 40000,
          IOWaitMicroseconds: 10000,
          SelectedMarks: 0,
        },
      });
    }

    // Another real table: analytics.metrics — SELECT
    await seedQueryLog(ctx, {
      event_time: T1,
      query_kind: 'Select',
      tables: ['analytics.metrics'],
      databases: ['analytics'],
      query: 'SELECT avg(value) FROM analytics.metrics',
      normalized_query_hash: 3001,
      read_rows: 20000,
      read_bytes: 800000,
      memory_usage: 4194304,
      query_duration_ms: 500,
      profileEvents: {
        RealTimeMicroseconds: 400000,
        IOWaitMicroseconds: 50000,
        SelectedMarks: 100,
      },
    });

    // Table function row — should be EXCLUDED
    await seedQueryLog(ctx, {
      event_time: T1,
      query_kind: 'Select',
      tables: ['_table_function.numbers'],
      databases: ['_table_function'],
      query: 'SELECT number FROM numbers(1000)',
      normalized_query_hash: 9001,
      profileEvents: { RealTimeMicroseconds: 5000 },
    });

    // System table row — should be EXCLUDED by default
    await seedQueryLog(ctx, {
      event_time: T1,
      query_kind: 'Select',
      tables: ['system.query_log'],
      databases: ['system'],
      query: 'SELECT * FROM system.query_log',
      normalized_query_hash: 9002,
      profileEvents: { RealTimeMicroseconds: 10000 },
    });

    // INFORMATION_SCHEMA row — should be EXCLUDED by default
    await seedQueryLog(ctx, {
      event_time: T1,
      query_kind: 'Select',
      tables: ['INFORMATION_SCHEMA.tables'],
      databases: ['INFORMATION_SCHEMA'],
      query: 'SELECT * FROM INFORMATION_SCHEMA.TABLES',
      normalized_query_hash: 9003,
      profileEvents: { RealTimeMicroseconds: 2000 },
    });

    // Non-initial query — should be EXCLUDED (is_initial_query = 0)
    await seedQueryLog(ctx, {
      event_time: T1,
      query_kind: 'Select',
      is_initial_query: 0,
      tables: ['default.events'],
      databases: ['default'],
      query: 'SELECT count() FROM default.events -- forwarded',
      normalized_query_hash: 1001,
      profileEvents: { RealTimeMicroseconds: 100000 },
    });

    // ── Seed part_log data (for stress surface merges) ───────────────

    await seedPartLog(ctx, {
      event_time: T1, event_type: 'MergeParts', database: 'default', table: 'events',
      duration_ms: 150, read_rows: 5000, read_bytes: 200000, peak_memory_usage: 1048576,
      profileEvents: { RealTimeMicroseconds: 80000, IOWaitMicroseconds: 20000 },
    });
    await seedPartLog(ctx, { event_time: T1, event_type: 'NewPart', database: 'default', table: 'events', duration_ms: 0 });
    await seedPartLog(ctx, {
      event_time: T2, event_type: 'MergeParts', database: 'default', table: 'events',
      duration_ms: 200, read_rows: 8000, read_bytes: 300000, peak_memory_usage: 2097152,
      profileEvents: { RealTimeMicroseconds: 120000, IOWaitMicroseconds: 30000 },
    });
    // Merge for analytics.metrics table
    await seedPartLog(ctx, {
      event_time: T1, event_type: 'MergeParts', database: 'analytics', table: 'metrics',
      duration_ms: 100, read_rows: 3000, read_bytes: 150000, peak_memory_usage: 524288,
      profileEvents: { RealTimeMicroseconds: 50000, IOWaitMicroseconds: 10000 },
    });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      if (!ctx.keepData) {
        await dropShadowDatabase(ctx.client);
      }
      await stopClickHouse(ctx);
    }
  }, 30_000);

  // ── buildSurfaceTimeFilter unit tests ─────────────────────────────

  describe('buildSurfaceTimeFilter', () => {
    it('converts fractional hours to integer minutes', () => {
      const result = buildSurfaceTimeFilter('event_time', { hours: 0.25 });
      expect(result.clause).toContain('INTERVAL');
      expect(result.clause).toContain('MINUTE');
      expect(result.params.minutes).toBe(15);
    });

    it('defaults to 24 hours = 1440 minutes', () => {
      const result = buildSurfaceTimeFilter('event_time', {});
      expect(result.params.minutes).toBe(1440);
    });

    it('uses BETWEEN for absolute time ranges', () => {
      const result = buildSurfaceTimeFilter('event_time', {
        startTime: '2026-03-25T09:00',
        endTime: '2026-03-25T11:00',
      });
      expect(result.clause).toContain('BETWEEN');
      // T should be replaced with space
      expect(result.params.start_time).toBe('2026-03-25 09:00');
      expect(result.params.end_time).toBe('2026-03-25 11:00');
    });

    it('uses the specified column name', () => {
      const result = buildSurfaceTimeFilter('q.event_time', { hours: 1 });
      expect(result.clause).toContain('q.event_time');
    });
  });

  // ── resourceLanesSystem query ─────────────────────────────────────

  describe('resourceLanesSystem', () => {
    it('returns only real user tables, filtering out _table_function, system, INFORMATION_SCHEMA', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesSystem(tf.clause), { max_lanes: 10, ...tf.params });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      const laneIds = [...new Set(rows.map(r => String(r.lane_id)))];

      expect(laneIds).toContain('default.events');
      expect(laneIds).toContain('analytics.metrics');
      expect(laneIds).not.toContain('_table_function.numbers');
      expect(laneIds).not.toContain('system.query_log');
      expect(laneIds).not.toContain('INFORMATION_SCHEMA.tables');
    });

    it('includes both Select and Insert queries', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesSystem(tf.clause), { max_lanes: 10, ...tf.params });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      // default.events has both SELECTs (3 rows) and INSERTs (2 rows) = 5 total query_count
      const eventsRows = rows.filter(r => String(r.lane_id) === 'default.events');
      const totalQC = eventsRows.reduce((sum, r) => sum + Number(r.query_count), 0);
      expect(totalQC).toBe(5); // 3 selects + 2 inserts
    });

    it('excludes non-initial queries (is_initial_query = 0)', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesSystem(tf.clause), { max_lanes: 10, ...tf.params });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      // We seeded 5 initial queries for default.events + 1 non-initial
      // Non-initial should be excluded, so total = 5
      const eventsRows = rows.filter(r => String(r.lane_id) === 'default.events');
      const totalQC = eventsRows.reduce((sum, r) => sum + Number(r.query_count), 0);
      expect(totalQC).toBe(5);
    });

    it('respects max_lanes limit', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesSystem(tf.clause), { max_lanes: 1, ...tf.params });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      const laneIds = [...new Set(rows.map(r => String(r.lane_id)))];
      expect(laneIds.length).toBe(1);
    });

    it('includes system tables when excludeSystemTables is false', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesSystem(tf.clause, false), { max_lanes: 20, ...tf.params });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      const laneIds = [...new Set(rows.map(r => String(r.lane_id)))];

      // system tables should now be included (but _table_function still excluded)
      expect(laneIds).toContain('system.query_log');
      expect(laneIds).not.toContain('_table_function.numbers');
    });

    it('returns expected resource columns', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesSystem(tf.clause), { max_lanes: 10, ...tf.params });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      expect(rows.length).toBeGreaterThan(0);
      const row = rows[0];
      expect(row).toHaveProperty('ts');
      expect(row).toHaveProperty('lane_id');
      expect(row).toHaveProperty('lane_label');
      expect(row).toHaveProperty('query_count');
      expect(row).toHaveProperty('total_duration_ms');
      expect(row).toHaveProperty('total_read_rows');
      expect(row).toHaveProperty('total_read_bytes');
      expect(row).toHaveProperty('total_memory');
      expect(row).toHaveProperty('total_cpu_us');
      expect(row).toHaveProperty('total_io_wait_us');
      expect(row).toHaveProperty('total_selected_marks');
    });
  });

  // ── resourceLanesSystemTotals ─────────────────────────────────────

  describe('resourceLanesSystemTotals', () => {
    it('aggregates all queries per minute', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesSystemTotals(tf.clause), tf.params);
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      expect(rows.length).toBeGreaterThan(0);

      // T1 has: 1 select events + 1 insert events + 1 analytics select + 1 _table_function + 1 system + 1 INFORMATION_SCHEMA + 1 non-initial
      // But totals include ALL types (including system/table_function), only filtered by type=QueryFinish, query_kind IN, is_initial_query
      // T1: select events + insert events + analytics select + table_function select + system select + INFORMATION_SCHEMA select = 6 initial queries
      const t1Row = rows.find(r => String(r.ts).includes('10:00'));
      expect(t1Row).toBeDefined();
      expect(Number(t1Row!.query_count)).toBe(6); // all 6 initial queries at T1
    });

    it('returns resource columns without lane info', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesSystemTotals(tf.clause), tf.params);
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      const row = rows[0];
      expect(row).toHaveProperty('ts');
      expect(row).toHaveProperty('total_cpu_us');
      expect(row).toHaveProperty('total_memory');
      expect(row).not.toHaveProperty('lane_id');
    });
  });

  // ── resourceLanesTable (drill-down) ───────────────────────────────

  describe('resourceLanesTable', () => {
    it('groups by normalized_query_hash for a specific table', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesTable(tf.clause), {
        database: 'default',
        table_name: 'events',
        max_lanes: 10,
        ...tf.params,
      });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      const laneIds = [...new Set(rows.map(r => String(r.lane_id)))];
      // We seeded 2 different normalized_query_hash values (1001 for SELECTs, 2001 for INSERTs)
      expect(laneIds.length).toBe(2);
      expect(laneIds).toContain('1001');
      expect(laneIds).toContain('2001');
    });

    it('lane_label contains a query substring', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesTable(tf.clause), {
        database: 'default',
        table_name: 'events',
        max_lanes: 10,
        ...tf.params,
      });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      for (const row of rows) {
        expect(String(row.lane_label).length).toBeGreaterThan(0);
      }
    });
  });

  // ── Stress surface queries ────────────────────────────────────────

  describe('stressSurfaceQueries', () => {
    it('returns per-minute aggregated SELECT stress for a table', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(stressSurfaceQueries(tf.clause), {
        database: 'default',
        table_name: 'events',
        ...tf.params,
      });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      // We seeded 3 SELECT rows across 3 minutes
      expect(rows.length).toBe(3);
      for (const row of rows) {
        expect(Number(row.query_count)).toBe(1);
        expect(Number(row.total_duration_ms)).toBe(200);
        expect(Number(row.total_cpu_us)).toBe(150000);
      }
    });
  });

  describe('stressSurfaceInserts', () => {
    it('returns per-minute insert activity for a table', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(stressSurfaceInserts(tf.clause), {
        database: 'default',
        table_name: 'events',
        ...tf.params,
      });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      // We seeded 2 INSERT rows at T1 and T2
      expect(rows.length).toBe(2);
      for (const row of rows) {
        expect(Number(row.insert_count)).toBe(1);
        expect(Number(row.inserted_rows)).toBe(10000);
      }
    });
  });

  describe('stressSurfaceMerges', () => {
    it('returns per-minute merge activity from part_log', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(stressSurfaceMerges(tf.clause), {
        database: 'default',
        table_name: 'events',
        ...tf.params,
      });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      // T1: 1 MergeParts + 1 NewPart, T2: 1 MergeParts
      expect(rows.length).toBe(2);

      const t1Row = rows.find(r => String(r.ts).includes('10:00'));
      expect(t1Row).toBeDefined();
      expect(Number(t1Row!.merges)).toBe(1);
      expect(Number(t1Row!.new_parts)).toBe(1);
      expect(Number(t1Row!.merge_ms)).toBe(150);
    });
  });

  // ── Pattern surface ───────────────────────────────────────────────

  describe('patternSurface', () => {
    it('returns per (time, pattern) data for SELECT queries only', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(patternSurface(tf.clause), {
        database: 'default',
        table_name: 'events',
        ...tf.params,
      });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      // Only SELECTs (hash 1001) should appear, not INSERTs (hash 2001)
      const hashes = [...new Set(rows.map(r => String(r.normalized_query_hash)))];
      expect(hashes).toContain('1001');
      expect(hashes).not.toContain('2001');

      for (const row of rows) {
        expect(Number(row.avg_duration_ms)).toBeGreaterThan(0);
        expect(String(row.sample_query).length).toBeGreaterThan(0);
      }
    });
  });

  // ── Merge resource lanes queries ────────────────────────────────

  describe('resourceLanesMerges', () => {
    it('returns per (minute, table) merge resource data', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesMerges(tf.clause), tf.params);
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      const laneIds = [...new Set(rows.map(r => String(r.lane_id)))];
      expect(laneIds).toContain('default.events');
      expect(laneIds).toContain('analytics.metrics');

      // default.events has 2 MergeParts events (T1 + T2)
      const eventsRows = rows.filter(r => String(r.lane_id) === 'default.events');
      expect(eventsRows.length).toBe(2); // one per minute bucket

      // Verify resource columns
      const t1Row = eventsRows.find(r => String(r.ts).includes('10:00'));
      expect(t1Row).toBeDefined();
      expect(Number(t1Row!.merge_count)).toBe(1);
      expect(Number(t1Row!.total_cpu_us)).toBe(80000);
      expect(Number(t1Row!.total_memory)).toBe(1048576);
      expect(Number(t1Row!.total_read_bytes)).toBe(200000);
    });

    it('only includes MergeParts events (not NewPart)', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesMerges(tf.clause), tf.params);
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      // T1 has 1 MergeParts + 1 NewPart for default.events — only MergeParts counted
      const t1Events = rows.filter(r =>
        String(r.lane_id) === 'default.events' && String(r.ts).includes('10:00'),
      );
      expect(t1Events.length).toBe(1);
      expect(Number(t1Events[0].merge_count)).toBe(1);
    });
  });

  describe('resourceLanesMergeTotals', () => {
    it('returns system-wide merge totals per minute', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesMergeTotals(tf.clause), tf.params);
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      expect(rows.length).toBeGreaterThan(0);

      // T1 has 2 MergeParts (default.events + analytics.metrics)
      const t1Row = rows.find(r => String(r.ts).includes('10:00'));
      expect(t1Row).toBeDefined();
      expect(Number(t1Row!.merge_count)).toBe(2);
      expect(Number(t1Row!.total_cpu_us)).toBe(80000 + 50000); // events + metrics
      expect(Number(t1Row!.total_memory)).toBe(1048576 + 524288);
    });

    it('does not include lane_id', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesMergeTotals(tf.clause), tf.params);
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      expect(rows[0]).not.toHaveProperty('lane_id');
    });
  });

  describe('resourceLanesTableMerges', () => {
    it('returns per-minute merge data for a specific table', async () => {
      const tf = buildSurfaceTimeFilter('event_time', { startTime: START, endTime: END });
      const sql = buildQuery(resourceLanesTableMerges(tf.clause), {
        database: 'default',
        table_name: 'events',
        ...tf.params,
      });
      const rows = await shadowAdapter.executeQuery<Record<string, unknown>>(sql);

      // default.events has merges at T1 and T2
      expect(rows.length).toBe(2);

      const t2Row = rows.find(r => String(r.ts).includes('10:01'));
      expect(t2Row).toBeDefined();
      expect(Number(t2Row!.merge_count)).toBe(1);
      expect(Number(t2Row!.total_cpu_us)).toBe(120000);
      expect(Number(t2Row!.total_memory)).toBe(2097152);
    });
  });

  // ── AnalyticsService methods ──────────────────────────────────────

  describe('AnalyticsService.getSystemResourceLanes', () => {
    it('returns correct shape with lanes, totals, and merges', async () => {
      const result = await service.getSystemResourceLanes({
        startTime: START,
        endTime: END,
        maxLanes: 10,
      });

      expect(result.level).toBe('system');
      expect(result.lanes.length).toBeGreaterThan(0);
      expect(result.totals.length).toBeGreaterThan(0);
      expect(result.merges).toBeDefined();
      expect(result.merges!.length).toBeGreaterThan(0);
      expect(result.mergeTotals).toBeDefined();
      expect(result.mergeTotals!.length).toBeGreaterThan(0);

      // Verify lane row shape
      const lane = result.lanes[0];
      expect(lane).toHaveProperty('ts');
      expect(lane).toHaveProperty('lane_id');
      expect(lane).toHaveProperty('lane_label');
      expect(lane).toHaveProperty('total_cpu_us');
      expect(lane).toHaveProperty('total_memory');
      expect(lane).toHaveProperty('total_selected_marks');

      // Verify merge row shape
      const merge = result.merges![0];
      expect(merge).toHaveProperty('ts');
      expect(merge).toHaveProperty('lane_id');
      expect(merge).toHaveProperty('merge_count');
      expect(merge).toHaveProperty('total_cpu_us');
      expect(merge).toHaveProperty('total_memory');

      // Verify totals row shape
      const total = result.totals[0];
      expect(total).toHaveProperty('ts');
      expect(total).toHaveProperty('total_cpu_us');
    });

    it('merge lane_ids match table names for system level', async () => {
      const result = await service.getSystemResourceLanes({
        startTime: START,
        endTime: END,
        maxLanes: 10,
      });

      const mergeLaneIds = [...new Set(result.merges!.map(m => m.lane_id))];
      expect(mergeLaneIds).toContain('default.events');
      expect(mergeLaneIds).toContain('analytics.metrics');
    });

    it('filters system tables by default', async () => {
      const result = await service.getSystemResourceLanes({
        startTime: START,
        endTime: END,
        maxLanes: 20,
      });

      const laneIds = [...new Set(result.lanes.map(l => l.lane_id))];
      expect(laneIds).not.toContain('system.query_log');
      expect(laneIds).not.toContain('_table_function.numbers');
      expect(laneIds).toContain('default.events');
    });

    it('includes system tables when excludeSystemTables is false', async () => {
      const result = await service.getSystemResourceLanes({
        startTime: START,
        endTime: END,
        maxLanes: 20,
        excludeSystemTables: false,
      });

      const laneIds = [...new Set(result.lanes.map(l => l.lane_id))];
      expect(laneIds).toContain('system.query_log');
    });
  });

  describe('AnalyticsService.getTableResourceLanes', () => {
    it('returns drill-down with query patterns and merge data', async () => {
      const result = await service.getTableResourceLanes({
        database: 'default',
        table: 'events',
        startTime: START,
        endTime: END,
        maxLanes: 10,
      });

      expect(result.level).toBe('table');
      expect(result.drillTable).toBe('default.events');
      expect(result.lanes.length).toBeGreaterThan(0);
      expect(result.totals.length).toBeGreaterThan(0);

      const laneIds = [...new Set(result.lanes.map(l => l.lane_id))];
      expect(laneIds.length).toBe(2); // hash 1001 + hash 2001

      // Merge data should be present with __merges__ lane_id
      expect(result.merges).toBeDefined();
      expect(result.merges!.length).toBeGreaterThan(0);
      const mergeLaneIds = [...new Set(result.merges!.map(m => m.lane_id))];
      expect(mergeLaneIds).toContain('__merges__');
    });
  });

  describe('AnalyticsService.getStressSurfaceData', () => {
    it('returns queries, inserts, and merges', async () => {
      const result = await service.getStressSurfaceData({
        database: 'default',
        table: 'events',
        startTime: START,
        endTime: END,
      });

      expect(result.table).toBe('default.events');
      expect(result.queries.length).toBe(3);
      expect(result.inserts.length).toBe(2);
      expect(result.merges.length).toBe(2);

      // Verify query row shape
      const q = result.queries[0];
      expect(q.query_count).toBe(1);
      expect(q.total_cpu_us).toBe(150000);

      // Verify insert row shape
      const ins = result.inserts[0];
      expect(ins.insert_count).toBe(1);
      expect(ins.inserted_rows).toBe(10000);
    });
  });

  describe('AnalyticsService.getPatternSurfaceData', () => {
    it('returns SELECT-only pattern breakdown', async () => {
      const result = await service.getPatternSurfaceData({
        database: 'default',
        table: 'events',
        startTime: START,
        endTime: END,
      });

      expect(result.length).toBeGreaterThan(0);
      const hashes = [...new Set(result.map(r => r.normalized_query_hash))];
      expect(hashes).toContain('1001');
      expect(hashes).not.toContain('2001');
    });
  });
});
