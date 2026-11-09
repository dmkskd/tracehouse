import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { TimelineService } from '../../services/timeline-service.js';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { buildQuery, tagQuery, utcDateTime } from '../../queries/builder.js';
import * as queries from '../../queries/timeline-queries.js';
import { GET_MERGE_HISTORY_BY_PART_NAME } from '../../queries/merge-queries.js';
import { HttpAdapter } from '../../adapters/http-adapter.js';
import { GrafanaAdapter } from '../../adapters/grafana-adapter.js';

// Runtime import keeps this cross-workspace test from pulling frontend files
// into the core library's declaration build (whose rootDir is core/src).
const frontendBufferPath = new URL('../../../../../frontend/src/components/timeline/timeline-navigator-buffer.ts', import.meta.url).href;
const { mergeTimelinePoints } = await import(frontendBufferPath);

/**
 * Regression reproduction for non-UTC server and column timezones.
 * Uses the production navigator SQL, service mapping, and frontend range filter.
 * Only the log source is substituted with deterministic minute samples, generated
 * from absolute epochs using the real server timezone (not session_timezone).
 * CH_TEST_URL can target the normal Docker server; otherwise starts Copenhagen.
 */
describe('Time Travel server timezone round trip', { tags: ['query-analysis'] }, () => {
  let ctx: TestClickHouseContext;

  beforeAll(async () => {
    ctx = await startClickHouse({ timezone: 'Europe/Copenhagen' });
    const result = await ctx.client.query({
      query: 'SELECT version() AS version, timezone() AS timezone', format: 'JSONEachRow',
    });
    console.info('Timezone reproduction server:', await result.json());
  }, 120_000);

  afterAll(async () => { if (ctx) await stopClickHouse(ctx); });

  it('HTTP strings and Grafana string/time fields preserve the same Copenhagen instant', async () => {
    const url = new URL(process.env.CH_TEST_URL ?? ctx.container!.getConnectionUrl());
    const http = new HttpAdapter({
      host: url.hostname, port: Number(url.port || 8123), secure: url.protocol === 'https:',
      user: decodeURIComponent(url.username || 'default'), password: decodeURIComponent(url.password),
      database: 'default', connect_timeout: 10, send_receive_timeout: 30,
    });
    try {
      const epoch = Date.parse('2026-10-25T01:30:00Z') / 1000;
      const sql = tagQuery(buildQuery(queries.SERVER_MEMORY_TIMESERIES, {
        start_time: utcDateTime(new Date(epoch * 1000)), end_time: utcDateTime(new Date(epoch * 1000)),
      }).replaceAll('{{cluster_aware:system.metric_log}}', fixture(epoch, 'Europe/Copenhagen')),
      'TraceHouse:TimeTravel:timezoneTest');
      const direct = await http.executeQuery<{ t: string; v: number }>(sql);
      const grafana = new GrafanaAdapter(async submittedSql => {
        const rows = await http.executeQuery<{ t: string; v: number }>(tagQuery(submittedSql, 'TraceHouse:TimeTravel:timezoneTest'));
        return [{ fields: [
          { name: 't', type: 'string', values: rows.map(row => row.t) },
          { name: 'numeric_time', type: 'time', values: rows.map(() => epoch * 1000) },
        ] }];
      });
      const framed = await grafana.executeQuery<{ t: string; numeric_time: string }>(sql);
      expect(direct).toHaveLength(1);
      expect(framed).toHaveLength(1);
      expect(framed[0].t).toBe(direct[0].t);
      for (const value of [direct[0].t, framed[0].t, framed[0].numeric_time]) {
        expect(Date.parse(value.replace(' ', 'T') + 'Z')).toBe(epoch * 1000);
      }
    } finally { await http.close(); }
  });

  // Typed datetime columns in these subqueries deliberately differ from the
  // server timezone. Epoch seeds distinguish the two occurrences of a DST hour.
  function fixture(epoch: number, timezone: string, parts = false): string {
    return `(SELECT
      toDateTime(${epoch}, '${timezone}') AS event_time,
      toDate(event_time) AS event_date,
      fromUnixTimestamp64Micro(toInt64(${epoch} * 1000000 + 123456), '${timezone}') AS event_time_microseconds,
      event_time - INTERVAL 10 SECOND AS query_start_time,
      'fixture-query' AS query_id, 'default' AS user, 'SELECT 1' AS query,
      'Select' AS query_kind, 'QueryFinish' AS type, toUInt64(10000) AS query_duration_ms,
      toUInt64(2097152) AS memory_usage, memory_usage AS peak_memory_usage,
      toUInt64(10000) AS duration_ms, toUInt32(0) AS error, 0 AS exception_code, '' AS exception,
      map('OSCPUVirtualTimeMicroseconds', toUInt64(100000), 'UserTimeMicroseconds', toUInt64(100000)) AS ProfileEvents,
      toUInt64(100) AS read_bytes, toUInt64(100) AS written_bytes,
      'default' AS database, 'events' AS table, 'part-fixture' AS part_name,
      part_name AS result_part_name, 'RegularMerge' AS merge_reason, 'Regular' AS merge_type,
      arrayJoin(${parts ? '[0, 1]' : '[0]'}) AS is_mutation,
      if(is_mutation = 1, 'MutatePart', 'MergeParts') AS event_type,
      toUInt64(2097152) AS CurrentMetric_MemoryTracking,
      toUInt64(100) AS ProfileEvent_NetworkSendBytes, toUInt64(200) AS ProfileEvent_NetworkReceiveBytes,
      toUInt64(300) AS ProfileEvent_OSReadBytes, toUInt64(400) AS ProfileEvent_OSWriteBytes,
      'CGroupUserTime' AS metric, toFloat64(0.25) AS value,
      toFloat64(10) AS elapsed, toFloat64(0.5) AS progress, toUInt8(1) AS is_initial_query,
      toUInt64(100) AS bytes_read_uncompressed, toUInt64(100) AS bytes_written_uncompressed,
      'partition' AS partition_id, toUInt64(1) AS rows, toUInt64(100) AS size_in_bytes,
      ['old-part'] AS merged_from, toUInt64(100) AS bytes_uncompressed, toUInt64(1) AS read_rows,
      'Horizontal' AS merge_algorithm, 'default' AS disk_name, '/fixture' AS path_on_disk
    )`;
  }

  for (const timezone of ['UTC', 'Europe/Copenhagen', 'America/New_York']) {
    for (const instant of [
      '2026-11-05T12:00:00Z', '2026-07-05T12:00:00Z',
      '2026-10-25T00:30:00Z', '2026-10-25T01:30:00Z', // repeated Copenhagen hour
      '2026-03-29T00:59:00Z', '2026-03-29T01:00:00Z', // spring clock jump
      '2026-11-06T00:30:00Z', // previous local date in New York
    ]) {
      it(`${timezone}: metric and per-host projections preserve ${instant}`, async () => {
        const epoch = Date.parse(instant) / 1000;
        for (const template of [
          queries.SERVER_MEMORY_TIMESERIES, queries.SERVER_CPU_TIMESERIES,
          queries.SERVER_NETWORK_TIMESERIES, queries.SERVER_DISK_IO_TIMESERIES,
          queries.CLUSTER_MEMORY_TIMESERIES, queries.CLUSTER_CPU_TIMESERIES,
          queries.CLUSTER_NETWORK_TIMESERIES, queries.CLUSTER_DISK_IO_TIMESERIES,
          queries.CPU_SPIKE_TIMESERIES,
          queries.NAVIGATOR_MEMORY_TIMESERIES, queries.NAVIGATOR_CPU_TIMESERIES,
          queries.NAVIGATOR_NETWORK_TIMESERIES, queries.NAVIGATOR_DISK_TIMESERIES,
        ]) {
          const sql = buildQuery(template, {
            start_time: utcDateTime(instant), end_time: utcDateTime(instant), bucket_seconds: 60,
          }).replaceAll('{hostname_filter}', '').replace(/\{\{cluster_aware:system\.[a-z_]+\}\}/g, fixture(epoch, timezone));
          const rows = await ctx.rawAdapter.executeQuery<{ t: string }>(sql);
          expect(rows, sql).toHaveLength(1);
          expect(Date.parse(rows[0].t.replace(' ', 'T') + 'Z'), sql).toBe(Date.parse(instant));
        }
      });

      it(`${timezone}: historical operations survive clamping at ${instant}`, async () => {
        const epoch = Date.parse(instant) / 1000;
        const errors: unknown[] = [];
        const adapter: IClickHouseAdapter = {
          async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
            // Capacity metadata isn't relevant to the timestamp round trip.
            if (!/TimeTravel:(serverMemory|queries|queryCount|merges|mergeStats|mergeProfile|mutations|mutationCount|mutationProfile) \*\//.test(sql)) return [];
            const replaced = sql.replace(/\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
              (_, table: string) => fixture(epoch, timezone, table === 'part_log'));
            try { return await ctx.rawAdapter.executeQuery<T>(replaced); }
            catch (error) { errors.push(error); throw error; }
          },
        };
        const timeline = await new TimelineService(adapter).getTimeline({
          timestamp: new Date(instant), windowSeconds: 5, includeRunning: false,
        });
        expect(errors).toEqual([]);
        expect(timeline.queries).toHaveLength(1);
        expect(timeline.merges).toHaveLength(1);
        expect(timeline.mutations).toHaveLength(1);
        expect(timeline.queries[0].start_time).toBe(new Date((epoch - 10) * 1000).toISOString());
        expect(timeline.queries[0].end_time).toBe(new Date(epoch * 1000).toISOString());
        expect(Date.parse(timeline.queries[0].points[0].t.replace(' ', 'T') + 'Z')).toBe((epoch - 5) * 1000);
        for (const operation of [...timeline.merges, ...timeline.mutations]) {
          expect(operation.start_time).toBe(new Date((epoch - 5) * 1000).toISOString());
          expect(operation.end_time).toBe(new Date(epoch * 1000).toISOString());
        }
        for (const [eventType, operation] of [['MergeParts', timeline.merges[0]], ['MutatePart', timeline.mutations[0]]] as const) {
          expect(operation.operation_id).toBe(JSON.stringify([eventType, instant.replace('T', ' ').replace('Z', '.123456')]));
        }
        // Time Travel opens details by database/table/part, independently of
        // its internal operation_id. Keep that lookup working after normalization.
        const detailSql = buildQuery(GET_MERGE_HISTORY_BY_PART_NAME, {
          database: 'default', table: 'events', part_name: timeline.merges[0].part_name,
        }).replaceAll('{{cluster_aware:system.part_log}}', fixture(epoch, timezone));
        const details = await ctx.rawAdapter.executeQuery<{ part_name: string; event_time_us: string }>(detailSql);
        expect(details).toHaveLength(1);
        expect(details[0].part_name).toBe(timeline.merges[0].part_name);
        expect(details[0].event_time_us).toBe(instant.replace('T', ' ').replace('Z', '.123456Z'));
      });
    }

    it(`${timezone}: running query, merge and mutation projections use UTC`, async () => {
      for (const template of [queries.RUNNING_QUERIES_TIMELINE, queries.RUNNING_MERGES_TIMELINE]) {
        const sql = buildQuery(template, { activity_limit: 10 })
          .replaceAll('{query_order_by}', 'memory_usage')
          .replace(/\{\{cluster_aware:system\.merges\}\}|system\.processes/g, fixture(1793880000, timezone, true));
        const rows = await ctx.rawAdapter.executeQuery<{ qst?: string; merge_start?: string; expected_epoch: number }>(
          `SELECT *, toUnixTimestamp(now() - INTERVAL 10 SECOND) AS expected_epoch FROM (${sql})`,
        );
        expect(rows).toHaveLength(2);
        for (const row of rows) {
          expect(Date.parse((row.qst ?? row.merge_start)!.replace(' ', 'T') + 'Z') / 1000).toBe(Number(row.expected_epoch));
        }
      }
    });
  }

  for (const metric of ['memory', 'cpu', 'network', 'disk'] as const) {
    it(`${metric}: real system log points remain inside the requested window`, async () => {
      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
      const table = metric === 'cpu' ? 'asynchronous_metric_log' : 'metric_log';
      const result = await ctx.client.query({
        query: `SELECT toUnixTimestamp(max(event_time)) AS last_epoch FROM system.${table}`,
        format: 'JSONEachRow',
      });
      const [row] = await result.json<{ last_epoch: number }>();
      expect(Number(row.last_epoch)).toBeGreaterThan(0);
      const endMs = (Math.floor(Number(row.last_epoch) / 60) + 1) * 60_000;
      const startMs = endMs - 5 * 60_000;
      const data = await new TimelineService(ctx.adapter).getNavigatorMetric({
        startTime: new Date(startMs), endTime: new Date(endMs), metric, bucketSeconds: 60,
      });
      expect(data.points.length).toBeGreaterThan(0);
      const visible = mergeTimelinePoints([], data.points.map(point => ({
        t: point.t, v: point.average_v,
      })), { startMs, endMs });
      console.info({ realSystemLog: table, metric, returned: data.points.length, visible: visible.length });
      expect(visible).toHaveLength(data.points.length);
    });
  }

  for (const [season, end] of [
    ['winter', '2026-11-05T12:00:00Z'],
    ['summer', '2026-07-05T12:00:00Z'],
  ] as const) {
    for (const metric of ['memory', 'cpu', 'network', 'disk'] as const) {
      for (const minutes of [5, 360]) {
        it(`${season}: ${metric}, ${minutes} minute window preserves all sample instants`, async () => {
          const endMs = Date.parse(end);
          const startMs = endMs - minutes * 60_000;
          const fixture = `(
            SELECT
              toDateTime(toUInt32(${startMs / 1000} + number * 60), timezone()) AS event_time,
              toUInt64(1048576) AS CurrentMetric_MemoryTracking,
              toUInt64(100) AS ProfileEvent_NetworkSendBytes,
              toUInt64(200) AS ProfileEvent_NetworkReceiveBytes,
              toUInt64(300) AS ProfileEvent_OSReadBytes,
              toUInt64(400) AS ProfileEvent_OSWriteBytes,
              'CGroupUserTime' AS metric, toFloat64(0.25) AS value
            FROM numbers(${minutes})
          )`;
          const adapter: IClickHouseAdapter = {
            executeQuery: sql => ctx.rawAdapter.executeQuery(sql
              .replaceAll('{{cluster_aware:system.metric_log}}', fixture)
              .replaceAll('{{cluster_aware:system.asynchronous_metric_log}}', fixture)),
          };
          const data = await new TimelineService(adapter).getNavigatorMetric({
            startTime: new Date(startMs), endTime: new Date(endMs), metric, bucketSeconds: 60,
          });
          // Confirms the SQL filter found the right rows before UI interpretation.
          expect(data.points).toHaveLength(minutes);
          const points = data.points.map(point => ({ t: point.t, v: point.average_v }));
          const visible = mergeTimelinePoints([], points, { startMs, endMs });
          console.info({ season, metric, minutes, returned: points.length,
            visible: visible.length, firstReturned: points[0].t,
            expectedFirstUTC: new Date(startMs).toISOString() });
          // Correct behavior: changing the server timezone cannot move an instant.
          expect(new Date(points[0].t.replace(' ', 'T') + 'Z').getTime()).toBe(startMs);
          expect(visible).toHaveLength(minutes);
        });
      }
    }
  }
});
