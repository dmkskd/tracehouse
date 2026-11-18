import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { ResourceAttributionService, resourceSnapshotAt } from '../../services/resource-attribution-service.js';
import type { IClickHouseAdapter } from '../../adapters/types.js';

const startMs = Date.parse('2026-11-18T10:00:00Z');
const sampleSelection = { availability: { processesHistory: true, queryMetricLog: false } };

/** Production SQL against deterministic log sources, executed by a real ClickHouse server. */
describe('whole-population resource attribution', () => {
  let ctx: TestClickHouseContext;
  let service: ResourceAttributionService;
  const queryLog = `(SELECT toDate(fromUnixTimestamp64Milli(${startMs})) AS event_date,
    fromUnixTimestamp64Milli(${startMs}) AS query_start_time_microseconds,
    concat('q',toString(number)) AS query_id, '' AS initial_query_id, 'default' AS user,
    'SELECT fixture' AS query, 'Select' AS query_kind, ['db.table'] AS tables,
    'QueryFinish' AS type, toUInt64(10000) AS query_duration_ms, toUInt64(999999) AS memory_usage,
    toUInt32(if(number=1,735,0)) AS exception_code, toUInt64(123) AS normalized_query_hash,
    map('OSCPUVirtualTimeMicroseconds',toUInt64(10000000),'NetworkSendBytes',toUInt64(1000),'OSReadBytes',toUInt64(2000)) AS ProfileEvents
    FROM numbers(250))`;
  const partLog = `(SELECT toDate(fromUnixTimestamp64Milli(${startMs})) AS event_date,
    fromUnixTimestamp64Milli(${startMs+10000}) AS event_time_microseconds,
    'db' AS database,'table' AS table,'part' AS part_name,'MergeParts' AS event_type,
    toUInt64(10000) AS duration_ms,toUInt64(999999) AS peak_memory_usage,toUInt32(0) AS error,
    map('OSCPUVirtualTimeMicroseconds',toUInt64(10000000)) AS ProfileEvents FROM numbers(0))`;
  const samples = `(SELECT fromUnixTimestamp64Milli(${startMs}+(number+1)*1000) AS sample_time,
    'q0' AS query_id,'' AS initial_query_id,'SELECT fixture' AS query,toUInt64(if(number=0,100,300)) AS memory_usage,
    map('OSCPUVirtualTimeMicroseconds',toUInt64(if(number=0,1000000,4000000))) AS ProfileEvents,
    toUInt64(0) AS read_bytes,toUInt64(0) AS written_bytes FROM numbers(2))`;

  beforeAll(async () => {
    ctx = await startClickHouse();
    const adapter: IClickHouseAdapter = { executeQuery: (sql, options) => ctx.rawAdapter.executeQuery(
      sql.replaceAll('{{cluster_aware:system.query_log}}',queryLog)
        .replaceAll('{{cluster_aware:system.part_log}}',partLog)
        .replaceAll('{{cluster_aware:tracehouse.processes_history}}',samples)
        .replaceAll('{{cluster_aware:system.processes}}','system.processes')
        .replaceAll('{{cluster_aware:system.merges}}','system.merges') as typeof sql, options) };
    service = new ResourceAttributionService(adapter);
  }, 120000);
  afterAll(async () => { if (ctx) await stopClickHouse(ctx); });
  const request = { startMs, endMs: startMs+10000, includeRunning: false, mode: 'cpu' as const };

  it('includes all 250 executions and clips accumulated CPU at range boundaries', async () => {
    const full = await service.getRange(request);
    expect(full.groups).toHaveLength(1);
    expect(full.groups[0]).toMatchObject({ operationCount: 250, estimatedCount: 250, value: 2500000000, failedCount: 1, failureValue: 10000000 });
    const clipped = await service.getRange({ ...request, startMs: startMs+1000, endMs: startMs+3000 });
    expect(clipped.groups[0].value).toBe(500000000);
    const disjoint = await service.getRange({ ...request, startMs: startMs+10000, endMs: startMs+11000 });
    expect(disjoint.groups).toEqual([]);
  });

  it('joins signed snapshot timestamps to samples and uses execution averages only in gaps', async () => {
    const window = await service.getSnapshots({ ...request, sampleSelection });
    expect(resourceSnapshotAt(window,startMs+1999).groups[0]).toMatchObject({ value: 252000000, sampledCount: 1, estimatedCount: 249, operationCount: 250 });
    expect(resourceSnapshotAt(window,startMs+4000).groups[0]).toMatchObject({ value: 250000000, sampledCount: 0, estimatedCount: 250 });
    const range = await service.getRange({ ...request, sampleSelection });
    expect(range.groups[0]).toMatchObject({ value: 2502000000, sampledCount: 1, estimatedCount: 250, sampledMs: 1000, estimatedMs: 2499000 });
  });

  it('preserves unknown memory, integrates observed occupancy, and never substitutes execution peak memory', async () => {
    const missing = await service.getRange({ ...request, mode: 'memory' });
    expect(missing.groups[0]).toMatchObject({ value: 0, unavailableCount: 250, estimatedCount: 0 });
    const range = await service.getRange({ ...request, mode: 'memory', sampleSelection });
    expect(range.groups[0]).toMatchObject({ value: 900, sampledCount: 1, unavailableCount: 249, sampledMs: 4000, missingMs: 2496000 });
    const window = await service.getSnapshots({ ...request, mode: 'memory', sampleSelection });
    expect(resourceSnapshotAt(window,startMs+2000).groups[0]).toMatchObject({ value: 300, sampledCount: 1, unavailableCount: 249 });
    expect(resourceSnapshotAt(window,startMs+5000).groups[0]).toMatchObject({ value: 0, unavailableCount: 250 });
  });

  it('limits details without changing totals and preserves local execution identities', async () => {
    const summary = await service.getRange({ ...request, sampleSelection });
    const details = await service.getDetails({ mode: 'cpu', sampleSelection, selection: summary.selection, groups: summary.groups, limit: 5 });
    expect(details.operations).toHaveLength(5);
    expect(details.operations[0]).toMatchObject({ value: 12000000, basis: 'mixed', source: { query_id: 'q0' } });
    expect(new Set(details.operations.map(row => row.id)).size).toBe(5);
    expect((await service.getRange({ ...request, sampleSelection })).groups).toEqual(summary.groups);
  });

  it('accumulates network and disk bytes without converting them to average rates', async () => {
    expect((await service.getRange({ ...request, mode: 'network' })).groups[0].value).toBe(250000);
    expect((await service.getRange({ ...request, mode: 'disk' })).groups[0].value).toBe(500000);
    const network = await service.getSnapshots({ ...request, mode: 'network' });
    expect(resourceSnapshotAt(network,startMs+1000).groups[0].value).toBe(25000);
  });

  it('supports live-source unions without duplicating completed executions', async () => {
    const summary = await service.getRange({ ...request, includeRunning: true });
    expect(summary.groups[0]).toMatchObject({ operationCount: 250, value: 2500000000 });
  });

  it('applies server and query-hash filters before aggregation', async () => {
    expect((await service.getRange({ ...request, hostname: 'nonexistent-host' })).groups).toEqual([]);
    expect((await service.getRange({ ...request, queryHashOnly: true, normalizedQueryHash: '456' })).groups).toEqual([]);
    expect((await service.getRange({ ...request, queryHashOnly: true, normalizedQueryHash: '123' })).groups[0].operationCount).toBe(250);
  });
});
