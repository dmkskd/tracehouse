import { describe, expect, it, vi } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { buildResourceAttributionSQL } from '../../queries/resource-attribution-queries.js';
import { ResourceAttributionService, resourceSnapshotAt } from '../resource-attribution-service.js';

const startMs = Date.parse('2026-11-18T10:00:00Z');
const request = { mode: 'cpu' as const, startMs, endMs: startMs+60000, snapshots: false, mergeSamplesAvailable: false };

describe('resource aggregation request boundaries', () => {
  it('rejects invalid windows, hash expressions, and excessive detail requests before execution', () => {
    for (const change of [{ startMs: NaN }, { endMs: startMs }, { endMs: startMs+86400001 },
      { snapshots: true, startMs: startMs+1 }, { snapshots: true, endMs: startMs+602000 },
      { normalizedQueryHash: '123 OR 1=1' }, { details: { groups: [], limit: 5 } },
      { details: { groups: [{ kind: 'query' as const, activity: 'SELECT', tables: [], tableState: 'none' as const }], limit: 10001 } }])
      expect(() => buildResourceAttributionSQL({ ...request, ...change })).toThrow();
  });
  it('uses one unbounded aggregate statement, and applies LIMIT only to expanded details', () => {
    expect(buildResourceAttributionSQL(request)).not.toMatch(/\bLIMIT\b/i);
    const sql = buildResourceAttributionSQL({ ...request, details: { groups: [{ kind: 'query', activity: 'SELECT', tables: ["db.x' OR 1=1 --"], tableState: 'recorded' }], limit: 5 } });
    expect(sql.match(/\bLIMIT\b/g)).toHaveLength(1);
    expect(sql).toMatch(/FROM evaluated WHERE/);
    expect(sql).toMatch(/LIMIT 5$/);
    expect(sql).toContain("db.x\\' OR 1=1 --");
  });
  it('does not recursively expand distributed operation scans or read samples twice', () => {
    const sql = buildResourceAttributionSQL({ ...request, includeRunning: true,
      sampleSelection: { availability: { processesHistory: false, queryMetricLog: true } } });
    expect(sql.match(/FROM operations\b/g)).toHaveLength(1);
    expect(sql.match(/FROM query_samples\b/g)).toHaveLength(1);
    expect(sql).not.toContain('SELECT local_id FROM operations');
    expect(sql).not.toContain('SELECT hostname,local_id FROM logged');
    expect(sql).toContain('groupBitmapState');
    expect(sql).not.toContain('initial_query_id');
  });
  it('retries missing optional sample sources with an explicit warning', async () => {
    const executeQuery = vi.fn().mockRejectedValueOnce(new Error('UNKNOWN_TABLE tracehouse.processes_history')).mockResolvedValue([]);
    const service = new ResourceAttributionService({ executeQuery } as IClickHouseAdapter);
    const result = await service.getRange({ ...request, sampleSelection: { availability: { processesHistory: true, queryMetricLog: false } } });
    expect(executeQuery).toHaveBeenCalledTimes(2);
    expect(executeQuery.mock.calls[1][0]).not.toContain('tracehouse.processes_history');
    expect(result.warnings).toHaveLength(1);
    expect(result.warnings[0]).toContain('execution averages');
  });
  it('propagates query and transport errors instead of reporting a partial total', async () => {
    const executeQuery = vi.fn().mockRejectedValue(new Error('NO_COMMON_TYPE'));
    const service = new ResourceAttributionService({ executeQuery } as IClickHouseAdapter);
    await expect(service.getSnapshots(request)).rejects.toThrow('NO_COMMON_TYPE');
    expect(executeQuery).toHaveBeenCalledTimes(1);
  });
  it('looks up one-second snapshots without executing another query, including empty seconds', async () => {
    const executeQuery = vi.fn().mockResolvedValue([]);
    const window = await new ResourceAttributionService({ executeQuery } as IClickHouseAdapter).getSnapshots(request);
    for (let offset=0;offset<60000;offset+=37) {
      const summary = resourceSnapshotAt(window,startMs+offset);
      expect(summary.selection).toEqual({ kind: 'instant', atMs: startMs+Math.floor(offset/1000)*1000 });
      expect(summary.groups).toEqual([]);
    }
    expect(executeQuery).toHaveBeenCalledTimes(1);
    expect(() => resourceSnapshotAt(window,startMs-1)).toThrow('outside');
    expect(() => resourceSnapshotAt(window,startMs+60000)).toThrow('outside');
  });
});
