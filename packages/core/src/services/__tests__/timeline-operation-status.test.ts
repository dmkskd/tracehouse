import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { buildOperationRows } from '../../utils/timeline-operations.js';
import { TimelineService } from '../timeline-service.js';

describe('TimelineService operation status', () => {
  it('preserves part_log errors through to merge and mutation rows', async () => {
    const detailQueries: string[] = [];
    const adapter: IClickHouseAdapter = {
      async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
        const merge = sql.includes('source:TraceHouse:TimeTravel:merges */');
        const mutation = sql.includes('source:TraceHouse:TimeTravel:mutations */');
        if (!merge && !mutation) return [];
        detailQueries.push(sql);
        return [0, '241'].map((error, index) => ({
          part_name: `part-${index}`, host: 'node-1', tbl: 'default.events',
          peak_memory_usage: 10_000_000, duration_ms: 1000, error,
          merge_start: '2026-10-27 12:00:00', merge_end: '2026-10-27 12:00:01',
          mut_start: '2026-10-27 12:00:00', mut_end: '2026-10-27 12:00:01',
        })) as unknown as T[];
      },
    };
    const timeline = await new TimelineService(adapter).getTimeline({
      timestamp: new Date('2026-10-27T12:00:00Z'), windowSeconds: 60, includeRunning: false,
    });
    expect(detailQueries).toHaveLength(2);
    for (const sql of detailQueries) expect(sql).toMatch(/\berror\s*,/);
    expect(timeline.merges.map(merge => merge.error)).toEqual([0, 241]);
    expect(timeline.mutations.map(mutation => mutation.error)).toEqual([0, 241]);
    const rows = buildOperationRows(timeline, {
      metricMode: 'memory', sortField: 'metric', sortDir: 'desc',
      scope: 'window', pinnedMs: null, zoomRange: null,
    });
    expect(rows.map(row => row.failed)).toEqual([false, true, false, true]);
  });
});
