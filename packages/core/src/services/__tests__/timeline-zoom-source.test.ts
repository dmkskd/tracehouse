import { describe, expect, it } from 'vitest';
import { TimelineService } from '../timeline-service.js';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import type { MemoryTimeline } from '../../types/timeline.js';

function timeline(): MemoryTimeline {
  return {
    window_start: '2026-09-18 12:00:00', window_end: '2026-09-18 12:00:02', target: 'all',
    server_memory: [], server_cpu: [], server_network_send: [], server_network_recv: [], server_disk_read: [], server_disk_write: [],
    server_total_ram: 1, cpu_cores: 1, host_count: 1,
    queries: [{ query_id: 'root', label: 'root', user: 'default', peak_memory: 10, duration_ms: 2000, cpu_us: 2000000, net_send: 2000, net_recv: 0, disk_read: 4000, disk_write: 0, start_time: '2026-09-18 12:00:00', end_time: '2026-09-18 12:00:02', points: [] }],
    merges: [], mutations: [], query_count: 1, merge_count: 0, merge_peak_total: 0, mutation_count: 0,
  };
}

describe('Time Travel zoom source propagation', () => {
  it('uses interval work once, averages sub-second samples, and sums independent local executions', async () => {
    const baseMs = Date.parse('2026-09-18T12:00:00Z');
    const calls: string[] = [];
    const rows = [250, 500].flatMap(offset => [1, 2].map(factor => ({
      query_id: 'root', sample_query_id: factor === 1 ? 'root' : 'child', hostname: 'host',
      ts_ms: String(baseMs + offset), dt: '0.25', memory_usage: String(factor * 10),
      cpu_us: String(factor * 250000), net_send_bytes: String(factor * 250), net_recv_bytes: '0',
    })));
    const adapter: IClickHouseAdapter = {
      async executeQuery<T extends Record<string, unknown>>(sql: string) {
        calls.push(sql);
        return (sql.includes('merges_history') ? [] : rows) as unknown as T[];
      },
    };
    const input = timeline();
    const enriched = await new TimelineService(adapter).getZoomData(input, baseMs, baseMs + 2000, 'host', {
      availability: { processesHistory: true, queryMetricLog: true }, preference: 'query_metric_log',
    });
    expect(calls.some(sql => sql.includes('system.query_metric_log'))).toBe(true);
    expect(calls.some(sql => sql.includes('tracehouse.processes_history'))).toBe(false);
    expect(enriched.queries[0].zoomSamples).toEqual([{ ms: baseMs, memory: 30, cpu_cores: 3, net_rate: 3000, disk_rate: 0 }]);
    expect(enriched.queries[0].zoomMissing).toEqual(['disk']);
    expect(enriched.queries[0].disk_read).toBe(4000);
    expect(input.queries[0].zoomSamples).toBeUndefined();
  });

  it('does not retain old query samples when neither query source is available', async () => {
    const input = timeline();
    input.queries[0].zoomSamples = [{ ms: 1, memory: 1, cpu_cores: 1, net_rate: 1, disk_rate: 1 }];
    const calls: string[] = [];
    const adapter: IClickHouseAdapter = { async executeQuery() { return []; } };
    adapter.executeQuery = async (sql: string) => { calls.push(sql); return []; };
    const result = await new TimelineService(adapter).getZoomData(input, Date.parse('2026-09-18T12:00:00Z'), Date.parse('2026-09-18T12:00:02Z'), null, {
      availability: { processesHistory: false, queryMetricLog: false },
    });
    expect(calls).toHaveLength(1);
    expect(calls[0]).toContain('merges_history');
    expect(result.queries[0].zoomSamples).toBeUndefined();
  });
});
