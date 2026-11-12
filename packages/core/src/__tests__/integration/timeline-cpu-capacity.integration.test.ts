import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { TimelineService } from '../../services/timeline-service.js';
import type { IClickHouseAdapter, TaggedQuery } from '../../adapters/types.js';
import { tagQuery } from '../../queries/builder.js';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';

describe('Time Travel CPU metric formats', { tags: ['observability'] }, () => {
  let ctx: TestClickHouseContext;
  beforeAll(async () => { ctx = await startClickHouse(); }, 120_000);
  afterAll(async () => { if (ctx) await stopClickHouse(ctx); });

  for (const format of ['legacy', 'keyed', 'scalar'] as const) {
    const keyed = format === 'keyed';
    for (const currentFallback of [false, true]) {
      it(`preserves 12 cores of usage with ${format} metrics, ${currentFallback ? 'current' : 'historical'} capacity`, async () => {
        const adapter: IClickHouseAdapter = {
          async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
            if (sql.includes('TimeTravel:cpuMetricSchema')) {
              return ['asynchronous_metric_log', 'asynchronous_metrics'].map(table => ({ table, has_keys: Number(keyed) })) as unknown as T[];
            }
            if (sql.includes('TimeTravel:serverCpu')) {
              return [{ t: '2026-11-12 16:35:00', v: 12_000_000, interval_ms: 1000 }] as unknown as T[];
            }
            if (sql.includes('TimeTravel:cpuCapacityHistorical') || sql.includes('TimeTravel:cpuCapacityCurrent')) {
              if (currentFallback && sql.includes('TimeTravel:cpuCapacityHistorical')) return [];
              const source = keyed
                ? sql.includes('TimeTravel:cpuCapacityCurrent')
                  ? `(SELECT 'OSUserTimeCPU' AS metric, toFloat64('nan') AS value,
                      mapFromArrays(arrayMap(n -> toString(n), range(14)), arrayMap(n -> toFloat64(0.5), range(14))) AS key_values)`
                  : `(SELECT 'OSUserTimeCPU' AS metric, toString(number % 14) AS key, toFloat64(0.5) AS value,
                      toDateTime('2026-11-12 16:35:00', 'UTC') AS event_time FROM numbers(28))`
                : format === 'scalar'
                  ? `(SELECT 'OSUserTimeCPU' AS metric, toFloat64(0.5) AS value,
                      toDateTime('2026-11-12 16:35:00', 'UTC') AS event_time)`
                : `(SELECT concat('OSUserTimeCPU', toString(number)) AS metric, toFloat64(0.5) AS value,
                    toDateTime('2026-11-12 16:35:00', 'UTC') AS event_time FROM numbers(14))`;
              return ctx.rawAdapter.executeQuery<T>(tagQuery(sql.replace(
                /\{\{cluster_aware:system\.asynchronous_metric(?:_log|s)\}\}/g, source,
              ), 'TraceHouse:TimeTravel:cpuCapacityRegression'));
            }
            return [];
          },
        };
        const result = await new TimelineService(adapter).getTimeline({
          timestamp: new Date('2026-11-12T16:35:00Z'), windowSeconds: 60,
          activeMetric: 'cpu', includeRunning: false,
        });
        expect(result.total_cpu_cores).toBe(format === 'scalar' ? undefined : 14);
        expect(result.cpu_capacity_complete).toBe(format !== 'scalar');
        expect(result.server_cpu[0]?.v).toBe(12_000_000);
      });
    }
  }
});
