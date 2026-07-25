import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { MonitoringCapabilitiesService } from '../../services/monitoring-capabilities.js';
import { QueryExecutionAnalysisService } from '../../services/query-execution-analysis.js';
import { sourceTag, TAB_ANALYTICS } from '../../queries/source-tags.js';
import {
  EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION,
  EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION_LABEL,
} from '../../types/execution-analysis.js';
import { isClickHouseVersionAtLeast } from '../../utils/clickhouse-version.js';
import {
  startClickHouse,
  stopClickHouse,
  type TestClickHouseContext,
} from './setup/clickhouse-container.js';

const CONTAINER_TIMEOUT = 120_000;

describe('EXPLAIN ANALYZE integration', { tags: ['query-analysis'] }, () => {
  let ctx: TestClickHouseContext;

  beforeAll(async () => {
    ctx = await startClickHouse();
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) await stopClickHouse(ctx);
  }, 30_000);

  it('detects support and executes runtime query-plan analysis when available', async () => {
    const capabilities = await new MonitoringCapabilitiesService(ctx.adapter).probe();
    const capability = capabilities.capabilities.find(item => item.id === 'explain_analyze');

    if (!isClickHouseVersionAtLeast(
      capabilities.serverVersion,
      EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION.major,
      EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION.minor,
    )) {
      expect(capability).toMatchObject({
        available: false,
        category: 'profiling',
        source: `server version ≥ ${EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION_LABEL}`,
      });
      return;
    }

    expect(capability).toMatchObject({
      available: true,
      category: 'profiling',
      source: `server version ≥ ${EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION_LABEL}`,
    });

    const result = await new QueryExecutionAnalysisService(ctx.adapter).analyze(
      'SELECT number % 10 AS bucket, count() FROM numbers_mt(100000) GROUP BY bucket',
      sourceTag(TAB_ANALYTICS, 'queryExecutionAnalysisTest'),
      { processors: true },
    );

    expect(result.output).toContain('Query summary:');
    expect(result.output).toContain('Peak memory:');
    expect(result.output).toContain('I/O: rows');
    expect(result.output).toContain('parallelism');
    expect(result.output).toContain('Time per processor');
  });
});
