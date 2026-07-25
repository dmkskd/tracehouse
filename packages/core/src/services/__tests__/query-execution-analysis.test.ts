import { describe, expect, it, vi } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { QueryExecutionAnalysisService } from '../query-execution-analysis.js';

function adapter(lines: string[] = ['Query summary:', '  Time: 1.00 ms']): IClickHouseAdapter {
  return {
    supportsExplicitQueryId: true,
    executeQuery: vi.fn(),
    executeRawQuery: vi.fn().mockResolvedValue(lines),
  };
}

describe('QueryExecutionAnalysisService', { tags: ['query-analysis'] }, () => {
  it('executes a tagged EXPLAIN ANALYZE and preserves the evolving raw output', async () => {
    const mock = adapter();
    const service = new QueryExecutionAnalysisService(mock);

    const result = await service.analyze(
      'SELECT count() FROM system.numbers LIMIT 10;',
      'TraceHouse:Analytics:queryExecutionAnalysis',
      { queryId: 'analysis-1' },
    );

    expect(result).toEqual({
      kind: 'explain_analyze',
      query: 'SELECT count() FROM system.numbers LIMIT 10',
      output: 'Query summary:\n  Time: 1.00 ms',
      processors: false,
      queryId: 'analysis-1',
    });
    expect(mock.executeRawQuery).toHaveBeenCalledWith(
      expect.stringMatching(/^EXPLAIN ANALYZE SELECT count\(\)/),
      undefined,
      { queryId: 'analysis-1' },
    );
    expect(mock.executeRawQuery).toHaveBeenCalledWith(
      expect.stringContaining('source:TraceHouse:Analytics:queryExecutionAnalysis'),
      undefined,
      { queryId: 'analysis-1' },
    );
  });

  it('opts into per-processor timing without changing the default', async () => {
    const mock = adapter();
    const service = new QueryExecutionAnalysisService(mock);

    const result = await service.analyze('SELECT 1', 'TraceHouse:Queries:test', {
      processors: true,
      database: 'analytics',
    });

    expect(result.processors).toBe(true);
    expect(mock.executeRawQuery).toHaveBeenCalledWith(
      expect.stringMatching(/^EXPLAIN ANALYZE processors = 1 SELECT 1/),
      'analytics',
      undefined,
    );
  });

  it('rejects empty input before reaching ClickHouse', async () => {
    const mock = adapter();
    const service = new QueryExecutionAnalysisService(mock);

    await expect(service.analyze('  ; ', 'TraceHouse:Queries:test')).rejects.toThrow(
      'A query is required',
    );
    expect(mock.executeRawQuery).not.toHaveBeenCalled();
  });

  it('rejects non-SELECT statements before reaching ClickHouse', async () => {
    const mock = adapter();
    const service = new QueryExecutionAnalysisService(mock);

    await expect(
      service.analyze('INSERT INTO events SELECT * FROM staging', 'TraceHouse:Queries:test'),
    ).rejects.toThrow('only available for SELECT');
    expect(mock.executeRawQuery).not.toHaveBeenCalled();
  });

  it('accepts commented and CTE-based SELECT statements', async () => {
    const mock = adapter();
    const service = new QueryExecutionAnalysisService(mock);

    await service.analyze(
      '/* generated */ WITH source AS (SELECT 1) SELECT * FROM source',
      'TraceHouse:Queries:test',
    );

    expect(mock.executeRawQuery).toHaveBeenCalledOnce();
  });

  it('reports adapters that cannot return raw plans', async () => {
    const mock: IClickHouseAdapter = { executeQuery: vi.fn() };
    const service = new QueryExecutionAnalysisService(mock);

    await expect(service.analyze('SELECT 1', 'TraceHouse:Queries:test')).rejects.toThrow(
      'not supported by this connection adapter',
    );
  });
});
