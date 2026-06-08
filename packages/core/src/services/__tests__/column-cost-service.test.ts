import { describe, expect, it, vi } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { ColumnCostService } from '../column-cost-service.js';

describe('ColumnCostService', { tags: ['query-analysis'] }, () => {
  it('runs client analysis through tagged generated SQL and calculates percentages', async () => {
    const executeQuery = vi.fn()
      .mockResolvedValueOnce([{ name: 'a', type: 'String' }, { name: 'b', type: 'String' }])
      .mockResolvedValueOnce([{ __bytes_a: 10, __bytes_b: 30 }]);
    const adapter: IClickHouseAdapter = { executeQuery };
    const service = new ColumnCostService(adapter);

    const result = await service.runClientAnalysis('SELECT a, b FROM t');

    expect(result.total).toBe(40);
    expect(result.costs).toEqual([
      { column: 'b', bytes: 30, pct: 75 },
      { column: 'a', bytes: 10, pct: 25 },
    ]);
    expect(executeQuery).toHaveBeenCalledWith(expect.stringContaining('source:TraceHouse:Queries:columnCostDescribe'));
    expect(executeQuery).toHaveBeenCalledWith(expect.stringContaining('DESCRIBE (SELECT a, b FROM t)'));
    expect(executeQuery).toHaveBeenCalledWith(expect.stringContaining('source:TraceHouse:Queries:columnCostClient'));
    expect(executeQuery).toHaveBeenCalledWith(expect.stringContaining('sum(byteSize(`a`))'));
  });

  it('uses tagged core query_log lookups for server analysis', async () => {
    const executeQuery = vi.fn()
      .mockResolvedValueOnce([{ name: 'a', type: 'String' }, { name: 'b', type: 'String' }])
      .mockResolvedValueOnce([{ __ccost: 1 }])
      .mockResolvedValueOnce([{ __ccost: 1 }])
      .mockResolvedValueOnce([{ c: 1 }])
      .mockResolvedValueOnce([
        { query: 'SELECT count() AS `__ccost_100_0`', read_bytes: 100 },
        { query: 'SELECT count() AS `__ccost_100_1`', read_bytes: 300 },
      ]);
    const adapter: IClickHouseAdapter = { executeQuery };
    const service = new ColumnCostService(adapter);
    const dateNow = vi.spyOn(Date, 'now')
      .mockReturnValueOnce(100)
      .mockReturnValue(200);

    const result = await service.runServerAnalysis('SELECT a, b FROM t', {
      flushIntervalMs: 0,
      sleep: async () => {},
    });

    expect(result.total).toBe(400);
    expect(result.costs.map(c => [c.column, c.readBytes, c.pct])).toEqual([
      ['b', 300, 75],
      ['a', 100, 25],
    ]);
    expect(executeQuery).toHaveBeenCalledWith(expect.stringContaining('source:TraceHouse:Queries:columnCostDescribe'));
    expect(executeQuery).toHaveBeenCalledWith(expect.stringContaining('source:TraceHouse:Queries:columnCostServerColumn'));
    expect(executeQuery).toHaveBeenCalledWith(expect.stringContaining('SELECT `a` FROM (SELECT a, b FROM t)'));
    expect(executeQuery).toHaveBeenCalledWith(expect.stringContaining('source:TraceHouse:Queries:columnCostLogCheck'));
    expect(executeQuery).toHaveBeenCalledWith(expect.stringContaining('source:TraceHouse:Queries:columnCostLogLookup'));
    dateNow.mockRestore();
  });
});
