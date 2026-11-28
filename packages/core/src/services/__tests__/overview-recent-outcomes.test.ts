import { describe, expect, it, vi } from 'vitest';
import { OverviewService } from '../overview-service.js';
import type { IClickHouseAdapter } from '../../adapters/types.js';

describe('recent query outcomes', () => {
  it('retains the recorded exception code alongside the execution measurements', async () => {
    const rows = [0, 241, 159].map(exception_code => ({
      hostname: 'node-02', query_id: `q-${exception_code}`, user: 'default', elapsed: 5, ended_ago_sec: 2,
      query_kind: 'SELECT', query: 'SELECT 1', memory_usage: 1024, exception_code,
    }));
    const adapter = { executeQuery: vi.fn(async (sql: string) => sql.includes('system.query_log') ? rows : []) } as unknown as IClickHouseAdapter;
    const result = await new OverviewService(adapter).getRecentActivity(120);
    expect(result.queries.map(query => ({ id: query.queryId, code: query.exceptionCode, duration: query.elapsed }))).toEqual([
      { id: 'q-0', code: 0, duration: 5 }, { id: 'q-241', code: 241, duration: 5 }, { id: 'q-159', code: 159, duration: 5 },
    ]);
  });
});
