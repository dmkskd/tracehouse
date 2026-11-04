import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { EventContextService } from '../../services/event-context-service.js';
import {
  startClickHouse,
  stopClickHouse,
  type TestClickHouseContext,
} from './setup/clickhouse-container.js';

describe('Event context server logs', { tags: ['observability'] }, () => {
  let ctx: TestClickHouseContext;

  beforeAll(async () => {
    ctx = await startClickHouse();
  }, 120_000);

  afterAll(async () => {
    if (ctx) await stopClickHouse(ctx);
  }, 30_000);

  it('ranks the log line carrying the event error code ahead of closer errors', async () => {
    const marker = `event_ctx_${Date.now()}`;
    const fail = async (sql: string) => {
      await expect(ctx.client.query({
        query: sql,
        clickhouse_settings: { allow_custom_error_code_in_throwif: 1 },
      })).rejects.toThrow(marker);
    };

    // 999 = KEEPER_EXCEPTION, 395 = FUNCTION_THROW_IF_VALUE_IS_NON_ZERO.
    await fail(`SELECT throwIf(1, '${marker} keeper', toInt32(999))`);
    await new Promise(resolve => setTimeout(resolve, 1500));
    await fail(`SELECT throwIf(1, '${marker} unrelated')`);
    await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

    const hostRows = await (await ctx.client.query({
      query: 'SELECT hostname() AS host',
      format: 'JSONEachRow',
    })).json<{ host: string }>();

    // Event time sits on the unrelated error, so proximity alone would pick it.
    const result = await new EventContextService(ctx.adapter).getContext({
      eventTime: new Date().toISOString(),
      hostname: hostRows[0].host,
      exceptionName: 'KEEPER_EXCEPTION',
      availableCapabilities: ['text_log'],
      windowSeconds: 120,
      limit: 1,
    });

    expect(result.logs.status).toBe('loaded');
    expect(result.logs.data).toHaveLength(1);
    expect(result.logs.data[0].is_event_error).toBe(true);
    expect(result.logs.data[0].message).toContain(`${marker} keeper`);
    expect(result.logs.data[0].message).toContain('(KEEPER_EXCEPTION)');
  });
});
