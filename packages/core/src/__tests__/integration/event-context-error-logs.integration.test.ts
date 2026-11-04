import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { EventContextService } from '../../services/event-context-service.js';
import { MonitoringCapabilitiesService } from '../../services/monitoring-capabilities.js';
import {
  startClickHouse,
  stopClickHouse,
  type TestClickHouseContext,
} from './setup/clickhouse-container.js';

describe('Event context server logs', { tags: ['observability'] }, () => {
  let ctx: TestClickHouseContext;
  // Older images (23.8) ship without text_log; the product gates on the
  // text_log capability, so these tests skip instead of failing.
  let hasTextLog = false;
  const textLogMissing = 'system.text_log is not enabled on this server';

  beforeAll(async () => {
    ctx = await startClickHouse();
    const rows = await (await ctx.client.query({
      query: "SELECT count() AS n FROM system.tables WHERE database = 'system' AND name = 'text_log'",
      format: 'JSONEachRow',
    })).json<{ n: string }>();
    hasTextLog = Number(rows[0].n) > 0;
  }, 120_000);

  afterAll(async () => {
    if (ctx) await stopClickHouse(ctx);
  }, 30_000);

  it('ranks the log line carrying the event error code ahead of closer errors', async ({ skip }) => {
    if (!hasTextLog) skip(textLogMissing);
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

  it('does not match the echo of its own query text in text_log', async ({ skip }) => {
    if (!hasTextLog) skip(textLogMissing);
    const hostRows = await (await ctx.client.query({
      query: 'SELECT hostname() AS host',
      format: 'JSONEachRow',
    })).json<{ host: string }>();
    const service = new EventContextService(ctx.adapter);
    const options = {
      eventTime: new Date().toISOString(),
      hostname: hostRows[0].host,
      exceptionName: 'KEEPER_EXCEPTION',
      availableCapabilities: ['text_log'],
      windowSeconds: 120,
      limit: 500,
    };

    // First run puts the query text, which names KEEPER_EXCEPTION, into text_log.
    await service.getContext(options);
    await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
    const echoes = await (await ctx.client.query({
      query: `
        SELECT count() AS n FROM system.text_log
        WHERE event_time >= now() - INTERVAL 5 MINUTE
          AND logger_name = 'executeQuery'
          AND position(message, 'is_event_error') > 0
          AND position(message, 'KEEPER_EXCEPTION') > 0
      `,
      format: 'JSONEachRow',
    })).json<{ n: string }>();
    expect(Number(echoes[0].n)).toBeGreaterThan(0);

    const result = await service.getContext(options);
    const matched = result.logs.data.filter(entry => entry.is_event_error);
    expect(matched.length).toBeGreaterThan(0);
    for (const entry of matched) {
      expect(entry.message).toMatch(/Code: 999\. /);
      expect(entry.message).not.toContain('is_event_error');
    }
  });

  it('reads the live system.errors counter and the current process start', async () => {
    const hostRows = await (await ctx.client.query({
      query: 'SELECT hostname() AS host',
      format: 'JSONEachRow',
    })).json<{ host: string }>();
    const service = new EventContextService(ctx.adapter);
    // Capabilities come from the real probe, as in the app, so a mismatched
    // capability id shows up here as 'unavailable'.
    const probe = await new MonitoringCapabilitiesService(ctx.adapter).probe();
    const base = {
      eventTime: new Date().toISOString(),
      hostname: hostRows[0].host,
      remote: false,
      availableCapabilities: probe.capabilities.filter(c => c.available).map(c => c.id),
    };
    const read = async (exceptionName: string) => {
      const result = await service.getErrorState({ ...base, exceptionName });
      expect(result.source.detail ?? '').toBe('');
      expect(result.source.status).toBe('loaded');
      expect(result.source.data).toHaveLength(1);
      return result.source.data[0];
    };

    const before = await read('KEEPER_EXCEPTION');
    // 23.8 drops the custom message when throwIf gets a custom code, so the
    // assertion relies on the counter delta, not on the message text.
    await expect(ctx.client.query({
      query: "SELECT throwIf(1, 'event_err_state', toInt32(999))",
      clickhouse_settings: { allow_custom_error_code_in_throwif: 1 },
    })).rejects.toThrow();
    const after = await read('KEEPER_EXCEPTION');

    expect(after.has_counter).toBe(true);
    expect(after.value).toBe(before.value + 1);
    expect(after.last_error_message).toBeTruthy();
    expect(Date.parse(after.last_error_time!)).toBeGreaterThan(Date.now() - 120_000);
    expect(Date.parse(after.process_started_at)).toBeLessThanOrEqual(Date.parse(after.last_error_time!));

    // An error this server never raised still yields the host row, without a counter.
    const never = await read('CHECKSUM_DOESNT_MATCH');
    expect(never.has_counter).toBe(false);
    expect(never.last_error_time).toBeUndefined();
  });
});
