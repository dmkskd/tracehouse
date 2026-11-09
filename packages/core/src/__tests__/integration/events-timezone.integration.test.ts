import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { EventsService } from '../../services/events-service.js';
import { EventContextService } from '../../services/event-context-service.js';
import { EVENT_SOURCE_DEFINITIONS } from '../../types/events.js';
import { resolveEventIdentity } from '../../utils/event-identity.js';
import { eventTimezoneFixture as fixture } from './fixtures/event-timezone-fixture.js';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';

/** Production queries/mapping with typed, epoch-seeded fixture sources. This is
 * read-only even in CH_TEST_URL mode; no system logs or user tables are changed. */
describe('Events and context timezone round trips', { tags: ['observability'] }, () => {
  let ctx: TestClickHouseContext;
  beforeAll(async () => { ctx = await startClickHouse({ timezone: 'Europe/Copenhagen' }); }, 120_000);
  afterAll(async () => { if (ctx) await stopClickHouse(ctx); });

  function adapter(epoch: number, zone: string): IClickHouseAdapter {
    return {
      executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
        /\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
        (_, table: string) => fixture(table, epoch, zone),
      )),
    };
  }

  for (const zone of ['UTC', 'Europe/Copenhagen', 'America/New_York']) {
    it(`${zone}: a readonly link before a narrower scan stays unresolved without continuity evidence`, async () => {
      const epoch = Date.parse('2026-11-08T17:00:00Z') / 1000;
      const continuous: IClickHouseAdapter = { executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
        '{{cluster_aware:system.metric_log}}', `(SELECT toDateTime(${epoch} + number, '${zone}') AS event_time,
          toDate(event_time) AS event_date, toUInt64(1) AS CurrentMetric_ReadonlyReplica FROM numbers(901))`,
      )) };
      const service = new EventsService(continuous);
      const wide = await service.getEvents({ startTime: new Date(epoch * 1000).toISOString(),
        endTime: new Date((epoch + 900) * 1000).toISOString(), availableCapabilities: ['metric_log_replication_state'] });
      const narrow = await service.getEvents({ startTime: new Date((epoch + 600) * 1000).toISOString(),
        endTime: new Date((epoch + 900) * 1000).toISOString(), availableCapabilities: ['metric_log_replication_state'] });
      expect(wide.coverage.filter(source => source.status === 'failed')).toEqual([]);
      expect(narrow.coverage.filter(source => source.status === 'failed')).toEqual([]);
      const original = wide.events.find(event => event.kind === 'replica_readonly')!;
      const later = narrow.events.find(event => event.kind === 'replica_readonly')!;
      expect(Date.parse(original.started_at!)).toBe(epoch * 1000);
      expect(Date.parse(later.started_at!)).toBe((epoch + 300) * 1000);
      expect(Date.parse(later.occurred_at)).toBe((epoch + 600) * 1000);
      expect(resolveEventIdentity(narrow.events, original.id)).toBeUndefined();
    });
    it(`${zone}: read-only state clips to UTC range bounds and error-state clocks remain absolute`, async () => {
      const epoch = Date.parse('2026-11-08T17:00:00Z') / 1000;
      const result = await new EventsService(adapter(epoch, zone)).getEvents({
        startTime: new Date((epoch + 2) * 1000).toISOString(),
        endTime: new Date((epoch + 10) * 1000).toISOString(),
        availableCapabilities: ['metric_log_replication_state'],
      });
      expect(result.coverage.filter(source => source.status === 'failed')).toEqual([]);
      const readonly = result.events.find(event => event.kind === 'replica_readonly');
      expect(readonly).toBeDefined();
      expect(Date.parse(readonly!.occurred_at)).toBe((epoch + 2) * 1000);
      expect(Date.parse(readonly!.ended_at!)).toBe((epoch + 5) * 1000);
      const wide = await new EventsService(adapter(epoch, zone)).getEvents({
        startTime: new Date((epoch - 10) * 1000).toISOString(),
        endTime: new Date((epoch + 10) * 1000).toISOString(),
        availableCapabilities: ['metric_log_replication_state'],
      });
      expect(wide.events.find(event => event.kind === 'replica_readonly')!.id).toBe(readonly!.id);
      const oldClippedId = `event-v2:metric_log:${readonly!.hostname}:${new Date((epoch + 2) * 1000).toISOString().replace('.000Z', '.000000Z')}:replica_readonly`;
      expect(resolveEventIdentity(wide.events, oldClippedId)?.id).toBe(readonly!.id);

      const clock = await ctx.rawAdapter.executeQuery<{ started: number }>(
        'SELECT toUnixTimestamp(now() - toIntervalSecond(uptime())) AS started',
      );
      const state = await new EventContextService(adapter(epoch, zone)).getErrorState({
        eventTime: new Date(epoch * 1000).toISOString(),
        hostname: '', exceptionName: 'NOT_ENOUGH_SPACE', remote: false,
        availableCapabilities: ['system_errors'],
      });
      expect(state.source.status, state.source.detail).toBe('loaded');
      expect(state.source.data).toHaveLength(1);
      expect(state.source.data[0].has_counter).toBe(true);
      expect(Date.parse(state.source.data[0].last_error_time!)).toBe(epoch * 1000);
      expect(Math.abs(Date.parse(state.source.data[0].process_started_at) - Number(clock[0].started) * 1000)).toBeLessThanOrEqual(2000);
    });
    for (const instant of [
      '2026-11-08T17:00:00Z', '2026-07-05T12:00:00Z',
      '2026-10-25T00:30:00Z', '2026-10-25T01:30:00Z',
      '2026-03-29T00:59:00Z', '2026-03-29T01:00:00Z',
      '2026-11-08T00:30:00Z',
    ]) {
      it(`${zone}: every event source retains its instant at ${instant}`, async () => {
        const epoch = Date.parse(instant) / 1000;
        const result = await new EventsService(adapter(epoch, zone)).getEvents({
          startTime: new Date((epoch - 10) * 1000).toISOString(),
          endTime: new Date((epoch + 10) * 1000).toISOString(),
          availableCapabilities: [...EVENT_SOURCE_DEFINITIONS.map(source => source.capability), 'distributed_limit_by'],
        });
        expect(result.coverage.filter(source => source.status !== 'loaded')).toEqual([]);
        expect(result.events).toHaveLength(EVENT_SOURCE_DEFINITIONS.length);
        for (const event of result.events) {
          expect(event.id).toMatch(/^event-v2:/);
          expect(event.legacy_ids).toHaveLength(1);
          expect(resolveEventIdentity(result.events, event.legacy_ids![0])?.id).toBe(event.id);
          const expectedMs = epoch * 1000 + (event.kind === 'server_restart' ? -1000
            : ['server_crash', 'error_burst', 'replica_readonly', 'replication_data_loss'].includes(event.kind) ? 0 : 123);
          expect(Date.parse(event.occurred_at), event.kind).toBe(expectedMs);
          expect(Date.parse(event.occurred_at)).toBeGreaterThanOrEqual((epoch - 10) * 1000);
          expect(Date.parse(event.occurred_at)).toBeLessThanOrEqual((epoch + 10) * 1000);
          if (event.observed_at) expect(Date.parse(event.observed_at)).toBe(epoch * 1000);
          if (event.ended_at) expect(Date.parse(event.ended_at)).toBe((epoch + 5) * 1000);
        }
      });

      it(`${zone}: context samples, workload and logs align at ${instant}`, async () => {
        const epoch = Date.parse(instant) / 1000;
        const result = await new EventContextService(adapter(epoch, zone)).getContext({
          eventTime: instant, queryId: 'fixture-query', windowSeconds: 10,
          availableCapabilities: ['query_log', 'metric_log', 'text_log'],
        });
        for (const source of [result.workload, result.metrics, result.logs]) {
          expect(source.status, source.detail).toBe('loaded');
          expect(source.data.length).toBeGreaterThan(0);
        }
        expect(Date.parse(result.workload.data[0].start_time)).toBe((epoch - 10) * 1000 + 123);
        expect(Date.parse(result.workload.data[0].end_time)).toBe(epoch * 1000 + 123);
        expect(Date.parse(result.logs.data[0].time)).toBe(epoch * 1000 + 123);
        expect(result.metrics.snapshots).toHaveLength(1);
        expect(Date.parse(result.metrics.snapshots[0].time)).toBe(epoch * 1000);
        expect(result.metrics.snapshots[0].sample_age_ms).toBe(0);
      });
    }
  }

  it('keeps distinct microsecond occurrences and declines their ambiguous old ID', async () => {
    const epoch = Date.parse('2026-11-08T17:00:00Z') / 1000;
    const actual: IClickHouseAdapter = {
      executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
        /\{\{cluster_aware:system\.query_log\}\}/g,
        fixture('query_log', epoch, 'Europe/Copenhagen')
          .replace('FROM numbers(1)', 'FROM numbers(2)')
          .replace('+ 123456,', '+ 123456 + number,'),
      )),
    };
    const result = await new EventsService(actual).getEvents({
      startTime: new Date((epoch - 1) * 1000).toISOString(),
      endTime: new Date((epoch + 1) * 1000).toISOString(), availableCapabilities: ['query_log'],
    });
    expect(result.events).toHaveLength(2);
    expect(new Set(result.events.map(event => event.id)).size).toBe(2);
    expect(result.events[0].legacy_ids).toEqual(result.events[1].legacy_ids);
    expect(resolveEventIdentity(result.events, result.events[0].legacy_ids![0])).toBeUndefined();
  });
});
