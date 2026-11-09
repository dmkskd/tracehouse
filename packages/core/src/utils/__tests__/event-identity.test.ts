import { describe, expect, it } from 'vitest';
import type { OperationalEvent } from '../../types/events.js';
import { buildEventIdentity, resolveEventIdentity } from '../event-identity.js';

function event(utc: string, legacy: string): OperationalEvent {
  return {
    ...buildEventIdentity(['query_log', 'host', utc, 'query', 'Create'], utc, legacy),
    occurred_at: utc, kind: 'ddl', category: 'changes', severity: 'info',
    precision: 'exact', title: 'DDL', source: 'system.query_log', capability: 'query_log',
  };
}

describe('operational event identities', () => {
  it('resolves only a uniquely observed readonly interval after a window changes its ID', () => {
    const requested = 'event-v2:metric_log:host:2026-11-08T17:00:02.000000Z:replica_readonly';
    const state: OperationalEvent = {
      ...buildEventIdentity(['metric_log', 'host', '', 'replica_readonly'], '2026-11-08 17:00:00', ''),
      hostname: 'host', kind: 'replica_readonly', category: 'replication', severity: 'warning',
      precision: 'sampled', source: 'system.metric_log', capability: 'metric_log_replication_state', title: 'Readonly',
      occurred_at: '2026-11-08T17:00:02Z', started_at: '2026-11-08T17:00:00Z',
      observed_at: '2026-11-08T17:00:03Z', ended_at: '2026-11-08T17:00:05Z',
    };
    expect(resolveEventIdentity([state], requested)).toBe(state);
    expect(resolveEventIdentity([state, { ...state, id: 'different-overlapping-episode' }], requested)).toBeUndefined();
    expect(resolveEventIdentity([{ ...state, hostname: 'other-host' }], requested)).toBeUndefined();
    expect(resolveEventIdentity([state], requested.replace('17:00:02', '17:00:06'))).toBeUndefined();
    expect(resolveEventIdentity([{ ...state, ended_at: undefined, observed_at: '2026-11-08T17:00:01Z' }], requested)).toBeUndefined();
    // A narrower scan may only observe a later portion of an episode. Those
    // samples cannot prove continuity with an earlier linked occurrence.
    expect(resolveEventIdentity([{ ...state, id: 'later-observed-episode', started_at: '2026-11-08T17:00:03Z',
      occurred_at: '2026-11-08T17:00:03Z' }], requested)).toBeUndefined();
  });
  it('uses UTC microseconds and retains the exact pre-fix opaque ID as an alias', () => {
    const value = event('2026-11-08 17:00:00.123456', '2026-11-08 18:00:00.123456');
    expect(value.id).toBe('event-v2:query_log:host:2026-11-08T17:00:00.123456Z:query:Create');
    expect(value.legacy_ids).toEqual(['query_log:host:2026-11-08T18:00:00.123Z:query:Create']);
    expect(resolveEventIdentity([value], value.legacy_ids![0])).toBe(value);
    expect(resolveEventIdentity([value], value.id)).toBe(value);
  });

  it('does not collapse microsecond events or guess an ambiguous old millisecond ID', () => {
    const first = event('2026-11-08 17:00:00.123456', '2026-11-08 18:00:00.123456');
    const second = event('2026-11-08 17:00:00.123457', '2026-11-08 18:00:00.123457');
    expect(first.id).not.toBe(second.id);
    expect(first.legacy_ids).toEqual(second.legacy_ids);
    expect(resolveEventIdentity([first, second], first.legacy_ids![0])).toBeUndefined();
    expect(resolveEventIdentity([first, second], second.id)).toBe(second);
  });

  it('distinguishes the repeated Copenhagen hour without guessing a legacy target', () => {
    const first = event('2026-10-25 00:30:00', '2026-10-25 02:30:00');
    const second = event('2026-10-25 01:30:00', '2026-10-25 02:30:00');
    expect(first.id).not.toBe(second.id);
    expect(resolveEventIdentity([first, second], first.legacy_ids![0])).toBeUndefined();
  });

  it('deduplicates the same event across navigator buffers and ignores unknown IDs', () => {
    const value = event('2026-11-08 17:00:00', '2026-11-08 18:00:00');
    expect(resolveEventIdentity([value, { ...value }], value.legacy_ids![0])?.id).toBe(value.id);
    expect(resolveEventIdentity([value], 'not-a-real-event')).toBeUndefined();
    expect(resolveEventIdentity([value], null)).toBeUndefined();
  });
});
