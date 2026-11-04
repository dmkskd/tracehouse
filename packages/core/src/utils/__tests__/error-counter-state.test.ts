import { describe, expect, it } from 'vitest';
import { describeErrorCounterState } from '../error-counter-state.js';
import type { EventErrorCounter } from '../../types/event-context.js';

const EVENT = '2026-11-04T13:15:56.000Z';
const fmt = (iso: string) => iso.slice(11, 19);

function counter(overrides: Partial<EventErrorCounter> = {}): EventErrorCounter {
  return {
    hostname: 'host-a',
    process_started_at: '2026-11-04T13:15:45.000Z',
    has_counter: true,
    value: 2,
    last_error_time: '2026-11-04T13:15:55.000Z',
    last_error_message: 'Code: 999. Coordination::Exception: x (KEEPER_EXCEPTION)',
    ...overrides,
  };
}

describe('describeErrorCounterState', () => {
  it('reports no later occurrence when the last error is at or before the event', () => {
    const state = describeErrorCounterState(EVENT, 'KEEPER_EXCEPTION', counter(), fmt);
    expect(state.kind).toBe('no_later_occurrence');
    expect(state.summary).toBe(
      'No occurrence after this event: last at 13:15:55, 2 since the process started at 13:15:45.',
    );
    expect(state.message_label).toBe('Last recorded message, at 13:15:55');
  });

  it('reports a later occurrence when the counter moved after the event', () => {
    const state = describeErrorCounterState(EVENT, 'KEEPER_EXCEPTION', counter({
      value: 7,
      last_error_time: '2026-11-04T13:30:00.000Z',
    }), fmt);
    expect(state.kind).toBe('later_occurrence');
    expect(state.summary).toContain('last at 13:30:00, 7 since');
    expect(state.message_label).toBe('Last message, after this event, at 13:30:00');
  });

  it('does not use counters from a process that started after the event', () => {
    const restarted = describeErrorCounterState(EVENT, 'KEEPER_EXCEPTION', counter({
      process_started_at: '2026-11-04T14:00:00.000Z',
      last_error_time: '2026-11-04T14:00:05.000Z',
      value: 1,
    }), fmt);
    expect(restarted.kind).toBe('restarted_since');
    expect(restarted.summary).toContain('do not cover it');
    expect(restarted.summary).toContain('Since then: 1 × KEEPER_EXCEPTION, last at 14:00:05.');
    expect(restarted.message_label).toBe('Last message since the restart, at 14:00:05');

    const clean = describeErrorCounterState(EVENT, 'KEEPER_EXCEPTION', counter({
      process_started_at: '2026-11-04T14:00:00.000Z',
      has_counter: false,
      value: 0,
      last_error_time: undefined,
    }), fmt);
    expect(clean.summary).toContain('No KEEPER_EXCEPTION since then.');
  });

  it('distinguishes a missing counter from a host that did not answer', () => {
    expect(describeErrorCounterState(EVENT, 'KEEPER_EXCEPTION', counter({
      has_counter: false,
      value: 0,
      last_error_time: undefined,
    }), fmt).kind).toBe('no_counter');
    expect(describeErrorCounterState(EVENT, 'KEEPER_EXCEPTION', undefined, fmt).kind)
      .toBe('host_not_returned');
  });
});
