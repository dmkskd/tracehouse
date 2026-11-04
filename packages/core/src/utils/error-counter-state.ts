/**
 * Relates an error event (a system.error_log sample) to the host's live
 * system.errors counter. The counter lives in memory and resets when the
 * server restarts, so every statement is scoped to the current process and
 * nothing is inferred beyond what the counter records.
 */

import type { EventErrorCounter, EventErrorState } from '../types/event-context.js';

function ms(value: string | undefined): number {
  if (!value) return Number.NaN;
  return Date.parse(value);
}

function messageLabel(
  counter: EventErrorCounter,
  prefix: string,
  formatTime: (iso: string) => string,
): string | undefined {
  if (!counter.has_counter || !counter.last_error_message || !counter.last_error_time) return undefined;
  return `${prefix} ${formatTime(counter.last_error_time)}`;
}

export function describeErrorCounterState(
  eventTime: string,
  exceptionName: string,
  counter: EventErrorCounter | undefined,
  formatTime: (iso: string) => string,
): EventErrorState {
  if (!counter) {
    return {
      kind: 'host_not_returned',
      summary: 'The host did not return its error counters, so the current state is unknown.',
    };
  }

  const eventMs = ms(eventTime);
  const startedMs = ms(counter.process_started_at);
  if (startedMs > eventMs) {
    const since = counter.has_counter && counter.value > 0 && counter.last_error_time
      ? ` Since then: ${counter.value} × ${exceptionName}, last at ${formatTime(counter.last_error_time)}.`
      : ` No ${exceptionName} since then.`;
    return {
      kind: 'restarted_since',
      summary: `The server restarted at ${formatTime(counter.process_started_at)}, after this event, `
        + 'so its current counters do not cover it.' + since,
      message_label: messageLabel(counter, `Last message since the restart, at`, formatTime),
      counter,
    };
  }

  if (!counter.has_counter || counter.value === 0 || !counter.last_error_time) {
    return {
      kind: 'no_counter',
      summary: `system.errors has no ${exceptionName} counter for the current process `
        + `(started ${formatTime(counter.process_started_at)}).`,
      counter,
    };
  }

  const lastMs = ms(counter.last_error_time);
  if (lastMs > eventMs) {
    return {
      kind: 'later_occurrence',
      summary: `Occurred again after this event: last at ${formatTime(counter.last_error_time)}, `
        + `${counter.value} since the process started at ${formatTime(counter.process_started_at)}.`,
      message_label: messageLabel(counter, 'Last message, after this event, at', formatTime),
      counter,
    };
  }

  return {
    kind: 'no_later_occurrence',
    summary: `No occurrence after this event: last at ${formatTime(counter.last_error_time)}, `
      + `${counter.value} since the process started at ${formatTime(counter.process_started_at)}.`,
    message_label: messageLabel(counter, 'Last recorded message, at', formatTime),
    counter,
  };
}
