import type { OperationalEvent } from '../types/events.js';
import { parseAbsoluteTimestamp } from './absolute-timestamp.js';

const SQL_TIME = /^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?$/;

/** Versioned UTC identity plus an opaque alias matching the old ID algorithm.
 * `utcTime` must be the UTC SQL projection; `legacyTime` is used ONLY as an
 * opaque wall-clock key, never interpreted as the event's actual instant. */
export function buildEventIdentity(
  parts: Array<string | number | undefined>,
  utcTime: unknown,
  legacyTime: unknown,
): Pick<OperationalEvent, 'id' | 'legacy_ids'> {
  const canonical = [...parts];
  const utc = SQL_TIME.exec(String(utcTime ?? '').trim());
  if (utc) canonical[2] = `${utc[1].replace(' ', 'T')}.${(utc[2] ?? '').padEnd(6, '0')}Z`;

  const legacy = [...parts];
  const local = SQL_TIME.exec(String(legacyTime ?? '').trim());
  // The old mapper formatted this wall clock as ISO, truncating to milliseconds.
  // Reproduce its opaque key without parsing a server-local value as an instant.
  if (local) legacy[2] = `${local[1].replace(' ', 'T')}.${(local[2] ?? '').padEnd(3, '0').slice(0, 3)}Z`;
  return {
    id: ['event-v2', ...canonical].map(part => String(part ?? '')).join(':'),
    legacy_ids: [legacy.map(part => String(part ?? '')).join(':')],
  };
}

/** Resolve canonical IDs first, or one unambiguous legacy alias. Duplicate
 * buffers of the same canonical event do not make an alias ambiguous. */
export function resolveEventIdentity(
  events: readonly OperationalEvent[],
  id: string | null | undefined,
): OperationalEvent | undefined {
  if (!id) return undefined;
  const exact = events.find(event => event.id === id);
  if (exact) return exact;
  const matches = new Map(events.filter(event => event.legacy_ids?.includes(id))
    .map(event => [event.id, event]));
  if (matches.size) return matches.size === 1 ? matches.values().next().value : undefined;

  // Readonly episodes are reconstructed from bounded samples. A changed query
  // window can move their first observed sample (and pre-migration clipped IDs).
  // Resolve only a uniquely observed interval on the same host containing the
  // UTC point encoded by a v2 ID. This is not a nearest-event/time fallback.
  const state = /^event-v2:metric_log:(.*):(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,9}Z):replica_readonly$/.exec(id);
  if (!state) return undefined;
  const target = parseAbsoluteTimestamp(state[2]);
  const candidates = new Map(events.filter(event => {
    if (event.kind !== 'replica_readonly' || event.hostname !== state[1]) return false;
    const start = parseAbsoluteTimestamp(event.started_at ?? event.occurred_at);
    const end = parseAbsoluteTimestamp(event.ended_at ?? event.observed_at ?? event.occurred_at);
    return start <= target && (event.ended_at ? target < end : target <= end);
  }).map(event => [event.id, event]));
  return candidates.size === 1 ? candidates.values().next().value : undefined;
}
