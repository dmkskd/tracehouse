import type { OperationalEvent } from '@tracehouse/core';
import { buildEventLayerMarks } from '../events/event-layer-model';
import { EVENT_SEVERITY_VALUES } from '../events/event-model';
import { Cs, deckBaseY, heightCpu, HOST_LANE_BLOCK, LANE_GAP, MIN_DIM, type BlockEntry } from './arena-types';

export const ARENA_EVENT_HORIZON_SEC = 120;
export const ARENA_EVENT_LIMIT = 120;
export const arenaQueryKey = (queryId: string, hostname?: string) => JSON.stringify(['query', hostname ?? '', queryId]);

export interface ArenaEventMark {
  id: string;
  events: OperationalEvent[];
  event: OperationalEvent;
  time: number;
  end?: number;
  open?: boolean;
  entry?: BlockEntry;
  y: number;
  z: number;
  hostWidth: number;
}

/** Exact local identity only: a distributed initial_query_id is not a local
 * operation match, nor is a table name enough to identify a merge. */
export function eventOperation(event: OperationalEvent, entries: readonly BlockEntry[]): BlockEntry | undefined {
  if (!event.hostname) return undefined;
  return entries.find(entry => entry.hostname === event.hostname && (
    (!entry.isMerge && !!event.query_id && entry.queryId === event.query_id)
    || (entry.isMerge && !!event.database && !!event.table && !!event.part_name
      && entry.database === event.database && entry.table === event.table && entry.partName === event.part_name)
  ));
}

/** Hosts without current operations still retain their own spatial area. */
export function arenaEventHosts(known: readonly string[], entries: readonly BlockEntry[], events: readonly OperationalEvent[]): string[] {
  return [...new Set([...known, ...entries.map(e => e.hostname ?? ''), ...events.map(e => e.hostname ?? '')])].sort();
}

export function buildArenaEventMarks(events: readonly OperationalEvent[], entries: readonly BlockEntry[], hosts: readonly string[], now: number, splitByHost = true): ArenaEventMark[] {
  const window = { startMs: now - ARENA_EVENT_HORIZON_SEC * 1000, endMs: now };
  // Never combine different hosts, unrelated operations, or event types.
  const groups = new Map<string, { entry?: BlockEntry; contextLane?: number; events: OperationalEvent[] }>();
  for (const event of events) {
    const entry = eventOperation(event, entries);
    // In merged mode, a server observation sits near that server's activity,
    // without claiming an operation match or changing any operation's lane.
    const time = Date.parse(event.occurred_at);
    const distance = (candidate: BlockEntry) => Math.max(candidate.startTime - time, time - (candidate.endTime ?? now), 0);
    const contextLane = !entry && !splitByHost && event.hostname
      ? entries.filter(candidate => candidate.hostname === event.hostname)
        .sort((a, b) => distance(a) - distance(b) || a.lane - b.lane)[0]?.lane
      : undefined;
    const key = JSON.stringify([event.hostname ?? '', entry?.id ?? null, event.kind, contextLane]);
    const group = groups.get(key) ?? { entry, contextLane, events: [] };
    group.events.push(event); groups.set(key, group);
  }
  const marks: ArenaEventMark[] = [];
  for (const { entry, contextLane, events: group } of groups.values()) {
    const hostIndex = hosts.indexOf(group[0].hostname ?? '');
    if (hostIndex < 0) continue;
    const y = entry ? deckBaseY(entry.deck) + Math.max(MIN_DIM, heightCpu(entry) * Cs) : .025;
    const z = entry ? -entry.lane * LANE_GAP : splitByHost
      ? -(hostIndex * HOST_LANE_BLOCK + (HOST_LANE_BLOCK - 1) / 2) * LANE_GAP
      : contextLane !== undefined ? -contextLane * LANE_GAP : (hostIndex + 1) * LANE_GAP;
    const base = { entry, y, z, hostWidth: splitByHost ? HOST_LANE_BLOCK * LANE_GAP - .18 : LANE_GAP * .7 };
    const layer = buildEventLayerMarks(group, window, 1200);
    for (const point of layer.points) marks.push({ ...base, id: point.id, events: point.events, event: point.primaryEvent, time: point.occurredAtMs });
    for (const band of layer.bands) marks.push({ ...base, id: band.event.id, events: [band.event], event: band.event,
      time: Date.parse(band.event.started_at ?? band.event.occurred_at), end: band.endMs, open: band.open });
  }
  // A bounded number of meshes, with the most severe observations retained first.
  return marks.sort((a, b) => EVENT_SEVERITY_VALUES.indexOf(a.event.severity) - EVENT_SEVERITY_VALUES.indexOf(b.event.severity)
    || b.time - a.time || a.id.localeCompare(b.id)).slice(0, ARENA_EVENT_LIMIT);
}
