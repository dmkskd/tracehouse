/**
 * Backfill of the resource arena registry with already finished queries and
 * merges, shared by the 3D and swimlane views.
 */

import { packLanes, type CompletedMergeInfo, type CompletedQueryInfo, type LaneItem, type RecentActivity } from '@tracehouse/core';
import { deckOf, HOST_LANE_BLOCK, type Deck } from './arena-types';

/** Fields every arena entry type has; the registry holds richer entries. */
export interface HistoryEntryBase {
  id: string;
  deck: Deck;
  lane: number;
  hostname?: string;
  startTime: number;
  endTime: number | null;
}

/** Normalised finished operation handed to each arena's entry factory. */
export interface HistoryItem {
  id: string;
  kind: string;
  isMerge: boolean;
  elapsed: number;
  cpu: number;
  mem: number;
  progress: number;
  ioReadRate: number;
  rowsRead: number;
  bytesRead: number;
  queryId?: string;
  user?: string;
  /** Query text for queries, `db.table` for merges; arenas truncate it for display */
  label: string;
  tableHint: string;
  profileEvents?: CompletedQueryInfo['profileEvents'];
  readBytesPerSec?: number;
  writeBytesPerSec?: number;
  numParts?: number;
  mergeType?: string;
  database?: string;
  table?: string;
  partName?: string;
  hostname?: string;
  startTime: number;
  endTime: number;
}

/** Same id scheme as the live poll, so a running item always matches its registry entry. */
function mergeId(m: CompletedMergeInfo): string {
  return `m:${m.hostname ? m.hostname + ':' : ''}${m.database}.${m.table}.${m.partName}`;
}

function tableHintOf(query: string): string {
  const tm = query.match(/(?:FROM|INTO|TABLE|JOIN)\s+([`"]?[\w.]+[`"]?)/i);
  return tm ? tm[1].replace(/[`"]/g, '') : 'unknown';
}

export function historyItems(activity: RecentActivity, horizonSec: number, now: number): HistoryItem[] {
  const items: HistoryItem[] = [];
  for (const q of activity.queries) {
    if (q.endedAgoSec > horizonSec) continue;
    const endTime = now - q.endedAgoSec * 1000;
    items.push({
      id: q.queryId, kind: q.queryKind || 'OTHER', isMerge: false,
      elapsed: q.elapsed, cpu: q.cpuCores, mem: q.memoryUsage, progress: q.progress,
      ioReadRate: q.ioReadRate, rowsRead: q.rowsRead, bytesRead: q.bytesRead,
      queryId: q.queryId, user: q.user, label: q.query, tableHint: tableHintOf(q.query), profileEvents: q.profileEvents,
      hostname: q.hostname,
      startTime: endTime - q.elapsed * 1000, endTime,
    });
  }
  for (const m of activity.merges) {
    if (m.endedAgoSec > horizonSec) continue;
    const endTime = now - m.endedAgoSec * 1000;
    items.push({
      id: mergeId(m), kind: m.isMutation ? 'MUTATION' : 'MERGE', isMerge: true,
      elapsed: m.elapsed, cpu: m.cpuEstimate || 0.3, mem: m.memoryUsage, progress: m.progress,
      ioReadRate: m.readBytesPerSec, rowsRead: m.rowsRead, bytesRead: 0,
      readBytesPerSec: m.readBytesPerSec, writeBytesPerSec: m.writeBytesPerSec,
      numParts: m.numParts, mergeType: m.mergeType,
      database: m.database, table: m.table, partName: m.partName,
      label: `${m.database}.${m.table}`, tableHint: `${m.database}.${m.table}`,
      hostname: m.hostname,
      startTime: endTime - m.elapsed * 1000, endTime,
    });
  }
  return items;
}

/**
 * Re-pack lanes so finished entries do not overlap in time, honouring the
 * per-host lane blocks of the split view. Running entries keep their lane unless
 * it is outside the block they belong to (split toggled), in which case they are
 * re-packed too; this matches the 3D poll's own correction and fixes the swimlane,
 * which has none.
 */
export function repackFinishedLanes<E extends HistoryEntryBase>(
  registry: Map<string, E>,
  splitActive: boolean,
  now: number,
): void {
  const hosts = splitActive
    ? [...new Set([...registry.values()].map(e => e.hostname || '').filter(Boolean))].sort()
    : [];
  const laneItems: LaneItem[] = [...registry.values()].map(e => {
    const hostIdx = splitActive && e.hostname ? Math.max(0, hosts.indexOf(e.hostname)) : 0;
    const laneOffset = splitActive ? hostIdx * HOST_LANE_BLOCK : 0;
    const running = e.endTime === null;
    const inBlock = splitActive
      ? e.lane >= laneOffset && e.lane < laneOffset + HOST_LANE_BLOCK
      : e.lane < HOST_LANE_BLOCK;
    return {
      id: e.id,
      group: `${e.deck}|${hostIdx}`,
      laneOffset,
      laneCount: splitActive ? HOST_LANE_BLOCK : undefined,
      startMs: e.startTime,
      endMs: running ? now : e.endTime!,
      lane: running && inBlock ? e.lane : undefined,
    };
  });
  for (const [id, lane] of packLanes(laneItems)) {
    registry.get(id)!.lane = lane;
  }
}

/**
 * Add finished operations that are not in the registry yet, then re-pack lanes.
 * `makeEntry` builds the arena-specific entry (colour, label) from a HistoryItem.
 */
export function seedRegistry<E extends HistoryEntryBase>(
  registry: Map<string, E>,
  activity: RecentActivity,
  opts: { horizonSec: number; splitActive: boolean; now: number },
  makeEntry: (item: HistoryItem, deck: Deck) => E,
): void {
  for (const item of historyItems(activity, opts.horizonSec, opts.now)) {
    if (registry.has(item.id)) continue;
    registry.set(item.id, makeEntry(item, deckOf(item.kind, item.isMerge)));
  }
  repackFinishedLanes(registry, opts.splitActive, opts.now);
}
