import { describe, it, expect } from 'vitest';
import type { CompletedMergeInfo, CompletedQueryInfo } from '@tracehouse/core';
import { historyItems, seedRegistry, repackFinishedLanes, type HistoryEntryBase } from '../arena-history';
import { HOST_LANE_BLOCK } from '../arena-types';

const NOW = 1_000_000_000;

const query = (queryId: string, elapsed: number, endedAgoSec: number, extra: Partial<CompletedQueryInfo> = {}): CompletedQueryInfo => ({
  queryId, user: 'default', elapsed, endedAgoSec, cpuCores: 0.5, memoryUsage: 1, ioReadRate: 0,
  rowsRead: 0, bytesRead: 0, progress: 100, queryKind: 'Select', query: 'SELECT 1 FROM db.t', hostname: 'h1',
  ...extra,
});

const merge = (partName: string, elapsed: number, endedAgoSec: number): CompletedMergeInfo => ({
  database: 'db', table: 't', partName, elapsed, endedAgoSec, progress: 1, memoryUsage: 1,
  readBytesPerSec: 0, writeBytesPerSec: 0, rowsRead: 0, numParts: 2, isMutation: false,
  cpuEstimate: 0, mergeType: 'RegularMerge', hostname: 'h1',
});

describe('historyItems', () => {
  it('derives start/end from endedAgoSec and elapsed, and drops items past the horizon', () => {
    const items = historyItems(
      { queries: [query('q1', 4, 10), query('old', 2, 500)], merges: [merge('all_1_2_1', 3, 20)] },
      120,
      NOW,
    );
    expect(items.map(i => i.id)).toEqual(['q1', 'm:h1:db.t.all_1_2_1']);
    expect(items[0].endTime).toBe(NOW - 10_000);
    expect(items[0].startTime).toBe(NOW - 14_000);
    expect(items[0].tableHint).toBe('db.t');
    expect(items[1].kind).toBe('MERGE');
  });
});

describe('seedRegistry', () => {
  type Entry = HistoryEntryBase & { tag?: string };
  const make = (it: { id: string; startTime: number; endTime: number }, deck: HistoryEntryBase['deck']): Entry =>
    ({ id: it.id, deck, lane: 0, startTime: it.startTime, endTime: it.endTime });

  it('does not overwrite running entries and keeps their lane', () => {
    const registry = new Map<string, Entry>([
      ['q-running', { id: 'q-running', deck: 'select', lane: 0, startTime: NOW - 30_000, endTime: null }],
    ]);
    seedRegistry(
      registry,
      { queries: [query('q-running', 5, 8), query('q-done', 5, 20)], merges: [] },
      { horizonSec: 120, splitActive: false, now: NOW },
      make,
    );
    expect(registry.get('q-running')!.endTime).toBeNull();
    expect(registry.get('q-running')!.lane).toBe(0);
    // q-done overlaps the running entry in time, so it must not share its lane
    expect(registry.get('q-done')!.lane).toBe(1);
  });
});

describe('repackFinishedLanes in split view', () => {
  const entry = (id: string, hostname: string, lane: number, endTime: number | null): HistoryEntryBase =>
    ({ id, deck: 'select', lane, hostname, startTime: NOW - 20_000, endTime });

  it('moves a running entry into its host block when split is turned on', () => {
    const registry = new Map<string, HistoryEntryBase>([
      ['a', entry('a', 'host-a', 0, null)],
      ['b', entry('b', 'host-b', 0, null)],
    ]);
    repackFinishedLanes(registry, true, NOW);
    expect(registry.get('a')!.lane).toBe(0);
    expect(registry.get('b')!.lane).toBe(HOST_LANE_BLOCK);
  });

  it('never lets a host spill into the next host block', () => {
    const registry = new Map<string, HistoryEntryBase>();
    for (let i = 0; i < HOST_LANE_BLOCK + 3; i++) registry.set(`a${i}`, entry(`a${i}`, 'host-a', 0, NOW - 1000));
    registry.set('b0', entry('b0', 'host-b', HOST_LANE_BLOCK, NOW - 1000));
    repackFinishedLanes(registry, true, NOW);
    const hostALanes = [...registry.values()].filter(e => e.hostname === 'host-a').map(e => e.lane);
    expect(Math.max(...hostALanes)).toBe(HOST_LANE_BLOCK - 1);
    expect(registry.get('b0')!.lane).toBe(HOST_LANE_BLOCK);
  });
});

