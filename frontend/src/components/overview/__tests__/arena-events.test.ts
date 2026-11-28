import { describe, expect, it } from 'vitest';
import type { OperationalEvent } from '@tracehouse/core';
import { arenaEventHosts, arenaQueryKey, buildArenaEventMarks, eventOperation, ARENA_EVENT_LIMIT } from '../arena-events';
import { HOST_LANE_BLOCK, LANE_GAP, type BlockEntry } from '../arena-types';
import { repackFinishedLanes } from '../arena-history';

const now = Date.parse('2026-11-28T14:00:00Z');
const event = (extra: Partial<OperationalEvent> = {}): OperationalEvent => ({
  id: 'event-1', occurred_at: new Date(now - 5000).toISOString(), hostname: 'node-02',
  kind: 'query_failure', category: 'queries', severity: 'error', precision: 'exact', title: 'Query failed',
  source: 'system.query_log', capability: 'query_log', query_id: 'shared-id', ...extra,
});
const entry = (hostname: string, extra: Partial<BlockEntry> = {}): BlockEntry => ({
  id: arenaQueryKey('shared-id', hostname), queryId: 'shared-id', hostname, kind: 'SELECT', color: '#58a6ff',
  label: 'SELECT 1', tableHint: 'db.t', isMerge: false, deck: 'select', lane: 6,
  startTime: now - 20000, endTime: now - 5000, cpu: 1, mem: 1000, elapsed: 15, progress: 100,
  ioReadRate: 0, rowsRead: 0, bytesRead: 0, ...extra,
});

describe('spatial arena events', () => {
  it('matches query identity AND host, never initial query id or table coincidence', () => {
    const a = entry('node-01'), b = entry('node-02');
    expect(a.id).not.toBe(b.id);
    expect(eventOperation(event(), [a, b])).toBe(b);
    expect(eventOperation(event({ hostname: undefined }), [b])).toBeUndefined();
    expect(eventOperation(event({ query_id: undefined, initial_query_id: b.queryId, table: 't' }), [b])).toBeUndefined();
  });
  it('matches a merge only with a complete local part identity', () => {
    const merge = entry('node-02', { isMerge: true, queryId: undefined, database: 'db', table: 't', partName: 'p' });
    expect(eventOperation(event({ query_id: undefined, database: 'db', table: 't', part_name: 'p' }), [merge])).toBe(merge);
    expect(eventOperation(event({ query_id: undefined, database: 'db', table: 't' }), [merge])).toBeUndefined();
  });
  it('keeps nearby events on different servers separate and reserves quiet server areas', () => {
    const events = [event(), event({ id: 'other', hostname: 'node-01' })];
    const hosts = arenaEventHosts(['node-03'], [], events);
    expect(hosts).toEqual(['node-01', 'node-02', 'node-03']);
    const marks = buildArenaEventMarks(events, [], hosts, now);
    expect(marks).toHaveLength(2);
    expect(marks[0].events).toHaveLength(1);
    expect(marks[0].z).not.toBe(marks[1].z);
    const q = entry('node-03');
    const registry = new Map([[q.id, q]]);
    repackFinishedLanes(registry, true, now, hosts);
    expect(q.lane).toBe(2 * HOST_LANE_BLOCK);
    expect(Math.abs(marks[0].z - marks[1].z)).toBeCloseTo(HOST_LANE_BLOCK * LANE_GAP);
  });
  it('uses operation lane and CPU top without inventing an association for an absent short query', () => {
    const b = entry('node-02');
    const mark = buildArenaEventMarks([event()], [b], ['node-02'], now)[0];
    expect(mark.entry).toBe(b);
    expect(mark.z).toBe(-b.lane * LANE_GAP);
    expect(mark.y).toBe(.6);
    const unmatched = buildArenaEventMarks([event({ query_id: 'short-query' })], [b], ['node-02'], now)[0];
    expect(unmatched.entry).toBeUndefined();
    expect(unmatched.y).toBe(.025);
  });
  it('retains an open episode that began before the horizon and discards expired points', () => {
    const episode = event({ id: 'episode', kind: 'replica_readonly', query_id: undefined,
      occurred_at: new Date(now - 600000).toISOString() });
    const marks = buildArenaEventMarks([episode, event({ occurred_at: new Date(now - 180000).toISOString() })], [], ['node-02'], now);
    expect(marks).toHaveLength(1);
    expect(marks[0]).toMatchObject({ id: 'episode', open: true, end: now });
  });
  it('adds merged-mode markers without moving or resizing operations, including events from another server', () => {
    const a = entry('node-01', { lane: 2 }), b = entry('node-02', { lane: 9 });
    const entries = [a, b], before = structuredClone(entries);
    const events = [event({ id: 'local', query_id: 'not-sampled' }), event({ id: 'quiet-host', hostname: 'node-03', query_id: undefined })];
    const hosts = arenaEventHosts([], entries, events);
    expect(buildArenaEventMarks([], entries, hosts, now, false)).toEqual([]);
    const marks = buildArenaEventMarks(events, entries, hosts, now, false);
    expect(entries).toEqual(before);
    const local = marks.find(mark => mark.event.id === 'local')!;
    expect(local.z).toBe(-b.lane * LANE_GAP);
    expect(local.entry).toBeUndefined();
    expect(local.hostWidth).toBeLessThan(LANE_GAP);
    expect(marks.find(mark => mark.event.id === 'quiet-host')!.z).toBeGreaterThan(0);
    buildArenaEventMarks(events.slice(1), entries, hosts, now, false);
    expect(entries).toEqual(before);
  });
  it('bounds mesh count and retains severe observations ahead of information events', () => {
    const events = Array.from({ length: ARENA_EVENT_LIMIT + 30 }, (_, i) => event({ id: String(i), hostname: `h-${i}`, severity: 'info' }));
    events.push(event({ id: 'critical', hostname: 'critical-host', severity: 'critical' }));
    const marks = buildArenaEventMarks(events, [], arenaEventHosts([], [], events), now);
    expect(marks).toHaveLength(ARENA_EVENT_LIMIT);
    expect(marks[0].event.id).toBe('critical');
  });
});
