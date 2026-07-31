import { describe, expect, it } from 'vitest';
import type { MergeHistoryRecord, MergeInfo } from '../../../stores/mergeStore';
import {
  buildMergeActivityRecords,
  createMergeActivityState,
  filterMergeActivity,
  hasReplicaMergeActivity,
  isMergeStuck,
  limitMergeActivityRecords,
  mergeActivityKey,
  mergeActivityHosts,
  mergeActivityStatuses,
  reconcileMergeActivity,
  sortMergeActivityRecords,
} from '../merge-activity-model';

const active = (part: string, overrides: Partial<MergeInfo> = {}): MergeInfo => ({
  database: 'default',
  table: 'events',
  elapsed: 2,
  progress: 0.5,
  num_parts: 2,
  source_part_names: ['p1', 'p2'],
  result_part_name: part,
  total_size_bytes_compressed: 10,
  rows_read: 2,
  rows_written: 1,
  memory_usage: 10,
  merge_type: 'Regular',
  merge_algorithm: 'Horizontal',
  is_mutation: false,
  bytes_read_uncompressed: 20,
  bytes_written_uncompressed: 10,
  columns_written: 1,
  thread_id: 1,
  hostname: 'node-1',
  ...overrides,
});

const completed = (part: string): MergeHistoryRecord => ({
  event_time: '2026-07-26T00:00:02.000Z',
  event_type: 'MergeParts',
  database: 'default',
  table: 'events',
  part_name: part,
  partition_id: 'all',
  rows: 1,
  size_in_bytes: 10,
  duration_ms: 2000,
  merge_reason: 'Regular',
  source_part_names: ['p1', 'p2'],
  bytes_uncompressed: 20,
  read_bytes: 20,
  read_rows: 2,
  peak_memory_usage: 10,
  size_diff: 0,
  size_diff_pct: 0,
  rows_diff: -1,
  hostname: 'node-1',
});

describe('reconcileMergeActivity', () => {
  it('bridges the gap between system.merges and system.part_log', () => {
    const state = createMergeActivityState();
    const merge = active('all_1_2_1');

    expect(reconcileMergeActivity(state, [merge], [], 1_000).live[0]?.status).toBe('running');
    expect(reconcileMergeActivity(state, [], [], 2_000).live[0]?.status).toBe('finalizing');
    expect(reconcileMergeActivity(state, [], [completed('all_1_2_1')], 3_000).live).toHaveLength(0);
  });

  it('keeps replica-local merges distinct', () => {
    expect(mergeActivityKey(active('part', { hostname: 'one' })))
      .not.toBe(mergeActivityKey(active('part', { hostname: 'two' })));
  });

  it('flags stalled and finalizing merges without treating ordinary work as stuck', () => {
    expect(isMergeStuck(active('normal', { elapsed: 120, progress: 0.2 }))).toBe(false);
    expect(isMergeStuck(active('stalled', { elapsed: 601, progress: 0 }))).toBe(true);
    expect(isMergeStuck(active('finalizing', { elapsed: 1_801, progress: 1 }))).toBe(true);
  });
});

describe('merge activity records', () => {
  it('normalizes live and completed merges to one lifecycle shape', () => {
    const merge = active('all_1_2_1', { elapsed: 2, progress: 0.5 });
    const history = completed('all_3_4_1');
    const records = buildMergeActivityRecords(
      [{ merge, status: 'running' }],
      [history],
      Date.parse('2026-07-26T00:00:04.000Z'),
    );

    expect(records[0]).toMatchObject({
      activitySource: 'running',
      status: 'running',
      startedAt: '2026-07-26T00:00:02.000Z',
      durationMs: 2000,
      rowsRead: 2,
      rowsWritten: 1,
      progress: 0.5,
    });
    expect(records[1]).toMatchObject({
      activitySource: 'history',
      status: 'ok',
      startedAt: '2026-07-26T00:00:00.000Z',
      durationMs: 2000,
      rowsRead: 2,
      rowsWritten: 1,
      progress: 1,
    });
  });

  it('pins active work above history while sharing the selected sort', () => {
    const oldLive = active('old-live', { elapsed: 10 });
    const newLive = active('new-live', { elapsed: 2 });
    const olderHistory = completed('older-history');
    const newerHistory = {
      ...completed('newer-history'),
      event_time: '2026-07-26T00:00:03.000Z',
    };
    const records = buildMergeActivityRecords(
      [
        { merge: oldLive, status: 'running' },
        { merge: newLive, status: 'running' },
      ],
      [olderHistory, newerHistory],
      Date.parse('2026-07-26T00:00:20.000Z'),
    );

    const sorted = sortMergeActivityRecords(records, {
      field: 'event_time',
      direction: 'desc',
    });

    expect(sorted.map(record => record.partName)).toEqual([
      'new-live',
      'old-live',
      'newer-history',
      'older-history',
    ]);
  });

  it('represents failed terminal merges without claiming complete progress', () => {
    const failed = {
      ...completed('failed'),
      error: 241,
      exception: 'Memory limit exceeded',
    };

    expect(buildMergeActivityRecords([], [failed])[0]).toMatchObject({
      status: 'error',
      progress: null,
      error: 241,
      exception: 'Memory limit exceeded',
    });
  });

  it('applies status filters to the shared lifecycle rather than separate tables', () => {
    const running = active('running');
    const finalizing = active('finalizing');
    const success = completed('success');
    const error = { ...completed('error'), error: 241 };
    const snapshot = {
      live: [
        { merge: running, status: 'running' as const },
        { merge: finalizing, status: 'finalizing' as const },
      ],
      recent: [success, error],
    };

    expect(filterMergeActivity(snapshot, { status: ['Running'] })).toMatchObject({
      live: [{ merge: running }, { merge: finalizing }],
      recent: [],
    });
    expect(filterMergeActivity(snapshot, { status: ['OK'] })).toMatchObject({
      live: [],
      recent: [success],
    });
    expect(filterMergeActivity(snapshot, { status: ['Error'] })).toMatchObject({
      live: [],
      recent: [error],
    });
  });

  it('hides live merges that started after the selected range ended', () => {
    const running = active('running');
    const success = completed('success');
    const now = Date.parse('2026-07-31T16:53:00.000Z');

    const filtered = filterMergeActivity({
      live: [{ merge: running, status: 'running' }],
      recent: [success],
    }, {
      timeRange: 'CUSTOM:2026-07-31T15:53:00.000Z,2026-07-31T16:44:00.000Z',
    }, now);

    expect(filtered.live).toEqual([]);
    expect(filtered.recent).toEqual([success]);
  });

  it('keeps a 30-minute live merge that crossed a range ending 10 minutes ago', () => {
    const merge = active('spanning-live', { elapsed: 30 * 60 });
    const now = Date.parse('2026-07-31T16:53:00.000Z');

    const filtered = filterMergeActivity({
      live: [{ merge, status: 'running' }],
      recent: [],
    }, {
      timeRange: 'CUSTOM:2026-07-31T15:53:00.000Z,2026-07-31T16:43:00.000Z',
    }, now);

    expect(filtered.live.map(item => item.merge.result_part_name)).toEqual(['spanning-live']);
  });

  it('filters failed history by ClickHouse error code', () => {
    const memoryError = { ...completed('memory'), error: 241 };
    const thrownError = { ...completed('throw-if'), error: 395 };

    expect(filterMergeActivity(
      { live: [], recent: [memoryError, thrownError] },
      { status: ['Error'], errorCode: [395] },
    ).recent).toEqual([thrownError]);
  });

  it('uses the same host, part, size, and replica filters for both sources', () => {
    const liveMatch = active('match-live', {
      hostname: 'node-2',
      total_size_bytes_compressed: 20,
    });
    const liveReplica = active('match-replica', {
      hostname: 'node-2',
      total_size_bytes_compressed: 20,
      is_replica_merge: true,
    });
    const historyMatch = {
      ...completed('match-history'),
      hostname: 'node-2',
      size_in_bytes: 20,
    };
    const historyOtherHost = {
      ...completed('match-other'),
      hostname: 'node-1',
      size_in_bytes: 20,
    };

    const filtered = filterMergeActivity({
      live: [
        { merge: liveMatch, status: 'running' },
        { merge: liveReplica, status: 'running' },
      ],
      recent: [historyMatch, historyOtherHost],
    }, {
      hostname: ['node-2'],
      partName: 'match',
      minSizeBytes: 15,
      hideReplicaMerges: true,
    });

    expect(filtered.live.map(item => item.merge.result_part_name)).toEqual(['match-live']);
    expect(filtered.recent.map(record => record.part_name)).toEqual(['match-history']);
  });

  it('ORs merge status, host, database, table, and category values', () => {
    const live = active('live', { hostname: 'node-1', database: 'db_a', table: 'table_a' });
    const ok = { ...completed('ok'), hostname: 'node-2', database: 'db_b', table: 'table_b' };
    const error = {
      ...completed('error'),
      hostname: 'node-3',
      database: 'db_c',
      table: 'table_c',
      error: 1,
    };

    const filtered = filterMergeActivity({
      live: [{ merge: live, status: 'running' }],
      recent: [ok, error],
    }, {
      status: ['Running', 'OK'],
      hostname: ['node-1', 'node-2'],
      database: ['db_a', 'db_b'],
      table: ['table_a', 'table_b'],
      category: ['Regular', 'TTLDelete'],
    });

    expect(filtered.live.map(item => item.merge.result_part_name)).toEqual(['live']);
    expect(filtered.recent.map(record => record.part_name)).toEqual(['ok']);
  });

  it('derives shared host and replica filter options from both lifecycle sources', () => {
    const replica = active('replica', { hostname: 'node-2', is_replica_merge: true });
    const ok = completed('ok');
    const error = { ...completed('error'), hostname: 'node-3', error: 1 };
    const snapshot = {
      live: [{ merge: replica, status: 'running' as const }],
      recent: [ok, error],
    };

    expect(mergeActivityHosts(snapshot)).toEqual(['node-1', 'node-2', 'node-3']);
    expect(hasReplicaMergeActivity(snapshot)).toBe(true);
  });

  it('always offers every lifecycle status independently of the loaded page', () => {
    expect(mergeActivityStatuses()).toEqual(['Running', 'OK', 'Error']);
  });

  it('treats limit as one cap across live and completed rows', () => {
    const records = limitMergeActivityRecords(
      buildMergeActivityRecords([
        { merge: active('live-1'), status: 'running' },
        { merge: active('live-2'), status: 'running' },
      ], [
        completed('done-1'),
        completed('done-2'),
      ], 5_000),
      3,
    );

    expect(records.map(record => record.partName)).toEqual([
      'live-1',
      'live-2',
      'done-1',
    ]);
  });
});
