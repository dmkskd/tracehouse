import { describe, expect, it } from 'vitest';
import type { QueryHistoryItem, RunningQuery } from '../../../stores/queryStore';
import {
  buildQueryActivityRecords,
  queryActivityKey,
  querySelectionToSeries,
  sortQueryActivityRecords,
} from '../query-activity-model';

const running = (queryId: string, overrides: Partial<RunningQuery> = {}): RunningQuery => ({
  query_id: queryId,
  user: 'default',
  query: 'SELECT 1',
  query_kind: 'Select',
  elapsed_seconds: 2,
  memory_usage: 10,
  read_rows: 1,
  read_bytes: 8,
  total_rows_approx: 1,
  progress: 1,
  hostname: 'node-1',
  ...overrides,
});

const completed = (queryId: string): QueryHistoryItem => ({
  query_id: queryId,
  query_type: 'QueryFinish',
  query_kind: 'Select',
  query_start_time: '2026-07-26T00:00:00.000Z',
  query_duration_ms: 2500,
  read_rows: 1,
  read_bytes: 8,
  result_rows: 1,
  result_bytes: 8,
  memory_usage: 10,
  query: 'SELECT 1',
  exception: null,
  user: 'default',
  client_hostname: '',
  type: 'success',
  efficiency_score: null,
  hostname: 'node-1',
});

describe('query activity records', () => {
  it('uses hostname as part of the activity identity', () => {
    expect(queryActivityKey(running('same', { hostname: 'one' })))
      .not.toBe(queryActivityKey(running('same', { hostname: 'two' })));
  });

  it('merges live and completed queries into one activity list', () => {
    const records = buildQueryActivityRecords({
      live: [running('live')],
      recent: [completed('done')],
    }, {}, 5_000);

    expect(records.map(record => record.activitySource)).toEqual(['running', 'history']);
    expect(records[0]?.type).toBe('running');
    expect(records[0]?.query_start_time).toBe('1970-01-01T00:00:03.000Z');
  });

  it('supports status:running without hiding the live rows behind the history range', () => {
    const records = buildQueryActivityRecords({
      live: [running('live')],
      recent: [completed('done')],
    }, {
      status: ['running'],
      startTime: '2030-01-01T00:00:00.000Z',
      endTime: '2030-01-01T01:00:00.000Z',
    }, 5_000);

    expect(records.map(record => record.query_id)).toEqual(['live']);
  });

  it('ORs values within categorical filters while ANDing across fields', () => {
    const records = buildQueryActivityRecords({
      live: [
        running('query-a', { user: 'alice', hostname: 'node-1', query_kind: 'Select' }),
        running('query-b', { user: 'bob', hostname: 'node-2', query_kind: 'Insert' }),
        running('query-c', { user: 'carol', hostname: 'node-1', query_kind: 'Select' }),
      ],
      recent: [],
    }, {
      queryId: ['query-a', 'query-b'],
      user: ['alice', 'bob'],
      hostname: ['node-1', 'node-2'],
      queryKind: ['Select', 'Insert'],
      status: ['running', 'error'],
    }, 5_000);

    expect(records.map(record => record.query_id)).toEqual(['query-a', 'query-b']);
  });

  it('combines selected running and terminal statuses', () => {
    const success = completed('success');
    const error = { ...completed('error'), type: 'error', exception: 'boom' };
    const snapshot = {
      live: [running('live')],
      recent: [success, error],
    };

    expect(buildQueryActivityRecords(snapshot, { status: ['running', 'error'] }, 5_000)
      .map(record => record.query_id)).toEqual(['live', 'error']);
    expect(buildQueryActivityRecords(snapshot, { status: ['success', 'error'] }, 5_000)
      .map(record => record.query_id)).toEqual(['success', 'error']);
  });

  it('filters terminal failures by ClickHouse exception code', () => {
    const unknownTable = {
      ...completed('unknown-table'),
      type: 'error',
      exception: 'Code: 60. UNKNOWN_TABLE',
      exception_code: 60,
    };
    const cancelled = {
      ...completed('cancelled'),
      type: 'error',
      exception: 'Code: 394. QUERY_WAS_CANCELLED',
      exception_code: 394,
    };

    const records = buildQueryActivityRecords({
      live: [running('live')],
      recent: [unknownTable, cancelled],
    }, {
      status: ['error'],
      exceptionCode: [394],
    }, 5_000);

    expect(records.map(record => record.query_id)).toEqual(['cancelled']);
  });

  it('pins live queries above newer completed rows and orders live by elapsed time', () => {
    const records = buildQueryActivityRecords({
      live: [
        running('short', { elapsed_seconds: 2 }),
        running('long', { elapsed_seconds: 20 }),
      ],
      recent: [completed('done')],
    }, {}, 30_000);

    expect(sortQueryActivityRecords(records, {
      field: 'query_start_time',
      direction: 'desc',
    }).map(record => record.query_id)).toEqual(['long', 'short', 'done']);
  });

  it('never infers running state after a query leaves the live source', () => {
    const records = buildQueryActivityRecords({
      live: [],
      recent: [completed('done')],
    }, {}, 5_000);

    expect(records.map(record => record.activitySource)).toEqual(['history']);
  });

  it('treats limit as one cap across live and completed rows', () => {
    const records = buildQueryActivityRecords({
      live: [running('live-1'), running('live-2')],
      recent: [completed('done-1'), completed('done-2')],
    }, { limit: 3 }, 5_000);

    expect(records.map(record => record.query_id)).toEqual([
      'live-1',
      'live-2',
      'done-1',
    ]);
  });

  it('uses the explicit store source for running status and timeline conversion', () => {
    const live = querySelectionToSeries(running('live'), 'running', 5_000);
    const history = querySelectionToSeries(completed('done'), 'history', 5_000);

    expect(live).toMatchObject({
      query_id: 'live',
      is_running: true,
      status: 'Running',
      start_time: '1970-01-01T00:00:03.000Z',
      end_time: '1970-01-01T00:00:05.000Z',
    });
    expect(history).toMatchObject({
      query_id: 'done',
      is_running: false,
      status: 'QueryFinish',
    });
  });

  it('preserves the exact ClickHouse exception type for query details', () => {
    const beforeStart = {
      ...completed('failed'),
      query_type: 'ExceptionBeforeStart',
      type: 'error',
      exception: 'Code: 60. Unknown table',
    };

    expect(querySelectionToSeries(beforeStart, 'history', 5_000))
      .toMatchObject({
        query_id: 'failed',
        status: 'ExceptionBeforeStart',
        exception: 'Code: 60. Unknown table',
      });
  });
});
