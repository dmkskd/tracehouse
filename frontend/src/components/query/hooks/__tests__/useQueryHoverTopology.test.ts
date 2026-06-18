import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { QueryAnalyzer, SubQueryInfo } from '@tracehouse/core';
import type { QueryHistoryItem } from '../../../../stores/queryStore';
import { useQueryHoverTopology } from '../useQueryHoverTopology';

const makeQuery = (overrides: Partial<QueryHistoryItem>): QueryHistoryItem => ({
  query_id: 'parent',
  query_type: 'QueryFinish',
  query_kind: 'SELECT',
  query_start_time: '2026-06-18T12:00:00.000Z',
  query_duration_ms: 10,
  read_rows: 1,
  read_bytes: 1,
  result_rows: 1,
  result_bytes: 1,
  memory_usage: 1,
  query: 'SELECT 1',
  exception: null,
  user: 'default',
  client_hostname: '',
  type: 'QueryFinish',
  efficiency_score: null,
  ...overrides,
});

const makeChild = (queryId: string, durationMs = 10): SubQueryInfo => ({
  query_id: queryId,
  hostname: 'node-a',
  query_duration_ms: durationMs,
  memory_usage: 1024,
  read_rows: 10,
  read_bytes: 100,
  query_preview: 'SELECT 1',
  exception_code: 0,
  exception: '',
  query_start_time_microseconds: '2026-06-18 12:00:00.000001',
});

const makeAnalyzer = (
  getSubQueriesForInitialQueries: QueryAnalyzer['getSubQueriesForInitialQueries'],
): QueryAnalyzer => ({
  getSubQueriesForInitialQueries,
}) as QueryAnalyzer;

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

describe('useQueryHoverTopology', () => {
  it('does not fetch child queries while hover preview is disabled', () => {
    const getSubQueriesForInitialQueries = vi.fn();
    const analyzer = makeAnalyzer(getSubQueriesForInitialQueries);

    renderHook(() => useQueryHoverTopology({
      enabled: false,
      queryAnalyzer: analyzer,
      history: [makeQuery({ query_id: 'parent', is_initial_query: 1 })],
      coordinatorIds: new Set(['parent']),
      startTime: '2026-06-18',
    }));

    expect(getSubQueriesForInitialQueries).not.toHaveBeenCalled();
  });

  it('fetches real child rows for coordinator and worker rows from the same root query', async () => {
    const childRows = [makeChild('child-a')];
    const getSubQueriesForInitialQueries = vi.fn().mockResolvedValue(new Map([['parent', childRows]]));
    const analyzer = makeAnalyzer(getSubQueriesForInitialQueries);
    const coordinator = makeQuery({ query_id: 'parent', is_initial_query: 1 });
    const worker = makeQuery({ query_id: 'child-visible', is_initial_query: 0, initial_query_id: 'parent' });
    const plain = makeQuery({ query_id: 'plain', is_initial_query: 1 });

    const { result } = renderHook(() => useQueryHoverTopology({
      enabled: true,
      queryAnalyzer: analyzer,
      history: [coordinator, worker, plain],
      coordinatorIds: new Set(['parent']),
      startTime: '2026-06-18',
    }));

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(getSubQueriesForInitialQueries).toHaveBeenCalledWith(['parent'], '2026-06-18');
    expect(result.current.getChildQueriesForQuery(coordinator)).toBe(childRows);
    expect(result.current.getChildQueriesForQuery(worker)).toBe(childRows);
    expect(result.current.getChildQueriesForQuery(plain)).toBeUndefined();
  });

  it('ignores stale child-query responses after the preview target set changes', async () => {
    const first = deferred<Map<string, SubQueryInfo[]>>();
    const second = deferred<Map<string, SubQueryInfo[]>>();
    const getSubQueriesForInitialQueries = vi.fn()
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(second.promise);
    const analyzer = makeAnalyzer(getSubQueriesForInitialQueries);
    const parentA = makeQuery({ query_id: 'parent-a', is_initial_query: 1 });
    const parentB = makeQuery({ query_id: 'parent-b', is_initial_query: 1 });

    const { result, rerender } = renderHook(
      ({ history }) => useQueryHoverTopology({
        enabled: true,
        queryAnalyzer: analyzer,
        history,
        coordinatorIds: new Set(['parent-a', 'parent-b']),
        startTime: '2026-06-18',
      }),
      { initialProps: { history: [parentA] } },
    );

    rerender({ history: [parentB] });

    await act(async () => {
      second.resolve(new Map([['parent-b', [makeChild('child-b')]]]));
      await second.promise;
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.getChildQueriesForQuery(parentB)?.[0]?.query_id).toBe('child-b');

    await act(async () => {
      first.resolve(new Map([['parent-a', [makeChild('child-a')]]]));
      await first.promise;
    });

    expect(result.current.getChildQueriesForQuery(parentA)).toBeUndefined();
    expect(result.current.getChildQueriesForQuery(parentB)?.[0]?.query_id).toBe('child-b');
  });
});
