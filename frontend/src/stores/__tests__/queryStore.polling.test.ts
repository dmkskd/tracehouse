import { afterEach, describe, expect, it, vi } from 'vitest';
import type { QueryAnalyzer, QueryMetrics } from '@tracehouse/core';
import { QueryWebSocket, useQueryStore } from '../queryStore';

const LIVE_QUERY: QueryMetrics = {
  query_id: 'live-query',
  user: 'default',
  query: 'SELECT 1',
  query_kind: 'Select',
  elapsed_seconds: 1,
  memory_usage: 0,
  read_rows: 0,
  read_bytes: 0,
  total_rows_approx: 0,
  progress: 0,
};

afterEach(() => {
  vi.useRealTimers();
  useQueryStore.getState().clearQueries();
});

describe('QueryWebSocket polling adapter', () => {
  it('fetches once when automatic refresh is paused', async () => {
    vi.useFakeTimers();
    const getRunningQueries = vi.fn().mockResolvedValue([LIVE_QUERY]);
    const poller = new QueryWebSocket({ getRunningQueries } as unknown as QueryAnalyzer, 0);

    poller.connect();
    await vi.waitFor(() => {
      expect(useQueryStore.getState().runningQueries).toEqual([LIVE_QUERY]);
    });
    await vi.advanceTimersByTimeAsync(30_000);

    expect(getRunningQueries).toHaveBeenCalledTimes(1);
    poller.disconnect();
  });

  it('forwards the unified result limit to the live query source', async () => {
    const getRunningQueries = vi.fn().mockResolvedValue([]);
    const poller = new QueryWebSocket(
      { getRunningQueries } as unknown as QueryAnalyzer,
      0,
      100,
    );

    poller.connect();
    await vi.waitFor(() => expect(getRunningQueries).toHaveBeenCalledWith(100));
    poller.disconnect();
  });
});
