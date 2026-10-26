/**
 * requestRefresh() must give the latest completion a refetch after its log row
 * can have been flushed, even when it lands while an earlier round is pending.
 */

import type { PropsWithChildren } from 'react';
import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ClickHouseContext, type ClickHouseServices } from '@tracehouse/ui-shared';
import { useRecentActivity } from '../useRecentActivity';

afterEach(() => {
  vi.useRealTimers();
});

function harness() {
  // Count backfill rounds by their query_log read.
  const executeQuery = vi.fn(async () => []);
  const services = { adapter: { executeQuery }, environmentDetector: undefined } as unknown as ClickHouseServices;
  const wrapper = ({ children }: PropsWithChildren) => (
    <ClickHouseContext.Provider value={services}>{children}</ClickHouseContext.Provider>
  );
  const hook = renderHook(() => useRecentActivity(120, 1000, true), { wrapper });
  const rounds = () => executeQuery.mock.calls.filter(([sql]) => String(sql).includes('system.query_log')).length;
  return { hook, rounds };
}

describe('useRecentActivity.requestRefresh', () => {
  it('re-arms the trailing refetch for a completion that arrives while a round is pending', async () => {
    vi.useFakeTimers();
    const { hook, rounds } = harness();
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(rounds()).toBe(1); // initial backfill

    act(() => hook.result.current.requestRefresh());
    await act(async () => { await vi.advanceTimersByTimeAsync(15_000); }); // first refetch at 8s
    expect(rounds()).toBe(2);

    // A second completion at 15s, before the first request's trailing refetch (20s).
    act(() => hook.result.current.requestRefresh());
    await act(async () => { await vi.advanceTimersByTimeAsync(10_000); }); // t=25s: its first refetch at 23s
    expect(rounds()).toBe(3);
    await act(async () => { await vi.advanceTimersByTimeAsync(10_000); }); // t=35s: trailing re-armed to 35s
    expect(rounds()).toBe(4);
  });
});
