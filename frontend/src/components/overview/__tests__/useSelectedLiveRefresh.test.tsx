/**
 * The panel's 1s refresh must report completion itself (the page poll may be
 * paused) and must not keep an old override standing after an error.
 */

import type { PropsWithChildren } from 'react';
import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ClickHouseContext, type ClickHouseServices } from '@tracehouse/ui-shared';
import { useSelectedLiveRefresh } from '../useSelectedLiveRefresh';
import type { BlockEntry } from '../arena-types';

afterEach(() => {
  vi.useRealTimers();
});

const selected: BlockEntry = {
  id: 'q1', kind: 'SELECT', color: 'hsl(217, 91%, 60%)', label: 'SELECT 1', tableHint: 'db.t',
  isMerge: false, deck: 'select', lane: 0, startTime: 0, endTime: null,
  cpu: 1, mem: 1, elapsed: 1, progress: 10, ioReadRate: 0, rowsRead: 0, bytesRead: 0, queryId: 'q1',
};

const processesRow = (readRows: number) => ({
  hostname: 'h1', query_id: 'q1', user: 'default', elapsed: 2, memory_usage: 100,
  read_rows: readRows, read_bytes: 0, total_rows_approx: 1000, query_kind: 'Select', query: 'SELECT 1',
  user_time_us: 0, system_time_us: 0, os_read_bytes: 0, os_write_bytes: 0,
  selected_parts: 0, selected_marks: 0, mark_cache_hits: 0, mark_cache_misses: 0,
});

function harness(executeQuery: ReturnType<typeof vi.fn>) {
  const services = { adapter: { executeQuery }, environmentDetector: undefined } as unknown as ClickHouseServices;
  const wrapper = ({ children }: PropsWithChildren) => (
    <ClickHouseContext.Provider value={services}>{children}</ClickHouseContext.Provider>
  );
  return renderHook(({ entry }: { entry: BlockEntry }) => useSelectedLiveRefresh(entry), { wrapper, initialProps: { entry: selected } });
}

const tick = () => act(async () => { await vi.advanceTimersByTimeAsync(1000); });

describe('useSelectedLiveRefresh', () => {
  it('reports completion when the operation is gone, keeping its last values', async () => {
    vi.useFakeTimers();
    const executeQuery = vi.fn()
      .mockResolvedValueOnce([processesRow(500)])
      .mockResolvedValue([]);
    const hook = harness(executeQuery);

    await tick();
    expect(hook.result.current).toEqual({ metrics: expect.objectContaining({ progress: 50 }), finishedAt: null });

    await tick();
    expect(hook.result.current.finishedAt).not.toBeNull();
    expect(hook.result.current.metrics).toMatchObject({ progress: 50 });

    // Done: no further lookups.
    const calls = executeQuery.mock.calls.length;
    await tick();
    expect(executeQuery.mock.calls.length).toBe(calls);
  });

  it('drops its override on an error so the page sample shows again', async () => {
    vi.useFakeTimers();
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const executeQuery = vi.fn()
      .mockResolvedValueOnce([processesRow(500)])
      .mockRejectedValue(new Error('connection reset'));
    const hook = harness(executeQuery);

    await tick();
    expect(hook.result.current.metrics).not.toBeNull();
    await tick();
    expect(hook.result.current).toEqual({ metrics: null, finishedAt: null });
  });

  it('stops overriding once the block is finished, so the final record shows', async () => {
    vi.useFakeTimers();
    const executeQuery = vi.fn().mockResolvedValueOnce([processesRow(500)]).mockResolvedValue([]);
    const hook = harness(executeQuery);
    await tick();
    await tick();
    expect(hook.result.current.finishedAt).not.toBeNull();

    // The arena marks the entry finished; later its final record arrives.
    hook.rerender({ entry: { ...selected, endTime: 5000, final: true } });
    expect(hook.result.current).toEqual({ metrics: null, finishedAt: null });
  });
});
