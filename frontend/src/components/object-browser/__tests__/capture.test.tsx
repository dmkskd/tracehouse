import { act, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup } from '@testing-library/react';
import { ClickHouseContext } from '@tracehouse/ui-shared';
import type { ClickHouseServices } from '@tracehouse/ui-shared';
import type { Capture } from '@tracehouse/core/services/object-browser';
import { collectObjects } from '@tracehouse/core/services/object-browser';
import { useObjectCapture } from '../useObjectCapture';

vi.mock('@tracehouse/core/services/object-browser', async importOriginal => ({
  ...await importOriginal<typeof import('@tracehouse/core/services/object-browser')>(), collectObjects: vi.fn(),
}));
afterEach(() => { cleanup();vi.clearAllMocks(); });
const input = { principal: 'alice', operation: 'SELECT' as const, database: 'demo', table: 'trips' };
const service = { adapter: { executeQuery: vi.fn() } } as unknown as ClickHouseServices;

describe('manual object capture lifetime', () => {
  it('does not read on mount and rejects an outstanding result after unmount', async () => {
    let resolve!: (capture: Capture) => void;
    vi.mocked(collectObjects).mockImplementationOnce(() => new Promise(r => { resolve = r; }));
    const { result, unmount } = renderHook(() => useObjectCapture(), {
      wrapper: ({ children }) => <ClickHouseContext.Provider value={service}>{children}</ClickHouseContext.Provider>,
    });
    expect(collectObjects).not.toHaveBeenCalled();
    let pending!: Promise<void>;
    act(() => { pending = result.current.collect(input); });
    await waitFor(() => expect(collectObjects).toHaveBeenCalledTimes(1));
    const cancellation = vi.mocked(collectObjects).mock.calls[0][2]!.cancellation!;
    expect(cancellation.aborted).toBe(false);
    unmount();expect(cancellation.aborted).toBe(true);
    await act(async () => { resolve({} as Capture);await pending; });
  });
  it('disposes the prior request when refreshed and publishes only the latest one', async () => {
    const resolves: ((capture: Capture) => void)[] = [];
    vi.mocked(collectObjects).mockImplementation(() => new Promise(r => resolves.push(r)));
    const { result } = renderHook(() => useObjectCapture(), {
      wrapper: ({ children }) => <ClickHouseContext.Provider value={service}>{children}</ClickHouseContext.Provider>,
    });
    let first!: Promise<void>, second!: Promise<void>;
    act(() => { first = result.current.collect(input);second = result.current.collect(input); });
    expect(vi.mocked(collectObjects).mock.calls[0][2]!.cancellation!.aborted).toBe(true);
    const newest = { schemaVersion: 1 } as Capture;
    await act(async () => { resolves[1](newest);await second;resolves[0]({ schemaVersion: 0 } as unknown as Capture);await first; });
    expect(result.current.capture).toBe(newest);
  });
});
