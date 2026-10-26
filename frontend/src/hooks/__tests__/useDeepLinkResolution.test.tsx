import { act, renderHook } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { LinkResolver, LinkStatus } from '@tracehouse/core';
import { useDeepLinkResolution } from '../useDeepLinkResolution';

const resolver = { pollIntervalMs: 1_000, pendingFlushWindowMs: 5_000 } as unknown as LinkResolver;

describe('useDeepLinkResolution', () => {
  it("clears a settled lookup's banner when a new lookup starts", async () => {
    const hook = renderHook(() => useDeepLinkResolution());

    await act(async () => {
      await hook.result.current.run(resolver, 'query', 'old-id', async () => null, async (): Promise<LinkStatus> => ({ state: 'not_found' }));
    });
    expect(hook.result.current.banner).toMatchObject({ subject: 'old-id', status: { state: 'not_found' } });

    // The new lookup is still loading: the old "not found" must not stay up.
    let resolveFetch!: (v: string | null) => void;
    act(() => {
      void hook.result.current.run(resolver, 'query', 'new-id', () => new Promise<string | null>(r => { resolveFetch = r; }), async () => ({ state: 'not_found' }));
    });
    expect(hook.result.current.banner).toBeNull();

    await act(async () => { resolveFetch('record'); });
    expect(hook.result.current.banner).toBeNull();
  });
});
