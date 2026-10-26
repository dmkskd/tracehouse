/**
 * useDeepLinkResolution — runs a deep-link lookup through resolveDeepLink and
 * keeps the status (running / pending flush / not found / error) for
 * <DeepLinkStatusBanner>. Starting a new lookup, dismissing, or unmounting
 * stops any polling in flight.
 */

import { useState, useRef, useCallback, useEffect } from 'react';
import { resolveDeepLink } from '@tracehouse/core';
import type { DeepLinkKind, LinkResolver, LinkStatus } from '@tracehouse/core';

export interface DeepLinkBannerState {
  kind: DeepLinkKind;
  subject: string;
  status: LinkStatus | null;
  error?: string;
}

export function useDeepLinkResolution() {
  const [banner, setBanner] = useState<DeepLinkBannerState | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const run = useCallback(<T>(
    resolver: LinkResolver,
    kind: DeepLinkKind,
    subject: string,
    fetch: () => Promise<T | null>,
    diagnose: () => Promise<LinkStatus>,
  ): Promise<T | null> => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    // A previous lookup's banner (e.g. a settled "not found") describes the old
    // link; dismissing it would otherwise cancel this new lookup.
    setBanner(null);
    return resolveDeepLink({
      fetch,
      diagnose,
      signal: controller.signal,
      pollIntervalMs: resolver.pollIntervalMs,
      pendingFlushWindowMs: resolver.pendingFlushWindowMs,
      onStatus: status => setBanner(status ? { kind, subject, status } : null),
    }).catch((err: unknown) => {
      if (!controller.signal.aborted) {
        setBanner({ kind, subject, status: null, error: err instanceof Error ? err.message : String(err) });
      }
      throw err;
    });
  }, []);

  const dismiss = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    setBanner(null);
  }, []);

  useEffect(() => () => abortRef.current?.abort(), []);

  return { run, dismiss, banner };
}
