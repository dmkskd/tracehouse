/**
 * useAppLocation - Router-agnostic location hook
 * 
 * This hook provides location information that works in both:
 * - Standalone frontend (uses react-router-dom)
 * - Grafana plugin (uses stub/Grafana's location service)
 * 
 * Pages should use this instead of importing useLocation directly
 * from react-router-dom to ensure compatibility with both contexts.
 */

import { useCallback, useEffect, useRef } from 'react';
import { useLocation, useNavigate, useParams, useSearchParams as useRouterSearchParams } from 'react-router-dom';

export interface AppLocation {
  pathname: string;
  search: string;
  hash: string;
  state: unknown;
}

export function useAppLocation(): AppLocation {
  const location = useLocation();
  return {
    pathname: location.pathname,
    search: location.search,
    hash: location.hash,
    state: location.state,
  };
}

/**
 * Router search params whose updates keep the navigation state. A page that rewrites its own
 * URL (Time Travel, Queries, Merges) would otherwise drop `state.from`, and after a reload or
 * remount it no longer knows the page it was opened from. Options passed by the caller win.
 */
export function useSearchParams(): ReturnType<typeof useRouterSearchParams> {
  const [params, setParams] = useRouterSearchParams();
  const { state } = useLocation();
  // The setter keeps one identity: callers write the URL from effects that depend on it, and
  // each write gives the location a new (cloned) state object.
  const latestState = useRef(state);
  useEffect(() => { latestState.current = state; }, [state]);
  const setKeepingState = useCallback<ReturnType<typeof useRouterSearchParams>[1]>(
    (next, options) => setParams(next, { state: latestState.current, ...options }), [setParams]);
  return [params, setKeepingState];
}

// Re-export router hooks for convenience
export { useNavigate, useParams };
