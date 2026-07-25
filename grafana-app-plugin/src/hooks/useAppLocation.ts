/**
 * useAppLocation - Grafana plugin implementation
 *
 * Uses LocationContext for navigation and Grafana's locationService
 * for URL search params, so query parameters work across all pages.
 */

import { createContext, useContext, useState, useCallback, useEffect, useMemo } from 'react';
import { locationService } from '@grafana/runtime';

export interface AppLocation {
  pathname: string;
  search: string;
  hash: string;
  state: unknown;
}

export interface LocationContextValue {
  location: AppLocation;
  navigate: (to: string, options?: { state?: unknown; replace?: boolean }) => void;
}

// Create our own context - this is set up in App.tsx
export const LocationContext = createContext<LocationContextValue | null>(null);

const PLUGIN_BASE_PATH = '/a/dmkskd-tracehouse-app';

/** Map standalone frontend routes onto Grafana's app-plugin route namespace. */
export function toPluginPath(path: string): string {
  if (!path.startsWith('/') || path.startsWith(PLUGIN_BASE_PATH)) return path;
  return `${PLUGIN_BASE_PATH}${path}`;
}

export function useAppLocation(): AppLocation {
  const ctx = useContext(LocationContext);
  if (!ctx) {
    // Fallback for when context isn't ready yet
    return { pathname: '/', search: '', hash: '', state: null };
  }
  return ctx.location;
}

export function useNavigate() {
  const ctx = useContext(LocationContext);
  return (to: string | number, options?: { state?: unknown; replace?: boolean }) => {
    if (typeof to === 'number') {
      locationService.getHistory().go(to);
      return;
    }

    ctx?.navigate(to, options);
    const target = toPluginPath(to);
    if (options?.replace) locationService.replace(target);
    else locationService.push(target);
  };
}

// Stub for useParams - returns empty object in Grafana context
export function useParams<T extends Record<string, string | undefined> = Record<string, string | undefined>>(): T {
  return {} as T;
}

function getSearch(): string {
  return locationService.getLocation().search;
}

/**
 * Reads/writes URL search params via Grafana's locationService.
 * This replaces the no-op stub so pages like QueryTracer and
 * DashboardViewer can pass state through the URL.
 */
export function useSearchParams(): [URLSearchParams, (params: URLSearchParams, opts?: { replace?: boolean }) => void] {
  const [search, setSearch] = useState(getSearch);

  useEffect(() => {
    const unlisten = locationService.getHistory().listen((location: { search: string }) => {
      setSearch(location.search);
    });
    return unlisten;
  }, []);

  const params = useMemo(() => new URLSearchParams(search), [search]);

  const setParams = useCallback(
    (newParams: URLSearchParams, opts?: { replace?: boolean }) => {
      const query: Record<string, string | null> = {};

      // Null out all current params first
      const currentParams = new URLSearchParams(getSearch());
      currentParams.forEach((_val, key) => {
        query[key] = null;
      });

      // Set new params
      newParams.forEach((val, key) => {
        query[key] = val;
      });

      const replace = opts?.replace !== false; // default to replace
      locationService.partial(query, replace);
    },
    [],
  );

  return [params, setParams];
}
