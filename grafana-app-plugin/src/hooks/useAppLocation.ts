/**
 * useAppLocation - Grafana plugin implementation
 * 
 * Uses our own LocationContext instead of react-router to avoid
 * context issues with Grafana's plugin loading system.
 */

import { createContext, useContext, useState, useCallback } from 'react';

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
    if (ctx && typeof to === 'string') {
      ctx.navigate(to, options);
    }
  };
}

// Stub for useParams - returns empty object in Grafana context
export function useParams<T extends Record<string, string | undefined> = Record<string, string | undefined>>(): T {
  return {} as T;
}

// Stub for useSearchParams - returns empty URLSearchParams
export function useSearchParams(): [URLSearchParams, (params: URLSearchParams) => void] {
  const [params] = useState(() => new URLSearchParams());
  const setParams = useCallback((_newParams: URLSearchParams) => {
    // No-op in Grafana context
  }, []);
  return [params, setParams];
}
