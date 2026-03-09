/**
 * useUrlState — Grafana plugin shim.
 *
 * In the Grafana plugin context there's no react-router, so URL state
 * is kept in local component state only (no URL sync). The API surface
 * matches the frontend version so Analytics.tsx works unchanged.
 */

import { useCallback, useState } from 'react';

export interface AnalyticsUrlState {
  tab?: string;
  preset?: number;
  sql?: string;
  view?: string;
  chart?: string;
  labels?: string;
  values?: string;
  group?: string;
  style?: string;
  db?: string;
  lookback?: number;
  fullscreen?: boolean;
  fromDashboard?: string;
}

export function encodeSql(sql: string): string {
  if (/^[a-zA-Z0-9\s_.*,'()=<>]+$/.test(sql) && sql.length < 200) return sql;
  return 'b64:' + btoa(unescape(encodeURIComponent(sql)));
}

export function decodeSql(encoded: string): string {
  if (encoded.startsWith('b64:')) {
    try { return decodeURIComponent(escape(atob(encoded.slice(4)))); } catch { return ''; }
  }
  return encoded;
}

/**
 * Grafana-compatible version: state lives in React state, not the URL.
 */
export function useAnalyticsUrlState() {
  const [state, setState] = useState<AnalyticsUrlState>({
    tab: 'tables',
    view: 'table',
    style: '2d',
    lookback: 7,
  });

  const update = useCallback(
    (partial: Partial<AnalyticsUrlState>, _opts?: { push?: boolean }) => {
      setState(prev => ({ ...prev, ...partial }));
    },
    [],
  );

  const copyShareableUrl = useCallback(
    async (_overrides?: Partial<AnalyticsUrlState>) => {
      try {
        await navigator.clipboard.writeText(window.location.href);
        return true;
      } catch {
        return false;
      }
    },
    [],
  );

  return { state, update, copyShareableUrl };
}
