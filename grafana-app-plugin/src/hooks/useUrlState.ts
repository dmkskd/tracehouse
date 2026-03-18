/**
 * useUrlState — Grafana plugin version.
 *
 * Uses Grafana's locationService to sync analytics state to URL query params,
 * so links are fully shareable (same as the standalone app).
 *
 * The API surface matches the frontend version so Analytics.tsx works unchanged.
 */

import { useCallback, useEffect, useMemo, useState } from 'react';
import { locationService } from '@grafana/runtime';

export interface AnalyticsUrlState {
  tab?: string;
  preset?: number;
  sql?: string;
  view?: string;
  chart?: string;
  group_by?: string;
  value?: string;
  series?: string;
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

const ANALYTICS_DEFAULTS: AnalyticsUrlState = {
  tab: 'tables',
  view: 'table',
  style: '2d',
  lookback: 7,
};

function parseParams(search: string): AnalyticsUrlState {
  const params = new URLSearchParams(search);
  const state: AnalyticsUrlState = {};
  const tab = params.get('tab');
  if (tab) state.tab = tab;
  const preset = params.get('preset');
  if (preset !== null) state.preset = parseInt(preset, 10);
  const sql = params.get('sql');
  if (sql) state.sql = decodeSql(sql);
  const view = params.get('view');
  if (view) state.view = view;
  const chart = params.get('chart');
  if (chart) state.chart = chart;
  const group_by = params.get('group_by');
  if (group_by) state.group_by = group_by;
  const value = params.get('value');
  if (value) state.value = value;
  const series = params.get('series');
  if (series) state.series = series;
  const style = params.get('style');
  if (style) state.style = style;
  const db = params.get('db');
  if (db) state.db = db;
  const lookback = params.get('lookback');
  if (lookback !== null) state.lookback = parseInt(lookback, 10);
  const fullscreen = params.get('fullscreen');
  if (fullscreen === '1') state.fullscreen = true;
  const fromDashboard = params.get('fromDashboard');
  if (fromDashboard) state.fromDashboard = fromDashboard;
  return state;
}

/** All param keys we manage — used to null out stale keys with locationService.partial() */
const ALL_KEYS = ['tab', 'preset', 'sql', 'view', 'chart', 'group_by', 'value', 'series', 'style', 'db', 'lookback', 'fullscreen', 'fromDashboard'] as const;

function buildParams(state: AnalyticsUrlState): Record<string, string> {
  const params: Record<string, string> = {};
  if (state.tab && state.tab !== ANALYTICS_DEFAULTS.tab) params.tab = state.tab;
  if (state.preset !== undefined) params.preset = String(state.preset);
  if (state.sql && state.preset === undefined) params.sql = encodeSql(state.sql);
  if (state.view && state.view !== ANALYTICS_DEFAULTS.view) params.view = state.view;
  if (state.chart) params.chart = state.chart;
  if (state.group_by) params.group_by = state.group_by;
  if (state.value) params.value = state.value;
  if (state.series) params.series = state.series;
  if (state.style && state.style !== ANALYTICS_DEFAULTS.style) params.style = state.style;
  if (state.db) params.db = state.db;
  if (state.lookback && state.lookback !== ANALYTICS_DEFAULTS.lookback) params.lookback = String(state.lookback);
  if (state.fullscreen) params.fullscreen = '1';
  if (state.fromDashboard) params.fromDashboard = state.fromDashboard;
  return params;
}

/**
 * Build a partial() update that sets wanted params and explicitly nulls out
 * stale ones so locationService.partial() removes them from the URL.
 */
function buildPartialUpdate(state: AnalyticsUrlState): Record<string, string | null> {
  const wanted = buildParams(state);
  const update: Record<string, string | null> = {};
  for (const key of ALL_KEYS) {
    update[key] = key in wanted ? wanted[key] : null;
  }
  return update;
}

function getSearch(): string {
  try {
    return locationService.getLocation().search;
  } catch {
    // Fallback if locationService isn't available in the sandbox
    return window.location.search;
  }
}

/**
 * Syncs analytics state to Grafana's URL via locationService.
 * Reads state from query params on mount and subscribes to location changes.
 * Falls back to window.location if locationService is unavailable.
 */
export function useAnalyticsUrlState() {
  const [search, setSearch] = useState(getSearch);

  useEffect(() => {
    try {
      const unlisten = locationService.getHistory().listen((location: { search: string }) => {
        setSearch(location.search);
      });
      return unlisten;
    } catch {
      // locationService unavailable — poll window.location as fallback
      const interval = setInterval(() => {
        setSearch(prev => {
          const current = window.location.search;
          return current !== prev ? current : prev;
        });
      }, 300);
      return () => clearInterval(interval);
    }
  }, []);

  const state = useMemo(
    () => ({ ...ANALYTICS_DEFAULTS, ...parseParams(search) }),
    [search],
  );

  const update = useCallback(
    (partial: Partial<AnalyticsUrlState>, opts?: { push?: boolean }) => {
      const current = parseParams(getSearch());
      const merged = { ...current, ...partial };
      try {
        const partialUpdate = buildPartialUpdate(merged);
        locationService.partial(partialUpdate, opts?.push ? false : true);
      } catch {
        // Fallback: use window.history directly
        const qs = new URLSearchParams(buildParams(merged)).toString();
        const url = qs ? `${window.location.pathname}?${qs}` : window.location.pathname;
        if (opts?.push) {
          window.history.pushState(null, '', url);
        } else {
          window.history.replaceState(null, '', url);
        }
        setSearch(window.location.search);
      }
    },
    [],
  );

  const copyShareableUrl = useCallback(
    async (overrides?: Partial<AnalyticsUrlState>) => {
      const current = parseParams(getSearch());
      const merged = { ...current, ...overrides };
      const params = buildParams(merged);
      const qs = new URLSearchParams(params).toString();
      const base = window.location.href.split('?')[0];
      const url = qs ? `${base}?${qs}` : base;
      try {
        await navigator.clipboard.writeText(url);
        return true;
      } catch {
        return false;
      }
    },
    [],
  );

  return { state, update, copyShareableUrl };
}
