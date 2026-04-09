/**
 * useUrlState — sync component state to URL search params for shareable links.
 *
 * Works with React Router's search params. Each page defines a schema of
 * param names → serializers, and the hook keeps the URL in sync.
 *
 * URL format:  /analytics?tab=misc&preset=3&view=chart&chart=bar&style=3d
 *
 * Design notes (adapted from k8s-compass urlState.ts):
 *  - Preset index is preferred over raw SQL for shorter URLs
 *  - Complex SQL is base64-encoded to survive URL encoding
 *  - Undefined/default values are omitted to keep URLs clean
 *  - Uses replaceState (no history spam while tweaking controls)
 */

import { useCallback, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';

// ─── Generic schema-driven URL state ───

export type UrlParamType = 'string' | 'number' | 'boolean';

export interface UrlParamDef<T = unknown> {
  type: UrlParamType;
  default?: T;
}

export type UrlSchema = Record<string, UrlParamDef>;

export type UrlStateFromSchema<S extends UrlSchema> = {
  [K in keyof S]: S[K]['type'] extends 'number'
    ? number | undefined
    : S[K]['type'] extends 'boolean'
      ? boolean | undefined
      : string | undefined;
};

function parseParam(raw: string | null, def: UrlParamDef): unknown {
  if (raw === null || raw === '') return def.default;
  switch (def.type) {
    case 'number': { const n = Number(raw); return Number.isFinite(n) ? n : def.default; }
    case 'boolean': return raw === '1' || raw === 'true';
    default: return raw;
  }
}

function serializeParam(value: unknown, def: UrlParamDef): string | undefined {
  if (value === undefined || value === null || value === '') return undefined;
  if (value === def.default) return undefined;
  if (def.type === 'boolean') return value ? '1' : '0';
  return String(value);
}

/**
 * Generic schema-driven URL state hook.
 *
 * Only touches keys defined in the schema — all other search params
 * (e.g. qd_id from useQueryDeepLink) are preserved on update.
 */
export function useUrlState<S extends UrlSchema>(schema: S) {
  const [searchParams, setSearchParams] = useSearchParams();

  const state = useMemo(() => {
    const result: Record<string, unknown> = {};
    for (const [key, def] of Object.entries(schema)) {
      result[key] = parseParam(searchParams.get(key), def);
    }
    return result as UrlStateFromSchema<S>;
  }, [searchParams, schema]);

  const update = useCallback(
    (partial: Partial<UrlStateFromSchema<S>>, opts?: { push?: boolean }) => {
      setSearchParams(prev => {
        const next = new URLSearchParams(prev); // preserve unknown params
        for (const [key, def] of Object.entries(schema)) {
          const value = key in partial
            ? (partial as Record<string, unknown>)[key]
            : parseParam(prev.get(key), def);
          const serialized = serializeParam(value, def);
          if (serialized !== undefined) next.set(key, serialized);
          else next.delete(key);
        }
        return next;
      }, { replace: !opts?.push });
    },
    [setSearchParams, schema],
  );

  return { state, update };
}

// ─── SQL encoding (matches k8s-compass pattern) ───

export function encodeSql(sql: string): string {
  if (/^[a-zA-Z0-9\s_.*,'()=<>]+$/.test(sql) && sql.length < 200) {
    return sql;
  }
  return 'b64:' + btoa(unescape(encodeURIComponent(sql)));
}

export function decodeSql(encoded: string): string {
  if (encoded.startsWith('b64:')) {
    try {
      return decodeURIComponent(escape(atob(encoded.slice(4))));
    } catch {
      return '';
    }
  }
  return encoded;
}

// ─── Analytics URL state ───

export interface AnalyticsUrlState {
  tab?: string;          // 'tables' | 'misc'
  preset?: number;       // index into PRESET_QUERIES
  sql?: string;          // custom SQL (decoded)
  view?: string;         // 'table' | 'chart' | 'queries'
  chart?: string;        // chart type
  group_by?: string;     // group-by column
  value?: string;        // value column
  series?: string;       // series column
  style?: string;        // '2d' | '3d'
  db?: string;           // selected database
  lookback?: number;     // lookback days
  fullscreen?: boolean;  // chart fullscreen mode
  fromDashboard?: string; // dashboard ID we navigated from
  noAutoExecute?: boolean; // skip auto-execute on mount (e.g. expensive queries opened from detail modal)
}

const ANALYTICS_DEFAULTS: AnalyticsUrlState = {
  tab: 'dashboards',
  view: 'table',
  style: '2d',
  lookback: 7,
};

/** Read analytics state from current URL search params */
function parseAnalyticsParams(params: URLSearchParams): AnalyticsUrlState {
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
  const noAutoExecute = params.get('noAutoExecute');
  if (noAutoExecute === '1') state.noAutoExecute = true;
  return state;
}

/** Write analytics state to URL search params, omitting defaults */
function buildAnalyticsParams(state: AnalyticsUrlState): Record<string, string> {
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
  if (state.noAutoExecute) params.noAutoExecute = '1';
  return params;
}

/** All param keys managed by useAnalyticsUrlState */
const ANALYTICS_KEYS = ['tab', 'preset', 'sql', 'view', 'chart', 'group_by', 'value', 'series', 'style', 'db', 'lookback', 'fullscreen', 'fromDashboard', 'noAutoExecute'] as const;

// ─── Hook ───

/**
 * Reads analytics URL state from search params and provides an updater
 * that merges partial state and syncs back to the URL.
 *
 * Uses `replaceState` by default so chart control tweaks don't pollute
 * browser history. Call `update(state, { push: true })` for navigation-
 * worthy changes (e.g. selecting a preset).
 */
export function useAnalyticsUrlState() {
  const [searchParams, setSearchParams] = useSearchParams();

  const state = useMemo(() => parseAnalyticsParams(searchParams), [searchParams]);

  const update = useCallback(
    (partial: Partial<AnalyticsUrlState>, opts?: { push?: boolean }) => {
      setSearchParams(prev => {
        const current = parseAnalyticsParams(prev);
        const merged = { ...current, ...partial };
        const built = buildAnalyticsParams(merged);
        // Preserve unknown params (e.g. qd_id from useQueryDeepLink)
        const next = new URLSearchParams(prev);
        for (const key of ANALYTICS_KEYS) next.delete(key);
        for (const [k, v] of Object.entries(built)) next.set(k, v);
        return next;
      }, { replace: !opts?.push });
    },
    [setSearchParams],
  );

  /** Generate a full shareable URL for the current state */
  const getShareableUrl = useCallback(
    (overrides?: Partial<AnalyticsUrlState>) => {
      const merged = { ...state, ...overrides };
      const params = buildAnalyticsParams(merged);
      const qs = new URLSearchParams(params).toString();
      const base = window.location.href.split('?')[0];
      return qs ? `${base}?${qs}` : base;
    },
    [state],
  );

  /** Copy shareable URL to clipboard */
  const copyShareableUrl = useCallback(
    async (overrides?: Partial<AnalyticsUrlState>) => {
      const url = getShareableUrl(overrides);
      try {
        await navigator.clipboard.writeText(url);
        return true;
      } catch {
        return false;
      }
    },
    [getShareableUrl],
  );

  return { state, update, getShareableUrl, copyShareableUrl };
}
