/**
 * Dashboard definitions — a dashboard is a titled grid of preset query references.
 *
 * Dashboards are stored as JSON in localStorage so users can create, edit,
 * and delete them. A set of built-in dashboards is seeded on first use.
 *
 * JSON schema (what gets stored / imported / exported):
 * {
 *   "id": "ops-overview",
 *   "title": "Operations Overview",
 *   "description": "High-level server health",
 *   "columns": 2,
 *   "panels": [
 *     { "queryName": "Advanced Dashboard#Queries/second" },
 *     { "queryName": "Advanced Dashboard#CPU Usage (cores)" }
 *   ]
 * }
 */

import { PRESET_QUERIES, type Query, loadCustomQueries, resetCustomQueries } from './presetQueries';
import { resolveQueryRef } from './metaLanguage';

// ─── Types ───

export interface DashboardPanel {
  /** Query reference — namespaced as 'Group#Query Name', or bare 'Query Name' for backward compat */
  queryName: string;
}

export interface Dashboard {
  id: string;
  title: string;
  description?: string;
  columns: 1 | 2 | 3 | 4;
  panels: DashboardPanel[];
  /** true for shipped defaults (user can clone but not delete the originals) */
  builtin?: boolean;
}

// ─── Resolve panel → Query ───

export function resolvePanel(panel: DashboardPanel): Query | undefined {
  const allQueries = [...PRESET_QUERIES, ...loadCustomQueries()];
  return resolveQueryRef(panel.queryName, undefined, allQueries);
}

// ─── localStorage persistence ───

const STORAGE_KEY = 'tracehouse-dashboards-user';
/** Legacy key — migrated once then removed */
const LEGACY_STORAGE_KEY = 'tracehouse-dashboards';

function generateId(): string {
  return `dash-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
}

/** IDs of all builtin dashboards, used to filter them out of legacy storage. */
let _builtinIds: Set<string> | null = null;
function getBuiltinIds(): Set<string> {
  if (!_builtinIds) _builtinIds = new Set(BUILTIN_DASHBOARDS.map(d => d.id));
  return _builtinIds;
}

/**
 * Load all dashboards: builtins from code + user-created from localStorage.
 * Builtins always reflect the latest code — no localStorage caching needed.
 */
export function loadDashboards(): Dashboard[] {
  const builtins = BUILTIN_DASHBOARDS.map(d => ({ ...d, builtin: true }));
  const userDashboards = loadUserDashboards();
  return [...builtins, ...userDashboards];
}

/** Load only user-created dashboards from localStorage. */
function loadUserDashboards(): Dashboard[] {
  // One-time migration from legacy key (which mixed builtins + user)
  migrateLegacyStorage();

  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as Dashboard[];
      if (Array.isArray(parsed)) return parsed;
    }
  } catch { /* corrupt — ignore */ }
  return [];
}

/** Migrate from old storage format that mixed builtins and user dashboards. */
function migrateLegacyStorage(): void {
  const legacy = localStorage.getItem(LEGACY_STORAGE_KEY);
  if (!legacy) return;
  try {
    const parsed = JSON.parse(legacy) as Dashboard[];
    if (Array.isArray(parsed)) {
      // Keep only non-builtin dashboards
      const userOnly = parsed.filter(d => !d.builtin && !getBuiltinIds().has(d.id));
      if (userOnly.length > 0) {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(userOnly));
      }
    }
  } catch { /* ignore corrupt data */ }
  localStorage.removeItem(LEGACY_STORAGE_KEY);
}

/** Persist user dashboards to localStorage (builtins are never stored). */
export function saveDashboards(dashboards: Dashboard[]): void {
  const userOnly = dashboards.filter(d => !d.builtin && !getBuiltinIds().has(d.id));
  localStorage.setItem(STORAGE_KEY, JSON.stringify(userOnly));
}

/** Clear user dashboards and custom queries. */
export function resetDashboardsToBuiltin(): Dashboard[] {
  localStorage.removeItem(STORAGE_KEY);
  localStorage.removeItem(LEGACY_STORAGE_KEY);
  resetCustomQueries();
  return loadDashboards();
}

/** Add or update a dashboard. Returns the updated list. */
export function upsertDashboard(dashboards: Dashboard[], dashboard: Omit<Dashboard, 'id'> & { id?: string }): Dashboard[] {
  const id = dashboard.id || generateId();
  const entry: Dashboard = { ...dashboard, id } as Dashboard;
  const idx = dashboards.findIndex(d => d.id === id);
  const next = idx >= 0
    ? dashboards.map(d => d.id === id ? entry : d)
    : [...dashboards, entry];
  saveDashboards(next);
  return next;
}

/** Delete a dashboard by id. Returns the updated list. */
export function deleteDashboard(dashboards: Dashboard[], id: string): Dashboard[] {
  const next = dashboards.filter(d => d.id !== id);
  saveDashboards(next);
  return next;
}

/** Export a single dashboard as a JSON string (for copy/paste or file save). */
export function exportDashboardJson(dashboard: Dashboard): string {
  const { builtin, ...clean } = dashboard;
  return JSON.stringify(clean, null, 2);
}

/** Import a dashboard from a JSON string. Throws on invalid input. */
export function importDashboardJson(json: string): Omit<Dashboard, 'builtin'> {
  const obj = JSON.parse(json);
  if (!obj.title || !Array.isArray(obj.panels)) {
    throw new Error('Invalid dashboard JSON: needs at least "title" and "panels"');
  }
  return {
    id: obj.id || generateId(),
    title: obj.title,
    description: obj.description,
    columns: [1, 2, 3, 4].includes(obj.columns) ? obj.columns : 2,
    panels: obj.panels.map((p: { queryName?: string }) => ({
      queryName: p.queryName ?? '',
    })),
  };
}

// ─── Built-in seed dashboards ───

const BUILTIN_DASHBOARDS: Dashboard[] = [
  {
    id: 'ops-overview',
    title: 'Operations Overview',
    description: 'Full server health dashboard — mirrors the ClickHouse built-in "Overview" dashboard',
    columns: 2,
    panels: [
      { queryName: 'Advanced Dashboard#Queries/second' },
      { queryName: 'Advanced Dashboard#CPU Usage (cores)' },
      { queryName: 'Advanced Dashboard#Queries Running' },
      { queryName: 'Advanced Dashboard#Merges Running' },
      { queryName: 'Advanced Dashboard#Selected Bytes/second' },
      { queryName: 'Advanced Dashboard#IO Wait (seconds)' },
      { queryName: 'Advanced Dashboard#CPU Wait (seconds)' },
      { queryName: 'Advanced Dashboard#OS CPU Usage (Userspace)' },
      { queryName: 'Advanced Dashboard#OS CPU Usage (Kernel)' },
      { queryName: 'Advanced Dashboard#Read From Disk (bytes/sec)' },
      { queryName: 'Advanced Dashboard#Read From Filesystem (bytes/sec)' },
      { queryName: 'Advanced Dashboard#Memory Tracked (bytes)' },
      { queryName: 'Advanced Dashboard#In-Memory Caches (bytes)' },
      { queryName: 'Advanced Dashboard#Load Average (15 min)' },
      { queryName: 'Advanced Dashboard#Selected Rows/second' },
      { queryName: 'Advanced Dashboard#Inserted Rows/second' },
      { queryName: 'Advanced Dashboard#Total MergeTree Parts' },
      { queryName: 'Advanced Dashboard#Max Parts For Partition' },
      { queryName: 'Advanced Dashboard#Concurrent Network Connections' },
    ],
  },
  {
    id: 'insert-health',
    title: 'Insert Health',
    description: 'Monitor ingestion pipeline: new parts, batch rates, durations',
    columns: 2,
    panels: [
      { queryName: 'Inserts#New Parts Created' },
      { queryName: 'Inserts#Sync Insert Batches' },
      { queryName: 'Inserts#Insert Duration & Batch Count' },
      { queryName: 'Inserts#Insert Duration Quantiles (hourly)' },
      { queryName: 'Inserts#Written Rows & Bytes' },
      { queryName: 'Advanced Dashboard#Inserted Rows/second' },
    ],
  },
  {
    id: 'select-perf',
    title: 'SELECT Performance',
    description: 'Query latency trends, per-user breakdown, read distribution',
    columns: 2,
    panels: [
      { queryName: 'Selects#SELECT Duration Trend (hourly)' },
      { queryName: 'Selects#Queries by User' },
      { queryName: 'Selects#Read Rows Distribution' },
      { queryName: 'Advanced Dashboard#Selected Rows/second' },
      { queryName: 'Advanced Dashboard#Selected Bytes/second' },
    ],
  },
  {
    id: 'storage-parts',
    title: 'Storage & Parts',
    description: 'Disk usage, part counts, and merge pressure indicators',
    columns: 2,
    panels: [
      { queryName: 'Overview#Biggest Tables' },
      { queryName: 'Overview#Database Sizes' },
      { queryName: 'Overview#Active Parts by Table' },
      { queryName: 'Parts#MaxPartCountForPartition' },
      { queryName: 'Advanced Dashboard#Max Parts For Partition' },
      { queryName: 'Advanced Dashboard#Total MergeTree Parts' },
    ],
  },
  {
    id: 'merge-monitoring',
    title: 'Merge Monitoring',
    description: 'Track merge health: active merges, throughput, pool utilization, errors, and historical trends',
    columns: 2,
    panels: [
      { queryName: 'Merges#Active Merges' },
      { queryName: 'Merges#Merge Throughput (bytes/sec)' },
      { queryName: 'Merges#Merges Running (trend)' },
      { queryName: 'Merges#Background Pool Utilization' },
      { queryName: 'Merges#Merge Events Over Time' },
      { queryName: 'Merges#Merge Duration by Table' },
      { queryName: 'Merges#Merge I/O Pressure' },
      { queryName: 'Merges#Merge Errors' },
    ],
  },
  {
    id: 'self-monitoring',
    title: 'App Self-Monitoring',
    description: 'Track our own app footprint: query cost per component, error rates, server load share',
    columns: 2,
    panels: [
      { queryName: 'Self-Monitoring#App Query Duration by Component' },
      { queryName: 'Self-Monitoring#App Query Volume by Component' },
      { queryName: 'Self-Monitoring#App Query Timeline (5min buckets)' },
      { queryName: 'Self-Monitoring#App Query Cost Details' },
      { queryName: 'Self-Monitoring#App Query Duration Trend (hourly)' },
      { queryName: 'Self-Monitoring#App % of Server Load' },
      { queryName: 'Self-Monitoring#Slowest App Queries' },
      { queryName: 'Self-Monitoring#App Failed Queries' },
    ],
  },
];
