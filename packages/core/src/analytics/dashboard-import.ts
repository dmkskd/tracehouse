import { parseQueryMetadata } from './meta-language.js';
import type { Dashboard, DashboardFilter, DashboardPanel } from './dashboard-types.js';

export type { Dashboard, DashboardFilter, DashboardPanel, DashboardGroup } from './dashboard-types.js';

type ImportedDashboard = Omit<Dashboard, 'builtin'>;

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function parseDashboard(value: unknown, titleOverride?: string | null): ImportedDashboard {
  if (!isRecord(value) || !Array.isArray(value.panels)) {
    throw new Error('Invalid dashboard JSON: needs at least "title" and "panels"');
  }
  const title = titleOverride ?? value.title;
  if (typeof title !== 'string' || !title.trim()) {
    throw new Error('Dashboard title must be a non-empty string');
  }
  let queries: string[] | undefined;
  if (value.queries !== undefined) {
    if (!Array.isArray(value.queries)) throw new Error('Dashboard queries must be an array of SQL strings');
    const names = new Set<string>();
    queries = value.queries.map((sql: unknown) => {
      const query = typeof sql === 'string' ? parseQueryMetadata(sql, 'custom') : null;
      if (!query) throw new Error('Each embedded query needs a valid @meta title and group');
      const key = `${query.group}#${query.name}`;
      if (names.has(key)) throw new Error(`Duplicate embedded query: ${key}`);
      names.add(key);
      return sql as string;
    });
  }
  const panels: DashboardPanel[] = value.panels.map((panel: unknown) => {
    if (!isRecord(panel) || typeof panel.queryName !== 'string') {
      throw new Error('Each dashboard panel needs a queryName string');
    }
    return {
      queryName: panel.queryName,
      ...(Boolean(panel.section) && { section: String(panel.section) }),
      ...(Boolean(panel.requiredCapability) && { requiredCapability: String(panel.requiredCapability) }),
    };
  });
  let filters: DashboardFilter[] | undefined;
  if (value.filters !== undefined) {
    if (!Array.isArray(value.filters)) throw new Error('Dashboard filters must be an array');
    filters = value.filters.map((filter: unknown) => {
      if (!isRecord(filter) || typeof filter.param !== 'string' || !filter.param.trim()
        || typeof filter.query !== 'string' || !filter.query.trim()) {
        throw new Error('Each dashboard filter needs non-empty param and query strings');
      }
      return {
        param: filter.param,
        label: String(filter.label ?? filter.param),
        query: filter.query,
        ...(typeof filter.dependsOn === 'string' && { dependsOn: filter.dependsOn }),
      };
    });
  }
  return {
    id: `dash-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
    title: title.trim(),
    ...(typeof value.description === 'string' && { description: value.description }),
    group: 'Custom',
    ...(typeof value.category === 'string' && { category: value.category }),
    ...(queries !== undefined && { queries }),
    ...(Boolean(value.source) && { source: String(value.source) }),
    columns: ([1, 2, 3, 4].includes(value.columns as number) ? value.columns : 2) as Dashboard['columns'],
    panels,
    ...(filters && filters.length > 0 && { filters }),
  };
}

/** Parse dashboard data. This validates its structure, not the safety of its SQL. */
export function importDashboardJson(json: string): ImportedDashboard {
  return parseDashboard(JSON.parse(json));
}

export interface EmbeddedSql {
  kind: 'query' | 'filter';
  label: string;
  sql: string;
}

/** Include all SQL definitions, including targets that run only on interaction. */
export function listEmbeddedSql(dashboard: Pick<Dashboard, 'queries' | 'filters'>): EmbeddedSql[] {
  return [
    ...(dashboard.queries ?? []).map(sql => {
      const query = parseQueryMetadata(sql, 'custom');
      return { kind: 'query' as const, label: query ? `${query.group}#${query.name}` : 'Untitled query', sql };
    }),
    ...(dashboard.filters ?? []).map(filter => ({ kind: 'filter' as const, label: filter.label, sql: filter.query })),
  ];
}

export interface DashboardImportOptions {
  nameOverride?: string | null;
  existingTitles?: readonly string[];
  sqlAcknowledged?: boolean;
}

export type DashboardImportInspection =
  | { ok: true; title: string; dashboard: ImportedDashboard; embeddedSql: EmbeddedSql[]; nameClash: boolean; canImport: boolean }
  | { ok: false; title: string; error: string; nameClash: false; canImport: false };

/** Inspect the exact normalised payload that will be imported, including its name override. */
export function inspectDashboardImport(json: string, options: DashboardImportOptions = {}): DashboardImportInspection {
  let title = options.nameOverride ?? '';
  try {
    const parsed: unknown = JSON.parse(json);
    if (options.nameOverride == null && isRecord(parsed) && typeof parsed.title === 'string') title = parsed.title;
    const dashboard = parseDashboard(parsed, options.nameOverride);
    const embeddedSql = listEmbeddedSql(dashboard);
    const nameClash = (options.existingTitles ?? []).some(existing =>
      existing.trim().toLocaleLowerCase() === dashboard.title.toLocaleLowerCase());
    return {
      ok: true, title, dashboard, embeddedSql, nameClash,
      canImport: !nameClash && (embeddedSql.length === 0 || options.sqlAcknowledged === true),
    };
  } catch (error) {
    return { ok: false, title, error: error instanceof Error ? error.message : String(error), nameClash: false, canImport: false };
  }
}

/** Recheck validation and consent at submission; return only the payload that was inspected. */
export function acceptDashboardImport(json: string, options: DashboardImportOptions = {}): ImportedDashboard {
  const inspection = inspectDashboardImport(json, options);
  if (!inspection.ok) throw new Error(inspection.error);
  if (inspection.nameClash) throw new Error('A dashboard with this name already exists');
  if (!inspection.canImport) throw new Error('Review the embedded SQL and confirm that you trust its source');
  return inspection.dashboard;
}
