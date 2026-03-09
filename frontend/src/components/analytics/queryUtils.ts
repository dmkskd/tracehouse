/**
 * Query types, constants, and utility functions: metadata parsing, time range
 * resolution, drill-down parameter handling, RAG evaluation, and custom query
 * persistence.
 *
 * Extracted from presetQueries.ts to keep that file focused on query definitions.
 */

/* ─── types ─── */

export type QueryGroup = 'Overview' | 'Inserts' | 'Selects' | 'Parts' | 'Merges' | 'Resources' | 'Advanced Dashboard' | 'Self-Monitoring' | 'Custom';

export type ChartType = 'bar' | 'line' | 'pie' | 'area' | 'grouped_bar' | 'stacked_bar' | 'grouped_line';
export type ChartStyle = '2d' | '3d';

/** RAG (Red/Amber/Green) threshold rule for a column. Values below greenBelow are green, below amberBelow are amber, otherwise red. */
export interface RagRule {
  column: string;
  greenBelow: number;
  amberBelow: number;
}

export interface PresetQuery {
  name: string;
  description: string;
  sql: string;
  group: QueryGroup;
  chartType?: ChartType;
  chartStyle?: ChartStyle;
  /** Parsed from @chart labels= */
  labelColumn?: string;
  /** Parsed from @chart values= */
  valueColumn?: string;
  /** Parsed from @chart group= (for grouped/stacked charts) */
  groupColumn?: string;
  /** Attribution URL parsed from -- Source: comment */
  source?: string;
  /** Default time range interval, e.g. '1 DAY', '2 HOUR'. Parsed from @meta interval='...' */
  defaultInterval?: string;
  /** Column whose value gets passed on click. Parsed from @drill on=... */
  drillOnColumn?: string;
  /** Target query title to navigate to on click. Parsed from @drill into='...' */
  drillIntoQuery?: string;
  /** RAG cell decoration rules parsed from @rag directives */
  ragRules?: RagRule[];
  /** Column whose value becomes a clickable link. Parsed from @link on=... */
  linkOnColumn?: string;
  /** Target query title to open in a modal popup. Parsed from @link into='...' */
  linkIntoQuery?: string;
}

export interface CustomQuery {
  name: string;
  description: string;
  sql: string;
  chartType?: ChartType;
  chartStyle?: ChartStyle;
}

/* ─── constants ─── */

export const QUERY_GROUPS: Record<QueryGroup, { color: string; builtin: boolean }> = {
  'Overview':           { color: '#58a6ff', builtin: true },
  'Advanced Dashboard': { color: '#d2a8ff', builtin: true },
  'Inserts':            { color: '#3fb950', builtin: true },
  'Selects':            { color: '#a78bfa', builtin: true },
  'Parts':              { color: '#f0883e', builtin: true },
  'Merges':             { color: '#e3b341', builtin: true },
  'Resources':          { color: '#f85149', builtin: true },
  'Self-Monitoring':    { color: '#f0c674', builtin: true },
  'Custom':             { color: '#79c0ff', builtin: false },
};

export const CHART_TYPE_LABELS: Record<string, string> = {
  bar: 'Bar',
  line: 'Line',
  pie: 'Pie',
  area: 'Area',
  grouped_bar: 'Grouped Bar',
  stacked_bar: 'Stacked Bar',
  grouped_line: 'Grouped Line',
};

export const MAX_SIDEBAR_QUERIES = 12;

/** Predefined time range options for the picker. null interval = use query's own @meta default. */
export const TIME_RANGE_OPTIONS: { label: string; interval: string | null }[] = [
  { label: '15m',     interval: '15 MINUTE' },
  { label: '1h',      interval: '1 HOUR' },
  { label: '6h',      interval: '6 HOUR' },
  { label: '1d',      interval: '1 DAY' },
  { label: '2d',      interval: '2 DAY' },
  { label: '7d',      interval: '7 DAY' },
  { label: '30d',     interval: '30 DAY' },
];

/* ─── RAG (Red/Amber/Green) evaluation ─── */

/** Resolve RAG color for a numeric cell value. Returns a CSS color or undefined if no rule matches. */
export function getRagColor(column: string, value: unknown, rules?: RagRule[]): string | undefined {
  if (!rules) return undefined;
  const rule = rules.find(r => r.column === column);
  if (!rule) return undefined;
  const num = typeof value === 'number' ? value : Number(value);
  if (isNaN(num)) return undefined;
  if (num < rule.greenBelow) return '#22c55e'; // green
  if (num < rule.amberBelow) return '#f59e0b'; // amber
  return '#ef4444'; // red
}

/* ─── time range variable resolution ─── */

/**
 * Replace {{time_range}} placeholders with a ClickHouse time expression.
 *
 * `userInterval` is the override from the time range picker (null = use default).
 *   - A regular interval string like '2 HOUR' → `now() - INTERVAL 2 HOUR`
 *   - A custom range string like 'CUSTOM:<iso_start>,<iso_end>' → `toDateTime('<start>')`
 *     (the end is handled separately via {{time_range_end}})
 * `defaultInterval` is the query's own @meta interval.
 */
export function resolveTimeRange(sql: string, defaultInterval?: string, userInterval?: string | null): string {
  if (!sql.includes('{{time_range}}')) return sql;
  const interval = userInterval ?? defaultInterval;
  if (!interval) return sql;
  if (interval.startsWith('CUSTOM:')) {
    const [rawStart, rawEnd] = interval.slice(7).split(',');
    // datetime-local inputs produce "YYYY-MM-DDTHH:MM" (no seconds).
    // ClickHouse toDateTime() needs "YYYY-MM-DD HH:MM:SS".
    const normaliseDT = (v: string) => {
      let s = v.replace('T', ' ');
      if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(s)) s += ':00';
      return s;
    };
    const start = normaliseDT(rawStart);
    const end = normaliseDT(rawEnd);
    let resolved = sql;
    // Handle toDate({{time_range}}) pattern
    resolved = resolved.replace(
      /event_date\s*>=\s*toDate\(\{\{time_range\}\}\)/g,
      `event_date >= toDate('${start}') AND event_date <= toDate('${end}')`
    );
    // Handle event_time > {{time_range}} pattern
    resolved = resolved.replace(
      /event_time\s*>\s*\{\{time_range\}\}/g,
      `event_time > toDateTime('${start}') AND event_time < toDateTime('${end}')`
    );
    // Fallback: any remaining {{time_range}}
    resolved = resolved.replaceAll('{{time_range}}', `toDateTime('${start}')`);
    return resolved;
  }
  return sql.replaceAll('{{time_range}}', `now() - INTERVAL ${interval}`);
}

/**
 * Replace {{drill:column | fallback}} placeholders with drill-down filter values.
 *
 * With drill context (e.g. drillParams = { database: 'nyc_taxi' }):
 *   {{drill:database | 1=1}} → database = 'nyc_taxi'
 *
 * Without drill context (standalone, drillParams = {}):
 *   {{drill:database | 1=1}} → 1=1
 */
export function resolveDrillParams(sql: string, drillParams: Record<string, string>): string {
  // {{drill:column | fallback}} → column = 'value' (equality condition)
  let result = sql.replace(
    /\{\{drill:(\w+)\s*\|\s*([^}]+)\}\}/g,
    (_match, column: string, fallback: string) => {
      const value = drillParams[column];
      if (value !== undefined) {
        const escaped = value.replace(/'/g, "''");
        return `${column} = '${escaped}'`;
      }
      return fallback.trim();
    }
  );
  // {{drill_value:column | fallback}} → 'value' (raw quoted value for custom expressions)
  result = result.replace(
    /\{\{drill_value:(\w+)\s*\|\s*([^}]+)\}\}/g,
    (_match, column: string, fallback: string) => {
      const value = drillParams[column];
      if (value !== undefined) {
        const escaped = value.replace(/'/g, "''");
        return `'${escaped}'`;
      }
      return fallback.trim();
    }
  );
  return result;
}

/** Get the human-readable resolved value for a given interval (for tooltips). */
export function describeTimeRange(defaultInterval?: string, userInterval?: string | null): string {
  const interval = userInterval ?? defaultInterval;
  if (!interval) return '(no time filter)';
  if (interval.startsWith('CUSTOM:')) {
    const [start, end] = interval.slice(7).split(',');
    return `${start} → ${end || 'now'}`;
  }
  return `now() - INTERVAL ${interval}`;
}

/* ─── metadata parser ─── */

export function parseQueryMetadata(sql: string): PresetQuery | null {
  const metaMatch = sql.match(/--\s*@meta:\s*(.+)/i);
  if (!metaMatch) return null;

  const m = metaMatch[1];
  const titleMatch = m.match(/title='([^']+)'/);
  const groupMatch = m.match(/group='([^']+)'/);
  const descMatch  = m.match(/description='([^']+)'/);
  const intervalMatch = m.match(/interval='([^']+)'/);
  if (!titleMatch || !groupMatch) return null;

  const group = groupMatch[1].trim() as QueryGroup;
  if (!QUERY_GROUPS[group]) {
    // Dynamically register unknown groups as non-builtin (user-created)
    (QUERY_GROUPS as Record<string, { color: string; builtin: boolean }>)[group] = { color: '#79c0ff', builtin: false };
  }

  let chartType: ChartType | undefined;
  let chartStyle: ChartStyle | undefined;
  let labelColumn: string | undefined;
  let valueColumn: string | undefined;
  let groupColumn: string | undefined;

  const chartMatch = sql.match(/--\s*@chart:\s*(.+)/i);
  if (chartMatch) {
    const c = chartMatch[1];
    const t = c.match(/type=(\w+)/);
    const s = c.match(/style=(\w+)/);
    const l = c.match(/labels=(\w+)/);
    const v = c.match(/values=(\w+)/);
    const g = c.match(/group=(\w+)/);
    if (t) chartType = t[1] as ChartType;
    if (s) chartStyle = s[1] as ChartStyle;
    if (l) labelColumn = l[1];
    if (v) valueColumn = v[1];
    if (g) groupColumn = g[1];
  }

  const sourceMatch = sql.match(/--\s*Source:\s*(https?:\/\/\S+)/i);

  let drillOnColumn: string | undefined;
  let drillIntoQuery: string | undefined;
  const drillMatch = sql.match(/--\s*@drill:\s*(.+)/i);
  if (drillMatch) {
    const dr = drillMatch[1];
    const onMatch = dr.match(/on=(\w+)/);
    const intoMatch = dr.match(/into='([^']+)'/);
    if (onMatch) drillOnColumn = onMatch[1];
    if (intoMatch) drillIntoQuery = intoMatch[1];
  }

  let linkOnColumn: string | undefined;
  let linkIntoQuery: string | undefined;
  const linkMatch = sql.match(/--\s*@link:\s*(.+)/i);
  if (linkMatch) {
    const lk = linkMatch[1];
    const onMatch = lk.match(/on=(\w+)/);
    const intoMatch = lk.match(/into='([^']+)'/);
    if (onMatch) linkOnColumn = onMatch[1];
    if (intoMatch) linkIntoQuery = intoMatch[1];
  }

  const ragRules: RagRule[] = [];
  const ragRegex = /--\s*@rag:\s*(.+)/gi;
  let ragMatch;
  while ((ragMatch = ragRegex.exec(sql)) !== null) {
    const r = ragMatch[1];
    const colMatch = r.match(/column=(\w+)/);
    const greenMatch = r.match(/green<(\d+(?:\.\d+)?)/);
    const amberMatch = r.match(/amber<(\d+(?:\.\d+)?)/);
    if (colMatch && greenMatch && amberMatch) {
      ragRules.push({
        column: colMatch[1],
        greenBelow: Number(greenMatch[1]),
        amberBelow: Number(amberMatch[1]),
      });
    }
  }

  return {
    name: titleMatch[1].trim(),
    description: descMatch?.[1]?.trim() ?? '',
    group,
    sql,
    chartType,
    chartStyle,
    labelColumn,
    valueColumn,
    groupColumn,
    source: sourceMatch?.[1],
    defaultInterval: intervalMatch?.[1]?.trim(),
    drillOnColumn,
    drillIntoQuery,
    linkOnColumn,
    linkIntoQuery,
    ragRules: ragRules.length > 0 ? ragRules : undefined,
  };
}

/* ─── Custom user queries (localStorage) ─── */

const CUSTOM_QUERIES_KEY = 'tracehouse-custom-queries';

/** Build a SQL string with embedded @meta header for a custom query. */
export function buildCustomQuerySql(name: string, description: string, bodySql?: string, group?: string): string {
  const desc = description ? ` description='${description.replace(/'/g, "\\'")}'` : '';
  const g = group?.trim() || 'Custom';
  const body = bodySql ?? `SELECT
    database,
    table,
    count() AS count
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY count DESC
LIMIT 10`;
  return `-- @meta: title='${name.replace(/'/g, "\\'")}' group='${g.replace(/'/g, "\\'")}'${desc}\n${body}`;
}

/** Load user-created queries from localStorage. Parses @meta from raw SQL. */
export function loadCustomQueries(): PresetQuery[] {
  try {
    const raw = localStorage.getItem(CUSTOM_QUERIES_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as string[];
      if (Array.isArray(parsed)) {
        return parsed
          .map(sql => typeof sql === 'string' ? parseQueryMetadata(sql) : null)
          .filter((q): q is PresetQuery => q !== null);
      }
    }
  } catch { /* ignore corrupt data */ }
  return [];
}

/** Save custom queries to localStorage as raw SQL strings. */
function saveCustomQuerySqls(sqls: string[]): void {
  localStorage.setItem(CUSTOM_QUERIES_KEY, JSON.stringify(sqls));
}

/** Load raw SQL strings from localStorage. */
function loadCustomQuerySqls(): string[] {
  try {
    const raw = localStorage.getItem(CUSTOM_QUERIES_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return parsed.filter(s => typeof s === 'string');
    }
  } catch { /* ignore */ }
  return [];
}

/** Check if a query name is already taken (across builtins + custom). */
export function isQueryNameTaken(name: string, presetQueries: PresetQuery[]): boolean {
  return presetQueries.some(q => q.name === name) || loadCustomQueries().some(q => q.name === name);
}

/** Add or update a custom query by raw SQL. The @meta title is the key. Returns updated parsed list. */
export function addCustomQuery(query: CustomQuery, presetQueries: PresetQuery[]): PresetQuery[] {
  if (presetQueries.some(q => q.name === query.name)) {
    throw new Error(`Name "${query.name}" is already used by a built-in query`);
  }
  const sqls = loadCustomQuerySqls();
  const existing = sqls.map(s => parseQueryMetadata(s)).filter((q): q is PresetQuery => q !== null);
  const idx = existing.findIndex(q => q.name === query.name);
  if (idx >= 0) {
    sqls[idx] = query.sql;
  } else {
    sqls.push(query.sql);
  }
  saveCustomQuerySqls(sqls);
  return loadCustomQueries();
}

/** Delete a custom query by name. Returns updated list. */
export function deleteCustomQuery(name: string): PresetQuery[] {
  const sqls = loadCustomQuerySqls();
  const filtered = sqls.filter(s => {
    const parsed = parseQueryMetadata(s);
    return parsed?.name !== name;
  });
  saveCustomQuerySqls(filtered);
  return loadCustomQueries();
}

/** Wipe all custom queries. */
export function resetCustomQueries(): void {
  localStorage.removeItem(CUSTOM_QUERIES_KEY);
}

/** Get all queries: builtins + custom. */
export function getAllQueries(presetQueries: PresetQuery[]): PresetQuery[] {
  return [...presetQueries, ...loadCustomQueries()];
}
