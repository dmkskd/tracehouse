/**
 * Meta language parser for query explorer directives.
 *
 * Parses structured comments embedded in SQL queries:
 *   -- @meta:       title, group, description, interval
 *   -- @chart:      type, group_by, value, series, style, unit, orientation
 *   -- @cell: column-level table decorations (rag, gauge, sparkline)
 *   -- @drill:      click-through navigation between queries
 *   -- @link:       modal popup navigation between queries
 *   -- Source:      attribution URL
 */

/* ─── types ─── */

export type ChartType = 'bar' | 'line' | 'pie' | 'area' | 'grouped_bar' | 'stacked_bar' | 'grouped_line';
export type ChartStyle = '2d' | '3d';

/** Cell style type — determines how a table column is decorated. */
export type CellStyleType = 'rag' | 'gauge' | 'sparkline';

/** Unified cell style rule parsed from @cell: directives. */
export type CellStyleRule = RagCellStyle | GaugeCellStyle | SparklineCellStyle;

/** RAG (Red/Amber/Green) cell style — conditional coloring based on thresholds or text values. */
export interface RagCellStyle {
  column: string;
  type: 'rag';
  mode: 'numeric' | 'text';
  /** Numeric mode */
  direction?: 'asc' | 'desc';
  greenThreshold?: number;
  amberThreshold?: number;
  /** Text mode — comma-separated values for each level */
  greenValues?: string[];
  amberValues?: string[];
  redValues?: string[];
}

/** Gauge cell style — inline horizontal bar. */
export interface GaugeCellStyle {
  column: string;
  type: 'gauge';
  /** Fixed number or another column name for the bar's 100% value */
  max: number | string;
  unit?: string;
}

/** Sparkline cell style — tiny inline SVG trend. */
export interface SparklineCellStyle {
  column: string;
  type: 'sparkline';
  /** Optional horizontal reference line value (e.g. 0 for delta charts) */
  ref?: number;
  color?: string;
  fill?: boolean;
}

/** @deprecated Use CellStyleRule with type='rag' instead. Alias for backward compat in consumer code. */
export type RagRule = RagCellStyle;
/** @deprecated Use CellStyleRule with type='gauge' instead. */
export type GaugeRule = GaugeCellStyle;
/** @deprecated Use CellStyleRule with type='sparkline' instead. */
export type SparklineRule = SparklineCellStyle;

/** Result of parsing all -- directives from a SQL string. */
export interface ParsedDirectives {
  meta?: {
    title: string;
    group: string;
    description?: string;
    interval?: string;
  };
  chart?: {
    type: ChartType;
    style?: ChartStyle;
    groupByColumn?: string;
    valueColumn?: string;
    seriesColumn?: string;
    descriptionColumn?: string;
  };
  drill?: {
    on: string;
    into: string;
  };
  link?: {
    on: string;
    into: string;
  };
  /** @part_link: click a part name to open PartInspector. */
  partLink?: {
    /** Column containing the part name */
    on: string;
    /** Column or drill-param name for the database */
    database: string;
    /** Column or drill-param name for the table */
    table: string;
  };
  /** All cell style rules (rag, gauge, sparkline) from @cell: directives. */
  cellStyles: CellStyleRule[];
  source?: string;
}

/** Detailed chart directive result (superset of chart in ParsedDirectives).
 *  Supports multi-column values, orientation, unit — used for rendering config. */
export interface ChartDirective {
  type?: ChartType;
  groupByColumn?: string;
  valueColumn?: string;
  valueColumns?: string[];
  seriesColumn?: string;
  orientation?: 'horizontal' | 'vertical';
  visualization?: '2d' | '3d';
  title?: string;
  description?: string;
  /** Column name whose value is shown as description in chart tooltips */
  descriptionColumn?: string;
  unit?: string;
  /** Override the default chart color (hex, e.g. '#f59e0b'). Used for stroke and area fill. */
  color?: string;
}

/* ─── constants ─── */

export type QueryGroup = 'Overview' | 'Inserts' | 'Selects' | 'Parts' | 'Merges' | 'Resources' | 'Advanced Dashboard' | 'Self-Monitoring' | 'Grafana Imports' | 'Custom';

export const QUERY_GROUPS: Record<QueryGroup, { color: string; builtin: boolean }> = {
  'Overview':           { color: '#58a6ff', builtin: true },
  'Advanced Dashboard': { color: '#d2a8ff', builtin: true },
  'Inserts':            { color: '#3fb950', builtin: true },
  'Selects':            { color: '#a78bfa', builtin: true },
  'Parts':              { color: '#f0883e', builtin: true },
  'Merges':             { color: '#e3b341', builtin: true },
  'Resources':          { color: '#f85149', builtin: true },
  'Self-Monitoring':    { color: '#f0c674', builtin: true },
  'Grafana Imports':   { color: '#38bdf8', builtin: true },
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

/* ─── RAG evaluation ─── */

/** Resolve RAG color for a cell value. Accepts either CellStyleRule[] or legacy RagRule[]. */
export function getRagColor(column: string, value: unknown, rules?: CellStyleRule[] | RagCellStyle[]): string | undefined {
  if (!rules) return undefined;
  const rule = rules.find(r => r.column === column && (r as CellStyleRule).type === 'rag') as RagCellStyle | undefined;
  if (!rule) return undefined;

  if (rule.mode === 'text') {
    const s = String(value).toLowerCase().trim();
    if (rule.greenValues?.some(v => v === s)) return '#22c55e';
    if (rule.amberValues?.some(v => v === s)) return '#f59e0b';
    if (rule.redValues?.some(v => v === s)) return '#ef4444';
    return undefined;
  }

  const num = typeof value === 'number' ? value : Number(value);
  if (isNaN(num)) return undefined;
  if (rule.direction === 'desc') {
    if (num > rule.greenThreshold!) return '#22c55e';
    if (num > rule.amberThreshold!) return '#f59e0b';
    return '#ef4444';
  }
  if (num < rule.greenThreshold!) return '#22c55e';
  if (num < rule.amberThreshold!) return '#f59e0b';
  return '#ef4444';
}

/* ─── parsers ─── */

/** Parse a single @cell: line body into a CellStyleRule, or null if invalid. */
function parseCellStyleLine(body: string): CellStyleRule | null {
  const colMatch = body.match(/column=(\w+)/);
  if (!colMatch) return null;
  const column = colMatch[1];

  const typeMatch = body.match(/type=(\w+)/);
  const type = typeMatch ? typeMatch[1] : 'rag'; // default to rag for backward compat

  switch (type) {
    case 'rag': {
      // Numeric: green>N amber>N or green<N amber<N
      const greenNum = body.match(/green([<>])(\d+(?:\.\d+)?)/);
      const amberNum = body.match(/amber([<>])(\d+(?:\.\d+)?)/);
      if (greenNum && amberNum) {
        return {
          column, type: 'rag', mode: 'numeric',
          direction: greenNum[1] === '>' ? 'desc' : 'asc',
          greenThreshold: Number(greenNum[2]),
          amberThreshold: Number(amberNum[2]),
        };
      }
      // Text: green=ok,healthy amber=degraded red=error,down
      const greenText = body.match(/green=([^\s]+)/);
      const amberText = body.match(/amber=([^\s]+)/);
      const redText = body.match(/red=([^\s]+)/);
      if (greenText || amberText || redText) {
        return {
          column, type: 'rag', mode: 'text',
          greenValues: greenText?.[1].toLowerCase().split(','),
          amberValues: amberText?.[1].toLowerCase().split(','),
          redValues: redText?.[1].toLowerCase().split(','),
        };
      }
      return null;
    }
    case 'gauge': {
      const maxMatch = body.match(/max=(\w+(?:\.\d+)?)/);
      if (!maxMatch) return null;
      const maxVal = /^\d+(\.\d+)?$/.test(maxMatch[1]) ? Number(maxMatch[1]) : maxMatch[1];
      const unitMatch = body.match(/unit=(\S+)/);
      const rule: GaugeCellStyle = { column, type: 'gauge', max: maxVal };
      if (unitMatch) rule.unit = unitMatch[1];
      return rule;
    }
    case 'sparkline': {
      const rule: SparklineCellStyle = { column, type: 'sparkline' };
      const refMatch = body.match(/ref=(-?\d+(?:\.\d+)?)/);
      if (refMatch) rule.ref = Number(refMatch[1]);
      const colorMatch = body.match(/color=(#[0-9a-fA-F]{3,8})/);
      if (colorMatch) rule.color = colorMatch[1];
      const fillMatch = body.match(/fill=(true|false)/i);
      if (fillMatch) rule.fill = fillMatch[1].toLowerCase() === 'true';
      return rule;
    }
    default:
      return null;
  }
}

/** Parse all @cell: directives from raw SQL. */
export function parseCellStyles(sql: string): CellStyleRule[] {
  const rules: CellStyleRule[] = [];
  const regex = /--\s*@cell:\s*(.+)/gi;
  let m;
  while ((m = regex.exec(sql)) !== null) {
    const rule = parseCellStyleLine(m[1]);
    if (rule) rules.push(rule);
  }
  return rules;
}

/** @deprecated Parse legacy @rag: directives — use parseCellStyles instead. */
export function parseRagRules(sql: string): RagCellStyle[] {
  const rules: RagCellStyle[] = [];
  const ragRegex = /--\s*@rag:\s*(.+)/gi;
  let ragMatch;
  while ((ragMatch = ragRegex.exec(sql)) !== null) {
    const rule = parseCellStyleLine('type=rag ' + ragMatch[1]);
    if (rule && rule.type === 'rag') rules.push(rule);
  }
  return rules;
}

/** @deprecated Use parseCellStyles instead. */
export function parseGaugeRules(sql: string): GaugeCellStyle[] {
  return parseCellStyles(sql).filter((r): r is GaugeCellStyle => r.type === 'gauge');
}

/** @deprecated Use parseCellStyles instead. */
export function parseSparklineRules(sql: string): SparklineCellStyle[] {
  return parseCellStyles(sql).filter((r): r is SparklineCellStyle => r.type === 'sparkline');
}

/** Parse all -- directives from a SQL string. Returns null if no @meta directive found. */
export function parseDirectives(sql: string): ParsedDirectives | null {
  const metaMatch = sql.match(/--\s*@meta:\s*(.+)/i);
  if (!metaMatch) return null;

  const m = metaMatch[1];
  const titleMatch = m.match(/title='([^']+)'/);
  const groupMatch = m.match(/group='([^']+)'/);
  if (!titleMatch || !groupMatch) return null;

  const descMatch = m.match(/description='([^']+)'/);
  const intervalMatch = m.match(/interval='([^']+)'/);

  const group = groupMatch[1].trim();
  if (!QUERY_GROUPS[group as QueryGroup]) {
    (QUERY_GROUPS as Record<string, { color: string; builtin: boolean }>)[group] = { color: '#79c0ff', builtin: false };
  }

  const result: ParsedDirectives = {
    meta: {
      title: titleMatch[1].trim(),
      group,
      description: descMatch?.[1]?.trim(),
      interval: intervalMatch?.[1]?.trim(),
    },
    cellStyles: [...parseCellStyles(sql), ...parseRagRules(sql)],
  };

  const chartMatch = sql.match(/--\s*@chart:\s*(.+)/i);
  if (chartMatch) {
    const c = chartMatch[1];
    const t = c.match(/type=(\w+)/);
    const s = c.match(/style=(\w+)/);
    const l = c.match(/group_by=(\w+)/);
    const v = c.match(/value=([\w,]+)/);
    const g = c.match(/series=(\w+)/);
    const dc = c.match(/description=(\w+)/i);
    if (t) {
      result.chart = {
        type: t[1] as ChartType,
        style: s ? s[1] as ChartStyle : undefined,
        groupByColumn: l?.[1],
        valueColumn: v?.[1],
        seriesColumn: g?.[1],
        descriptionColumn: dc?.[1],
      };
    }
  }

  const drillMatch = sql.match(/--\s*@drill:\s*(.+)/i);
  if (drillMatch) {
    const dr = drillMatch[1];
    const onMatch = dr.match(/on=(\w+)/);
    const intoMatch = dr.match(/into='([^']+)'/);
    if (onMatch && intoMatch) {
      result.drill = { on: onMatch[1], into: intoMatch[1] };
    }
  }

  const linkMatch = sql.match(/--\s*@link:\s*(.+)/i);
  if (linkMatch) {
    const lk = linkMatch[1];
    const onMatch = lk.match(/on=(\w+)/);
    const intoMatch = lk.match(/into='([^']+)'/);
    if (onMatch && intoMatch) {
      result.link = { on: onMatch[1], into: intoMatch[1] };
    }
  }

  const partLinkMatch = sql.match(/--\s*@part_link:\s*(.+)/i);
  if (partLinkMatch) {
    const pl = partLinkMatch[1];
    const onMatch = pl.match(/on=(\w+)/);
    const dbMatch = pl.match(/database=(\w+)/);
    const tblMatch = pl.match(/table=(\w+)/);
    if (onMatch && dbMatch && tblMatch) {
      result.partLink = { on: onMatch[1], database: dbMatch[1], table: tblMatch[1] };
    }
  }

  const sourceMatch = sql.match(/--\s*@source:\s*(https?:\/\/\S+)/i);
  if (sourceMatch) result.source = sourceMatch[1];

  return result;
}

/** Parse @chart directive with full detail (multi-column values, orientation, unit).
 *  Also extracts title/description from @meta if present. */
export function parseChartDirective(sql: string): Partial<ChartDirective> | null {
  const cm = sql.match(/--\s*@chart:\s*(.+)/i);
  if (!cm) return null;
  const d = cm[1];
  const cfg: Partial<ChartDirective> = {};
  const t = d.match(/type=(\w+)/i); if (t) cfg.type = t[1] as ChartType;
  const l = d.match(/group_by=(\w+)/i); if (l) cfg.groupByColumn = l[1];
  const v = d.match(/value=([\w,]+)/i);
  if (v) {
    const cols = v[1].split(',').filter(Boolean);
    cfg.valueColumn = cols[0];
    if (cols.length > 1) cfg.valueColumns = cols;
  }
  const g = d.match(/series=(\w+)/i); if (g) cfg.seriesColumn = g[1];
  const o = d.match(/orientation=(\w+)/i);
  if (o) cfg.orientation = o[1].toLowerCase() === 'vertical' || o[1].toLowerCase() === 'v' ? 'vertical' : 'horizontal';
  const s = d.match(/style=(\w+)/i); if (s) cfg.visualization = s[1] as '2d' | '3d';
  const u = d.match(/unit=(\S+)/i); if (u) cfg.unit = u[1];
  const dc = d.match(/description=(\w+)/i); if (dc) cfg.descriptionColumn = dc[1];
  const co = d.match(/color=(#[0-9a-fA-F]{3,8}|\w+)/i); if (co) cfg.color = co[1];
  const mm = sql.match(/--\s*@meta:\s*(.+)/i);
  if (mm) {
    const ti = mm[1].match(/title='([^']+)'/); if (ti) cfg.title = ti[1];
    const de = mm[1].match(/description='([^']+)'/); if (de) cfg.description = de[1];
  }
  return Object.keys(cfg).length > 0 ? cfg : null;
}

/* ─── query ref resolution ─── */

/**
 * Resolve a query reference like 'Group#Name' or bare 'Name'.
 * - 'Group#Name': match by group + name (cross-group drill)
 * - Bare 'Name': match within sourceGroup first, then fall back to any group
 */
export function resolveQueryRef<T extends { name: string; group: string }>(
  ref: string,
  sourceGroup: string | undefined,
  queries: T[],
): T | undefined {
  const hashIdx = ref.indexOf('#');
  if (hashIdx >= 0) {
    const group = ref.slice(0, hashIdx);
    const name = ref.slice(hashIdx + 1);
    return queries.find(q => q.group === group && q.name === name);
  }
  // Bare name — prefer same group, then any
  if (sourceGroup) {
    const sameGroup = queries.find(q => q.group === sourceGroup && q.name === ref);
    if (sameGroup) return sameGroup;
  }
  return queries.find(q => q.name === ref);
}

/* ─── SQL → Query mapping ─── */

import type { Query, QueryType } from './types';

/** Parse raw SQL into a Query. Returns null if no valid @meta found. */
export function parseQueryMetadata(sql: string, type: QueryType = 'preset'): Query | null {
  const directives = parseDirectives(sql);
  if (!directives) return null;
  return {
    name: directives.meta!.title,
    description: directives.meta!.description ?? '',
    sql,
    group: directives.meta!.group as QueryGroup,
    type,
    directives,
  };
}

/** Build a SQL string with embedded @meta header. */
export function buildDirectiveHeader(name: string, description: string, group?: string): string {
  const desc = description ? ` description='${description.replace(/'/g, "\\'")}'` : '';
  const g = group?.trim() || 'Custom';
  return `-- @meta: title='${name.replace(/'/g, "\\'")}' group='${g.replace(/'/g, "\\'")}'${desc}`;
}
