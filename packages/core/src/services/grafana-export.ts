/**
 * Grafana Export — converts Tracehouse query + chart config into Grafana dashboard JSON.
 *
 * Pure mapping logic, no React or Grafana runtime dependencies.
 */

/* ─── Input types (mirroring the frontend types to avoid coupling) ─── */

export interface GrafanaExportInput {
  /** The raw SQL query */
  sql: string;
  /** Query title (from @meta or user label) */
  title: string;
  /** Chart configuration — if absent, exports as a table panel */
  chart?: {
    type: 'bar' | 'line' | 'pie' | 'area' | 'grouped_bar' | 'stacked_bar' | 'grouped_line';
    groupByColumn: string;
    valueColumn: string;
    valueColumns?: string[];
    seriesColumn?: string;
    orientation?: 'horizontal' | 'vertical';
    unit?: string;
  };
  /** RAG thresholds to map to Grafana thresholds */
  rag?: Array<{
    column: string;
    mode: 'numeric' | 'text';
    direction?: 'asc' | 'desc';
    greenThreshold?: number;
    amberThreshold?: number;
    greenValues?: string[];
    amberValues?: string[];
    redValues?: string[];
  }>;
  /** ClickHouse datasource UID — use '${DS_CLICKHOUSE}' for portable JSON */
  datasourceUid?: string;
}

/* ─── Grafana JSON types (subset we generate) ─── */

interface GrafanaThreshold {
  color: string;
  value: number | null;
}

interface GrafanaPanel {
  id: number;
  type: string;
  title: string;
  gridPos: { h: number; w: number; x: number; y: number };
  datasource: { type: string; uid: string };
  targets: Array<{ rawSql: string; refId: string; format: number }>;
  fieldConfig: {
    defaults: {
      thresholds?: { mode: string; steps: GrafanaThreshold[] };
      unit?: string;
    };
    overrides: unknown[];
  };
  options: Record<string, unknown>;
}

interface GrafanaDashboard {
  dashboard: {
    title: string;
    tags: string[];
    timezone: string;
    panels: GrafanaPanel[];
    schemaVersion: number;
    templating: { list: unknown[] };
  };
  overwrite: boolean;
}

/* ─── Chart type mapping ─── */

function mapPanelType(chartType: string | undefined): string {
  if (!chartType) return 'table';
  switch (chartType) {
    case 'line':
    case 'area':
    case 'grouped_line':
      return 'timeseries';
    case 'bar':
    case 'grouped_bar':
    case 'stacked_bar':
      return 'barchart';
    case 'pie':
      return 'piechart';
    default:
      return 'timeseries';
  }
}

function mapPanelOptions(chartType: string | undefined, orientation?: 'horizontal' | 'vertical'): Record<string, unknown> {
  const panelType = mapPanelType(chartType);
  switch (panelType) {
    case 'timeseries':
      return {
        tooltip: { mode: 'multi', sort: 'desc' },
        legend: { displayMode: 'list', placement: 'bottom' },
        ...(chartType === 'area' ? { fillOpacity: 20 } : {}),
      };
    case 'barchart':
      return {
        orientation: orientation === 'horizontal' ? 'horizontal' : 'auto',
        tooltip: { mode: 'multi' },
        legend: { displayMode: 'list', placement: 'bottom' },
        stacking: chartType === 'stacked_bar' ? 'normal' : 'none',
        groupWidth: chartType === 'grouped_bar' ? 0.7 : 0.8,
      };
    case 'piechart':
      return {
        tooltip: { mode: 'single' },
        legend: { displayMode: 'table', placement: 'right' },
        pieType: 'donut',
      };
    default:
      return {};
  }
}

/* ─── RAG → Grafana thresholds ─── */

function mapThresholds(rag: GrafanaExportInput['rag']): { mode: string; steps: GrafanaThreshold[] } | undefined {
  if (!rag?.length) return undefined;
  // Use the first numeric rule for panel-level thresholds
  const numericRule = rag.find(r => r.mode === 'numeric');
  if (!numericRule) return undefined;

  const steps: GrafanaThreshold[] = [];
  if (numericRule.direction === 'desc') {
    // Higher is better: red (base) → amber → green
    steps.push({ color: 'red', value: null });  // base
    if (numericRule.amberThreshold != null) steps.push({ color: 'orange', value: numericRule.amberThreshold });
    if (numericRule.greenThreshold != null) steps.push({ color: 'green', value: numericRule.greenThreshold });
  } else {
    // Lower is better: green (base) → amber → red
    steps.push({ color: 'green', value: null });  // base
    if (numericRule.greenThreshold != null) steps.push({ color: 'orange', value: numericRule.greenThreshold });
    if (numericRule.amberThreshold != null) steps.push({ color: 'red', value: numericRule.amberThreshold });
  }
  return { mode: 'absolute', steps };
}

/* ─── Unit mapping ─── */

function mapUnit(unit: string | undefined): string | undefined {
  if (!unit) return undefined;
  const map: Record<string, string> = {
    'ms': 'ms',
    's': 's',
    '%': 'percent',
    'MB': 'decmbytes',
    'GB': 'decgbytes',
    'bytes': 'bytes',
    'B': 'bytes',
    'KB': 'deckbytes',
    'TB': 'dectbytes',
    'ops': 'ops',
    'qps': 'ops',
    'req/s': 'reqps',
  };
  return map[unit] ?? undefined;
}

/* ─── Public API ─── */

/** Build a single Grafana panel JSON from a Tracehouse query. */
export function toGrafanaPanel(input: GrafanaExportInput, panelId = 1): GrafanaPanel {
  const dsUid = input.datasourceUid ?? '${DS_CLICKHOUSE}';
  const panelType = mapPanelType(input.chart?.type);

  // Strip Tracehouse-specific directives from the SQL (-- @meta, -- @chart, -- @rag, -- @drill, -- @link, -- Source)
  const cleanSql = input.sql
    .split('\n')
    .filter(line => !/^\s*--\s*@(meta|chart|rag|drill|link)\s*:/i.test(line) && !/^\s*--\s*Source:/i.test(line))
    .join('\n')
    .trim();

  return {
    id: panelId,
    type: panelType,
    title: input.title,
    gridPos: { h: 8, w: 12, x: 0, y: 0 },
    datasource: { type: 'grafana-clickhouse-datasource', uid: dsUid },
    targets: [{ rawSql: cleanSql, refId: 'A', format: 1 }],
    fieldConfig: {
      defaults: {
        thresholds: mapThresholds(input.rag),
        unit: mapUnit(input.chart?.unit),
      },
      overrides: [],
    },
    options: mapPanelOptions(input.chart?.type, input.chart?.orientation),
  };
}

/** Wrap a panel in a Grafana dashboard JSON importable via Dashboards → Import.
 *  Includes __inputs so Grafana prompts the user to pick their ClickHouse datasource. */
export function toGrafanaDashboard(input: GrafanaExportInput): Record<string, unknown> {
  return {
    __inputs: [
      {
        name: 'DS_CLICKHOUSE',
        label: 'ClickHouse',
        description: 'ClickHouse datasource for this dashboard',
        type: 'datasource',
        pluginId: 'grafana-clickhouse-datasource',
        pluginName: 'ClickHouse',
      },
    ],
    title: `Tracehouse: ${input.title}`,
    tags: ['tracehouse', 'clickhouse'],
    timezone: 'browser',
    panels: [toGrafanaPanel(input)],
    schemaVersion: 39,
    templating: { list: [] },
  };
}
