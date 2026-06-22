export interface GrafanaExportInput {
  /** The raw SQL query */
  sql: string;
  /** Query title (from @meta or user label) */
  title: string;
  /** Chart configuration — if absent, exports as a table panel */
  chart?: {
    type: 'bar' | 'line' | 'pie' | 'area' | 'grouped_bar' | 'stacked_bar' | 'grouped_line' | 'radar';
    labelColumn?: string;
    groupByColumn: string;
    valueColumn: string;
    valueColumns?: string[];
    seriesColumn?: string;
    /** Known values from seriesColumn, used to pivot grouped/stacked bar exports for Grafana. */
    seriesValues?: string[];
    orientation?: 'horizontal' | 'vertical';
    unit?: string;
    /** Fixed chart color from @chart color=#... */
    color?: string;
    profile?: string;
    axes?: Record<string, string>;
    ranges?: Record<string, GrafanaRadarAxisRange>;
    transforms?: Record<string, string>;
    valuesColumn?: string;
    labelsColumn?: string;
    colorByColumn?: string;
    /** Maximum categorical rows to render in dense Grafana panels. */
    maxRows?: number;
  };
  /** RAG thresholds to map to Grafana thresholds */
  rag?: GrafanaRagRuleInput[];
  /** Table cell decorations parsed from Tracehouse @cell directives. */
  cellStyles?: GrafanaCellStyle[];
  /** Actual result column names, when available, used to validate exact Grafana field matchers. */
  resultColumns?: string[];
  /** Actual result rows, when available, used to resolve column-reference settings such as gauge max=max_column. */
  resultRows?: Record<string, unknown>[];
  /** TraceHouse-only interactions that need support analysis before export. */
  interactions?: Array<{
    type: 'link' | 'drill' | 'part_link';
    on: string;
    into?: string;
  }>;
  /** ClickHouse datasource UID — use '${DS_CLICKHOUSE}' for portable JSON */
  datasourceUid?: string;
  /** Optional stable Grafana dashboard UID for upsert/import flows */
  dashboardUid?: string;
  /** Optional panel presentation settings */
  panel?: {
    width?: number;
    height?: number;
    transparent?: boolean;
    legendPlacement?: 'bottom' | 'right' | 'hidden';
  };
}

export type GrafanaCellStyle = GrafanaGaugeCellStyle | GrafanaRagCellStyle | GrafanaSparklineCellStyle | GrafanaRadarCellStyle;

export interface GrafanaGaugeCellStyle {
  column: string;
  type: 'gauge';
  max: number | string;
  unit?: string;
}

export interface GrafanaRagCellStyle {
  column: string;
  type: 'rag';
  mode: 'numeric' | 'text';
  direction?: 'asc' | 'desc';
  greenThreshold?: number;
  amberThreshold?: number;
  greenValues?: string[];
  amberValues?: string[];
  redValues?: string[];
}

export type GrafanaRagRuleInput = Omit<GrafanaRagCellStyle, 'type'> & { type?: 'rag' };

export interface GrafanaSparklineCellStyle {
  column: string;
  type: 'sparkline';
  ref?: number;
  color?: string;
  fill?: boolean;
}

export interface GrafanaRadarAxisRange {
  low: string;
  high: string;
}

export interface GrafanaRadarCellStyle {
  type: 'radar';
  column?: string;
  radarColumn?: string;
  profile?: string;
  axes?: Record<string, string>;
  ranges?: Record<string, GrafanaRadarAxisRange>;
  transforms?: Record<string, string>;
  labels?: string;
  color?: string;
  colorBy?: string;
  colorScale?: string;
  colors?: string;
}

export interface GrafanaThreshold {
  color: string;
  value: number | null;
}

export interface GrafanaPanel {
  id: number;
  type: string;
  title: string;
  gridPos: { h: number; w: number; x: number; y: number };
  datasource: { type: string; uid: string };
  transparent?: boolean;
  targets: Array<{ rawSql: string; refId: string; format: number }>;
  fieldConfig: {
    defaults: {
      thresholds?: { mode: string; steps: GrafanaThreshold[] };
      unit?: string;
      color?: Record<string, unknown>;
      custom?: Record<string, unknown>;
    };
    overrides: GrafanaFieldOverride[];
  };
  options: Record<string, unknown>;
  transformations?: GrafanaTransformation[];
}

export interface GrafanaFieldOverride {
  matcher: { id: string; options: string };
  properties: Array<{ id: string; value: unknown }>;
}

export interface GrafanaTransformation {
  id: string;
  options: Record<string, unknown>;
}

export type GrafanaSupportLevel = 'supported' | 'partial' | 'unsupported';
export type GrafanaExportDecision = 'map' | 'hide' | 'keep_raw' | 'drop' | 'split_panel';

export interface GrafanaExportCapability {
  tracehouseFeature: string;
  grafanaFeature?: string;
  level: GrafanaSupportLevel;
  message: string;
  decision: GrafanaExportDecision;
}

export interface GrafanaExportAnalysis {
  panelType: string;
  capabilities: GrafanaExportCapability[];
}

export interface GrafanaExportPlan {
  input: GrafanaExportInput;
  analysis: GrafanaExportAnalysis;
  panelType: string;
  datasourceUid: string;
  cleanSql: string;
  querySql: string;
  gridPos: { h: number; w: number; x: number; y: number };
  fieldDefaults: GrafanaPanel['fieldConfig']['defaults'];
  fieldOverrides: GrafanaFieldOverride[];
  options: Record<string, unknown>;
  transformations?: GrafanaTransformation[];
}
