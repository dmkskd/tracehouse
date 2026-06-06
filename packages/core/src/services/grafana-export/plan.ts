import type { GrafanaExportInput, GrafanaExportPlan } from './types.js';
import { analyzeGrafanaExport } from './analyze.js';
import { chartFieldDefaults, fieldOverrides, mapPanelOptions, panelTransformations } from './mappings.js';
import { grafanaSql } from './query.js';
import { cleanTracehouseSql, mapPanelType, panelGridHeight } from './utils.js';

export function buildGrafanaExportPlan(input: GrafanaExportInput): GrafanaExportPlan {
  const panelType = mapPanelType(input.chart?.type);
  const cleanSql = cleanTracehouseSql(input.sql);
  const gridHeight = Math.max(4, Math.min(36, Math.round(panelGridHeight(input, panelType))));
  const gridWidth = Math.max(4, Math.min(24, Math.round(input.panel?.width ?? 18)));

  return {
    input,
    analysis: analyzeGrafanaExport(input),
    panelType,
    datasourceUid: input.datasourceUid ?? '${DS_CLICKHOUSE}',
    cleanSql,
    querySql: grafanaSql(input, cleanSql, panelType),
    gridPos: { h: gridHeight, w: gridWidth, x: 0, y: 0 },
    fieldDefaults: chartFieldDefaults(input, panelType),
    fieldOverrides: fieldOverrides(input),
    options: mapPanelOptions(input),
    transformations: panelTransformations(input, panelType),
  };
}
