import type { GrafanaExportInput, GrafanaExportPlan, GrafanaPanel } from './types.js';
import { buildGrafanaExportPlan } from './plan.js';

export function renderGrafanaPanel(plan: GrafanaExportPlan, panelId = 1): GrafanaPanel {
  return {
    id: panelId,
    type: plan.panelType,
    title: plan.input.title,
    gridPos: plan.gridPos,
    datasource: { type: 'grafana-clickhouse-datasource', uid: plan.datasourceUid },
    ...(plan.input.panel?.transparent ? { transparent: true } : {}),
    targets: [{ rawSql: plan.querySql, refId: 'A', format: 1 }],
    fieldConfig: {
      defaults: plan.fieldDefaults,
      overrides: plan.fieldOverrides,
    },
    options: plan.options,
    ...(plan.transformations ? { transformations: plan.transformations } : {}),
  };
}

export function toGrafanaPanel(input: GrafanaExportInput, panelId = 1): GrafanaPanel {
  return renderGrafanaPanel(buildGrafanaExportPlan(input), panelId);
}

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
    ...(input.dashboardUid ? { uid: input.dashboardUid } : {}),
    tags: ['tracehouse', 'clickhouse'],
    timezone: 'browser',
    panels: [toGrafanaPanel(input)],
    schemaVersion: 39,
    templating: { list: [] },
  };
}
