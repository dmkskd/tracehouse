import type { GrafanaCellStyle, GrafanaExportAnalysis, GrafanaExportCapability, GrafanaExportInput } from './types.js';
import { mapPanelType, resolveNumericColumnMax, resolveResultColumn } from './utils.js';

function capability(capability: GrafanaExportCapability): GrafanaExportCapability {
  return capability;
}

function canResolveColumnMax(column: string, input: GrafanaExportInput): boolean {
  return resolveNumericColumnMax(column, input) !== undefined;
}

function chartCapability(input: GrafanaExportInput, panelType: string): GrafanaExportCapability {
  if (!input.chart) {
    return capability({
      tracehouseFeature: 'table result',
      grafanaFeature: 'table panel',
      level: 'supported',
      message: 'Query results export as a Grafana table panel.',
      decision: 'map',
    });
  }

  const chartType = input.chart.type;
  switch (chartType) {
    case 'bar':
      return capability({
        tracehouseFeature: '@chart type=bar',
        grafanaFeature: 'barchart panel',
        level: 'supported',
        message: 'Categorical bars map to Grafana barchart with the selected group and value fields.',
        decision: 'map',
      });
    case 'area':
      return capability({
        tracehouseFeature: '@chart type=area',
        grafanaFeature: 'timeseries panel with fill',
        level: 'supported',
        message: 'Area charts map to Grafana time series with smooth lines and fill opacity.',
        decision: 'map',
      });
    case 'grouped_line':
      return capability({
        tracehouseFeature: '@chart type=grouped_line',
        grafanaFeature: 'timeseries panel with multiple value fields',
        level: 'supported',
        message: 'Grouped line charts map to Grafana time series when values are represented as multiple numeric fields.',
        decision: 'map',
      });
    case 'line':
      return capability({
        tracehouseFeature: '@chart type=line',
        grafanaFeature: 'timeseries panel',
        level: 'supported',
        message: 'Line charts map to Grafana time series.',
        decision: 'map',
      });
    case 'grouped_bar':
    case 'stacked_bar':
      if (input.chart.seriesColumn && input.chart.seriesValues?.length) {
        return capability({
          tracehouseFeature: `@chart type=${chartType}`,
          grafanaFeature: 'barchart panel with pivoted series fields',
          level: 'supported',
          message: 'Series-based bars are pivoted to one Grafana row per group, with one numeric field per series.',
          decision: 'map',
        });
      }
      return capability({
        tracehouseFeature: `@chart type=${chartType}`,
        grafanaFeature: 'barchart panel',
        level: 'partial',
        message: 'Grafana barchart supports grouping/stacking, but this export needs result series values to pivot the data correctly.',
        decision: 'map',
      });
    case 'pie':
      return capability({
        tracehouseFeature: '@chart type=pie',
        grafanaFeature: 'piechart panel',
        level: 'supported',
        message: 'Pie charts map to Grafana donut pie charts.',
        decision: 'map',
      });
    case 'radar':
      return capability({
        tracehouseFeature: '@chart type=radar',
        grafanaFeature: 'radar panel',
        level: 'partial',
        message: 'Full radar chart export is not implemented yet. First-cut Grafana support only maps radar table cells as compact generated SVG badges.',
        decision: 'split_panel',
      });
    default:
      return capability({
        tracehouseFeature: `@chart type=${chartType}`,
        grafanaFeature: panelType,
        level: 'partial',
        message: 'Chart type falls back to the closest Grafana panel type.',
        decision: 'map',
      });
  }
}

function cellCapability(style: GrafanaCellStyle, input: GrafanaExportInput): GrafanaExportCapability {
  const styleColumn = 'column' in style ? style.column : undefined;
  const radarColumn = style.type === 'radar' ? style.radarColumn : undefined;
  const feature = radarColumn
    ? `@cell radar_column=${radarColumn} type=radar`
    : `@cell column=${styleColumn} type=${style.type}`;
  if (input.chart) {
    return capability({
      tracehouseFeature: feature,
      level: 'unsupported',
      message: 'Table cell decorations are ignored when exporting the chart view.',
      decision: 'drop',
    });
  }

  if (style.type === 'radar' && style.radarColumn) {
    return capability({
      tracehouseFeature: feature,
      grafanaFeature: 'table image cell',
      level: 'partial',
      message: 'Radar cells export as compact generated SVG badges. Grafana table image cells have limited size and no row-specific hover tooltip; profile-level coloring is approximated by normalized max pressure in exported SQL.',
      decision: 'map',
    });
  }

  if (!styleColumn) {
    return capability({
      tracehouseFeature: feature,
      level: 'unsupported',
      message: 'Not exported. The cell style does not identify a result column.',
      decision: 'drop',
    });
  }

  const matchedColumn = resolveResultColumn(styleColumn, input.resultColumns);
  if (!matchedColumn) {
    return capability({
      tracehouseFeature: feature,
      grafanaFeature: 'field override',
      level: 'unsupported',
      message: `Not exported. The result set does not contain a "${styleColumn}" column, so Grafana would not be able to match this field override.`,
      decision: 'drop',
    });
  }

  switch (style.type) {
    case 'gauge':
      if (typeof style.max !== 'number') {
        if (canResolveColumnMax(style.max, input)) {
          return capability({
            tracehouseFeature: feature,
            grafanaFeature: 'table gauge cell',
            level: 'supported',
            message: `Gauge cells map to Grafana table gauge cells on "${matchedColumn}"; max=${style.max} is resolved from the query result and exported as a static Grafana max.`,
            decision: 'map',
          });
        }
        return capability({
          tracehouseFeature: feature,
          grafanaFeature: 'table gauge cell',
          level: 'partial',
          message: 'Grafana table gauges can use a static max cleanly; column-reference max values are exported as a gauge without that dynamic scale.',
          decision: 'map',
        });
      }
      return capability({
        tracehouseFeature: feature,
        grafanaFeature: 'table gauge cell',
        level: 'supported',
        message: `Gauge cells map to Grafana table gauge cells on "${matchedColumn}" with min/max and unit overrides.`,
        decision: 'map',
      });
    case 'rag':
      if (style.mode !== 'numeric') {
        return capability({
          tracehouseFeature: feature,
          grafanaFeature: 'field thresholds',
          level: 'partial',
          message: 'Grafana threshold overrides are numeric; text RAG rules are not fully represented yet.',
          decision: 'map',
        });
      }
      return capability({
        tracehouseFeature: feature,
        grafanaFeature: 'field thresholds',
        level: 'supported',
        message: `Numeric RAG rules map to Grafana threshold coloring on "${matchedColumn}".`,
        decision: 'map',
      });
    case 'sparkline':
      return capability({
        tracehouseFeature: feature,
        grafanaFeature: 'table image cell',
        level: 'partial',
        message: `Array-valued sparklines export as generated SVG image cells on "${matchedColumn}". Grafana native table sparklines are not used because they require time-series-to-table shaped data.`,
        decision: 'map',
      });
    case 'radar':
      return capability({
        tracehouseFeature: '@cell type=radar',
        grafanaFeature: 'table image cell',
        level: 'partial',
        message: 'Radar cells export as compact generated SVG badges. Grafana table image cells have limited size and no row-specific hover tooltip; profile-level coloring is approximated by normalized max pressure in exported SQL.',
        decision: 'map',
      });
  }
}

function interactionCapabilities(input: GrafanaExportInput): GrafanaExportCapability[] {
  return (input.interactions ?? []).map(interaction => {
    const target = interaction.into ? ` into=${JSON.stringify(interaction.into)}` : '';
    const feature = `@${interaction.type} on=${interaction.on}${target}`;
    return capability({
      tracehouseFeature: feature,
      grafanaFeature: 'panel links / data links',
      level: 'unsupported',
      message: 'Not exported. TraceHouse query navigation opens another TraceHouse query with inherited parameters; this exporter does not currently translate that behavior into Grafana data links.',
      decision: 'drop',
    });
  });
}

export function analyzeGrafanaExport(input: GrafanaExportInput): GrafanaExportAnalysis {
  const panelType = mapPanelType(input.chart?.type);
  return {
    panelType,
    capabilities: [
      chartCapability(input, panelType),
      ...(input.cellStyles ?? []).map(style => cellCapability(style, input)),
      ...interactionCapabilities(input),
    ],
  };
}
