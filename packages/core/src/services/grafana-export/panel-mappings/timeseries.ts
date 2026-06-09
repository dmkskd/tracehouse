import type { GrafanaExportInput } from '../types.js';
import { legendOptions } from './common.js';

export function timeseriesPanelOptions(input: GrafanaExportInput): Record<string, unknown> {
  return {
    tooltip: { mode: 'multi', sort: 'desc' },
    legend: { ...legendOptions(input, 'bottom'), calcs: ['lastNotNull', 'max'] },
  };
}

export function timeseriesFieldCustomConfig(chartType: string | undefined): Record<string, unknown> {
  return {
    drawStyle: 'line',
    lineInterpolation: 'smooth',
    lineWidth: 2,
    fillOpacity: chartType === 'area' ? 18 : 0,
    pointSize: 4,
    showPoints: 'never',
    spanNulls: true,
  };
}
