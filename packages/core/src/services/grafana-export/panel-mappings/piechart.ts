import type { GrafanaExportInput, GrafanaTransformation } from '../types.js';
import { legendOptions } from './common.js';

export function piechartPanelOptions(input: GrafanaExportInput): Record<string, unknown> {
  return {
    tooltip: { mode: 'single' },
    legend: { ...legendOptions(input, 'right'), values: ['value', 'percent'] },
    reduceOptions: {
      values: false,
      calcs: ['lastNotNull'],
      fields: '',
    },
    pieType: 'donut',
    displayLabels: ['name', 'percent'],
  };
}

export function piechartTransformations(input: GrafanaExportInput): GrafanaTransformation[] | undefined {
  if (!input.chart?.groupByColumn || !input.chart.valueColumn) return undefined;

  return [
    {
      id: 'rowsToFields',
      options: {
        mappings: [
          { fieldName: input.chart.groupByColumn, handlerKey: 'field.name' },
          { fieldName: input.chart.valueColumn, handlerKey: 'field.value' },
        ],
      },
    },
  ];
}
