import type { GrafanaExportInput } from '../types.js';

export function legendOptions(
  input: GrafanaExportInput,
  defaultPlacement: 'bottom' | 'right',
): { displayMode: string; placement: string } {
  const legendPlacement = input.panel?.legendPlacement ?? defaultPlacement;
  return legendPlacement === 'hidden'
    ? { displayMode: 'hidden', placement: 'bottom' }
    : { displayMode: 'table', placement: legendPlacement };
}
