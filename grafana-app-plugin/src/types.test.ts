import { describe, expect, it } from 'vitest';
import { resolvePluginConfig } from './types';

describe('Query X-Ray admin defaults', () => {
  it('defaults to automatic selection when no admin preference exists', () => {
    expect(resolvePluginConfig().queryXraySource).toBe('auto');
  });

  it('preserves each configured default', () => {
    for (const queryXraySource of ['auto', 'processes_history', 'query_metric_log'] as const) {
      expect(resolvePluginConfig({ queryXraySource }).queryXraySource).toBe(queryXraySource);
    }
  });
});
