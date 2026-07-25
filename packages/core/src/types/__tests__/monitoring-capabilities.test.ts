import { describe, expect, it } from 'vitest';
import { deriveMonitoringFlags } from '../monitoring-capabilities.js';

describe('monitoring capability version fallbacks', () => {
  it('enables EXPLAIN ANALYZE at ClickHouse 26.7', () => {
    expect(deriveMonitoringFlags([], '26.7.1.1315').hasExplainAnalyze).toBe(true);
    expect(deriveMonitoringFlags([], '26.6.9.1').hasExplainAnalyze).toBe(false);
  });

  it('prefers an explicit positive capability result', () => {
    const flags = deriveMonitoringFlags([{
      id: 'explain_analyze',
      label: 'EXPLAIN ANALYZE',
      description: 'Runtime plans',
      available: true,
      category: 'profiling',
    }], '25.8.0');

    expect(flags.hasExplainAnalyze).toBe(true);
  });
});
