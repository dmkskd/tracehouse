import { describe, expect, test } from 'vitest';
import { PRESET_QUERIES } from '../presetQueries';
import { loadDashboards } from '../dashboards';

describe('preset queries', { tags: ['analytics'] }, () => {
  test('Most Expensive Selects exposes query pressure as a radar cell', () => {
    const query = PRESET_QUERIES.find(q => q.group === 'Selects' && q.name === 'Most Expensive Selects');

    expect(query).toBeDefined();

    const radar = query?.directives.cellStyles.find(style => style.type === 'radar');

    expect(radar).toMatchObject({
      type: 'radar',
      radarColumn: 'shape',
      profile: 'query_pressure',
      color: 'profile_level',
      axes: {
        time: 'query_duration_ms',
        memory: 'memory_usage',
        cpu: 'cpu_ms',
        io: 'io_bytes',
        scan: 'scan_pressure',
      },
      ranges: {
        time: { low: '100', high: '60000' },
        memory: { low: '32Mi', high: '8Gi' },
        cpu: { low: '100', high: '60000' },
        io: { low: '1Mi', high: '10Gi' },
        scan: { low: '0', high: '1' },
      },
    });
  });

  test('Server Pressure Radar provides a one-row resource pressure chart example', () => {
    const query = PRESET_QUERIES.find(q => q.group === 'Resources' && q.name === 'Server Pressure Radar');

    expect(query).toBeDefined();
    expect(query?.directives.chart).toMatchObject({
      type: 'radar',
      axes: {
        cpu: 'cpu_pressure',
        memory: 'memory_pressure',
        io: 'io_bytes',
        network: 'network_bytes',
        queries: 'active_queries',
      },
      ranges: {
        cpu: { low: '0', high: '1' },
        memory: { low: '0', high: '1' },
        io: { low: '1Mi', high: '10Gi' },
        network: { low: '1Mi', high: '10Gi' },
        queries: { low: '1', high: '1000' },
      },
    });
    expect(query?.sql).toContain('SELECT avg(cpu_pressure)');
    expect(query?.sql).toContain('event_time > {{time_range}}');
  });

  test('Operations Overview includes the server pressure radar panel', () => {
    Object.defineProperty(globalThis, 'localStorage', {
      value: {
        getItem: () => null,
        setItem: () => undefined,
        removeItem: () => undefined,
      },
      configurable: true,
    });
    const dashboard = loadDashboards().find(d => d.id === 'ops-overview');

    expect(dashboard?.panels.map(panel => panel.queryName)).toContain('Resources#Server Pressure Radar');
  });
});
