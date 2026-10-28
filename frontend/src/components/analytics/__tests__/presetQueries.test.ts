import { describe, expect, test } from 'vitest';
import { PRESET_QUERIES } from '../presetQueries';
import { loadDashboards } from '../dashboards';

describe('preset queries', { tags: ['analytics'] }, () => {
  test.each(['Table', 'Level Transition', 'Result Size'])('declares matching CPU and elapsed summaries by %s', dimension => {
    for (const metric of ['CPU', 'Elapsed Time']) {
      const query = PRESET_QUERIES.find(q => q.group === 'Merge Analytics' && q.name === `Merge ${metric} by ${dimension}`)!;
      // Table and level panels split successful merges from failed attempts; failed attempts have no result size.
      const [column, label] = metric === 'CPU' ? ['cpu_seconds', 'CPU'] : ['elapsed_seconds', 'duration'];
      const totals = dimension === 'Result Size'
        ? [{ valueColumn: `total_${column}`, label: `Total ${label}`, unit: 's' }]
        : [
          { valueColumn: `total_${column}`, label: `Successful ${label}`, unit: 's' },
          { valueColumn: `total_failed_${column}`, label: `Failed ${label}`, unit: 's' },
        ];
      expect(query.directives.summaries).toEqual({ metrics: [
        ...totals,
        { valueColumn: 'total_merge_count', label: 'Merges' },
      ], diagnostics: [] });
      expect(query.sql).toContain('unit=s style=2d');
    }
  });
  test('Merge Analytics cost panels and table drill targets resolve to registered queries', () => {
    Object.defineProperty(globalThis, 'localStorage', {
      value: { getItem: () => null }, configurable: true,
    });
    const dashboard = loadDashboards().find(d => d.id === 'merge-analytics');
    expect(dashboard?.filters?.some(filter => filter.param === 'tbl')).toBe(true);
    const panels = dashboard!.panels.slice(0, 7);
    expect(panels[0].section).toBe('Where merge CPU and time go');
    for (const panel of panels) {
      const query = PRESET_QUERIES.find(q => `${q.group}#${q.name}` === panel.queryName);
      expect(query).toBeDefined();
      expect(query!.sql).toContain('{{drill_value:tbl');
      if (query!.directives.drill) {
        const target = PRESET_QUERIES.find(q => q.group === query!.group && q.name === query!.directives.drill!.into);
        expect(target).toBeDefined();
        expect(target!.sql).toContain('{{drill_value:tbl');
      }
    }
  });

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
    expect(query?.sql).toContain("metric IN ('OSMemoryAvailable', 'OSMemoryTotal')");
    expect(query?.sql).not.toContain('CurrentMetric_MemoryTracking');
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

  test('Operational Events Explorer exposes detector queries and related activity', () => {
    Object.defineProperty(globalThis, 'localStorage', {
      value: {
        getItem: () => null,
        setItem: () => undefined,
        removeItem: () => undefined,
      },
      configurable: true,
    });
    const dashboard = loadDashboards().find(d => d.id === 'operational-events-explorer');
    const panelNames = dashboard?.panels.map(panel => panel.queryName) ?? [];

    expect(dashboard).toMatchObject({
      group: 'TraceHouse',
      category: 'Events',
      columns: 2,
    });
    expect(panelNames).toContain('Events#Event Source Availability');
    expect(panelNames).toContain('Events#Query Resource Failures');
    expect(panelNames).toContain('Events#Server Crashes');
    expect(panelNames).toContain('Events#Replication Failure Counters');
    expect(panelNames).toContain('Events#Warning+ Server Log Activity');
    expect(panelNames.every(name => PRESET_QUERIES.some(query => `${query.group}#${query.name}` === name))).toBe(true);
    expect(
      dashboard?.panels.find(panel => panel.queryName === 'Events#Server Crashes'),
    ).toMatchObject({ requiredCapability: 'crash_log' });
  });

  test('Event Source Availability reports cluster-wide host coverage', () => {
    const query = PRESET_QUERIES.find(
      item => item.group === 'Events' && item.name === 'Event Source Availability',
    );

    expect(query?.sql).toContain('{{cluster_aware:system.one}}');
    expect(query?.sql).toContain('{{cluster_aware:system.tables}}');
    expect(query?.sql).toContain('groupUniqArray(hostname())');
    expect(query?.sql).toContain('available_nodes');
    expect(query?.sql).toContain('cluster_nodes');
    expect(query?.sql).toContain('missing_on');
    expect(query?.sql).toContain("'partial'");
  });
});
