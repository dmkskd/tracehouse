import { describe, expect, it } from 'vitest';
import { analyzeGrafanaExport, buildGrafanaExportPlan, toGrafanaDashboard, toGrafanaPanel, type GrafanaExportInput } from '../grafana-export.js';

const baseSql = `
-- @meta: title='Biggest Tables' group='Overview'
-- @chart: type=bar group_by=table value=rows,bytes_size style=2d
-- @rag: rows numeric asc green=10 amber=100
SELECT
  database,
  table,
  sum(part_rows) AS rows,
  sum(part_bytes) AS bytes_size
FROM system.parts
GROUP BY database, table
ORDER BY bytes_size DESC
`;

describe('grafana-export', () => {
  it('exports a styled bar chart panel from Tracehouse chart metadata', () => {
    const panel = toGrafanaPanel({
      sql: baseSql,
      title: 'Biggest Tables',
      datasourceUid: 'clickhouse-main',
      chart: {
        type: 'bar',
        groupByColumn: 'table',
        valueColumn: 'rows',
        valueColumns: ['rows', 'bytes_size'],
      },
    });

    expect(panel.type).toBe('barchart');
    expect(panel.title).toBe('Biggest Tables');
    expect(panel.datasource.uid).toBe('clickhouse-main');
    expect(panel.gridPos).toEqual({ h: 10, w: 18, x: 0, y: 0 });
    expect(panel.targets[0].rawSql).not.toContain('@meta');
    expect(panel.targets[0].rawSql).not.toContain('@chart');
    expect(panel.targets[0].rawSql).toMatch(/^SELECT `table`, `rows`, `bytes_size`\nFROM \(/);
    expect(panel.targets[0].rawSql).toContain('sum(part_bytes) AS bytes_size');
    expect(panel.options).toMatchObject({
      orientation: 'horizontal',
      xField: 'table',
      xTickLabelRotation: 0,
      legend: { displayMode: 'list', placement: 'bottom' },
      tooltip: { mode: 'multi', sort: 'desc' },
      showValue: 'never',
    });
    expect(panel.fieldConfig.defaults).toMatchObject({
      unit: 'short',
      color: { mode: 'thresholds' },
      custom: { fillOpacity: 94, lineWidth: 0 },
    });
    expect(panel.fieldConfig.defaults.thresholds).toEqual({
      mode: 'percentage',
      steps: [
        { color: '#38bdf8', value: null },
        { color: '#22c55e', value: 20 },
        { color: '#6366f1', value: 40 },
        { color: '#7c3aed', value: 60 },
        { color: '#a855f7', value: 80 },
      ],
    });
    expect(panel.fieldConfig.overrides).toEqual([
      {
        matcher: { id: 'byName', options: 'rows' },
        properties: [
          { id: 'displayName', value: 'Rows' },
          { id: 'unit', value: 'short' },
        ],
      },
      {
        matcher: { id: 'byName', options: 'bytes_size' },
        properties: [
          { id: 'displayName', value: 'Bytes Size' },
          { id: 'unit', value: 'bytes' },
        ],
      },
    ]);
  });

  it('does not hide extra result fields for single-series bar charts', () => {
    const panel = toGrafanaPanel({
      sql: baseSql,
      title: 'Biggest Tables',
      chart: {
        type: 'bar',
        groupByColumn: 'table',
        valueColumn: 'bytes_size',
      },
    });

    expect(panel.options).toMatchObject({
      orientation: 'horizontal',
      legend: { displayMode: 'list', placement: 'bottom' },
    });
    expect(panel.fieldConfig.defaults.unit).toBe('bytes');
    expect(panel.fieldConfig.overrides).toEqual([
      {
        matcher: { id: 'byName', options: 'bytes_size' },
        properties: [
          { id: 'displayName', value: 'Bytes Size' },
          { id: 'unit', value: 'bytes' },
        ],
      },
    ]);
    expect(JSON.stringify(panel)).not.toContain('custom.hideFrom');
    expect(panel.targets[0].rawSql).toContain('sum(part_rows) AS rows');
    expect(panel.targets[0].rawSql).toContain('sum(part_bytes) AS bytes_size');
    expect(panel.targets[0].rawSql).toMatch(/^SELECT `table`, `bytes_size`\nFROM \(/);
  });

  it('exports a table panel when no chart config is present', () => {
    const panel = toGrafanaPanel({
      sql: 'SELECT name, value FROM system.metrics',
      title: 'Metrics',
    });

    expect(panel.type).toBe('table');
    expect(panel.title).toBe('Metrics');
    expect(panel.datasource.uid).toBe('${DS_CLICKHOUSE}');
    expect(panel.options).toMatchObject({ showHeader: true, cellHeight: 'sm' });
    expect(panel.gridPos).toEqual({ h: 9, w: 18, x: 0, y: 0 });
  });

  it('maps Table Health @cell gauge and RAG styles to Grafana table field overrides', () => {
    const input: GrafanaExportInput = {
      sql: `
-- @meta: title='Table Health' group='Overview'
-- @cell: column=disk_pct type=gauge max=100 unit=%
-- @cell: column=disk_pct type=rag green<30 amber<60
-- @cell: column=part_sizes type=sparkline color=#6366f1 fill=true
SELECT database, table, parts, disk_size, total_rows, disk_pct, part_sizes
FROM table_health
`,
      title: 'Table Health',
      cellStyles: [
        { column: 'disk_pct', type: 'gauge', max: 100, unit: '%' },
        { column: 'disk_pct', type: 'rag', mode: 'numeric', direction: 'asc', greenThreshold: 30, amberThreshold: 60 },
        { column: 'part_sizes', type: 'sparkline', color: '#6366f1', fill: true },
      ],
    };
    const panel = toGrafanaPanel(input);
    const analysis = analyzeGrafanaExport(input);
    const plan = buildGrafanaExportPlan(input);

    expect(panel.type).toBe('table');
    expect(plan.panelType).toBe('table');
    expect(panel.targets[0].rawSql).not.toContain('@cell');
    expect(panel.fieldConfig.defaults.thresholds).toBeUndefined();
    expect(panel.fieldConfig.overrides).toEqual([
      {
        matcher: { id: 'byName', options: 'disk_pct' },
        properties: [
          { id: 'displayName', value: 'Disk Pct' },
          { id: 'unit', value: 'percent' },
          { id: 'min', value: 0 },
          { id: 'max', value: 100 },
          { id: 'custom.cellOptions', value: { type: 'gauge', mode: 'basic' } },
          {
            id: 'thresholds',
            value: {
              mode: 'absolute',
              steps: [
                { color: 'green', value: null },
                { color: 'orange', value: 30 },
                { color: 'red', value: 60 },
              ],
            },
          },
          { id: 'color', value: { mode: 'thresholds' } },
        ],
      },
    ]);
    expect(panel.transformations).toEqual([
      {
        id: 'organize',
        options: {
          excludeByName: { part_sizes: true },
        },
      },
    ]);
    expect(analysis.capabilities).toMatchObject([
      {
        tracehouseFeature: 'table result',
        grafanaFeature: 'table panel',
        level: 'supported',
        decision: 'map',
      },
      {
        tracehouseFeature: '@cell column=disk_pct type=gauge',
        grafanaFeature: 'table gauge cell',
        level: 'supported',
        decision: 'map',
      },
      {
        tracehouseFeature: '@cell column=disk_pct type=rag',
        grafanaFeature: 'field thresholds',
        level: 'supported',
        decision: 'map',
      },
      {
        tracehouseFeature: '@cell column=part_sizes type=sparkline',
        grafanaFeature: 'table sparkline cell',
        level: 'unsupported',
        decision: 'hide',
      },
    ]);
  });

  it('maps RAG-only table cells to Grafana colored text thresholds', () => {
    const panel = toGrafanaPanel({
      sql: 'SELECT query_hash, cpu_per_sec FROM heavy_queries',
      title: 'CPU Heavy Queries',
      resultColumns: ['query_hash', 'Cpu_Per_Sec'],
      cellStyles: [
        { column: 'cpu_per_sec', type: 'rag', mode: 'numeric', direction: 'asc', greenThreshold: 0.5, amberThreshold: 1.5 },
      ],
    });

    expect(panel.type).toBe('table');
    expect(panel.fieldConfig.overrides).toEqual([
          {
            matcher: { id: 'byName', options: 'Cpu_Per_Sec' },
            properties: [
              { id: 'displayName', value: 'Cpu Per Sec' },
              { id: 'unit', value: 'short' },
              {
            id: 'thresholds',
            value: {
              mode: 'absolute',
              steps: [
                { color: 'green', value: null },
                { color: 'orange', value: 0.5 },
                { color: 'red', value: 1.5 },
              ],
            },
          },
          { id: 'color', value: { mode: 'thresholds' } },
          { id: 'custom.cellOptions', value: { type: 'color-text' } },
        ],
      },
    ]);
  });

  it('reports TraceHouse query interactions as unsupported in Grafana export analysis', () => {
    const analysis = analyzeGrafanaExport({
      sql: 'SELECT query_hash FROM query_costs',
      title: 'App Query Cost Details',
      resultColumns: ['query_hash'],
      interactions: [
        { type: 'link', on: 'query_hash', into: 'App Query Executions' },
      ],
    });

    expect(analysis.capabilities).toContainEqual({
      tracehouseFeature: '@link on=query_hash into="App Query Executions"',
      grafanaFeature: 'panel links / data links',
      level: 'unsupported',
      message: 'Not exported. TraceHouse query navigation opens another TraceHouse query with inherited parameters; this exporter does not currently translate that behavior into Grafana data links.',
      decision: 'drop',
    });
  });

  it('maps time-series area charts to smoother filled Grafana timeseries panels', () => {
    const panel = toGrafanaPanel({
      sql: 'SELECT event_time, p99_ms, rows FROM query_log',
      title: 'Latency',
      chart: {
        type: 'area',
        groupByColumn: 'event_time',
        valueColumn: 'p99_ms',
        color: '#f59e0b',
      },
    });

    expect(panel.type).toBe('timeseries');
    expect(panel.targets[0].rawSql).toMatch(/^SELECT `event_time`, `p99_ms`\nFROM \(/);
    expect(panel.targets[0].rawSql).toContain('SELECT event_time, p99_ms, rows FROM query_log');
    expect(panel.fieldConfig.defaults.unit).toBe('ms');
    expect(panel.fieldConfig.defaults.color).toEqual({ mode: 'fixed', fixedColor: '#f59e0b' });
    expect(panel.fieldConfig.defaults.custom).toMatchObject({
      drawStyle: 'line',
      lineInterpolation: 'smooth',
      fillOpacity: 18,
      spanNulls: true,
    });
    expect(panel.options).toMatchObject({
      tooltip: { mode: 'multi', sort: 'desc' },
      legend: { displayMode: 'table', placement: 'bottom' },
    });
  });

  it('maps Tracehouse time range placeholders to Grafana ClickHouse time macros', () => {
    const panel = toGrafanaPanel({
      sql: `
SELECT toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t, avg(ProfileEvent_Query) AS qps
FROM system.metric_log
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t
`,
      title: 'Queries/second',
      chart: {
        type: 'area',
        groupByColumn: 't',
        valueColumn: 'qps',
      },
    });

    expect(panel.title).toBe('Queries/second');
    expect(panel.targets[0].rawSql).toMatch(/^SELECT `t`, `qps`\nFROM \(/);
    expect(panel.targets[0].rawSql).toContain('WHERE $__timeFilter(event_time)');
    expect(panel.targets[0].rawSql).not.toContain('{{time_range}}');
    expect(panel.targets[0].rawSql).not.toContain('now() - INTERVAL');
  });

  it('keeps multi-value line chart colors deterministic', () => {
    const panel = toGrafanaPanel({
      sql: 'SELECT hour, p50, p95, p99 FROM insert_quantiles',
      title: 'Insert Duration Quantiles',
      chart: {
        type: 'grouped_line',
        groupByColumn: 'hour',
        valueColumn: 'p50',
        valueColumns: ['p50', 'p95', 'p99'],
        unit: 'ms',
      },
    });

    expect(panel.type).toBe('timeseries');
    expect(panel.targets[0].rawSql).toMatch(/^SELECT `hour`, `p50`, `p95`, `p99`\nFROM \(/);
    expect(panel.fieldConfig.overrides).toEqual([
      {
        matcher: { id: 'byName', options: 'p50' },
        properties: [
          { id: 'displayName', value: 'P50' },
          { id: 'color', value: { mode: 'fixed', fixedColor: '#3b82f6' } },
        ],
      },
      {
        matcher: { id: 'byName', options: 'p95' },
        properties: [
          { id: 'displayName', value: 'P95' },
          { id: 'color', value: { mode: 'fixed', fixedColor: '#f59e0b' } },
        ],
      },
      {
        matcher: { id: 'byName', options: 'p99' },
        properties: [
          { id: 'displayName', value: 'P99' },
          { id: 'color', value: { mode: 'fixed', fixedColor: '#ef4444' } },
        ],
      },
    ]);
  });

  it('keeps time-bucket bar charts vertical', () => {
    const panel = toGrafanaPanel({
      sql: 'SELECT t, errors FROM error_log',
      title: 'Errors',
      chart: {
        type: 'bar',
        groupByColumn: 't',
        valueColumn: 'errors',
      },
    });

    expect(panel.options).toMatchObject({
      orientation: 'vertical',
      xTickLabelRotation: 0,
    });
  });

  it('pivots series-based stacked bar exports into one Grafana row per group', () => {
    const panel = toGrafanaPanel({
      sql: `
SELECT component, metric, value_ms
FROM query_duration_components
ORDER BY component, metric
`,
      title: 'App Query Duration by Component',
      chart: {
        type: 'stacked_bar',
        groupByColumn: 'component',
        valueColumn: 'value_ms',
        seriesColumn: 'metric',
        seriesValues: ['p50', 'p95', 'p99'],
        unit: 'ms',
        maxRows: 12,
      },
    });

    expect(panel.type).toBe('barchart');
    expect(panel.options).toMatchObject({
      orientation: 'horizontal',
      xField: 'component',
      stacking: 'normal',
    });
    expect(panel.targets[0].rawSql).toContain('SELECT `component`, sumIf(`value_ms`, `metric` = \'p50\') AS `p50`, sumIf(`value_ms`, `metric` = \'p95\') AS `p95`, sumIf(`value_ms`, `metric` = \'p99\') AS `p99`');
    expect(panel.targets[0].rawSql).toContain('GROUP BY `component`');
    expect(panel.targets[0].rawSql).toContain('ORDER BY `p50` + `p95` + `p99` DESC');
    expect(panel.targets[0].rawSql).toContain('LIMIT 12');
    expect(panel.fieldConfig.overrides).toEqual([
      {
        matcher: { id: 'byName', options: 'p50' },
        properties: [
          { id: 'displayName', value: 'P50' },
          { id: 'unit', value: 'ms' },
          { id: 'color', value: { mode: 'fixed', fixedColor: '#3b82f6' } },
        ],
      },
      {
        matcher: { id: 'byName', options: 'p95' },
        properties: [
          { id: 'displayName', value: 'P95' },
          { id: 'unit', value: 'ms' },
          { id: 'color', value: { mode: 'fixed', fixedColor: '#f59e0b' } },
        ],
      },
      {
        matcher: { id: 'byName', options: 'p99' },
        properties: [
          { id: 'displayName', value: 'P99' },
          { id: 'unit', value: 'ms' },
          { id: 'color', value: { mode: 'fixed', fixedColor: '#ef4444' } },
        ],
      },
    ]);
  });

  it('caps dense horizontal bar exports and grows the panel height', () => {
    const panel = toGrafanaPanel({
      sql: baseSql,
      title: 'Part Sizes',
      chart: {
        type: 'bar',
        groupByColumn: 'name',
        valueColumn: 'bytes_size',
        maxRows: 50,
      },
    });

    expect(panel.title).toBe('Part Sizes');
    expect(panel.gridPos).toEqual({ h: 34, w: 18, x: 0, y: 0 });
    expect(panel.targets[0].rawSql).toMatch(/^SELECT `name`, `bytes_size`\nFROM \(/);
    expect(panel.targets[0].rawSql).toMatch(/\)\nLIMIT 50$/);
  });

  it('lets explicit panel height override dense bar auto-height', () => {
    const panel = toGrafanaPanel({
      sql: baseSql,
      title: 'Part Sizes',
      panel: { height: 12 },
      chart: {
        type: 'bar',
        groupByColumn: 'name',
        valueColumn: 'bytes_size',
        maxRows: 50,
      },
    });

    expect(panel.gridPos.h).toBe(12);
  });

  it('maps numeric RAG rules to Grafana thresholds', () => {
    const panel = toGrafanaPanel({
      sql: 'SELECT metric, value FROM system.metrics',
      title: 'Metric',
      chart: { type: 'bar', groupByColumn: 'metric', valueColumn: 'value' },
      rag: [
        { column: 'value', mode: 'numeric', direction: 'desc', amberThreshold: 50, greenThreshold: 90 },
      ],
    });

    expect(panel.fieldConfig.defaults.thresholds).toEqual({
      mode: 'absolute',
      steps: [
        { color: 'red', value: null },
        { color: 'orange', value: 50 },
        { color: 'green', value: 90 },
      ],
    });
  });

  it('reports table cell styles that cannot match a result column', () => {
    const analysis = analyzeGrafanaExport({
      sql: 'SELECT userCPUms FROM heavy_queries',
      title: 'CPU Heavy Queries',
      resultColumns: ['UserCPUms'],
      cellStyles: [
        { column: 'user_cpu_ms', type: 'rag', mode: 'numeric', direction: 'asc', greenThreshold: 10, amberThreshold: 100 },
      ],
    });

    expect(analysis.capabilities).toContainEqual({
      tracehouseFeature: '@cell column=user_cpu_ms type=rag',
      grafanaFeature: 'field override',
      level: 'unsupported',
      message: 'Not exported. The result set does not contain a "user_cpu_ms" column, so Grafana would not be able to match this field override.',
      decision: 'drop',
    });
  });

  it('wraps the panel in importable dashboard JSON with datasource input', () => {
    const input: GrafanaExportInput = {
      sql: 'SELECT table, sum(bytes) AS bytes FROM system.parts GROUP BY table',
      title: 'Storage',
      dashboardUid: 'tracehouse-storage',
      chart: { type: 'bar', groupByColumn: 'table', valueColumn: 'bytes' },
    };

    const dashboard = toGrafanaDashboard(input);

    expect(dashboard).toMatchObject({
      title: 'Tracehouse: Storage',
      uid: 'tracehouse-storage',
      tags: ['tracehouse', 'clickhouse'],
      timezone: 'browser',
      templating: { list: [] },
    });
    expect((dashboard.__inputs as Array<Record<string, unknown>>)[0]).toMatchObject({
      name: 'DS_CLICKHOUSE',
      pluginId: 'grafana-clickhouse-datasource',
    });
    expect((dashboard.panels as unknown[])).toHaveLength(1);
  });
});
