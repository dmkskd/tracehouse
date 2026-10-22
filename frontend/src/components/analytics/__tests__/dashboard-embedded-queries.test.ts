import { beforeEach, describe, expect, it, vi } from 'vitest';
import { exportDashboardJson, getDashboardQueries, importDashboardJson, loadDashboards, resolvePanel, type Dashboard } from '../dashboards';
import { PRESET_QUERIES } from '../presetQueries';
import { resolveQueryRef } from '../metaLanguage';

const root = `-- @meta: title='Root' group='Custom'
-- @drill: on=tbl into='Detail'
SELECT 'app.events' AS tbl`;
const detail = `-- @meta: title='Detail' group='Custom'
-- @link: on=tbl into='Root'
SELECT {{drill_value:tbl | 'all'}} AS tbl`;
const dashboard: Dashboard = {
  id: 'portable', title: 'Portable', columns: 2,
  panels: [{ queryName: 'Custom#Root', section: 'Costs', requiredCapability: 'part_log' }],
  queries: [root, detail],
  filters: [{ param: 'tbl', label: 'Table', query: 'SELECT name FROM system.tables', dependsOn: 'db' }],
};

beforeEach(() => {
  const values = new Map<string, string>();
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
  });
});

describe('dashboard-owned queries', () => {
  it('round-trips SQL, cyclic drill/link dependencies, sections, capabilities, and filters', () => {
    const imported = importDashboardJson(exportDashboardJson(dashboard, true));
    expect(imported.id).not.toBe(dashboard.id);
    expect(imported.queries).toEqual([root, detail]);
    expect(imported.panels).toEqual(dashboard.panels);
    expect(imported.filters).toEqual(dashboard.filters);
    const scope = getDashboardQueries(imported);
    const panel = resolvePanel(imported.panels[0], scope)!;
    expect(panel.sql).toBe(root);
    expect(resolveQueryRef(panel.directives.drill!.into, panel.group, scope)?.sql).toBe(detail);
    expect(localStorage.getItem('tracehouse-custom-queries')).toBeNull();
  });

  it('bundles referenced presets and all nested targets without embedding unrelated presets', () => {
    const source = loadDashboards().find(d => d.id === 'merge-analytics')!;
    const portable = importDashboardJson(exportDashboardJson(source, true));
    const scope = getDashboardQueries(portable);
    const included = scope.filter(q => portable.queries!.includes(q.sql));
    expect(included.length).toBeGreaterThan(source.panels.length);
    expect(included.length).toBeLessThan(PRESET_QUERIES.length);
    for (const query of included) {
      for (const target of [query.directives.drill, query.directives.link]) {
        if (target) expect(resolveQueryRef(target.into, query.group, included)).toBeDefined();
      }
    }
    expect(portable.id).not.toBe(source.id);
  });

  it('keeps dashboard edits isolated from presets and other dashboards', () => {
    const preset = PRESET_QUERIES[0];
    const override = `${preset.sql}\n-- dashboard-specific edit`;
    const panel = { queryName: `${preset.group}#${preset.name}` };
    expect(resolvePanel(panel, getDashboardQueries({ queries: [override] }))?.sql).toBe(override);
    expect(resolvePanel(panel)?.sql).toBe(preset.sql);
  });

  it('accepts legacy references and rejects invalid SQL definitions and unresolved exports', () => {
    expect(JSON.parse(exportDashboardJson(dashboard)).queries).toBeUndefined();
    expect(importDashboardJson(JSON.stringify({ title: 'Legacy', panels: [] })).queries).toBeUndefined();
    expect(() => importDashboardJson(JSON.stringify({ ...dashboard, queries: ['SELECT 1'] }))).toThrow(/@meta/);
    expect(() => importDashboardJson(JSON.stringify({ ...dashboard, queries: [root, root] }))).toThrow(/Duplicate/);
    expect(() => exportDashboardJson({ ...dashboard, queries: [], panels: [{ queryName: 'Missing#Unknown' }] }, true)).toThrow(/Query not found/);
  });
});
