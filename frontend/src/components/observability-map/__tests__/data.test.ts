import { describe, expect, it } from 'vitest';
import {
  OBSERVABILITY_DATA,
  LAST_ANALYSED_CH_VERSION,
  buildHierarchy,
  mergeAvailability,
} from '../data';

/**
 * Structural invariants for the observability catalog.
 *
 * The catalog is maintained by periodic bulk edits (re-diffing against a newer
 * ClickHouse release), so these guard the shape rather than the content: a
 * broken entry here is otherwise only visible by clicking through the sunburst.
 */

const allTables = OBSERVABILITY_DATA.children.flatMap(c =>
  c.children.map(t => ({ category: c.name, table: t })),
);

const SEMVER_MINOR = /^\d+\.\d+$/;

describe('observability catalog', { tags: ['observability'] }, () => {
  it('exposes a well-formed analysed-version marker', () => {
    expect(LAST_ANALYSED_CH_VERSION).toMatch(/^\d+\.\d+\.\d+\.\d+$/);
  });

  it('has no duplicate table names across categories', () => {
    const names = allTables.map(({ table }) => table.name);
    expect(names).toHaveLength(new Set(names).size);
  });

  it('gives every category a name and a colour', () => {
    for (const cat of OBSERVABILITY_DATA.children) {
      expect(cat.name).not.toHaveLength(0);
      expect(cat.color).toMatch(/^#[0-9a-fA-F]{6}$/);
    }
  });

  it.each(allTables)('$table.name is fully populated', ({ table }) => {
    // Most entries are real system tables; the catalog also carries a few
    // concept entries (e.g. "EXPLAIN variants") that are not tables.
    expect(table.name).toMatch(/^(system\.[a-z_0-9]+|[A-Z][\w ]+)$/);
    expect(table.desc.length).toBeGreaterThan(20);
    // Only real system tables have a column list; concept entries do not.
    if (table.name.startsWith('system.')) expect(table.cols.length).toBeGreaterThan(0);
    expect(table.queries.length).toBeGreaterThan(0);
  });

  it.each(allTables)('$table.name has runnable-looking diagnostics', ({ table }) => {
    for (const q of table.queries) {
      expect(q.label).not.toHaveLength(0);
      // Diagnostics are SELECTs, EXPLAINs, SHOWs or KILL statements.
      expect(q.sql).toMatch(/^\s*(SELECT|WITH|EXPLAIN|SHOW|KILL)\b/);
    }
  });

  it.each(allTables)('$table.name declares valid version prerequisites', ({ table }) => {
    if (table.since !== undefined) expect(table.since).toMatch(SEMVER_MINOR);
  });

  it.each(allTables)('$table.name lists each column once', ({ table }) => {
    expect(table.cols).toHaveLength(new Set(table.cols).size);
  });

  it('builds a two-ring hierarchy: category then table, with tables as leaves', () => {
    // The outer column ring was removed: its arc widths encoded a hand-assigned
    // importance score rather than any measurable quantity, and because d3 sums
    // leaf values upward it made the table and category widths meaningless too.
    const root = buildHierarchy(OBSERVABILITY_DATA);
    expect(root.children).toHaveLength(OBSERVABILITY_DATA.children.length);

    const built = root.children!.flatMap(c => c.children!);
    expect(built).toHaveLength(allTables.length);

    for (const table of built) {
      expect(table.meta!.type).toBe('table');
      expect(table.children).toBeUndefined();
    }
  });

  it('gives every table an equal slice of its category', () => {
    // Nothing about a system table has a magnitude worth encoding as arc width,
    // so a category's width reflects only how many tables it holds.
    const root = buildHierarchy(OBSERVABILITY_DATA);
    const values = root.children!.flatMap(c => c.children!.map(t => t.value));
    expect(new Set(values)).toEqual(new Set([1]));
  });

  it('carries the table payload the detail panel renders', () => {
    const root = buildHierarchy(OBSERVABILITY_DATA);

    for (const cat of root.children!) {
      for (const table of cat.children!) {
        expect(table.meta!.category).toBe(cat.name);
        expect(table.meta!.desc).not.toHaveLength(0);
        expect(table.meta!.queries!.length).toBeGreaterThan(0);
      }
    }
  });

  it('treats every table as available when the probe returns nothing', () => {
    const merged = mergeAvailability(OBSERVABILITY_DATA, new Map());
    for (const cat of merged.children) {
      for (const table of cat.children) expect(table.available).toBe(true);
    }
  });

  it('marks tables missing from the probe as unavailable', () => {
    const present = allTables[0].table.name;
    const merged = mergeAvailability(
      OBSERVABILITY_DATA,
      new Map([[present, { name: present, sorting_key: 'event_date', primary_key: 'event_time' }]]),
    );

    const flat = merged.children.flatMap(c => c.children);
    expect(flat.find(t => t.name === present)?.available).toBe(true);
    expect(flat.find(t => t.name === present)?.sortingKey).toBe('event_date');
    // Non-system entries are always treated as available, so only system
    // tables missing from the probe are marked unavailable.
    const systemTables = flat.filter(t => t.name.startsWith('system.'));
    expect(flat.filter(t => t.available === false).length).toBe(systemTables.length - 1);
  });
});
