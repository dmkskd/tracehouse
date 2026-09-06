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
    expect(table.children.length).toBeGreaterThan(0);
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
    for (const col of table.children) {
      if (col.since !== undefined) expect(col.since).toMatch(SEMVER_MINOR);
    }
  });

  it.each(allTables)('$table.name has positive column weights', ({ table }) => {
    for (const col of table.children) {
      expect(col.name).not.toHaveLength(0);
      expect(col.size).toBeGreaterThan(0);
    }
  });

  it('propagates column-level version prerequisites into the hierarchy', () => {
    const root = buildHierarchy(OBSERVABILITY_DATA);
    const columns = root.children!.flatMap(c => c.children!.flatMap(t => t.children ?? []));

    const versioned = columns.filter(c => c.meta?.since);
    expect(versioned.length).toBeGreaterThan(0);
    for (const col of versioned) {
      expect(col.meta!.type).toBe('column');
      expect(col.meta!.since).toMatch(SEMVER_MINOR);
    }
  });

  it('builds a hierarchy that preserves every table and column', () => {
    const root = buildHierarchy(OBSERVABILITY_DATA);
    expect(root.children).toHaveLength(OBSERVABILITY_DATA.children.length);

    const built = root.children!.flatMap(c => c.children!);
    expect(built).toHaveLength(allTables.length);

    const builtColumns = built.flatMap(t => t.children ?? []).length;
    const sourceColumns = allTables.reduce((n, { table }) => n + table.children.length, 0);
    expect(builtColumns).toBe(sourceColumns);
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
