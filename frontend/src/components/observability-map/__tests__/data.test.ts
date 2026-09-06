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

  it('tags every column node with its parent table', () => {
    // Column names are not unique within a category, so a consumer resolving a
    // selected column back to its table must use meta.table. Without it, generic
    // names like `value` or `description` resolve to whichever table lists them
    // first, opening the wrong table in the detail panel.
    const root = buildHierarchy(OBSERVABILITY_DATA);

    for (const cat of root.children!) {
      for (const table of cat.children!) {
        for (const col of table.children ?? []) {
          expect(col.meta!.table).toBe(table.name);
          expect(col.meta!.category).toBe(cat.name);
        }
      }
    }
  });

  it('has column names that repeat within a category, so name-only lookup is unsafe', () => {
    // Guards the assumption behind the test above: if this ever stops being true
    // the parent-table tagging is still correct, but the bug it prevents is real
    // today and this documents it.
    const duplicated = OBSERVABILITY_DATA.children.flatMap(cat => {
      const seen = new Map<string, number>();
      for (const t of cat.children) {
        for (const c of t.children) seen.set(c.name, (seen.get(c.name) ?? 0) + 1);
      }
      return [...seen.entries()].filter(([, n]) => n > 1).map(([name]) => `${cat.name}.${name}`);
    });

    expect(duplicated.length).toBeGreaterThan(0);
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
