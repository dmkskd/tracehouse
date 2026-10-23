import { describe, expect, it } from 'vitest';
import { acceptDashboardImport, importDashboardJson, inspectDashboardImport, listEmbeddedSql } from '../dashboard-import.js';

const root = "-- @meta: title='Root' group='Custom'\nSELECT 1";
const detail = "-- @meta: title='Detail' group='Custom'\nSELECT 2";
const payload = {
  title: 'Portable', panels: [{ queryName: 'Custom#Root' }], queries: [root, detail],
  filters: [{ param: 'db', label: 'Database', query: 'SELECT name FROM system.databases' }],
};

describe('dashboard import review', () => {
  it('lists every embedded definition and filter, and requires consent before accepting them', () => {
    const json = JSON.stringify(payload);
    expect(listEmbeddedSql(importDashboardJson(json))).toEqual([
      { kind: 'query', label: 'Custom#Root', sql: root },
      { kind: 'query', label: 'Custom#Detail', sql: detail },
      { kind: 'filter', label: 'Database', sql: payload.filters[0].query },
    ]);
    expect(() => acceptDashboardImport(json)).toThrow(/Review the embedded SQL/);
    expect(acceptDashboardImport(json, { sqlAcknowledged: true })).toMatchObject(payload);
    expect(acceptDashboardImport(JSON.stringify({ title: 'References', panels: payload.panels }))).toMatchObject({ title: 'References' });
    expect(() => acceptDashboardImport(JSON.stringify({ ...payload, queries: undefined }))).toThrow(/Review the embedded SQL/);
  });

  it.each([undefined, '', 42])('reviews SQL after replacing the original title %s', title => {
    const json = JSON.stringify({ ...payload, title });
    const options = { nameOverride: ' Renamed ' };
    expect(inspectDashboardImport(json, options)).toMatchObject({ ok: true, canImport: false, embeddedSql: { length: 3 } });
    expect(() => acceptDashboardImport(json, options)).toThrow(/Review the embedded SQL/);
    expect(acceptDashboardImport(json, { ...options, sqlAcknowledged: true })).toMatchObject({ title: 'Renamed', queries: payload.queries });
  });

  it('rejects duplicate names and malformed input even when consent was given', () => {
    const options = { sqlAcknowledged: true, existingTitles: [' PORTABLE '] };
    expect(inspectDashboardImport(JSON.stringify(payload), options)).toMatchObject({ nameClash: true, canImport: false });
    expect(() => acceptDashboardImport(JSON.stringify(payload), options)).toThrow(/already exists/);
    for (const invalid of [null, [], { title: 'Missing panels' }, { ...payload, panels: [null] },
      { ...payload, filters: [null] }, { ...payload, filters: {} }, { ...payload, queries: ['SELECT 1'] },
      { ...payload, queries: [root, root] }]) {
      const json = JSON.stringify(invalid);
      expect(inspectDashboardImport(json, options)).toMatchObject({ ok: false, canImport: false });
      expect(() => acceptDashboardImport(json, options)).toThrow();
    }
    expect(inspectDashboardImport('{')).toMatchObject({ ok: false, canImport: false });
    expect(inspectDashboardImport('{"title":"Draft"}')).toMatchObject({ ok: false, title: 'Draft' });
  });

  it('preserves authored SQL without treating metadata validation as statement safety', () => {
    const sql = "-- @meta: title='Root' group='Custom'\nDROP TABLE example";
    const imported = acceptDashboardImport(JSON.stringify({ ...payload, queries: [sql] }), { sqlAcknowledged: true });
    expect(imported.queries).toEqual([sql]);
    expect(listEmbeddedSql(imported)[0].sql).toBe(sql);
  });
});
