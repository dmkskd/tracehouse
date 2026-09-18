import { describe, expect, it } from 'vitest';
import { resolveQueryXRaySQL, resolveExportTemplate, resolveTimeRange, resolveDrillParams } from '../templateResolution';
import queries from '../queries/xray';
import selfMonitoring from '../queries/selfMonitoring';

const availability = { processesHistory: true, queryMetricLog: true };

describe('Series X-Ray source selection', () => {
  it('expands every supported overlay for either source before resolving other templates', () => {
    for (const preference of ['processes_history', 'query_metric_log'] as const) {
      for (const template of queries) {
        if (preference === 'query_metric_log' && template.includes('{{query_xray_overlay:read_mb_s}}')) continue;
        const sql = resolveDrillParams(resolveTimeRange(resolveQueryXRaySQL(template, { availability, preference }), '1 HOUR'), { db: "it's-db", tbl: 'db.table' });
        expect(sql).not.toContain('{{query_xray_overlay:');
        expect(sql).not.toContain('{{time_range}}');
        expect(sql).not.toContain('{{drill_value:');
        expect(sql).toContain(preference === 'query_metric_log' ? 'system.query_metric_log' : 'tracehouse.processes_history');
        expect(sql).toContain("it\\'s-db");
      }
    }
  });

  it('falls back from unavailable pins and refuses to fabricate progress bytes', () => {
    const cpu = queries[0];
    expect(resolveQueryXRaySQL(cpu, { availability: { processesHistory: true, queryMetricLog: false }, preference: 'query_metric_log' })).toContain('tracehouse.processes_history');
    expect(() => resolveQueryXRaySQL(queries[2], { availability, preference: 'query_metric_log' })).toThrow(/Sampled read_bytes is unavailable/);
    expect(() => resolveQueryXRaySQL(cpu, { availability: { processesHistory: false, queryMetricLog: false } })).toThrow(/No X-Ray source/);
  });

  it('carries the resolution failure to the Grafana export instead of dropping it', () => {
    // The export button is disabled on `error` and shows it as the tooltip, so
    // losing the message leaves a dead button with no stated reason.
    const resolved = resolveExportTemplate(queries[0], { availability, preference: 'query_metric_log' });
    expect(resolved.sql).toContain('system.query_metric_log');
    expect(resolved.error).toBeUndefined();

    const unavailable = resolveExportTemplate(queries[2], { availability, preference: 'query_metric_log' });
    expect(unavailable.sql).toBeUndefined();
    expect(unavailable.error).toMatch(/Sampled read_bytes is unavailable/);

    const noSource = resolveExportTemplate(queries[0], { availability: { processesHistory: false, queryMetricLog: false } });
    expect(noSource.error).toMatch(/No X-Ray source/);
  });

  it('clips absolute time ranges in UTC and leaves sampler diagnostics unchanged', () => {
    const sql = resolveTimeRange(resolveQueryXRaySQL(queries[0], { availability, preference: 'query_metric_log' }), '1 HOUR', 'CUSTOM:2026-09-01T12:00:00.000Z,2026-09-01T12:05:00.000Z');
    expect(sql).toContain("event_time > toDateTime('2026-09-01 12:00:00', 'UTC') AND event_time < toDateTime('2026-09-01 12:05:00', 'UTC')");
    for (const diagnostic of selfMonitoring) expect(resolveQueryXRaySQL(diagnostic, { availability, preference: 'query_metric_log' })).toBe(diagnostic);
  });
});
