import { describe, it, expect } from 'vitest';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildXRaySamplesSQL } from '../queries/xray-samples-queries.js';
import * as publicQueries from '../queries/index.js';

const availability = { processesHistory: true, queryMetricLog: true };

describe('Query X-Ray sample façade', () => {
  it('selects fresh sampler samples for a live query and metric log for finished queries', () => {
    expect(buildXRaySamplesSQL({ availability, queryState: 'running' }, ['q'])).toContain('tracehouse.processes_history');
    expect(buildXRaySamplesSQL({ availability, queryState: 'finished' }, ['q'])).toContain('system.query_metric_log');
  });

  it('falls back from an unavailable pin for both output shapes', () => {
    const selection = { availability: { processesHistory: true, queryMetricLog: false }, preference: 'query_metric_log' as const };
    expect(buildXRaySamplesSQL(selection, ['q'])).toContain('tracehouse.processes_history');
    expect(buildXRaySamplesSQL(selection, ['q'], { perHost: true })).toContain('hostname');
  });

  it('supports metric-log host samples and bounds the identity scan to the query date', () => {
    const sql = buildXRaySamplesSQL({ availability, preference: 'query_metric_log' }, ['q'], { perHost: true, startedAt: '2026-09-01 12:00:00' });
    expect(sql).toContain('system.query_metric_log');
    expect(sql).toContain('hostname');
    expect(sql).toContain('2026-09-01');
  });

  it('rejects unavailable sources and invalid sample requests', () => {
    expect(() => buildXRaySamplesSQL({ availability: { processesHistory: false, queryMetricLog: false } }, ['q'])).toThrow(/No X-Ray source/);
    expect(() => buildXRaySamplesSQL({ availability }, [])).toThrow(/at least one/);
    expect(() => buildXRaySamplesSQL({ availability }, ['a', 'b'], { perHost: true })).toThrow(/exactly one/);
  });
});

describe('Query X-Ray architecture', () => {
  const forbidden = ['buildProcessSamplesSQL', 'buildHostProcessSamplesSQL', 'buildQueryMetricLogSamplesSQL', 'buildHostQueryMetricLogSamplesSQL', 'buildZoomProcessSamplesSQL', 'buildXRayOverlaySQL'];

  it('keeps raw sample builders out of the public barrel', () => {
    for (const name of forbidden) expect(publicQueries).not.toHaveProperty(name);
  });

  it('prevents app modules from importing raw sample builders', () => {
    const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../../..');
    function check(directory: string) {
      for (const entry of readdirSync(directory, { withFileTypes: true })) {
        const path = resolve(directory, entry.name);
        if (entry.isDirectory()) check(path);
        else if (/\.tsx?$/.test(entry.name)) {
          const imports = readFileSync(path, 'utf8').match(/import\s[\s\S]*?\sfrom\s['"][^'"]+['"]/g) ?? [];
          for (const statement of imports) {
            for (const name of forbidden) expect(statement, `${path} imports ${name}`).not.toMatch(new RegExp(`\\b${name}\\b`));
          }
        }
      }
    }
    check(resolve(root, 'frontend/src'));
    check(resolve(root, 'grafana-app-plugin/src'));
  });
});
