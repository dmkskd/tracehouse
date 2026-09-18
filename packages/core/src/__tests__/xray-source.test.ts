import { describe, it, expect } from 'vitest';
import {
  selectQueryXRaySource,
  AUTO_PREFERRED_SOURCE,
  SYSTEM_LOG_FLUSH_LAG_MS,
  type QueryXRaySourceAvailability,
} from '../types/xray-source.js';
import {
  buildQueryMetricLogSamplesSQL,
  buildHostQueryMetricLogSamplesSQL,
  QUERY_METRIC_LOG_MISSING_FIELDS,
} from '../queries/query-metric-log-queries.js';
import { buildHostProcessSamplesSQL, mapHostProcessSampleRow } from '../queries/process-queries.js';

const BOTH: QueryXRaySourceAvailability = { processesHistory: true, queryMetricLog: true };
const SAMPLER_ONLY: QueryXRaySourceAvailability = { processesHistory: true, queryMetricLog: false };
const METRIC_LOG_ONLY: QueryXRaySourceAvailability = { processesHistory: false, queryMetricLog: true };
const NEITHER: QueryXRaySourceAvailability = { processesHistory: false, queryMetricLog: false };

describe('selectQueryXRaySource', { tags: ['query-analysis'] }, () => {
  describe('automatic selection', () => {
    it('prefers the configured auto source for a finished query when both exist', () => {
      const sel = selectQueryXRaySource({ availability: BOTH, queryState: 'finished' });
      expect(sel.source).toBe(AUTO_PREFERRED_SOURCE);
      expect(sel.reason).toBe('auto');
    });

    it('uses processes_history for a running query even when both exist', () => {
      const sel = selectQueryXRaySource({ availability: BOTH, queryState: 'running' });
      expect(sel.source).toBe('processes_history');
      expect(sel.lagMs).toBe(0);
    });

    it('treats unknown query state as possibly running', () => {
      const sel = selectQueryXRaySource({ availability: BOTH, queryState: 'unknown' });
      expect(sel.source).toBe('processes_history');
    });

    it('defaults query state to unknown when omitted', () => {
      expect(selectQueryXRaySource({ availability: BOTH }).source).toBe('processes_history');
    });

    it('reports the sole available source as only_available', () => {
      expect(selectQueryXRaySource({ availability: SAMPLER_ONLY, queryState: 'finished' }))
        .toMatchObject({ source: 'processes_history', reason: 'only_available' });
      expect(selectQueryXRaySource({ availability: METRIC_LOG_ONLY, queryState: 'finished' }))
        .toMatchObject({ source: 'query_metric_log', reason: 'only_available' });
    });
  });

  describe('overrides', () => {
    it('honours an explicit processes_history override for a finished query', () => {
      const sel = selectQueryXRaySource({
        availability: BOTH, queryState: 'finished', preference: 'processes_history',
      });
      expect(sel).toMatchObject({ source: 'processes_history', reason: 'override' });
    });

    it('honours an explicit query_metric_log override for a finished query', () => {
      const sel = selectQueryXRaySource({
        availability: BOTH, queryState: 'finished', preference: 'query_metric_log',
      });
      expect(sel).toMatchObject({ source: 'query_metric_log', reason: 'override' });
    });

    it('honours query_metric_log for a running query, disclosing lag and no clamp', () => {
      const sel = selectQueryXRaySource({
        availability: BOTH, queryState: 'running', preference: 'query_metric_log',
      });
      expect(sel).toMatchObject({ source: 'query_metric_log', reason: 'override' });
      expect(sel.note).toMatch(/lag/i);
    });

    it('falls back when the overridden source does not exist', () => {
      const sel = selectQueryXRaySource({
        availability: METRIC_LOG_ONLY, queryState: 'finished', preference: 'processes_history',
      });
      expect(sel).toMatchObject({ source: 'query_metric_log', reason: 'fallback' });
      expect(sel.note).toMatch(/not installed/i);
    });
  });

  describe('nothing available', () => {
    it('returns a null source when neither table exists', () => {
      const sel = selectQueryXRaySource({ availability: NEITHER, queryState: 'finished' });
      expect(sel.source).toBeNull();
      expect(sel.note).toMatch(/setup_sampling/);
    });

    it('still serves a running query from query_metric_log when the sampler is absent', () => {
      const sel = selectQueryXRaySource({ availability: METRIC_LOG_ONLY, queryState: 'running' });
      expect(sel).toMatchObject({ source: 'query_metric_log', reason: 'only_available' });
    });
  });

  describe('declared fidelity', () => {
    it('declares nothing missing for processes_history ', () => {
      const sel = selectQueryXRaySource({ availability: SAMPLER_ONLY, queryState: 'running' });
      expect(sel.missing).toEqual([]);
    });

    it('declares thread and progress fields missing for query_metric_log', () => {
      const sel = selectQueryXRaySource({ availability: METRIC_LOG_ONLY, queryState: 'finished' });
      expect(sel.missing).toContain('thread_count');
      expect(sel.missing).toContain('read_rows');
      expect(sel.lagMs).toBe(SYSTEM_LOG_FLUSH_LAG_MS);
    });

    it('keeps the selector and the SQL builder in agreement about missing fields', () => {
      const sel = selectQueryXRaySource({ availability: METRIC_LOG_ONLY, queryState: 'finished' });
      expect([...sel.missing].sort()).toEqual([...QUERY_METRIC_LOG_MISSING_FIELDS].sort());
    });
  });
});

describe('query_metric_log sample SQL', { tags: ['query-analysis'] }, () => {
  const ID = 'abc-123';

  it('emits the same column contract as the processes_history builder', () => {
    const columnsOf = (sql: string) =>
      Array.from(sql.matchAll(/\bAS ([a-z_]+)(?=,|\n)/g))
        .map(m => m[1])
        .filter(name => name in mapHostProcessSampleRow({}));
    const fromSampler = new Set(columnsOf(buildHostProcessSamplesSQL(ID)));
    const fromMetricLog = new Set(columnsOf(buildHostQueryMetricLogSamplesSQL(ID)));
    for (const column of fromSampler) {
      expect(fromMetricLog.has(column), `missing column ${column}`).toBe(true);
    }
  });


  it('escapes the query id in both the join and the filter', () => {
    const sql = buildQueryMetricLogSamplesSQL(["it's"]);
    expect(sql).not.toContain("= 'it's'");
    expect(sql).toContain("\\'");
  });

  it('bounds the query_log scan by event_date so the join prunes partitions', () => {
    expect(buildQueryMetricLogSamplesSQL([ID])).toMatch(/event_date >= toDate/);
  });

  it('bounds the scan around the known start time instead of a blind lookback', () => {
    const sql = buildQueryMetricLogSamplesSQL([ID], { startedAt: '2026-09-16 10:00:00' });
    expect(sql).toContain("event_date BETWEEN toDate('2026-09-16 10:00:00') - 1");
    expect(sql).not.toMatch(/event_date >= toDate\(now/);
  });

  it('normalizes a browser ISO-8601 start time, which toDate() cannot parse', () => {
    const sql = buildQueryMetricLogSamplesSQL([ID], { startedAt: '2026-09-17T20:51:36.000Z' });
    expect(sql).toContain("toDate('2026-09-17 20:51:36')");
    expect(sql).not.toContain('T20:51:36.000Z');
  });

  it('falls back to the lookback when the start time cannot be parsed', () => {
    const sql = buildQueryMetricLogSamplesSQL([ID], { startedAt: 'not-a-date' });
    expect(sql).toMatch(/event_date >= toDate\(now/);
    expect(sql).not.toContain('not-a-date');
  });



  it('takes the projection literally, so a $-pattern in a query id cannot corrupt the SQL', () => {
    const sql = buildQueryMetricLogSamplesSQL(["x$&y"]);
    expect(sql).not.toContain('{{PROJECTION}}');
    expect(sql).toContain("x$&y");
  });

  it('rejects an empty id list rather than emitting SQL matching undefined', () => {
    expect(() => buildQueryMetricLogSamplesSQL([])).toThrow(/at least one query id/);
  });










});
