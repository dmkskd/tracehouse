import { describe, expect, it } from 'vitest';
import { buildSelectedXRayOverlaySQL } from '../xray-overlay-queries.js';

describe('X-Ray overlay ordering contract', () => {
  it.each(['processes_history', 'query_metric_log'] as const)('%s orders the final projected rows', preference => {
    const sql = buildSelectedXRayOverlaySQL({ availability: { processesHistory: true, queryMetricLog: true }, preference }, 'cpu_cores', {
      start: "toDateTime64('2026-11-08 17:00:00', 6, 'UTC')",
      end: "toDateTime64('2026-11-08 17:00:03', 6, 'UTC')", queryIdsSQL: "SELECT 'root'",
    });
    expect(sql.trim()).toMatch(/\)\s+ORDER BY t, query_id$/);
  });
});
