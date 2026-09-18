import { selectQueryXRaySource, type SelectQueryXRaySourceInput } from '../types/xray-source.js';
import { buildXRayWindowSamplesSQL, type XRayWindowOptions } from './xray-window-queries.js';
/** Per-second resource overlays for the selected Query X-Ray source. */
export type XRayOverlayMetric = 'cpu_cores' | 'read_mb_s' | 'io_wait_s' | 'cpu_wait_s' | 'net_wait_s' | 'mem_mb';

/** Source-aware overlays: average local intervals, then sum local executions. */
export function buildSelectedXRayOverlaySQL(
  selection: SelectQueryXRaySourceInput,
  metric: XRayOverlayMetric,
  opts: XRayWindowOptions,
): string {
  const meta = selectQueryXRaySource(selection);
  if (metric === 'read_mb_s' && meta.source === 'query_metric_log') {
    throw new Error('Sampled read_bytes is unavailable in system.query_metric_log. Select processes_history for progress-byte throughput.');
  }
  const columns = { cpu_cores: 'cpu_us', cpu_wait_s: 'cpu_wait_us', io_wait_s: 'io_wait_us', net_wait_s: 'net_wait_us', read_mb_s: 'read_bytes' };
  const normalized = buildXRayWindowSamplesSQL(selection, opts);
  const value = metric === 'mem_mb'
    ? 'avg(memory_usage) / 1048576'
    : `sum(${columns[metric]}) / sum(dt) / ${metric === 'read_mb_s' ? '1048576' : '1e6'}`;
  return `SELECT t, query_id, round(sum(value), ${metric === 'cpu_cores' ? 2 : metric === 'mem_mb' || metric === 'read_mb_s' ? 1 : 3}) AS ${metric}
FROM (
  SELECT toStartOfInterval(event_time, INTERVAL 1 SECOND) AS t,
    query_id, hostname, sample_query_id, ${value} AS value
  FROM (${normalized})
  GROUP BY t, query_id, hostname, sample_query_id
)
GROUP BY t, query_id
ORDER BY t, query_id`;
}
