import { selectQueryXRaySource, type SelectQueryXRaySourceInput } from '../types/xray-source.js';
import { buildProcessSamplesSQL, buildHostProcessSamplesSQL } from './process-queries.js';
import { buildQueryMetricLogSamplesSQL, buildHostQueryMetricLogSamplesSQL, type QueryMetricLogSampleOptions } from './query-metric-log-queries.js';

/** Selection is part of the API: consumers cannot hardcode a concrete source. */
export function buildXRaySamplesSQL(
  selection: SelectQueryXRaySourceInput,
  ids: string[],
  opts: QueryMetricLogSampleOptions & { perHost?: boolean } = {},
): string {
  if (ids.length === 0) throw new Error('X-Ray requires at least one query id');
  const meta = selectQueryXRaySource(selection);
  if (!meta.source) throw new Error(meta.note ?? 'No Query X-Ray source available');
  if (opts.perHost) {
    if (ids.length !== 1) throw new Error('Per-host X-Ray requires exactly one query id');
    return meta.source === 'query_metric_log'
      ? buildHostQueryMetricLogSamplesSQL(ids[0], opts)
      : buildHostProcessSamplesSQL(ids[0]);
  }
  return meta.source === 'query_metric_log'
    ? buildQueryMetricLogSamplesSQL(ids, opts)
    : buildProcessSamplesSQL(ids);
}
