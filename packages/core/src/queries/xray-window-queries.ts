import { escapeValue } from './builder.js';
import { APP_SOURCE_LIKE } from './source-tags.js';
import { selectQueryXRaySource, type SelectQueryXRaySourceInput } from '../types/xray-source.js';

export interface XRayWindowOptions {
  /** Trusted SQL expressions supplied by the query layer, not user input. */
  start: string;
  end: string;
  /** Optional trusted SELECT yielding root query ids (e.g. a ranked CTE). */
  queryIdsSQL?: string;
  /** Earliest query start, including queries that started before the window. */
  identityStart?: string;
  hostname?: string | readonly string[];
}

const counters = {
  cpu_us: 'OSCPUVirtualTimeMicroseconds',
  cpu_wait_us: 'OSCPUWaitMicroseconds',
  io_wait_us: 'OSIOWaitMicroseconds',
  net_wait_us: 'NetworkReceiveElapsedMicroseconds',
  net_send_bytes: 'NetworkSendBytes',
  net_recv_bytes: 'NetworkReceiveBytes',
} as const;

/**
 * One normalized interval per local query and host. Windowing happens BEFORE
 * clipping to the viewport so the first visible sample retains its predecessor.
 * query_metric_log counters are already deltas and must never be differenced.
 */
export function buildXRayWindowSamplesSQL(selection: SelectQueryXRaySourceInput, opts: XRayWindowOptions): string {
  const meta = selectQueryXRaySource(selection);
  if (!meta.source) throw new Error(meta.note ?? 'No Query X-Ray source available');
  const metricLog = meta.source === 'query_metric_log';
  const hosts = typeof opts.hostname === 'string' ? [opts.hostname] : opts.hostname ?? [];
  const hostFilter = hosts.length ? `AND hostName() IN (${hosts.map(host => `'${escapeValue(host)}'`).join(', ')})` : '';
  const membership = opts.queryIdsSQL ? `AND query_id IN (${opts.queryIdsSQL})` : '';
  const start = opts.identityStart ?? opts.start;
  const identity = `SELECT
      hostName() AS hostname,
      l.query_id AS sample_query_id,
      if(empty(any(l.initial_query_id)), sample_query_id, any(l.initial_query_id)) AS query_id,
      min(l.query_start_time_microseconds) AS query_start
    FROM {{cluster_aware:system.query_log}} AS l
    WHERE event_date >= toDate(${start}) - 1
      AND event_date <= toDate(${opts.end}) + 1
      AND query NOT LIKE ${APP_SOURCE_LIKE}
      ${opts.queryIdsSQL ? `AND (l.query_id IN (${opts.queryIdsSQL}) OR l.initial_query_id IN (${opts.queryIdsSQL}))` : ''}
      ${hostFilter}
    GROUP BY hostname, sample_query_id`;
  const raw = metricLog ? `SELECT
      identity.query_id AS query_id, m.query_id AS sample_query_id,
      m.hostname AS hostname, m.event_time_microseconds AS sample_time,
      identity.query_start AS query_start, m.memory_usage AS memory_usage,
      ${Object.entries(counters).map(([alias, event]) => `m.ProfileEvent_${event} AS ${alias}`).join(',\n      ')}
    FROM (
      SELECT *, hostName() AS hostname
      FROM {{cluster_aware:system.query_metric_log}}
      WHERE event_date >= toDate(${start}) - 1
        AND event_date <= toDate(${opts.end})
        AND event_time_microseconds <= ${opts.end}
        ${hostFilter}
    ) AS m
    INNER JOIN (${identity}) AS identity
      ON m.hostname = identity.hostname AND m.query_id = identity.sample_query_id
      AND m.event_time_microseconds >= identity.query_start` : `SELECT
      if(empty(p.initial_query_id), p.query_id, p.initial_query_id) AS query_id,
      p.query_id AS sample_query_id, hostName() AS hostname,
      sample_time, sample_time AS query_start, memory_usage,
      ${Object.entries(counters).map(([alias, event]) => `ProfileEvents['${event}'] AS ${alias}`).join(',\n      ')},
      read_bytes, written_bytes
    FROM {{cluster_aware:tracehouse.processes_history}} AS p
    WHERE sample_time >= ${opts.start} - INTERVAL 1 MINUTE
      AND sample_time <= ${opts.end}
      AND query NOT LIKE ${APP_SOURCE_LIKE}
      ${hostFilter}`;
  const delta = (name: string) => metricLog ? name : `greatest(toFloat64(${name}) - toFloat64(lagInFrame(${name}, 1, ${name}) OVER w), 0)`;
  return `SELECT * FROM (
  SELECT query_id, sample_query_id, hostname, sample_time AS event_time, toUnixTimestamp64Milli(sample_time) AS ts_ms, memory_usage,
    greatest(dateDiff('microsecond', lagInFrame(sample_time, 1, query_start) OVER w, sample_time) / 1e6, 0.01) AS dt,
    ${Object.keys(counters).map(name => `${delta(name)} AS ${name}`).join(',\n    ')}${metricLog ? '' : `,\n    ${delta('read_bytes')} AS read_bytes,\n    ${delta('written_bytes')} AS written_bytes`}
  FROM (${raw})
  WINDOW w AS (PARTITION BY hostname, sample_query_id ORDER BY sample_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
WHERE event_time > ${opts.start} AND event_time <= ${opts.end}
  ${membership}
ORDER BY query_id, hostname, sample_query_id, event_time`;
}
