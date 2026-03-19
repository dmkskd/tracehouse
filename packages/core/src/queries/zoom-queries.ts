/**
 * SQL queries for zoom-mode timeline data.
 *
 * Fetches per-second process samples from tracehouse.processes_history
 * for a given time window. Unlike buildProcessSamplesSQL (which queries
 * by query_id), this fetches ALL active queries in the window.
 *
 * The service layer computes per-second deltas from cumulative counters.
 */

import { APP_SOURCE_LIKE } from './source-tags.js';

/**
 * Fetch raw process samples for all queries active in a time window.
 *
 * Returns cumulative counters per sample — the service computes deltas.
 * Filtered to exclude TraceHouse's own queries via source tag.
 *
 * @param hostname - Optional hostname filter (sanitized in service layer).
 */
export function buildZoomProcessSamplesSQL(hostname?: string): string {
  const hostFilter = hostname ? `AND hostName() = '${hostname.replace(/[^a-zA-Z0-9._\-]/g, '')}'` : '';
  return `
SELECT
    initial_query_id AS query_id,
    toUnixTimestamp64Milli(sample_time) AS ts_ms,
    memory_usage,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS pe_cpu,
    ProfileEvents['NetworkSendBytes'] AS pe_net_send,
    ProfileEvents['NetworkReceiveBytes'] AS pe_net_recv,
    read_bytes,
    written_bytes
FROM {{cluster_aware:tracehouse.processes_history}}
WHERE sample_time >= {start_time}
  AND sample_time <= {end_time}
  AND query NOT LIKE ${APP_SOURCE_LIKE}
  ${hostFilter}
ORDER BY initial_query_id, sample_time
`;
}

/**
 * Fetch raw merge samples for all merges active in a time window.
 *
 * merges_history has memory and I/O but no CPU ProfileEvents.
 */
export function buildZoomMergeSamplesSQL(hostname?: string): string {
  const hostFilter = hostname ? `AND hostName() = '${hostname.replace(/[^a-zA-Z0-9._\-]/g, '')}'` : '';
  return `
SELECT
    result_part_name AS part_name,
    is_mutation,
    toUnixTimestamp64Milli(sample_time) AS ts_ms,
    memory_usage,
    bytes_read_uncompressed,
    bytes_written_uncompressed
FROM {{cluster_aware:tracehouse.merges_history}}
WHERE sample_time >= {start_time}
  AND sample_time <= {end_time}
  ${hostFilter}
ORDER BY result_part_name, sample_time
`;
}
