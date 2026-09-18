/** Merge zoom sampling remains independent of Query X-Ray source selection. */

/**
 * Fetch raw merge samples for all merges active in a time window.
 *
 * merges_history has memory and I/O but no CPU ProfileEvents.
 */
export function buildZoomMergeSamplesSQL(hostname?: string | readonly string[]): string {
  const hostFilter = buildZoomHostFilter(hostname);
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

function buildZoomHostFilter(hostname?: string | readonly string[]): string {
  const hosts = (Array.isArray(hostname) ? hostname : hostname ? [hostname] : [])
    .map(host => host.replace(/[^a-zA-Z0-9._\-]/g, ''))
    .filter((host, index, values) => host.length > 0 && values.indexOf(host) === index);
  if (hosts.length === 0) return '';
  if (hosts.length === 1) return `AND hostName() = '${hosts[0]}'`;
  return `AND hostName() IN (${hosts.map(host => `'${host}'`).join(', ')})`;
}
