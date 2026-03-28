/**
 * Disk & storage monitoring queries — free space, disk health.
 *
 * Sources:
 * - ClickHouse docs: https://clickhouse.com/docs/operations/system-tables/disks
 */

const queries: string[] = [
  `-- @meta: title='Disk Free Space' group='Disks' description='Free space, total capacity, and usage percentage per disk across all cluster nodes — early warning for running out of storage'
-- @rag: column=used_pct green<70 amber<85
-- @source: https://clickhouse.com/docs/operations/system-tables/disks
SELECT
    hostName() AS host,
    name,
    path,
    type,
    formatReadableSize(free_space) AS free,
    formatReadableSize(total_space) AS total,
    formatReadableSize(total_space - free_space) AS used,
    round((total_space - free_space) / total_space * 100, 1) AS used_pct,
    formatReadableSize(unreserved_space) AS unreserved,
    formatReadableSize(keep_free_space) AS keep_free
FROM {{cluster_aware:system.disks}}
ORDER BY used_pct DESC`,
];

export default queries;
