/** Overview queries — table sizes, part counts, database sizes. */

const queries: string[] = [
  `-- @meta: title='Biggest Tables' group='Overview' description='Tables ranked by on-disk size with row counts and primary key memory'
-- @chart: type=bar group_by=table value=bytes_size style=2d
SELECT
    database,
    table,
    sum(part_rows) AS rows,
    max(mod_time) AS latest_modification,
    formatReadableSize(sum(part_bytes)) AS data_size,
    formatReadableSize(sum(pk_bytes)) AS primary_keys_size,
    any(part_engine) AS engine,
    sum(part_bytes) AS bytes_size
FROM (
    SELECT
        database,
        table,
        name,
        any(rows) AS part_rows,
        any(bytes) AS part_bytes,
        any(modification_time) AS mod_time,
        any(primary_key_bytes_in_memory) AS pk_bytes,
        any(engine) AS part_engine
    FROM {{cluster_aware:system.parts}}
    WHERE active
    GROUP BY database, table, name
)
GROUP BY database, table
ORDER BY bytes_size DESC
LIMIT 30`,

  `-- @meta: title='Active Parts by Table' group='Overview' description='Number of active parts per table — high counts may indicate merge pressure'
-- @chart: type=bar group_by=table value=part_count style=2d
SELECT
    concat(database, '.', table) AS table,
    count() AS part_count,
    sum(part_rows) AS total_rows,
    formatReadableSize(sum(part_disk)) AS disk_size
FROM (
    SELECT database, table, name, any(rows) AS part_rows, any(bytes_on_disk) AS part_disk
    FROM {{cluster_aware:system.parts}}
    WHERE active AND database NOT IN ('system','INFORMATION_SCHEMA','information_schema')
    GROUP BY database, table, name
)
GROUP BY database, table
ORDER BY part_count DESC
LIMIT 25`,

  `-- @meta: title='Database Sizes' group='Overview' description='Chart shows deduplicated data size. Table includes replicated size (total across all replicas) and replication factor.'
-- @chart: type=pie group_by=database value=size_bytes style=3d unit=bytes
-- @drill: on=database into='Table Sizes'
SELECT
    database,
    formatReadableSize(sum(logical_disk)) AS size,
    sum(logical_disk) AS size_bytes,
    formatReadableSize(sum(physical_disk)) AS replicated_size,
    sum(physical_disk) AS replicated_bytes,
    round(sum(physical_disk) / greatest(sum(logical_disk), 1), 2) AS replication_factor
FROM (
    SELECT database, table, name,
        any(bytes_on_disk) AS logical_disk,
        sum(bytes_on_disk) AS physical_disk
    FROM {{cluster_aware:system.parts}}
    WHERE active
    GROUP BY database, table, name
)
GROUP BY database
ORDER BY size_bytes DESC`,

  `-- @meta: title='Table Sizes' group='Overview' description='Chart shows deduplicated data size. Table includes replicated size (total across all replicas) and replication factor.'
-- @chart: type=pie group_by=table value=size_bytes style=3d unit=bytes
-- @drill: on=table into='Part Sizes'
SELECT
    table,
    formatReadableSize(sum(logical_disk)) AS size,
    sum(logical_disk) AS size_bytes,
    formatReadableSize(sum(physical_disk)) AS replicated_size,
    sum(physical_disk) AS replicated_bytes,
    round(sum(physical_disk) / greatest(sum(logical_disk), 1), 2) AS replication_factor,
    count() AS parts
FROM (
    SELECT database, table, name,
        any(bytes_on_disk) AS logical_disk,
        sum(bytes_on_disk) AS physical_disk
    FROM {{cluster_aware:system.parts}}
    WHERE active AND {{drill:database | 1=1}}
    GROUP BY database, table, name
)
GROUP BY table
ORDER BY size_bytes DESC
LIMIT 50`,

  `-- @meta: title='Part Sizes' group='Overview' description='Chart shows deduplicated part size. Table includes replicated size (total across all replicas) and replica count.'
-- @chart: type=bar group_by=name value=size_bytes style=2d unit=bytes
-- @part_link: on=name database=database table=table
SELECT
    database,
    table,
    name,
    formatReadableSize(any(bytes_on_disk)) AS size,
    any(bytes_on_disk) AS size_bytes,
    formatReadableSize(sum(bytes_on_disk)) AS replicated_size,
    sum(bytes_on_disk) AS replicated_bytes,
    count() AS replicas
FROM {{cluster_aware:system.parts}}
WHERE active AND {{drill:database | 1=1}} AND {{drill:table | 1=1}}
GROUP BY database, table, name
ORDER BY size_bytes DESC
LIMIT 100`,
  `-- @meta: title='Table Health' group='Overview' description='Per-table part count, disk usage gauge, and part-size distribution sparkline'
-- @cell: column=disk_pct type=gauge max=100 unit=%
-- @cell: column=disk_pct type=rag green<30 amber<60
-- @cell: column=part_sizes type=sparkline color=#6366f1 fill=true
SELECT
    database,
    table,
    count() AS parts,
    formatReadableSize(sum(bytes_on_disk)) AS disk_size,
    sum(rows) AS total_rows,
    round(sum(bytes_on_disk) * 100.0 / greatest((SELECT sum(bytes_on_disk) FROM system.parts WHERE active), 1), 1) AS disk_pct,
    arrayMap(x -> toUInt32(x), groupArray(bytes_on_disk)) AS part_sizes
FROM system.parts
WHERE active AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC
LIMIT 20`,
];

export default queries;
