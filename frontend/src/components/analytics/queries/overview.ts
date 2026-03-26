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

  `-- @meta: title='Database Sizes' group='Overview' description='Total disk usage per database'
-- @chart: type=pie group_by=database value=total_bytes style=3d
-- @drill: on=database into='Table Sizes'
SELECT
    database,
    formatReadableSize(sum(part_disk)) AS size,
    sum(part_disk) AS total_bytes,
    count() AS tables
FROM (
    SELECT database, table, name, any(bytes_on_disk) AS part_disk
    FROM {{cluster_aware:system.parts}}
    WHERE active
    GROUP BY database, table, name
)
GROUP BY database
ORDER BY total_bytes DESC`,

  `-- @meta: title='Table Sizes' group='Overview' description='Disk usage per table (drill from Database Sizes or view all)'
-- @chart: type=pie group_by=table value=total_bytes style=3d
-- @drill: on=table into='Part Sizes'
SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) AS size,
    sum(bytes_on_disk) AS total_bytes,
    count() AS parts
FROM {{cluster_aware:system.parts}}
WHERE active AND {{drill:database | 1=1}}
GROUP BY table
ORDER BY total_bytes DESC
LIMIT 50`,

  `-- @meta: title='Part Sizes' group='Overview' description='Disk usage per part (drill from Table Sizes or view all)'
-- @chart: type=bar group_by=name value=bytes_on_disk style=2d
SELECT
    name,
    formatReadableSize(bytes_on_disk) AS size,
    bytes_on_disk
FROM {{cluster_aware:system.parts}}
WHERE active AND {{drill:database | 1=1}} AND {{drill:table | 1=1}}
ORDER BY bytes_on_disk DESC
LIMIT 100`,
];

export default queries;
