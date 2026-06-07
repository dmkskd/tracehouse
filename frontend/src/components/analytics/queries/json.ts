/** Native JSON monitoring queries — subcolumn/path pressure, insert cost, merge cost. */

const queries: string[] = [
  `-- @meta: title='JSON Columns Inventory' group='JSON' description='Native JSON columns, configured path limits, and current storage footprint'
-- @cell: column=json_bytes type=gauge max=max_json_bytes
-- @cell: column=path_limit_pct type=gauge max=100 unit=%
-- @cell: column=path_limit_pct type=rag green<60 amber<85
SELECT
    *,
    max(json_bytes) OVER () AS max_json_bytes
FROM (
    SELECT
        concat(c.database, '.', c.table, '.', c.name) AS json_column,
        concat(c.database, '.', c.table) AS table_name,
        c.name AS column_name,
        c.type,
        if(extract(c.type, 'max_dynamic_paths=([0-9]+)') = '', 1024, toUInt64(extract(c.type, 'max_dynamic_paths=([0-9]+)'))) AS max_dynamic_paths,
        if(extract(c.type, 'max_dynamic_types=([0-9]+)') = '', 32, toUInt64(extract(c.type, 'max_dynamic_types=([0-9]+)'))) AS max_dynamic_types,
        countDistinct(pc.name) AS active_parts,
        countDistinct(subcolumn_name) AS materialized_subcolumns,
        round(materialized_subcolumns * 100 / nullIf(max_dynamic_paths, 0), 1) AS path_limit_pct,
        sum(pc.rows) AS rows_in_parts,
        formatReadableSize(sum(pc.column_bytes_on_disk)) AS json_size,
        sum(pc.column_bytes_on_disk) AS json_bytes
    FROM {{cluster_aware:system.columns}} AS c
    LEFT JOIN {{cluster_aware:system.parts_columns}} AS pc
        ON pc.database = c.database
       AND pc.table = c.table
       AND pc.column = c.name
       AND pc.active
    ARRAY JOIN pc.subcolumns.names AS subcolumn_name
    WHERE c.type LIKE 'JSON%'
    GROUP BY c.database, c.table, c.name, c.type
)
ORDER BY json_bytes DESC`,

  `-- @meta: title='JSON/String Storage Candidates' group='JSON' description='Native JSON columns plus String columns that look JSON-like by name; use as comparison candidates, not proof of identical payloads'
-- @cell: column=bytes_on_disk type=gauge max=max_bytes_on_disk
SELECT
    *,
    max(bytes_on_disk) OVER () AS max_bytes_on_disk
FROM (
    SELECT
        concat(database, '.', table) AS table_name,
        column AS column_name,
        multiIf(type LIKE 'JSON%', 'Native JSON', type = 'String', 'String', type) AS storage_type,
        multiIf(
            type LIKE 'JSON%', 'declared native JSON column',
            type = 'String' AND positionCaseInsensitive(column, 'json') > 0, 'String column name contains json',
            type = 'String' AND positionCaseInsensitive(column, 'payload') > 0, 'String column name contains payload',
            'included by fallback rule'
        ) AS inclusion_reason,
        if(
            type LIKE 'JSON%',
            'Native JSON storage. Comparable to String only when the ingest path writes equivalent payloads.',
            'Raw String storage candidate. May not contain the same payload as JSON columns unless schema/data generation makes that explicit.'
        ) AS comparison_note,
        type,
        countDistinct(name) AS active_parts,
        sum(rows) AS rows_in_parts,
        formatReadableSize(sum(column_bytes_on_disk)) AS size,
        sum(column_bytes_on_disk) AS bytes_on_disk,
        formatReadableSize(sum(column_data_uncompressed_bytes)) AS uncompressed_size,
        round(sum(column_data_uncompressed_bytes) / nullIf(sum(column_data_compressed_bytes), 0), 2) AS compression_ratio
    FROM {{cluster_aware:system.parts_columns}}
    WHERE active
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
      AND (
          type LIKE 'JSON%'
          OR (type = 'String' AND (positionCaseInsensitive(column, 'json') > 0 OR positionCaseInsensitive(column, 'payload') > 0))
      )
    GROUP BY database, table, column, type
)
ORDER BY bytes_on_disk DESC`,

  `-- @meta: title='JSON Subcolumn Pressure' group='JSON' description='Materialized JSON subcolumns versus configured max_dynamic_paths'
-- @cell: column=path_limit_pct type=gauge max=100 unit=%
-- @cell: column=path_limit_pct type=rag green<60 amber<85
SELECT
    concat(c.database, '.', c.table, '.', c.name) AS json_column,
    if(extract(c.type, 'max_dynamic_paths=([0-9]+)') = '', 1024, toUInt64(extract(c.type, 'max_dynamic_paths=([0-9]+)'))) AS max_dynamic_paths,
    countDistinct(subcolumn_name) AS materialized_subcolumns,
    round(materialized_subcolumns * 100 / nullIf(max_dynamic_paths, 0), 1) AS path_limit_pct
FROM {{cluster_aware:system.columns}} AS c
LEFT JOIN {{cluster_aware:system.parts_columns}} AS pc
    ON pc.database = c.database
   AND pc.table = c.table
   AND pc.column = c.name
   AND pc.active
ARRAY JOIN pc.subcolumns.names AS subcolumn_name
WHERE c.type LIKE 'JSON%'
GROUP BY c.database, c.table, c.name, c.type
ORDER BY path_limit_pct DESC, materialized_subcolumns DESC`,

  `-- @meta: title='JSON Subcolumn Storage' group='JSON' description='Which JSON paths are taking space on disk'
-- @cell: column=bytes_on_disk type=gauge max=max_bytes_on_disk
SELECT
    *,
    max(bytes_on_disk) OVER () AS max_bytes_on_disk
FROM (
    SELECT
        concat(database, '.', table, '.', column) AS json_column,
        subcolumn_name AS subcolumn,
        any(subcolumn_type) AS subcolumn_type,
        formatReadableSize(sum(subcolumn_bytes)) AS size,
        sum(subcolumn_bytes) AS bytes_on_disk,
        formatReadableSize(sum(subcolumn_uncompressed_bytes)) AS uncompressed_size,
        round(sum(subcolumn_uncompressed_bytes) / nullIf(sum(subcolumn_bytes), 0), 2) AS compression_ratio
    FROM {{cluster_aware:system.parts_columns}}
    ARRAY JOIN
        subcolumns.names AS subcolumn_name,
        subcolumns.types AS subcolumn_type,
        subcolumns.bytes_on_disk AS subcolumn_bytes,
        subcolumns.data_uncompressed_bytes AS subcolumn_uncompressed_bytes
    WHERE active
      AND type LIKE 'JSON%'
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
    GROUP BY database, table, column, subcolumn_name
)
ORDER BY bytes_on_disk DESC
LIMIT 50`,

  `-- @meta: title='JSON Column Storage by Part' group='JSON' description='JSON column bytes and active part spread for the selected table'
-- @cell: column=json_bytes type=gauge max=max_json_bytes
SELECT
    *,
    max(json_bytes) OVER () AS max_json_bytes
FROM (
    SELECT
        partition,
        countDistinct(name) AS part_count,
        sum(rows) AS rows,
        formatReadableSize(sum(column_bytes_on_disk)) AS json_size,
        sum(column_bytes_on_disk) AS json_bytes,
        formatReadableSize(sum(column_marks_bytes)) AS marks_size,
        sum(column_marks_bytes) AS marks_bytes
    FROM {{cluster_aware:system.parts_columns}}
    WHERE active
      AND type LIKE 'JSON%'
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
    GROUP BY partition
)
ORDER BY json_bytes DESC
LIMIT 50`,

  `-- @meta: title='JSON Insert Throughput' group='JSON' interval='1 HOUR' description='Rows/sec and duration for INSERTs targeting tables with JSON columns'
-- @chart: type=grouped_line group_by=minute value=rows_per_sec,avg_duration_ms style=2d
WITH json_tables AS (
    SELECT database, table
    FROM {{cluster_aware:system.columns}}
    WHERE type LIKE 'JSON%'
    GROUP BY database, table
)
SELECT
    toStartOfMinute(q.event_time) AS minute,
    concat(j.database, '.', j.table) AS table_name,
    sum(q.written_rows) AS rows_written,
    round(sum(q.written_rows) / nullIf(sum(q.query_duration_ms) / 1000, 0)) AS rows_per_sec,
    round(avg(q.query_duration_ms), 1) AS avg_duration_ms,
    count() AS batches
FROM {{cluster_aware:system.query_log}} AS q
INNER JOIN json_tables AS j
    ON has(q.databases, j.database)
   AND (has(q.tables, concat(j.database, '.', j.table)) OR has(q.tables, j.table))
WHERE q.event_time > {{time_range}}
  AND q.type = 'QueryFinish'
  AND q.query_kind = 'Insert'
  AND concat(j.database, '.', j.table) = {{drill_value:tbl | concat(j.database, '.', j.table)}}
GROUP BY minute, table_name
ORDER BY minute ASC`,

  `-- @meta: title='JSON Insert CPU & Memory' group='JSON' interval='1 HOUR' description='Completed JSON table inserts ranked by CPU, memory, and throughput'
WITH json_tables AS (
    SELECT database, table
    FROM {{cluster_aware:system.columns}}
    WHERE type LIKE 'JSON%'
    GROUP BY database, table
)
SELECT
    q.event_time,
    concat(j.database, '.', j.table) AS table_name,
    q.query_duration_ms,
    q.written_rows,
    round(q.written_rows / nullIf(q.query_duration_ms / 1000, 0)) AS rows_per_sec,
    round(q.ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 3) AS cpu_s,
    round(q.ProfileEvents['OSIOWaitMicroseconds'] / 1e6, 3) AS io_wait_s,
    formatReadableSize(q.memory_usage) AS memory,
    formatReadableSize(q.written_bytes) AS written_bytes,
    substring(replaceRegexpAll(q.query, '\\n', ' '), 1, 140) AS query_preview
FROM {{cluster_aware:system.query_log}} AS q
INNER JOIN json_tables AS j
    ON has(q.databases, j.database)
   AND (has(q.tables, concat(j.database, '.', j.table)) OR has(q.tables, j.table))
WHERE q.event_time > {{time_range}}
  AND q.type = 'QueryFinish'
  AND q.query_kind = 'Insert'
  AND concat(j.database, '.', j.table) = {{drill_value:tbl | concat(j.database, '.', j.table)}}
ORDER BY q.event_time DESC
LIMIT 50`,

  `-- @meta: title='JSON Merge Duration per M rows' group='JSON' interval='1 HOUR' description='Merge wall-clock duration normalized per million merged rows for native JSON tables'
-- @chart: type=line group_by=minute value=duration_ms_per_m_rows style=2d
WITH json_tables AS (
    SELECT database, table
    FROM {{cluster_aware:system.columns}}
    WHERE type LIKE 'JSON%'
    GROUP BY database, table
)
SELECT
    toStartOfMinute(p.event_time) AS minute,
    concat(p.database, '.', p.table) AS table_name,
    sum(p.rows) AS rows_merged,
    round(sum(p.duration_ms) / nullIf(sum(p.rows), 0) * 1000000, 2) AS duration_ms_per_m_rows
FROM {{cluster_aware:system.part_log}} AS p
INNER JOIN json_tables AS j
    ON p.database = j.database
   AND p.table = j.table
WHERE p.event_time > {{time_range}}
  AND p.event_type = 'MergeParts'
  AND concat(p.database, '.', p.table) = {{drill_value:tbl | concat(p.database, '.', p.table)}}
GROUP BY minute, table_name
ORDER BY minute ASC`,

  `-- @meta: title='JSON Merge Cost Breakdown' group='JSON' interval='1 HOUR' description='Completed merges for native JSON tables with wall time, CPU time, and size-normalized cost'
-- @cell: column=rows_merged type=gauge max=max_rows_merged
-- @cell: column=read_bytes type=gauge max=max_read_bytes
-- @cell: column=cpu_wall_ratio type=gauge max=max_cpu_wall_ratio
SELECT
    *,
    max(rows_merged) OVER () AS max_rows_merged,
    max(read_bytes) OVER () AS max_read_bytes,
    max(cpu_wall_ratio) OVER () AS max_cpu_wall_ratio
FROM (
    WITH json_tables AS (
        SELECT database, table
        FROM {{cluster_aware:system.columns}}
        WHERE type LIKE 'JSON%'
        GROUP BY database, table
    )
    SELECT
        toStartOfMinute(p.event_time) AS minute,
        concat(p.database, '.', p.table) AS table_name,
        count() AS merges,
        sum(p.rows) AS rows_merged,
        formatReadableSize(if(sum(p.read_bytes) > 0, sum(p.read_bytes), sum(p.size_in_bytes))) AS read_bytes_readable,
        if(sum(p.read_bytes) > 0, sum(p.read_bytes), sum(p.size_in_bytes)) AS read_bytes,
        sum(p.duration_ms) AS merge_duration_ms,
        round(sum(p.duration_ms) / 1000, 3) AS wall_s,
        round(sum(p.ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 3) AS cpu_s,
        round((sum(p.ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6) / nullIf(sum(p.duration_ms) / 1000, 0), 3) AS cpu_wall_ratio,
        round(sum(p.duration_ms) / nullIf(sum(p.rows), 0) * 1000000, 2) AS duration_ms_per_m_rows,
        round(sum(p.duration_ms) / nullIf(if(sum(p.read_bytes) > 0, sum(p.read_bytes), sum(p.size_in_bytes)) / 1073741824, 0), 2) AS duration_ms_per_gib,
        round(sum(p.ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6 / nullIf(sum(p.rows), 0) * 1000000, 3) AS cpu_s_per_m_rows,
        merge_algorithm,
        any(part_type) AS result_part_type
    FROM {{cluster_aware:system.part_log}} AS p
    INNER JOIN json_tables AS j
        ON p.database = j.database
       AND p.table = j.table
    WHERE p.event_time > {{time_range}}
      AND p.event_type = 'MergeParts'
      AND concat(p.database, '.', p.table) = {{drill_value:tbl | concat(p.database, '.', p.table)}}
    GROUP BY minute, table_name, merge_algorithm
)
ORDER BY minute DESC`,

  `-- @meta: title='Active JSON Inserts' group='JSON' description='Currently running INSERTs into tables that contain native JSON columns'
WITH json_tables AS (
    SELECT database, table
    FROM {{cluster_aware:system.columns}}
    WHERE type LIKE 'JSON%'
    GROUP BY database, table
)
SELECT
    p.elapsed,
    concat(j.database, '.', j.table) AS table_name,
    p.written_rows,
    round(p.written_rows / greatest(p.elapsed, 0.001)) AS rows_per_sec,
    formatReadableSize(p.written_bytes) AS written_bytes,
    formatReadableSize(p.memory_usage) AS memory,
    substring(replaceRegexpAll(p.query, '\\n', ' '), 1, 180) AS query_preview
FROM {{cluster_aware:system.processes}} AS p
INNER JOIN json_tables AS j
    ON positionCaseInsensitive(p.query, concat('INSERT INTO ', j.database, '.', j.table)) > 0
    OR positionCaseInsensitive(p.query, concat('INSERT INTO ', j.table)) > 0
WHERE p.query_kind = 'Insert'
  AND concat(j.database, '.', j.table) = {{drill_value:tbl | concat(j.database, '.', j.table)}}
ORDER BY p.elapsed DESC`,
];

export default queries;
