/** Merge analytics queries — throughput, duration scaling, part wait time, estimated merge cost. */

const queries: string[] = [
  `-- @meta: title='Merge Throughput by Table' group='Merge Analytics' interval='1 DAY' description='Average merge throughput (MB/s) per table — low values indicate I/O-bound or structurally expensive merges. Excludes mutation-triggered re-merges.'
-- @chart: type=bar group_by=tbl value=avg_mb_per_sec unit=MB/s style=2d
-- @drill: on=tbl into='Merge Throughput by Size Bucket'
SELECT
    concat(database, '.', table) AS tbl,
    count() AS merge_count,
    round(avg(duration_ms / 1000.0), 1) AS avg_duration_sec,
    formatReadableSize(avg(size_in_bytes)) AS avg_result_size,
    round(avg(size_in_bytes / 1e6 / greatest(duration_ms / 1000.0, 0.001)), 1) AS avg_mb_per_sec,
    round(avg(rows / greatest(duration_ms / 1000.0, 0.001)), 0) AS avg_rows_per_sec
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'MergeParts'
  AND merge_reason != 'NotAMerge'
  AND duration_ms > 0
  AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
  AND event_time > {{time_range}}
  AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
GROUP BY database, table
ORDER BY avg_mb_per_sec ASC
LIMIT 30`,

  `-- @meta: title='Merge Throughput by Size Bucket' group='Merge Analytics' interval='1 DAY' description='Average merge throughput (MB/s) by result part size bucket — shows whether larger merges are slower or faster per byte. Excludes mutation-triggered re-merges.'
-- @chart: type=bar group_by=size_bucket value=avg_mb_per_sec unit=MB/s style=2d
SELECT
    multiIf(
        size_in_bytes < 1048576,           '< 1 MB',
        size_in_bytes < 10485760,          '1-10 MB',
        size_in_bytes < 104857600,         '10-100 MB',
        size_in_bytes < 1073741824,        '100 MB-1 GB',
        size_in_bytes < 5368709120,        '1-5 GB',
        size_in_bytes < 10737418240,       '5-10 GB',
        size_in_bytes < 21474836480,       '10-20 GB',
        size_in_bytes < 53687091200,       '20-50 GB',
        size_in_bytes < 107374182400,      '50-100 GB',
        size_in_bytes < 161061273600,      '100-150 GB',
        '> 150 GB'
    ) AS size_bucket,
    min(size_in_bytes) AS _sort,
    count() AS merge_count,
    round(avg(size_in_bytes / 1e6 / greatest(duration_ms / 1000.0, 0.001)), 1) AS avg_mb_per_sec,
    round(quantile(0.95)(size_in_bytes / 1e6 / greatest(duration_ms / 1000.0, 0.001)), 1) AS p95_mb_per_sec,
    formatReadableSize(avg(size_in_bytes)) AS avg_result_size
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'MergeParts'
  AND merge_reason != 'NotAMerge'
  AND duration_ms > 0
  AND event_time > {{time_range}}
  AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
GROUP BY size_bucket
ORDER BY _sort ASC`,

  `-- @meta: title='Merge Duration by Table' group='Merge Analytics' interval='1 DAY' description='Average merge duration per table — identifies tables with the slowest merges. Excludes mutation-triggered re-merges.'
-- @chart: type=bar group_by=tbl value=avg_duration_sec unit=sec style=2d
-- @drill: on=tbl into='Merge Duration by Size Bucket'
SELECT
    concat(database, '.', table) AS tbl,
    count() AS merge_count,
    round(avg(duration_ms / 1000.0), 1) AS avg_duration_sec,
    round(quantile(0.95)(duration_ms / 1000.0), 1) AS p95_duration_sec,
    formatReadableSize(avg(size_in_bytes)) AS avg_result_size
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'MergeParts'
  AND merge_reason != 'NotAMerge'
  AND duration_ms > 0
  AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
  AND event_time > {{time_range}}
  AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
GROUP BY database, table
ORDER BY avg_duration_sec DESC
LIMIT 30`,

  `-- @meta: title='Merge Duration by Size Bucket' group='Merge Analytics' interval='1 DAY' description='How merge duration scales with result part size — shows whether large parts take disproportionately longer to merge. Excludes mutation-triggered re-merges.'
-- @chart: type=bar group_by=size_bucket value=avg_duration_sec unit=sec style=2d
SELECT
    multiIf(
        size_in_bytes < 1048576,           '< 1 MB',
        size_in_bytes < 10485760,          '1-10 MB',
        size_in_bytes < 104857600,         '10-100 MB',
        size_in_bytes < 1073741824,        '100 MB-1 GB',
        size_in_bytes < 5368709120,        '1-5 GB',
        size_in_bytes < 10737418240,       '5-10 GB',
        size_in_bytes < 21474836480,       '10-20 GB',
        size_in_bytes < 53687091200,       '20-50 GB',
        size_in_bytes < 107374182400,      '50-100 GB',
        size_in_bytes < 161061273600,      '100-150 GB',
        '> 150 GB'
    ) AS size_bucket,
    min(size_in_bytes) AS _sort,
    count() AS merge_count,
    round(avg(duration_ms / 1000.0), 1) AS avg_duration_sec,
    round(quantile(0.95)(duration_ms / 1000.0), 1) AS p95_duration_sec,
    formatReadableSize(avg(size_in_bytes)) AS avg_result_size
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'MergeParts'
  AND merge_reason != 'NotAMerge'
  AND duration_ms > 0
  AND event_time > {{time_range}}
  AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
GROUP BY size_bucket
ORDER BY _sort ASC`,

  `-- @meta: title='Estimated Merge Time (active parts)' group='Merge Analytics' description='Predicted merge duration for each active part based on historical throughput — highlights parts where mutations would be stuck waiting longest'
-- @chart: type=bar group_by=tbl value=estimated_merge_sec unit=sec style=2d
-- @drill: on=tbl into='Part Merge Estimates'
WITH merge_throughput AS (
    SELECT
        concat(database, '.', table) AS tbl,
        avg(size_in_bytes / greatest(duration_ms / 1000.0, 0.001)) AS bytes_per_sec
    FROM system.part_log
    WHERE event_type = 'MergeParts'
      AND merge_reason != 'NotAMerge'
      AND duration_ms > 0
    GROUP BY database, table
)
SELECT
    concat(p.database, '.', p.table) AS tbl,
    count() AS active_parts,
    round(max(p.data_uncompressed_bytes / greatest(t.bytes_per_sec, 1)), 1) AS estimated_merge_sec,
    formatReadableTimeDelta(max(p.data_uncompressed_bytes / greatest(t.bytes_per_sec, 1))) AS estimated_merge_time,
    formatReadableSize(max(p.data_uncompressed_bytes)) AS largest_part
FROM system.parts AS p
JOIN merge_throughput AS t ON t.tbl = concat(p.database, '.', p.table)
WHERE p.database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
  AND p.active = 1
  AND concat(p.database, '.', p.table) = {{drill_value:tbl | concat(p.database, '.', p.table)}}
GROUP BY p.database, p.table
ORDER BY estimated_merge_sec DESC
LIMIT 30`,

  `-- @meta: title='Part Merge Estimates' group='Merge Analytics' description='Per-part estimated merge duration for a specific table — drill from Estimated Merge Time to see individual parts'
-- @chart: type=bar group_by=part_name value=estimated_merge_sec unit=sec style=2d
WITH merge_throughput AS (
    SELECT
        concat(database, '.', table) AS tbl,
        avg(size_in_bytes / greatest(duration_ms / 1000.0, 0.001)) AS bytes_per_sec
    FROM system.part_log
    WHERE event_type = 'MergeParts'
      AND merge_reason != 'NotAMerge'
      AND duration_ms > 0
    GROUP BY database, table
)
SELECT
    p.name AS part_name,
    p.rows,
    formatReadableSize(p.bytes_on_disk) AS disk_size,
    formatReadableSize(p.data_uncompressed_bytes) AS uncompressed_size,
    round(p.data_uncompressed_bytes / greatest(t.bytes_per_sec, 1), 1) AS estimated_merge_sec,
    formatReadableTimeDelta(p.data_uncompressed_bytes / greatest(t.bytes_per_sec, 1)) AS estimated_merge_time
FROM system.parts AS p
JOIN merge_throughput AS t ON t.tbl = concat(p.database, '.', p.table)
WHERE p.active = 1
  AND concat(p.database, '.', p.table) = {{drill_value:tbl | concat(p.database, '.', p.table)}}
ORDER BY estimated_merge_sec DESC
LIMIT 50`,

  `-- @meta: title='Part Wait Time by Table' group='Merge Analytics' interval='7 DAY' description='How long parts sit idle before being picked up for a merge — for ReplacingMergeTree/CollapsingMergeTree this is how long stale/duplicate rows remain visible'
-- @chart: type=bar group_by=tbl value=avg_wait_sec unit=sec style=2d
-- @drill: on=tbl into='Part Wait Time by Size'
WITH source_parts AS (
    SELECT
        m.database, m.table, m.event_time AS merge_time,
        arrayJoin(m.merged_from) AS src_part
    FROM system.part_log AS m
    WHERE m.event_type = 'MergeParts'
      AND m.merge_reason != 'NotAMerge'
      AND m.database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND m.event_time > {{time_range}}
      AND concat(m.database, '.', m.table) = {{drill_value:tbl | concat(m.database, '.', m.table)}}
),
wait AS (
    SELECT
        s.database, s.table, s.merge_time,
        greatest(0, dateDiff('second', max(c.event_time), s.merge_time)) AS wait_sec
    FROM source_parts AS s
    INNER JOIN system.part_log AS c
      ON c.part_name = s.src_part AND c.database = s.database AND c.table = s.table
      AND c.event_type IN ('NewPart', 'MergeParts')
      AND c.event_time <= s.merge_time
    GROUP BY s.database, s.table, s.merge_time, s.src_part
)
SELECT
    concat(database, '.', table) AS tbl,
    count() AS parts_merged,
    round(avg(wait_sec), 0) AS avg_wait_sec,
    formatReadableTimeDelta(avg(wait_sec)) AS avg_wait_time,
    round(quantile(0.95)(wait_sec), 0) AS p95_wait_sec,
    formatReadableTimeDelta(quantile(0.95)(wait_sec)) AS p95_wait_time
FROM wait
GROUP BY database, table
ORDER BY avg_wait_sec DESC
LIMIT 30`,

  `-- @meta: title='Part Wait Time by Size' group='Merge Analytics' interval='7 DAY' description='How long parts wait before being picked up for a merge, broken down by size bucket — critical for ReplacingMergeTree and CollapsingMergeTree where stale/duplicate rows remain visible until the merge completes'
-- @chart: type=bar group_by=size_bucket value=avg_wait_sec unit=sec style=2d
-- @drill: on=size_bucket into='Part Wait Timeline'
WITH source_parts AS (
    SELECT
        m.database, m.table, m.event_time AS merge_time,
        arrayJoin(m.merged_from) AS src_part
    FROM system.part_log AS m
    WHERE m.event_type = 'MergeParts'
      AND m.merge_reason != 'NotAMerge'
      AND m.event_time > {{time_range}}
      AND concat(m.database, '.', m.table) = {{drill_value:tbl | concat(m.database, '.', m.table)}}
),
wait AS (
    SELECT
        s.database, s.table, s.merge_time,
        greatest(0, dateDiff('second', max(c.event_time), s.merge_time)) AS wait_sec,
        max(c.size_in_bytes) AS size_in_bytes
    FROM source_parts AS s
    INNER JOIN system.part_log AS c
      ON c.part_name = s.src_part AND c.database = s.database AND c.table = s.table
      AND c.event_type IN ('NewPart', 'MergeParts')
      AND c.event_time <= s.merge_time
    GROUP BY s.database, s.table, s.merge_time, s.src_part
)
SELECT
    multiIf(
        size_in_bytes < 1048576,           '< 1 MB',
        size_in_bytes < 10485760,          '1-10 MB',
        size_in_bytes < 104857600,         '10-100 MB',
        size_in_bytes < 1073741824,        '100 MB-1 GB',
        size_in_bytes < 5368709120,        '1-5 GB',
        size_in_bytes < 10737418240,       '5-10 GB',
        size_in_bytes < 21474836480,       '10-20 GB',
        size_in_bytes < 53687091200,       '20-50 GB',
        size_in_bytes < 107374182400,      '50-100 GB',
        size_in_bytes < 161061273600,      '100-150 GB',
        '> 150 GB'
    ) AS size_bucket,
    min(size_in_bytes) AS _sort,
    count() AS parts_merged,
    round(avg(wait_sec), 0) AS avg_wait_sec,
    formatReadableTimeDelta(avg(wait_sec)) AS avg_wait_time,
    round(quantile(0.95)(wait_sec), 0) AS p95_wait_sec,
    formatReadableTimeDelta(quantile(0.95)(wait_sec)) AS p95_wait_time,
    formatReadableSize(avg(size_in_bytes)) AS avg_part_size
FROM wait
GROUP BY size_bucket
ORDER BY _sort ASC`,

  `-- @meta: title='Part Wait Timeline' group='Merge Analytics' interval='7 DAY' description='Per-merge wait time for a specific size bucket — each point is one source part consumed by a merge'
-- @chart: type=line group_by=t value=wait_sec unit=sec style=2d
WITH source_parts AS (
    SELECT
        m.database, m.table, m.event_time AS merge_time,
        arrayJoin(m.merged_from) AS src_part
    FROM system.part_log AS m
    WHERE m.event_type = 'MergeParts'
      AND m.merge_reason != 'NotAMerge'
      AND m.event_time > {{time_range}}
      AND concat(m.database, '.', m.table) = {{drill_value:tbl | concat(m.database, '.', m.table)}}
),
wait AS (
    SELECT
        s.merge_time,
        s.src_part,
        greatest(0, dateDiff('second', max(c.event_time), s.merge_time)) AS wait_sec,
        multiIf(
            max(c.size_in_bytes) < 1048576,           '< 1 MB',
            max(c.size_in_bytes) < 10485760,          '1-10 MB',
            max(c.size_in_bytes) < 104857600,         '10-100 MB',
            max(c.size_in_bytes) < 1073741824,        '100 MB-1 GB',
            max(c.size_in_bytes) < 5368709120,        '1-5 GB',
            max(c.size_in_bytes) < 10737418240,       '5-10 GB',
            max(c.size_in_bytes) < 21474836480,       '10-20 GB',
            max(c.size_in_bytes) < 53687091200,       '20-50 GB',
            max(c.size_in_bytes) < 107374182400,      '50-100 GB',
            max(c.size_in_bytes) < 161061273600,      '100-150 GB',
            '> 150 GB'
        ) AS size_bucket,
        formatReadableSize(max(c.size_in_bytes)) AS part_size
    FROM source_parts AS s
    INNER JOIN system.part_log AS c
      ON c.part_name = s.src_part AND c.database = s.database AND c.table = s.table
      AND c.event_type IN ('NewPart', 'MergeParts')
      AND c.event_time <= s.merge_time
    GROUP BY s.database, s.table, s.merge_time, s.src_part
)
SELECT
    merge_time AS t,
    src_part,
    wait_sec,
    formatReadableTimeDelta(wait_sec) AS wait_time,
    part_size
FROM wait
WHERE {{drill:size_bucket | 1=1}}
ORDER BY t ASC
LIMIT 1000`,
];

export default queries;
