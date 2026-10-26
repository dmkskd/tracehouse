/** Completed-merge cost attribution. Keep one row per merge: expanding merged_from
 * would multiply CPU and elapsed time by the number of source parts. */
const mergeCosts = `-- Successful completed merges (event_type = 'MergeParts'), including work on each replica.
-- Mutations are logged as MutatePart and have their own charts; active merges are not in part_log yet.
-- CPU: core-seconds of user + system time. ClickHouse omits zero counters from ProfileEvents,
-- so a missing counter counts as 0; CPU is NULL only when the row has no counters at all.
-- cpu_coverage_pct: percentage of merges with counters.
-- CPU shares use recorded CPU only. All shares use the selected table and time range, before LIMIT.
-- Elapsed: sum of individual merge durations. Concurrent merges are counted separately.
-- Size buckets use compressed result bytes. CPU per input GiB uses uncompressed bytes read,
-- restricted to merges with CPU counters. Mixed input levels are shown as min-max.
WITH merge_costs AS (
    SELECT
        concat(database, '.', table) AS tbl,
        toString(merge_reason) AS reason,
        size_in_bytes,
        read_bytes,
        duration_ms / 1000.0 AS elapsed,
        if(empty(ProfileEvents), NULL,
            (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu,
        arrayMap(p -> toUInt64OrNull(extract(p, '^[^_]+_[0-9]+_[0-9]+_([0-9]+)(?:_[0-9]+)?$')), merged_from) AS source_levels,
        toUInt64OrNull(extract(part_name, '^[^_]+_[0-9]+_[0-9]+_([0-9]+)(?:_[0-9]+)?$')) AS result_level,
        if(empty(source_levels) OR arrayExists(l -> isNull(l), source_levels) OR isNull(result_level),
            'Unknown',
            concat(
                toString(arrayMin(source_levels)),
                if(arrayMin(source_levels) = arrayMax(source_levels), '', concat('-', toString(arrayMax(source_levels)))),
                ' → ', toString(result_level)
            )
        ) AS level_transition,
        multiIf(
            size_in_bytes < 1048576, '< 1 MiB',
            size_in_bytes < 10485760, '1-10 MiB',
            size_in_bytes < 104857600, '10-100 MiB',
            size_in_bytes < 1073741824, '100 MiB-1 GiB',
            size_in_bytes < 10737418240, '1-10 GiB',
            size_in_bytes < 107374182400, '10-100 GiB',
            '≥ 100 GiB'
        ) AS size_bucket
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MergeParts'
      AND error = 0
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)`;

// Shares use unrounded totals before LIMIT; CPU intensity uses only rows with
// CPU counters and uncompressed input bytes. Missing counters stay NULL.
const costMetrics = `count() AS merge_count,
    count(cpu) AS cpu_profiled_merges,
    round(100.0 * count(cpu) / count(), 1) AS cpu_coverage_pct,
    round(sum(cpu), 3) AS cpu_seconds,
    round(sum(elapsed), 3) AS elapsed_seconds,
    round(100.0 * sum(cpu) / nullIf(sum(sum(cpu)) OVER (), 0), 2) AS cpu_share_pct,
    round(100.0 * sum(elapsed) / nullIf(sum(sum(elapsed)) OVER (), 0), 2) AS elapsed_share_pct,
    round(100.0 * count() / sum(count()) OVER (), 2) AS merge_share_pct,
    round(avg(cpu), 3) AS avg_cpu_seconds,
    round(avg(elapsed), 3) AS avg_elapsed_seconds,
    round(quantile(0.95)(elapsed), 3) AS p95_elapsed_seconds,
    round(sum(cpu) / nullIf(sumIf(read_bytes, isNotNull(cpu)) / 1073741824.0, 0), 3) AS cpu_seconds_per_input_gib,
    formatReadableSize(sum(read_bytes)) AS input_uncompressed,
    formatReadableSize(sum(size_in_bytes)) AS result_compressed,
    sum(count()) OVER () AS total_merge_count,
    round(sum(sum(elapsed)) OVER (), 3) AS total_elapsed_seconds,
    round(sum(sum(cpu)) OVER (), 3) AS total_cpu_seconds`;

const reasonCostMetrics = `count() AS merge_count,
    count(cpu) AS cpu_profiled_merges,
    round(sum(cpu), 3) AS cpu_seconds,
    round(sum(elapsed), 3) AS elapsed_seconds,
    round(100.0 * sum(cpu) / nullIf(sum(sum(cpu)) OVER (), 0), 2) AS cpu_share_pct,
    round(100.0 * sum(elapsed) / nullIf(sum(sum(elapsed)) OVER (), 0), 2) AS elapsed_share_pct,
    round(100.0 * count() / sum(count()) OVER (), 2) AS merge_share_pct,
    sum(count()) OVER () AS total_merge_count,
    round(sum(sum(elapsed)) OVER (), 3) AS total_elapsed_seconds,
    round(sum(sum(cpu)) OVER (), 3) AS total_cpu_seconds`;

/** Row caps for unbounded dimensions; the summaries are computed before the LIMIT. */
const DIMENSION_LIMIT: Record<string, number> = { tbl: 30, level_transition: 10 };

function costQuery(title: string, dimension: string, metric: 'cpu_seconds' | 'elapsed_seconds', description: string, drill = ''): string {
  return `-- @meta: title='${title}' group='Merge Analytics' interval='1 DAY' description='${description}'
-- @chart: type=bar group_by=${dimension} value=${metric} unit=s style=2d
-- @summary: value=${metric === 'elapsed_seconds' ? 'total_elapsed_seconds' : 'total_cpu_seconds'} label='${metric === 'elapsed_seconds' ? 'Total duration' : 'Total CPU'}' unit=s
-- @summary: value=total_merge_count label='Merges'
${drill}
${mergeCosts}
SELECT
    ${dimension},
    ${costMetrics}
FROM merge_costs
GROUP BY ${dimension}
ORDER BY ${metric} DESC NULLS LAST, ${dimension}
${DIMENSION_LIMIT[dimension] ? `LIMIT ${DIMENSION_LIMIT[dimension]}` : ''}`;
}

const queries: string[] = [
  costQuery('Merge CPU by Table', 'tbl', 'cpu_seconds', 'Tables ranked by total merge CPU. Click a table for the cost breakdown.', "-- @drill: on=tbl into='Table Merge Cost Breakdown'"),
  costQuery('Merge Elapsed Time by Table', 'tbl', 'elapsed_seconds', 'Tables ranked by total merge duration.', "-- @drill: on=tbl into='Table Merge Cost Breakdown'"),
  costQuery('Merge CPU by Level Transition', 'level_transition', 'cpu_seconds', 'Top 10 level transitions by CPU (totals cover all merges). Source → result part level; mixed source levels appear as a range.'),
  costQuery('Merge Elapsed Time by Level Transition', 'level_transition', 'elapsed_seconds', 'Top 10 level transitions by total merge duration (totals cover all merges).'),
  costQuery('Merge CPU by Result Size', 'size_bucket', 'cpu_seconds', 'CPU by compressed result-part size.'),
  costQuery('Merge Elapsed Time by Result Size', 'size_bucket', 'elapsed_seconds', 'Total merge duration by compressed result-part size.'),
  `-- @meta: title='Merge Cost by Reason' group='Merge Analytics' interval='1 DAY' description='CPU and duration for regular, TTL delete, and TTL recompression merges.'
-- @cell: column=cpu_share_pct type=gauge max=100 unit=%
-- @cell: column=elapsed_share_pct type=gauge max=100 unit=%
-- @cell: column=merge_share_pct type=gauge max=100 unit=%
${mergeCosts}
SELECT
    reason,
    ${reasonCostMetrics}
FROM merge_costs
GROUP BY reason
ORDER BY cpu_seconds DESC NULLS LAST, elapsed_seconds DESC`,
  `-- @meta: title='Table Merge Cost Breakdown' group='Merge Analytics' interval='1 DAY' description='Merge costs by level, result size, and reason, ranked by CPU.'
${mergeCosts}
SELECT
    level_transition,
    size_bucket,
    reason,
    ${costMetrics}
FROM merge_costs
GROUP BY level_transition, size_bucket, reason
ORDER BY cpu_seconds DESC NULLS LAST, elapsed_seconds DESC
LIMIT 200`,
  `-- @meta: title='Mutation CPU by Table' group='Merge Analytics' interval='1 DAY' description='Top 10 tables by mutation CPU (totals cover all mutations). Mutations are not in the merge charts above.'
-- @chart: type=bar group_by=tbl value=cpu_seconds unit=s style=2d
-- @summary: value=total_cpu_seconds label='Total CPU' unit=s
-- @summary: value=total_parts_rewritten label='Parts rewritten'
-- Successful completed mutations (part_log MutatePart), including work on each replica.
-- Each row is one part rewritten by a mutation (ALTER UPDATE/DELETE, MATERIALIZE, ...).
-- CPU: user + system time (omitted counters are 0); NULL only when the row has no counters.
-- Shares and totals cover the selected table and time range, before LIMIT.
WITH mutation_costs AS (
    SELECT
        concat(database, '.', table) AS tbl,
        size_in_bytes,
        read_bytes,
        duration_ms / 1000.0 AS elapsed,
        if(empty(ProfileEvents), NULL,
            (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MutatePart'
      AND error = 0
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    tbl,
    count() AS parts_rewritten,
    count(cpu) AS cpu_profiled_parts,
    round(100.0 * count(cpu) / count(), 1) AS cpu_coverage_pct,
    round(sum(cpu), 3) AS cpu_seconds,
    round(sum(elapsed), 3) AS elapsed_seconds,
    round(100.0 * sum(cpu) / nullIf(sum(sum(cpu)) OVER (), 0), 2) AS cpu_share_pct,
    round(100.0 * sum(elapsed) / nullIf(sum(sum(elapsed)) OVER (), 0), 2) AS elapsed_share_pct,
    round(avg(cpu), 3) AS avg_cpu_seconds,
    round(avg(elapsed), 3) AS avg_elapsed_seconds,
    round(quantile(0.95)(elapsed), 3) AS p95_elapsed_seconds,
    formatReadableSize(sum(read_bytes)) AS input_uncompressed,
    formatReadableSize(sum(size_in_bytes)) AS result_compressed,
    sum(count()) OVER () AS total_parts_rewritten,
    round(sum(sum(elapsed)) OVER (), 3) AS total_elapsed_seconds,
    round(sum(sum(cpu)) OVER (), 3) AS total_cpu_seconds
FROM mutation_costs
GROUP BY tbl
ORDER BY cpu_seconds DESC NULLS LAST, tbl
LIMIT 10`,
  `-- @meta: title='Mutation Elapsed Time by Table' group='Merge Analytics' interval='1 DAY' description='Top 10 tables by total mutation duration (totals cover all mutations).'
-- @chart: type=bar group_by=tbl value=elapsed_seconds unit=s style=2d
-- @summary: value=total_elapsed_seconds label='Total duration' unit=s
-- @summary: value=total_parts_rewritten label='Parts rewritten'
-- Successful completed mutations (part_log MutatePart), including work on each replica.
-- Each row is one part rewritten by a mutation (ALTER UPDATE/DELETE, MATERIALIZE, ...).
-- CPU: user + system time (omitted counters are 0); NULL only when the row has no counters.
-- Shares and totals cover the selected table and time range, before LIMIT.
WITH mutation_costs AS (
    SELECT
        concat(database, '.', table) AS tbl,
        size_in_bytes,
        read_bytes,
        duration_ms / 1000.0 AS elapsed,
        if(empty(ProfileEvents), NULL,
            (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MutatePart'
      AND error = 0
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    tbl,
    count() AS parts_rewritten,
    count(cpu) AS cpu_profiled_parts,
    round(100.0 * count(cpu) / count(), 1) AS cpu_coverage_pct,
    round(sum(cpu), 3) AS cpu_seconds,
    round(sum(elapsed), 3) AS elapsed_seconds,
    round(100.0 * sum(cpu) / nullIf(sum(sum(cpu)) OVER (), 0), 2) AS cpu_share_pct,
    round(100.0 * sum(elapsed) / nullIf(sum(sum(elapsed)) OVER (), 0), 2) AS elapsed_share_pct,
    round(avg(cpu), 3) AS avg_cpu_seconds,
    round(avg(elapsed), 3) AS avg_elapsed_seconds,
    round(quantile(0.95)(elapsed), 3) AS p95_elapsed_seconds,
    formatReadableSize(sum(read_bytes)) AS input_uncompressed,
    formatReadableSize(sum(size_in_bytes)) AS result_compressed,
    sum(count()) OVER () AS total_parts_rewritten,
    round(sum(sum(elapsed)) OVER (), 3) AS total_elapsed_seconds,
    round(sum(sum(cpu)) OVER (), 3) AS total_cpu_seconds
FROM mutation_costs
GROUP BY tbl
ORDER BY elapsed_seconds DESC NULLS LAST, tbl
LIMIT 10`,
  `-- @meta: title='Mutation Cost by Command' group='Merge Analytics' interval='1 DAY' description='Top 20 completed mutations by CPU, with their ALTER command. ClickHouse keeps the last finished_mutations_to_keep (default 100) per table; older ones show without a command.'
-- @cell: column=cpu_share_pct type=gauge max=100 unit=%
-- Successful completed mutations (part_log MutatePart), including work on each replica,
-- grouped by mutation: a rewritten part's name ends in the mutation's version (e.g. all_1_5_2_7 = version 7).
-- The command comes from system.mutations, matched on table + version via block_numbers:
-- non-replicated tables record one version for all partitions (partition_id = ''),
-- replicated tables record one per partition.
-- CPU: user + system time (omitted counters are 0); NULL only when the row has no counters.
-- Shares cover the selected table and time range, before LIMIT.
WITH mutation_parts AS (
    SELECT
        database,
        table,
        partition_id,
        toUInt64OrNull(extract(part_name, '^[^_]+_[0-9]+_[0-9]+_[0-9]+_([0-9]+)$')) AS mutation_version,
        event_time,
        duration_ms / 1000.0 AS elapsed,
        if(empty(ProfileEvents), NULL,
            (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MutatePart'
      AND error = 0
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
),
mutation_commands AS (
    -- One row per (table, partition, version); replicas report the same mutation, so dedupe.
    SELECT
        database,
        table,
        partition_id,
        mutation_version,
        any(command) AS command
    FROM {{cluster_aware:system.mutations}}
    ARRAY JOIN
        block_numbers.partition_id AS partition_id,
        block_numbers.number AS mutation_version
    GROUP BY database, table, partition_id, mutation_version
)
SELECT
    concat(p.database, '.', p.table) AS tbl,
    p.mutation_version AS mutation_version,
    -- An unmatched LEFT JOIN yields '' (join_use_nulls = 0), so treat '' as no match.
    coalesce(nullIf(any(by_partition.command), ''), nullIf(any(all_partitions.command), ''),
        '(no longer in system.mutations)') AS command,
    count() AS parts_rewritten,
    round(sum(p.cpu), 3) AS cpu_seconds,
    round(sum(p.elapsed), 3) AS elapsed_seconds,
    round(100.0 * sum(p.cpu) / nullIf(sum(sum(p.cpu)) OVER (), 0), 2) AS cpu_share_pct,
    min(p.event_time) AS first_part_at,
    max(p.event_time) AS last_part_at
FROM mutation_parts AS p
LEFT JOIN mutation_commands AS by_partition
    ON by_partition.database = p.database
   AND by_partition.table = p.table
   AND by_partition.partition_id = p.partition_id
   AND by_partition.mutation_version = p.mutation_version
LEFT JOIN mutation_commands AS all_partitions
    ON all_partitions.database = p.database
   AND all_partitions.table = p.table
   AND all_partitions.partition_id = ''
   AND all_partitions.mutation_version = p.mutation_version
GROUP BY tbl, mutation_version
ORDER BY cpu_seconds DESC NULLS LAST, elapsed_seconds DESC
LIMIT 20`,
];

export default queries;
