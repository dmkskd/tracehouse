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

/** Result-size panels only: failed attempts have no result part, so these stay successful-only. */
function costQuery(title: string, dimension: string, metric: 'cpu_seconds' | 'elapsed_seconds', description: string): string {
  return `-- @meta: title='${title}' group='Merge Analytics' interval='1 DAY' description='${description}'
-- @chart: type=bar group_by=${dimension} value=${metric} unit=s style=2d
-- @summary: value=${metric === 'elapsed_seconds' ? 'total_elapsed_seconds' : 'total_cpu_seconds'} label='${metric === 'elapsed_seconds' ? 'Total duration' : 'Total CPU'}' unit=s
-- @summary: value=total_merge_count label='Merges'
${mergeCosts}
SELECT
    ${dimension},
    ${costMetrics}
FROM merge_costs
GROUP BY ${dimension}
ORDER BY ${metric} DESC NULLS LAST, ${dimension}`;
}

const queries: string[] = [
  `-- @meta: title='Merge CPU by Table' group='Merge Analytics' interval='1 DAY' description='Tables ranked by total merge CPU, split into successful merges and failed attempts. CPU on failed attempts produced no part. Click a table for the cost breakdown.'
-- @chart: type=stacked_bar group_by=tbl value=cpu_seconds,failed_cpu_seconds unit=s style=2d
-- @summary: value=total_cpu_seconds label='Successful CPU' unit=s
-- @summary: value=total_failed_cpu_seconds label='Failed CPU' unit=s
-- @summary: value=total_merge_count label='Merges'
-- @drill: on=tbl into='Table Merge Cost Breakdown'
-- Completed merges (event_type = 'MergeParts'), including work on each replica.
-- Successful merges (error = 0) feed every column except the failed_* ones.
-- Failed attempts (error != 0) write a row with the CPU spent until the exception; each retry adds a row.
-- CPU: core-seconds of user + system time. ClickHouse omits zero counters from ProfileEvents,
-- so a missing counter counts as 0; CPU is NULL only when the row has no counters at all.
-- Shares and totals use the selected table and time range, before LIMIT. Ranked by successful + failed CPU.
WITH merge_costs AS (
    SELECT
        concat(database, '.', table) AS tbl,
        error = 0 AS ok,
        size_in_bytes,
        read_bytes,
        duration_ms / 1000.0 AS elapsed,
        if(empty(ProfileEvents), NULL,
            (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MergeParts'
      -- A replica that fetches instead of merging logs a failed fetch as MergeParts with no merge reason: not a merge.
      AND (error = 0 OR merge_reason != 'NotAMerge')
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    tbl,
    countIf(ok) AS merge_count,
    countIf(ok AND isNotNull(cpu)) AS cpu_profiled_merges,
    round(100.0 * cpu_profiled_merges / nullIf(merge_count, 0), 1) AS cpu_coverage_pct,
    -- 0 when there are no rows of that outcome; NULL only when they all lack counters.
    if(countIf(ok) = 0, 0, round(sumIf(cpu, ok), 3)) AS cpu_seconds,
    round(sumIf(elapsed, ok), 3) AS elapsed_seconds,
    round(100.0 * sumIf(cpu, ok) / nullIf(sum(sumIf(cpu, ok)) OVER (), 0), 2) AS cpu_share_pct,
    round(100.0 * sumIf(elapsed, ok) / nullIf(sum(sumIf(elapsed, ok)) OVER (), 0), 2) AS elapsed_share_pct,
    round(100.0 * countIf(ok) / nullIf(sum(countIf(ok)) OVER (), 0), 2) AS merge_share_pct,
    round(avgIf(cpu, ok), 3) AS avg_cpu_seconds,
    round(avgIf(elapsed, ok), 3) AS avg_elapsed_seconds,
    round(quantileIf(0.95)(elapsed, ok), 3) AS p95_elapsed_seconds,
    round(sumIf(cpu, ok) / nullIf(sumIf(read_bytes, ok AND isNotNull(cpu)) / 1073741824.0, 0), 3) AS cpu_seconds_per_input_gib,
    formatReadableSize(sumIf(read_bytes, ok)) AS input_uncompressed,
    formatReadableSize(sumIf(size_in_bytes, ok)) AS result_compressed,
    countIf(NOT ok) AS failed_attempts,
    if(countIf(NOT ok) = 0, 0, round(sumIf(cpu, NOT ok), 3)) AS failed_cpu_seconds,
    if(countIf(NOT ok) = 0, 0, round(100.0 * sumIf(cpu, NOT ok) / nullIf(sum(cpu), 0), 1)) AS failed_cpu_pct,
    round(sumIf(elapsed, NOT ok), 3) AS failed_elapsed_seconds,
    sum(countIf(ok)) OVER () AS total_merge_count,
    round(sum(sumIf(elapsed, ok)) OVER (), 3) AS total_elapsed_seconds,
    round(sum(sumIf(cpu, ok)) OVER (), 3) AS total_cpu_seconds,
    round(sum(if(countIf(NOT ok) = 0, 0, sumIf(cpu, NOT ok))) OVER (), 3) AS total_failed_cpu_seconds
FROM merge_costs
GROUP BY tbl
ORDER BY sum(cpu) DESC NULLS LAST, tbl
LIMIT 30`,
  `-- @meta: title='Merge Elapsed Time by Table' group='Merge Analytics' interval='1 DAY' description='Tables ranked by total merge duration, split into successful merges and failed attempts. Click a table for the cost breakdown.'
-- @chart: type=stacked_bar group_by=tbl value=elapsed_seconds,failed_elapsed_seconds unit=s style=2d
-- @summary: value=total_elapsed_seconds label='Successful duration' unit=s
-- @summary: value=total_failed_elapsed_seconds label='Failed duration' unit=s
-- @summary: value=total_merge_count label='Merges'
-- @drill: on=tbl into='Table Merge Cost Breakdown'
-- Completed merges (event_type = 'MergeParts'), including work on each replica.
-- Successful merges (error = 0) feed every column except the failed_* ones.
-- Failed attempts (error != 0) record the time until the exception; each retry adds a row.
-- Elapsed: sum of individual merge durations. Concurrent merges are counted separately.
-- Shares and totals use the selected table and time range, before LIMIT. Ranked by successful + failed duration.
WITH merge_costs AS (
    SELECT
        concat(database, '.', table) AS tbl,
        error = 0 AS ok,
        size_in_bytes,
        read_bytes,
        duration_ms / 1000.0 AS elapsed,
        if(empty(ProfileEvents), NULL,
            (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MergeParts'
      -- A replica that fetches instead of merging logs a failed fetch as MergeParts with no merge reason: not a merge.
      AND (error = 0 OR merge_reason != 'NotAMerge')
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    tbl,
    countIf(ok) AS merge_count,
    countIf(ok AND isNotNull(cpu)) AS cpu_profiled_merges,
    round(100.0 * cpu_profiled_merges / nullIf(merge_count, 0), 1) AS cpu_coverage_pct,
    -- 0 when there are no rows of that outcome; NULL only when they all lack counters.
    if(countIf(ok) = 0, 0, round(sumIf(cpu, ok), 3)) AS cpu_seconds,
    round(sumIf(elapsed, ok), 3) AS elapsed_seconds,
    round(100.0 * sumIf(cpu, ok) / nullIf(sum(sumIf(cpu, ok)) OVER (), 0), 2) AS cpu_share_pct,
    round(100.0 * sumIf(elapsed, ok) / nullIf(sum(sumIf(elapsed, ok)) OVER (), 0), 2) AS elapsed_share_pct,
    round(100.0 * countIf(ok) / nullIf(sum(countIf(ok)) OVER (), 0), 2) AS merge_share_pct,
    round(avgIf(cpu, ok), 3) AS avg_cpu_seconds,
    round(avgIf(elapsed, ok), 3) AS avg_elapsed_seconds,
    round(quantileIf(0.95)(elapsed, ok), 3) AS p95_elapsed_seconds,
    round(sumIf(cpu, ok) / nullIf(sumIf(read_bytes, ok AND isNotNull(cpu)) / 1073741824.0, 0), 3) AS cpu_seconds_per_input_gib,
    formatReadableSize(sumIf(read_bytes, ok)) AS input_uncompressed,
    formatReadableSize(sumIf(size_in_bytes, ok)) AS result_compressed,
    countIf(NOT ok) AS failed_attempts,
    if(countIf(NOT ok) = 0, 0, round(sumIf(cpu, NOT ok), 3)) AS failed_cpu_seconds,
    round(sumIf(elapsed, NOT ok), 3) AS failed_elapsed_seconds,
    round(100.0 * sumIf(elapsed, NOT ok) / nullIf(sum(elapsed), 0), 1) AS failed_elapsed_pct,
    sum(countIf(ok)) OVER () AS total_merge_count,
    round(sum(sumIf(elapsed, ok)) OVER (), 3) AS total_elapsed_seconds,
    round(sum(sumIf(cpu, ok)) OVER (), 3) AS total_cpu_seconds,
    round(sum(sumIf(elapsed, NOT ok)) OVER (), 3) AS total_failed_elapsed_seconds
FROM merge_costs
GROUP BY tbl
ORDER BY sum(elapsed) DESC NULLS LAST, tbl
LIMIT 30`,
  `-- @meta: title='Merge CPU by Level Transition' group='Merge Analytics' interval='1 DAY' description='Top 10 level transitions by CPU, split into successful merges and failed attempts (totals cover all merges). Source → result part level; mixed source levels appear as a range.'
-- @chart: type=stacked_bar group_by=level_transition value=cpu_seconds,failed_cpu_seconds unit=s style=2d
-- @summary: value=total_cpu_seconds label='Successful CPU' unit=s
-- @summary: value=total_failed_cpu_seconds label='Failed CPU' unit=s
-- @summary: value=total_merge_count label='Merges'
-- Completed merges (event_type = 'MergeParts'), including work on each replica.
-- Successful merges (error = 0) feed every column except the failed_* ones.
-- Failed attempts (error != 0) keep their source parts and target part name, so their level is known; each retry adds a row.
-- CPU: core-seconds of user + system time. ClickHouse omits zero counters from ProfileEvents,
-- so a missing counter counts as 0; CPU is NULL only when the row has no counters at all.
-- Shares and totals use the selected table and time range, before LIMIT. Ranked by successful + failed CPU.
WITH merge_costs AS (
    SELECT
        error = 0 AS ok,
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
        ) AS level_transition
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MergeParts'
      -- A replica that fetches instead of merging logs a failed fetch as MergeParts with no merge reason: not a merge.
      AND (error = 0 OR merge_reason != 'NotAMerge')
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    level_transition,
    countIf(ok) AS merge_count,
    countIf(ok AND isNotNull(cpu)) AS cpu_profiled_merges,
    round(100.0 * cpu_profiled_merges / nullIf(merge_count, 0), 1) AS cpu_coverage_pct,
    -- 0 when there are no rows of that outcome; NULL only when they all lack counters.
    if(countIf(ok) = 0, 0, round(sumIf(cpu, ok), 3)) AS cpu_seconds,
    round(sumIf(elapsed, ok), 3) AS elapsed_seconds,
    round(100.0 * sumIf(cpu, ok) / nullIf(sum(sumIf(cpu, ok)) OVER (), 0), 2) AS cpu_share_pct,
    round(100.0 * sumIf(elapsed, ok) / nullIf(sum(sumIf(elapsed, ok)) OVER (), 0), 2) AS elapsed_share_pct,
    round(100.0 * countIf(ok) / nullIf(sum(countIf(ok)) OVER (), 0), 2) AS merge_share_pct,
    round(avgIf(cpu, ok), 3) AS avg_cpu_seconds,
    round(avgIf(elapsed, ok), 3) AS avg_elapsed_seconds,
    round(quantileIf(0.95)(elapsed, ok), 3) AS p95_elapsed_seconds,
    round(sumIf(cpu, ok) / nullIf(sumIf(read_bytes, ok AND isNotNull(cpu)) / 1073741824.0, 0), 3) AS cpu_seconds_per_input_gib,
    formatReadableSize(sumIf(read_bytes, ok)) AS input_uncompressed,
    formatReadableSize(sumIf(size_in_bytes, ok)) AS result_compressed,
    countIf(NOT ok) AS failed_attempts,
    if(countIf(NOT ok) = 0, 0, round(sumIf(cpu, NOT ok), 3)) AS failed_cpu_seconds,
    if(countIf(NOT ok) = 0, 0, round(100.0 * sumIf(cpu, NOT ok) / nullIf(sum(cpu), 0), 1)) AS failed_cpu_pct,
    round(sumIf(elapsed, NOT ok), 3) AS failed_elapsed_seconds,
    sum(countIf(ok)) OVER () AS total_merge_count,
    round(sum(sumIf(elapsed, ok)) OVER (), 3) AS total_elapsed_seconds,
    round(sum(sumIf(cpu, ok)) OVER (), 3) AS total_cpu_seconds,
    round(sum(if(countIf(NOT ok) = 0, 0, sumIf(cpu, NOT ok))) OVER (), 3) AS total_failed_cpu_seconds
FROM merge_costs
GROUP BY level_transition
ORDER BY sum(cpu) DESC NULLS LAST, level_transition
LIMIT 10`,
  `-- @meta: title='Merge Elapsed Time by Level Transition' group='Merge Analytics' interval='1 DAY' description='Top 10 level transitions by total merge duration, split into successful merges and failed attempts (totals cover all merges).'
-- @chart: type=stacked_bar group_by=level_transition value=elapsed_seconds,failed_elapsed_seconds unit=s style=2d
-- @summary: value=total_elapsed_seconds label='Successful duration' unit=s
-- @summary: value=total_failed_elapsed_seconds label='Failed duration' unit=s
-- @summary: value=total_merge_count label='Merges'
-- Completed merges (event_type = 'MergeParts'), including work on each replica.
-- Successful merges (error = 0) feed every column except the failed_* ones.
-- Failed attempts (error != 0) keep their source parts and target part name, so their level is known; each retry adds a row.
-- Elapsed: sum of individual merge durations. Concurrent merges are counted separately.
-- Shares and totals use the selected table and time range, before LIMIT. Ranked by successful + failed duration.
WITH merge_costs AS (
    SELECT
        error = 0 AS ok,
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
        ) AS level_transition
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MergeParts'
      -- A replica that fetches instead of merging logs a failed fetch as MergeParts with no merge reason: not a merge.
      AND (error = 0 OR merge_reason != 'NotAMerge')
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    level_transition,
    countIf(ok) AS merge_count,
    countIf(ok AND isNotNull(cpu)) AS cpu_profiled_merges,
    round(100.0 * cpu_profiled_merges / nullIf(merge_count, 0), 1) AS cpu_coverage_pct,
    -- 0 when there are no rows of that outcome; NULL only when they all lack counters.
    if(countIf(ok) = 0, 0, round(sumIf(cpu, ok), 3)) AS cpu_seconds,
    round(sumIf(elapsed, ok), 3) AS elapsed_seconds,
    round(100.0 * sumIf(cpu, ok) / nullIf(sum(sumIf(cpu, ok)) OVER (), 0), 2) AS cpu_share_pct,
    round(100.0 * sumIf(elapsed, ok) / nullIf(sum(sumIf(elapsed, ok)) OVER (), 0), 2) AS elapsed_share_pct,
    round(100.0 * countIf(ok) / nullIf(sum(countIf(ok)) OVER (), 0), 2) AS merge_share_pct,
    round(avgIf(cpu, ok), 3) AS avg_cpu_seconds,
    round(avgIf(elapsed, ok), 3) AS avg_elapsed_seconds,
    round(quantileIf(0.95)(elapsed, ok), 3) AS p95_elapsed_seconds,
    round(sumIf(cpu, ok) / nullIf(sumIf(read_bytes, ok AND isNotNull(cpu)) / 1073741824.0, 0), 3) AS cpu_seconds_per_input_gib,
    formatReadableSize(sumIf(read_bytes, ok)) AS input_uncompressed,
    formatReadableSize(sumIf(size_in_bytes, ok)) AS result_compressed,
    countIf(NOT ok) AS failed_attempts,
    if(countIf(NOT ok) = 0, 0, round(sumIf(cpu, NOT ok), 3)) AS failed_cpu_seconds,
    round(sumIf(elapsed, NOT ok), 3) AS failed_elapsed_seconds,
    round(100.0 * sumIf(elapsed, NOT ok) / nullIf(sum(elapsed), 0), 1) AS failed_elapsed_pct,
    sum(countIf(ok)) OVER () AS total_merge_count,
    round(sum(sumIf(elapsed, ok)) OVER (), 3) AS total_elapsed_seconds,
    round(sum(sumIf(cpu, ok)) OVER (), 3) AS total_cpu_seconds,
    round(sum(sumIf(elapsed, NOT ok)) OVER (), 3) AS total_failed_elapsed_seconds
FROM merge_costs
GROUP BY level_transition
ORDER BY sum(elapsed) DESC NULLS LAST, level_transition
LIMIT 10`,
  costQuery('Merge CPU by Result Size', 'size_bucket', 'cpu_seconds', 'CPU by compressed result-part size. Successful merges only: a failed attempt writes no part, so it has no result size.'),
  costQuery('Merge Elapsed Time by Result Size', 'size_bucket', 'elapsed_seconds', 'Total merge duration by compressed result-part size. Successful merges only: a failed attempt writes no part, so it has no result size.'),
  `-- @meta: title='Merge Cost by Reason' group='Merge Analytics' interval='1 DAY' description='CPU and duration for regular, TTL delete, and TTL recompression merges, with the CPU spent on failed attempts.'
-- @cell: column=cpu_share_pct type=gauge max=100 unit=%
-- @cell: column=elapsed_share_pct type=gauge max=100 unit=%
-- @cell: column=merge_share_pct type=gauge max=100 unit=%
-- @cell: column=failed_cpu_pct type=gauge max=100 unit=%
-- Completed merges (event_type = 'MergeParts'), including work on each replica.
-- Successful merges (error = 0) feed every column except the failed_* ones.
-- Failed attempts (error != 0) record the CPU and time until the exception; each retry adds a row.
-- CPU: user + system time (omitted counters are 0); NULL only when the row has no counters.
-- failed_cpu_pct: share of this reason's CPU (successful + failed) spent on failed attempts.
WITH merge_costs AS (
    SELECT
        toString(merge_reason) AS reason,
        error = 0 AS ok,
        duration_ms / 1000.0 AS elapsed,
        if(empty(ProfileEvents), NULL,
            (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MergeParts'
      -- A replica that fetches instead of merging logs a failed fetch as MergeParts with no merge reason: not a merge.
      AND (error = 0 OR merge_reason != 'NotAMerge')
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    reason,
    countIf(ok) AS merge_count,
    countIf(ok AND isNotNull(cpu)) AS cpu_profiled_merges,
    -- 0 when there are no rows of that outcome; NULL only when they all lack counters.
    if(countIf(ok) = 0, 0, round(sumIf(cpu, ok), 3)) AS cpu_seconds,
    round(sumIf(elapsed, ok), 3) AS elapsed_seconds,
    round(100.0 * sumIf(cpu, ok) / nullIf(sum(sumIf(cpu, ok)) OVER (), 0), 2) AS cpu_share_pct,
    round(100.0 * sumIf(elapsed, ok) / nullIf(sum(sumIf(elapsed, ok)) OVER (), 0), 2) AS elapsed_share_pct,
    round(100.0 * countIf(ok) / nullIf(sum(countIf(ok)) OVER (), 0), 2) AS merge_share_pct,
    countIf(NOT ok) AS failed_attempts,
    if(countIf(NOT ok) = 0, 0, round(sumIf(cpu, NOT ok), 3)) AS failed_cpu_seconds,
    if(countIf(NOT ok) = 0, 0, round(100.0 * sumIf(cpu, NOT ok) / nullIf(sum(cpu), 0), 1)) AS failed_cpu_pct,
    round(sumIf(elapsed, NOT ok), 3) AS failed_elapsed_seconds,
    sum(countIf(ok)) OVER () AS total_merge_count,
    round(sum(sumIf(elapsed, ok)) OVER (), 3) AS total_elapsed_seconds,
    round(sum(sumIf(cpu, ok)) OVER (), 3) AS total_cpu_seconds
FROM merge_costs
GROUP BY reason
ORDER BY sum(cpu) DESC NULLS LAST, sum(elapsed) DESC`,
  `-- @meta: title='Table Merge Cost Breakdown' group='Merge Analytics' interval='1 DAY' description='Merge costs by outcome, level, result size, and reason, ranked by CPU. Failed attempts write no part, so they have no result size; merge_count counts attempts on failed rows.'
-- @cell: column=cpu_share_pct type=gauge max=100 unit=%
-- @cell: column=elapsed_share_pct type=gauge max=100 unit=%
-- Completed merges (event_type = 'MergeParts'), including work on each replica.
-- outcome: succeeded (error = 0) or failed (error != 0). Each retry of a failed merge adds a row.
-- CPU: core-seconds of user + system time. ClickHouse omits zero counters from ProfileEvents,
-- so a missing counter counts as 0; CPU is NULL only when the row has no counters at all.
-- cpu_coverage_pct: percentage of rows with counters.
-- Shares and totals cover successful and failed rows for the selected table and time range, before LIMIT.
-- Size buckets use compressed result bytes. CPU per input GiB uses uncompressed bytes read,
-- restricted to rows with CPU counters (failed attempts count what they read before failing).
WITH merge_costs AS (
    SELECT
        if(error = 0, 'succeeded', 'failed') AS outcome,
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
            error != 0, 'no part (failed)',
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
      -- A replica that fetches instead of merging logs a failed fetch as MergeParts with no merge reason: not a merge.
      AND (error = 0 OR merge_reason != 'NotAMerge')
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    outcome,
    level_transition,
    size_bucket,
    reason,
    count() AS merge_count,
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
    round(sum(sum(cpu)) OVER (), 3) AS total_cpu_seconds
FROM merge_costs
GROUP BY outcome, level_transition, size_bucket, reason
ORDER BY cpu_seconds DESC NULLS LAST, elapsed_seconds DESC
LIMIT 200`,
  `-- @meta: title='Merge Failures by Table and Error' group='Merge Analytics' interval='1 DAY' description='CPU spent on failed merge attempts per table, split by why they failed. Server memory limit means the whole server was at max_server_memory_usage, so merges of any size fail; Merge memory limit means the merge itself was too big. Click a table for its failing merges.'
-- @chart: type=stacked_bar group_by=tbl value=failed_cpu_seconds series=failure_kind unit=s style=2d
-- @summary: value=total_failed_cpu_seconds label='Failed CPU' unit=s
-- @summary: value=total_failed_attempts label='Failed attempts'
-- @drill: on=tbl into='Top Failing Merges'
-- @cell: column=failed_cpu_share_pct type=gauge max=100 unit=%
-- One row per table and failure kind. Failed attempts only (error != 0), including work on each replica.
-- CPU: user + system time (omitted counters are 0); NULL only when the rows have no counters.
-- merges: distinct result parts that failed. Shares and totals cover the selected table and time range, before LIMIT.
WITH failures AS (
    SELECT
        concat(database, '.', table) AS tbl,
        part_name,
        event_time,
        duration_ms / 1000.0 AS elapsed,
        if(empty(ProfileEvents), NULL,
            (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu,
        multiIf(
            error = 0, 'succeeded',
            -- The whole server reached max_server_memory_usage: any merge asking for memory then fails, small ones too.
            -- Wording by version: 'Memory limit (total)' before 24.11, 'Global memory limit' in 24.11, '(total) memory limit' after.
            error = 241 AND (position(exception, '(total) memory limit') > 0
                OR position(exception, 'Global memory limit') > 0
                OR position(exception, 'Memory limit (total)') > 0), 'Server memory limit',
            -- The merge's own memory tracker.
            error = 241 AND position(exception, 'Background process (mutate/merge) memory limit') > 0, 'Merge memory limit',
            error = 241, 'Other memory limit',
            errorCodeToName(error)
        ) AS failure_kind
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MergeParts'
      -- A replica that fetches instead of merging logs a failed fetch as MergeParts with no merge reason: not a merge.
      AND (error = 0 OR merge_reason != 'NotAMerge')
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND error != 0
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    tbl,
    failure_kind,
    count() AS failed_attempts,
    uniqExact(part_name) AS merges,
    round(sum(cpu), 3) AS failed_cpu_seconds,
    round(sum(elapsed), 3) AS failed_elapsed_seconds,
    round(100.0 * sum(cpu) / nullIf(sum(sum(cpu)) OVER (), 0), 2) AS failed_cpu_share_pct,
    max(event_time) AS last_failure,
    sum(count()) OVER () AS total_failed_attempts,
    round(sum(sum(cpu)) OVER (), 3) AS total_failed_cpu_seconds
FROM failures
GROUP BY tbl, failure_kind
ORDER BY sum(sum(cpu)) OVER (PARTITION BY tbl) DESC NULLS LAST, tbl, failed_cpu_seconds DESC NULLS LAST, failure_kind
LIMIT 100`,
  `-- @meta: title='Top Failing Merges' group='Merge Analytics' interval='1 DAY' description='Merges that failed at least once, ranked by CPU spent on failed attempts. Each retry of the same merge is a new attempt; many attempts with the same error mean the merge keeps redoing the same work. Click a bar for its attempts, or a part name to open the merge on the Merges page.'
-- @chart: type=bar group_by=merge value=failed_cpu_seconds unit=s style=2d
-- @summary: value=total_failed_cpu_seconds label='Failed CPU' unit=s
-- @summary: value=total_failed_attempts label='Failed attempts'
-- @drill: on=merge into='Merge Attempts'
-- @merge_link: on=part_name database=database table=table part=part_name host=last_failure_host
-- One row per merge (table + result part name), summed over replicas and retries.
-- CPU: user + system time (omitted counters are 0); NULL only when the failed rows have no counters.
-- last_failure_kind, last_error and last_exception describe the latest failed attempt.
-- max_merge_peak_memory is the merge's own peak; a Server memory limit failure is caused by the server total, not by it.
-- succeeded: whether any attempt of this merge succeeded in the selected time range.
-- The part_name link opens the latest record of the merge on last_failure_host, the replica of the latest failure.
-- Totals cover the selected table and time range, before LIMIT.
WITH attempts AS (
    SELECT
        database,
        table,
        part_name,
        hostName() AS host,
        error,
        exception,
        event_time,
        event_time_microseconds,
        peak_memory_usage,
        duration_ms / 1000.0 AS elapsed,
        if(empty(ProfileEvents), NULL,
            (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu,
        multiIf(
            error = 0, 'succeeded',
            -- The whole server reached max_server_memory_usage: any merge asking for memory then fails, small ones too.
            -- Wording by version: 'Memory limit (total)' before 24.11, 'Global memory limit' in 24.11, '(total) memory limit' after.
            error = 241 AND (position(exception, '(total) memory limit') > 0
                OR position(exception, 'Global memory limit') > 0
                OR position(exception, 'Memory limit (total)') > 0), 'Server memory limit',
            -- The merge's own memory tracker.
            error = 241 AND position(exception, 'Background process (mutate/merge) memory limit') > 0, 'Merge memory limit',
            error = 241, 'Other memory limit',
            errorCodeToName(error)
        ) AS failure_kind
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MergeParts'
      -- A replica that fetches instead of merging logs a failed fetch as MergeParts with no merge reason: not a merge.
      AND (error = 0 OR merge_reason != 'NotAMerge')
      AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    concat(database, '.', table, ':', part_name) AS merge,
    part_name,
    countIf(error != 0) AS failed_attempts,
    round(sumIf(cpu, error != 0), 3) AS failed_cpu_seconds,
    round(sumIf(elapsed, error != 0), 3) AS failed_elapsed_seconds,
    -- event_time has second resolution; retries within one second would tie.
    argMaxIf(failure_kind, event_time_microseconds, error != 0) AS last_failure_kind,
    errorCodeToName(argMaxIf(error, event_time_microseconds, error != 0)) AS last_error,
    substring(argMaxIf(exception, event_time_microseconds, error != 0), 1, 300) AS last_exception,
    formatReadableSize(maxIf(peak_memory_usage, error != 0)) AS max_merge_peak_memory,
    if(countIf(error = 0) > 0, 'yes', 'no') AS succeeded,
    uniqExact(host) AS replicas,
    minIf(event_time, error != 0) AS first_failure,
    maxIf(event_time, error != 0) AS last_failure,
    argMaxIf(host, event_time_microseconds, error != 0) AS last_failure_host,
    database,
    table,
    sum(countIf(error != 0)) OVER () AS total_failed_attempts,
    round(sum(sumIf(cpu, error != 0)) OVER (), 3) AS total_failed_cpu_seconds
FROM attempts
GROUP BY database, table, part_name
HAVING failed_attempts > 0
ORDER BY failed_cpu_seconds DESC NULLS LAST, failed_attempts DESC, merge
LIMIT 30`,
  `-- @meta: title='Merge Attempts' group='Merge Analytics' interval='1 DAY' description='Every attempt of one merge, oldest first. Open merge shows it on the Merges page. Attempts that keep failing with Server memory limit will keep failing until the server has memory to spare; Merge memory limit means the merge itself is too big.'
-- @chart: type=bar group_by=attempt value=cpu_seconds unit=s style=2d
-- @merge_link: database=database table=table part=part_name
-- One row per attempt on each replica. CPU: user + system time; NULL only when the row has no counters.
-- merge_peak_memory is the merge's own peak; a Server memory limit failure is caused by the server total, not by it.
-- Open merge (database, table, part_name) appears when every row is the same merge, i.e. after the drill;
-- it names no replica because the attempts span them, so the Merges page opens the latest record on any.
WITH attempts AS (
    SELECT
        database,
        table,
        part_name,
        hostName() AS host,
        event_time_microseconds,
        error,
        exception,
        duration_ms / 1000.0 AS elapsed,
        peak_memory_usage,
        read_bytes,
        if(empty(ProfileEvents), NULL,
            (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6) AS cpu,
        multiIf(
            error = 0, 'succeeded',
            -- The whole server reached max_server_memory_usage: any merge asking for memory then fails, small ones too.
            -- Wording by version: 'Memory limit (total)' before 24.11, 'Global memory limit' in 24.11, '(total) memory limit' after.
            error = 241 AND (position(exception, '(total) memory limit') > 0
                OR position(exception, 'Global memory limit') > 0
                OR position(exception, 'Memory limit (total)') > 0), 'Server memory limit',
            -- The merge's own memory tracker.
            error = 241 AND position(exception, 'Background process (mutate/merge) memory limit') > 0, 'Merge memory limit',
            error = 241, 'Other memory limit',
            errorCodeToName(error)
        ) AS failure_kind
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type = 'MergeParts'
      -- A replica that fetches instead of merging logs a failed fetch as MergeParts with no merge reason: not a merge.
      AND (error = 0 OR merge_reason != 'NotAMerge')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND concat(database, '.', table, ':', part_name) = {{drill_value:merge | concat(database, '.', table, ':', part_name)}}
      AND concat(database, '.', table) = {{drill_value:tbl | concat(database, '.', table)}}
)
SELECT
    concat(toString(event_time_microseconds), ' ', host) AS attempt,
    host,
    if(error = 0, 'succeeded', errorCodeToName(error)) AS outcome,
    failure_kind,
    round(cpu, 3) AS cpu_seconds,
    round(elapsed, 3) AS elapsed_seconds,
    formatReadableSize(peak_memory_usage) AS merge_peak_memory,
    formatReadableSize(read_bytes) AS input_read,
    substring(exception, 1, 300) AS exception,
    database,
    table,
    part_name
FROM attempts
ORDER BY event_time_microseconds, host
LIMIT 500`,
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
