/** Completed-merge cost attribution. Keep one row per merge: expanding merged_from
 * would multiply CPU and elapsed time by the number of source parts. */
const mergeCosts = `-- Successful completed merges, including work on each replica. Excludes mutations and active merges.
-- CPU: core-seconds from OSCPUVirtualTimeMicroseconds, with user + system time as fallback.
-- cpu_coverage_pct: percentage of merges with CPU counters. Missing counters remain NULL.
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
        multiIf(
            mapContains(ProfileEvents, 'OSCPUVirtualTimeMicroseconds'),
                ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6,
            mapContains(ProfileEvents, 'UserTimeMicroseconds') AND mapContains(ProfileEvents, 'SystemTimeMicroseconds'),
                (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6,
            NULL
        ) AS cpu,
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
      AND merge_reason != 'NotAMerge'
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
${dimension === 'tbl' ? 'LIMIT 30' : ''}`;
}

const queries: string[] = [
  costQuery('Merge CPU by Table', 'tbl', 'cpu_seconds', 'Tables ranked by total merge CPU. Click a table for the cost breakdown.', "-- @drill: on=tbl into='Table Merge Cost Breakdown'"),
  costQuery('Merge Elapsed Time by Table', 'tbl', 'elapsed_seconds', 'Tables ranked by total merge duration.', "-- @drill: on=tbl into='Table Merge Cost Breakdown'"),
  costQuery('Merge CPU by Level Transition', 'level_transition', 'cpu_seconds', 'CPU by source → result part level. Mixed source levels appear as a range.'),
  costQuery('Merge Elapsed Time by Level Transition', 'level_transition', 'elapsed_seconds', 'Total duration of completed merges, grouped by level transition.'),
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
];

export default queries;
