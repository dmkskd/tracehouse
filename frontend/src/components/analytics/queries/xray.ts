/** Source-aware built-in overlays. Expanded before time and drill templates. */
const panels = [
  ['Query CPU Cores', 'cpu_cores', "ProfileEvents['OSCPUVirtualTimeMicroseconds']", 'CPU cores'],
  ['Query Memory', 'mem_mb', 'memory_usage', 'memory in MB'],
  ['Query read_bytes', 'read_mb_s', 'read_bytes', 'progress-byte throughput in MB/s (requires processes_history)'],
  ['Query CPU Wait', 'cpu_wait_s', "ProfileEvents['OSCPUWaitMicroseconds']", 'CPU run-queue wait in seconds/s'],
  ['Query Disk I/O Wait', 'io_wait_s', "ProfileEvents['OSIOWaitMicroseconds']", 'disk I/O wait in seconds/s'],
  ['Query Network Wait', 'net_wait_s', "ProfileEvents['NetworkReceiveElapsedMicroseconds']", 'network receive wait in seconds/s'],
];

const queries = panels.map(([title, metric, rank, description]) => `-- @meta: title='${title}' group='X-Ray' interval='1 HOUR' description='Per-second ${description} for the top 50 queries. Uses the Query X-Ray source preference. Filter by database/table; click a line to open its X-Ray.'
-- @chart: type=grouped_line group_by=t value=${metric} series=query_id style=2d render=overlay
-- @query_link: on=query_id
WITH top_q AS (
    SELECT query_id, min(query_start_time_microseconds) AS query_start
    FROM {{cluster_aware:system.query_log}}
    WHERE type = 'QueryFinish' AND is_initial_query = 1
      AND event_date >= toDate({{time_range}}) - 1
      AND event_time > {{time_range}}
      AND ({{drill_value:db  | ''}} = '' OR has(databases, {{drill_value:db  | ''}}))
      AND ({{drill_value:tbl | ''}} = '' OR has(tables, {{drill_value:tbl | ''}}))
    GROUP BY query_id
    ORDER BY max(${rank}) DESC
    LIMIT 50
)
{{query_xray_overlay:${metric}}}`);

export default queries;
