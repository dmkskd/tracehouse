# Thread Breakdown

Per-thread resource attribution for a query, shown as a Gantt timeline or sortable table.

## Data Source

`system.query_thread_log` — one row per thread that participated in the query. Requires the `query_thread_log` system table to be enabled.

## Metrics Per Thread

| Metric | Source Column | Description |
| --- | --- | --- |
| Thread Name | `thread_name` | ClickHouse thread type (QueryPipelineEx, TCPHandler, etc.) |
| Thread ID | `thread_id` | OS thread ID |
| CPU Time | `ProfileEvents['OSCPUVirtualTimeMicroseconds']` | CPU µs consumed by this thread |
| I/O Wait | `ProfileEvents['OSCPUWaitMicroseconds']` | I/O wait µs |
| Peak Memory | `peak_memory_usage` | High-water memory for this thread |
| Read Rows | `read_rows` | Rows read by this thread |
| Read Bytes | `read_bytes` | Bytes read |
| Written Bytes | `written_bytes` | Bytes written |
| Duration | `query_duration_ms` | Thread wall time |

## Timeline View

Each thread is rendered as a horizontal bar:
- **Position**: computed from `event_time_microseconds - initial_query_start_time_microseconds - duration`
- **Width**: proportional to `query_duration_ms`
- **Color**: by thread name (QueryPipelineEx = blue, TCPHandler = orange, MergeTreeIndex = purple, etc.)

The time axis spans from 0 to max thread end offset.

## Thread Types

| Thread | Role |
| --- | --- |
| QueryPipelineEx | Main query pipeline execution threads |
| QueryPullPipeEx | Pull-model pipeline threads |
| TCPHandler | TCP protocol handler (receives query, sends results) |
| HTTPHandler | HTTP protocol handler |
| MergeTreeIndex | Index evaluation (primary key, skip indexes) |
