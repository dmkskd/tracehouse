# Query Monitor

Monitor running and historical queries with per-query resource attribution and anatomy breakdowns.

## Running Queries

Live view of all currently executing queries from `system.processes`:
- Query text and query ID
- Elapsed time and progress
- Memory usage
- CPU time (from ProfileEvents)
- Read rows and bytes
- User and client info

## Query Anatomy

For each query, the anatomy view breaks down:
- **Read profile** - rows read, bytes read, marks read
- **Write profile** - rows written, bytes written
- **CPU profile** - user time, system time, wait time
- **I/O profile** - disk reads, disk writes, network bytes
- **Cache hits** - mark cache, uncompressed cache, compiled expression cache

## Query Filter Bar

Filter queries by:
- Query type (SELECT, INSERT, ALTER, etc.)
- User
- Time range
- Resource consumption thresholds
- Table accessed

## Flamegraph

The query anatomy includes a flamegraph view for visualizing query execution.

:::note ClickHouse Cloud
On ClickHouse Cloud, introspection functions are disabled by default. You need to enable them before flamegraph data can be collected.

Per session:
```sql
SET allow_introspection_functions = 1;
```

To enable permanently, set it at the user or role level:
```sql
ALTER USER my_user SETTINGS allow_introspection_functions = 1;
-- or
ALTER ROLE my_role SETTINGS allow_introspection_functions = 1;
```
:::

## Historical Analysis

Query history from `system.query_log` with:
- Execution statistics and trends
- Slow query identification
- Resource consumption patterns over time
- Query fingerprinting and grouping
