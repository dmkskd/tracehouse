# Aiven for ClickHouse

TraceHouse works with [Aiven for ClickHouse](https://aiven.io/clickhouse), but some features operate in degraded mode due to system table restrictions on the managed platform.

## Connection

Aiven doesn't return CORS headers, so you need the bundled proxy:

```bash
just proxy-start
```

See [Connecting](./connecting.md) for full setup details.

## Known Limitations

Aiven restricts access to several system log tables. The following are **not available** on Aiven for ClickHouse (as of March 2026):

| Unavailable System Table | Impact |
|---|---|
| `metric_log` | No native server metrics timeseries (CPU, memory, network, disk IO) |
| `asynchronous_metric_log` | No historical async metrics (RAM totals, CPU core counts from log) |
| `trace_log` | No flamegraphs or CPU sampling |
| `query_thread_log` | No per-thread query breakdown |
| `processors_profile_log` | No query pipeline profiling |
| `opentelemetry_span_log` | No distributed tracing spans |
| `crash_log` | No crash/fatal error records |
| `session_log` | No login/logout tracking |
| `zookeeper_log` | No ZooKeeper request log |
| `backup_log` | No backup operation history |
| `blob_storage_log` | No blob storage operation log |

Source: [Aiven documentation - System tables](https://aiven.io/docs/products/clickhouse/reference/clickhouse-system-tables)

### Time Travel - Degraded Mode

Without `metric_log`, the Time Travel page cannot show true server-level metrics. Instead, it operates in **degraded mode**:

- Charts display **estimated metrics synthesized from `query_log` ProfileEvents**
- CPU, memory, network, and disk IO values are derived by distributing each query's totals evenly across its duration
- **Gaps between queries appear as zero** - the chart only reflects periods where queries were actively running
- Values may **undercount actual server usage** since background processes (merges, system tasks) that don't appear in `query_log` are not captured
- Query, merge, and mutation timelines (the bands and tables) work normally since they use `query_log` and `part_log`

A warning banner and "⚠ Estimated" badge are shown when degraded mode is active.

### Features That Work Normally

These features use `query_log`, `part_log`, `system.parts`, and other available tables:

- Database Explorer (tables, parts, columns)
- Merge Tracker (merge history, merge timeline)
- Query Monitor (query history, running queries)
- Cluster Overview (topology, replica status)
- Engine Internals (part inspection, ordering key analysis)

### System Log TTL

Aiven sets a **1-hour TTL** on all system log tables. This means:

- Query history is limited to the last hour
- Time Travel can only look back ~1 hour
- Merge history is limited to recent events

You can work around this by creating materialized views to persist log data:

```sql
CREATE MATERIALIZED VIEW my_query_log
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY event_time
AS SELECT * FROM system.query_log;
```

### Capability Detection

TraceHouse automatically detects which system tables are available on connection. The Capabilities panel (visible in the connection sidebar) shows exactly what's available and what's missing on your Aiven instance.
