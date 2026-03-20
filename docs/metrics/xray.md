# X-Ray (Query & Merge)

Real-time and historical 3D visualization of query/merge resource consumption over time.

## Data Source

`tracehouse.processes_history` (query X-Ray) and `tracehouse.merges_history` (merge X-Ray) — sampled by the TraceHouse polling loop.

For distributed queries, the SQL uses `WHERE query_id = {id} OR initial_query_id = {id}` against `clusterAllReplicas(tracehouse.processes_history)` to capture data from the coordinator and all shard sub-queries.

## Metrics

All delta metrics are normalized to per-second rates regardless of the sampling interval.

### Cumulative (running totals at each sample)

| Metric | Source Column | Unit |
| --- | --- | --- |
| Memory | `memory_usage / 1048576` | MB |
| Peak Memory | `peak_memory_usage / 1048576` | MB |
| Thread Count | `length(thread_ids)` | count |
| Read Rows | `read_rows` | rows |
| Read Bytes | `read_bytes` | bytes |
| CPU Time | `ProfileEvents['OSCPUVirtualTimeMicroseconds']` | µs |
| I/O Wait | `ProfileEvents['OSCPUWaitMicroseconds']` | µs |
| Network Send/Recv | `ProfileEvents['NetworkSendBytes' / 'NetworkReceiveBytes']` | bytes |

### Delta (per-second rates, computed via `lagInFrame` window)

| Metric | Formula | Unit |
| --- | --- | --- |
| CPU Cores | `Δ(cpu_us) / 1e6 / Δt` | cores |
| I/O Wait | `Δ(io_wait_us) / 1e6 / Δt` | seconds/second |
| Read Throughput | `Δ(read_bytes) / 1048576 / Δt` | MB/s |
| Read Row Rate | `Δ(read_rows) / Δt` | rows/s |
| Network Send/Recv | `Δ(net_bytes) / 1024 / Δt` | KB/s |

Negative deltas (from counter resets) are clamped to 0 via `greatest(..., 0)`.

## 3D Corridor Visualization

The X-Ray renders a 3D corridor where:
- **X-axis** = time (seconds since query start)
- **Width** = CPU cores used (`d_cpu_cores`)
- **Height** = memory MB (`memory_mb`)

Additional metrics (I/O, read throughput, network) are shown as 2D timeline charts below the corridor.

## Multi-Host Behavior (Distributed Queries)

### Per-host query

The host-aware variant groups `BY hostname, t`. Window functions compute deltas `PARTITION BY hostname, query_id` (correct per-stream deltas), then the outer aggregation sums across query_ids within each host.

### Frontend aggregation

- **"ALL" mode**: `aggregateHostSamples()` sums all metrics across hosts at 0.5s time buckets. Totals are correct — no double-counting.
- **Per-host mode**: shows only that host's data.

### Co-located coordinator + shard

When the coordinator and a shard run on the same node, their samples are computed as separate delta streams (correct) then summed per hostname. The per-host view for that node shows combined coordinator + shard load. The coordinator cannot be isolated in X-Ray.

## Merge X-Ray

Same architecture, different source table (`tracehouse.merges_history`). Shows:
- Merge progress (0–100%)
- Read/write throughput (MB/s, rows/s)
- Memory usage during merge
- Correlated text log events on the timeline
