# X-Ray (Query & Merge)

Real-time and historical 3D visualization of query/merge resource consumption over time.

## Data Source

The query X-Ray reads **either** of two tables; the merge X-Ray always reads
`tracehouse.merges_history`.

| Source | Install | Serves | Cannot provide |
|---|---|---|---|
| `tracehouse.processes_history` | `setup_sampling.sh` | everything, at 1s resolution | - |
| `system.query_metric_log` | built in, CH 24.10+ | memory, CPU, wait and network curves, at `query_metric_log_interval` resolution, from the start of pipeline execution | thread counts, real progress counters, anything spent in query analysis |

The choice is made in one place: `selectQueryXRaySource()` in
`packages/core/src/types/xray-source.ts`. It is a pure function of three inputs
(what the connection has, the user's preference, whether the query is still
running) and returns the selection plus what that source could not supply. No
component reimplements the rule.

For distributed queries, the sampler SQL uses
`WHERE query_id = {id} OR initial_query_id = {id}` against
`clusterAllReplicas(tracehouse.processes_history)` to capture the coordinator
and all shard sub-queries.

### Reading from `system.query_metric_log`

[`system.query_metric_log`](https://clickhouse.com/docs/operations/system-tables/query_metric_log)
is the per-query counterpart of `system.metric_log`: it samples `memory_usage`,
`peak_memory_usage` and every ProfileEvent every `query_metric_log_interval` ms
(default 1000, settable per query, sub-second capable), plus a final row when
the query finishes. It is cheaper than the sampler on every axis - no
process-list scan, no repeated query text, and `ProfileEvent_*` as real columns
instead of a `Map` that must be read whole to extract one key.

**Its `ProfileEvent_*` columns are per-interval deltas, not cumulative
counters** - the same convention as `system.metric_log`, and the opposite of
`system.processes`. Verified on a live server: summing one query's 410 rows
reproduced its `system.query_log` total of 884.285051 CPU-seconds exactly. So
rates are read straight from the stored value, and the cumulative columns the
X-Ray contract expects are rebuilt with a running `sum() OVER w`. Differencing
them the way the sampler must would subtract two independent deltas and yield
noise around zero. `memory_usage` and `peak_memory_usage` are gauges, read
directly.

It carries only `query_id`, `hostname`, the event timestamps and the two memory
gauges (newer servers add `clickhouse_version` and `system_processor`, which the
X-Ray does not read, so the SQL runs unchanged back to 24.10). Everything else is
recovered by joining `system.query_log`:

| Needed | Recovered how |
|---|---|
| `initial_query_id` | `query_log` has it. `log_queries_min_type` defaults to `QUERY_START`, so the join resolves a query before it finishes, not only afterwards. |
| `peak_threads_usage` | `query_log`, as one scalar per query, read with `max()` rather than `argMax(..., event_time)`: `event_time` is second-resolution, so a short query's QueryStart and QueryFinish rows tie and argMax can return the QueryStart zero, silently disabling the clamp. Restores a real ceiling for the rate clamp, coarser than the sampler's per-interval bound. Absent until the query finishes, so a running query's rates are unclamped and the UI says so. |
| `elapsed` | `min(event_time_microseconds)` from `query_log` marks query start. Measuring from the first sample instead would zero the offset that `XRayTab` uses to align trace_log windows and the flamegraph. |

Two things no join recovers, declared in `QueryXRaySourceSelection.missing` and
dropped by the UI rather than approximated:

- **Thread counts.** Not exposed per sample anywhere.
- **Progress counters.** `system.processes.read_rows` / `read_bytes` are
  progress fields; the nearest counters are `ProfileEvent_SelectedRows` /
  `ProfileEvent_SelectedBytes`, which mean rows and bytes selected from parts.
  Close, not equal, so the read chart is hidden rather than relabelled.

Merges are absent entirely: `query_metric_log` is query-keyed and background
merges have no query context, so `merges_history` over `system.merges` remains
the only source for the merge X-Ray.

The overlapping metrics are the same numbers: `ProfileEvents['X']` in the
sampler and `ProfileEvent_X` here are the same query-scoped counter.

### Behaviour

| Situation | Source used |
|---|---|
| Both available, query finished | `system.query_metric_log` (`AUTO_PREFERRED_SOURCE`) |
| Both available, query running | `tracehouse.processes_history` - fresher, and the only one with a clamp ceiling mid-flight |
| Sampler not installed | `system.query_metric_log`, including for running queries |
| `query_metric_log` absent or pre-24.10 | `tracehouse.processes_history` |
| Neither | X-Ray reports what to install |

Users can pin a source from the badge in the X-Ray summary bar; `auto` is the
default. The badge names the active table and its tooltip gives the reason, the
unavailable fields, and the log flush lag when it applies.

Freshness is a disclosed property, not a gate: `query_metric_log` and
`query_log` flush on `flush_interval_milliseconds` (7500 in the stock config),
reported as `lagMs`. It does not disqualify the source.

### Caveats specific to this source

- **It covers pipeline execution, not the whole query.** The periodic collector
  is started by `QueryMetricLog::startQuery()`, called from `logQueryStart()`,
  which `executeQuery.cpp` reaches only after `interpreter->execute()`. Work done
  during interpretation is therefore never sampled, and scalar subqueries are
  evaluated during interpretation. `QueryMetricLogStatus::scheduleNext()` seeds
  the first deadline at *query start* plus one interval, so by then it is already
  stale, and the missed ticks are skipped rather than backfilled
  (`/// Skipping lost runs`, `next_collect_time = now`, `schedule()`): one
  collection runs immediately, carrying the entire elapsed query as a single
  interval delta, and `finishQuery()` adds the closing row.

  Measured on 26.8.2 with `query_metric_log_interval` at the default 1000ms:

  | Query | Duration | Rows | Span covered |
  |---|---|---|---|
  | `SELECT (SELECT count() FROM numbers_mt(30e9))` | 4052ms | 2 | 0.001s |
  | `SELECT count() FROM numbers_mt(30e9)` | 3800ms | 4 | 2.799s |

  Same work, same runtime; the only difference is that the first one does it in a
  scalar subquery. With `--send_logs_level=test` the server says so directly:
  `should have already run at ...  Scheduling it right now` versus
  `Scheduling next collecting task ... in 999 ms`.

  Memory follows the same shape. A query holding 600MB inside its subqueries
  reports a few hundred KB here, because the collector first looks after that
  memory is released. `tracehouse.processes_history` polls `system.processes`,
  which holds the query for its entire lifetime, so the sampler draws the curve
  the metric log cannot. The X-Ray does not yet detect or disclose this; a query
  whose cost sits in analysis looks like one point on a metric-log timeline and
  like a full curve on the sampler.

- **Sample alignment.** Each query samples on its own schedule, so a coordinator
  and its shards never share a timestamp. The roll-up views group on a
  `t_bucket` whose width tracks the observed sampling interval, capped at 0.5s
  to match `aggregateHostSamples()`. It must never exceed the interval: a fixed
  0.5s bucket collapses several samples of the SAME query when
  `query_metric_log_interval` is sub-second, and the roll-up then sums them,
  multiplying memory and every rate. The per-host view keeps raw resolution,
  since one host shares one clock.
- **`query_log` scan bound.** The identity join is bounded by `event_date`
  around the known query start (`opts.startedAt`), falling back to a two-day
  lookback when the start time is unknown.
- **Clamp disclosure.** With no ceiling available, rates are left unclamped
  rather than zeroed, and the CPU chart shows `⚠ unclamped`. When a ceiling did
  apply, capped samples are marked individually on the chart.
- **The teardown interval.** `query_metric_log` writes a closing row at query
  finish, capturing the moment every pooled thread detaches and merges its
  accumulated time into one short interval. The sampler never sees this: the
  query leaves `system.processes` before the next tick. The raw rate there is a
  ceiling, not work, so the headline peak reports the peak *sustained* rate via
  `peakSustainedCores()` and excludes capped intervals. Measured on one 409s
  query: both sources then agree on average cores (2.18 vs 2.15) and sustained
  peak (4.74 vs 4.48), while total CPU from `query_metric_log` is exact
  (884.3s) and the sampler undercounts by the teardown it missed (876.7s).

Time Travel zoom, the X-Ray Series dashboard and `QueryComparisonPanel` select
through the same rule, via `buildXRayWindowSamplesSQL()` /
`buildSelectedXRayOverlaySQL()` and `buildXRaySamplesSQL()`. Self-monitoring
stays sampler-only, because it diagnoses sampler health. A comparison across
several queries pins one source for the whole set, or it would plot different
quantities on one axis.

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

- **"ALL" mode**: `aggregateHostSamples()` sums all metrics across hosts at 0.5s time buckets. Totals are correct - no double-counting.
- **Per-host mode**: shows only that host's data.

### Co-located coordinator + shard

When the coordinator and a shard run on the same node, their samples are computed as separate delta streams (correct) then summed per hostname. The per-host view for that node shows combined coordinator + shard load. The coordinator cannot be isolated in X-Ray.

## Merge X-Ray

Same architecture, different source table (`tracehouse.merges_history`). Shows:
- Merge progress (0–100%)
- Read/write throughput (MB/s, rows/s)
- Memory usage during merge
- Correlated text log events on the timeline

### Source preference scope

The **Query X-Ray source** preference applies to Query X-Ray, comparison timelines,
Time Travel zoom, and Series X-Ray overlays. Overrides persist per connection;
Grafana administrators supply a runtime default that users can override. Choosing
Default inherits that value, while explicit Auto uses automatic source selection.

Time Travel and Series normalize sampler counters and metric-log interval deltas
before calculating rates, preserve local query/host partitions, and average
sub-second samples before summing distributed executions. They retain samples
preceding the visible window so its first interval has the correct duration.

`query_metric_log` does not supply progress read/write bytes. Series explains the
unavailable sampled read-throughput metric; Time Travel disk mode retains the
query-log average estimate. CPU, memory, network, and wait metrics use the selected
source. Merge timelines and sampler health diagnostics continue to use their
specific sampler tables.
