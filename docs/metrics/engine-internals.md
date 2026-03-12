# Engine Internals

## CPU Sampling Attribution

Real CPU consumption breakdown by thread pool, based on kernel profiler samples. This is the primary tool for diagnosing unattributed CPU — when the Time Travel server line shows high CPU but bands don't explain it (common during heavy merge/insert workloads where in-flight merges lack CPU data).

**Source:** `system.trace_log` with `trace_type = 'CPU'`
**Window:** 180 seconds (to account for trace_log flush lag under load)

```sql
SELECT thread_name, count() AS cpu_samples
FROM system.trace_log
WHERE event_time >= now() - INTERVAL 180 SECOND
  AND trace_type = 'CPU'
GROUP BY thread_name
ORDER BY cpu_samples DESC
```

**Thread pool classification:**

| Pattern | Category |
| --- | --- |
| `QueryPipelineEx`, `QueryPool`, `Parallel` | Queries |
| `Merge` (not `Mutat`) | Merges |
| `Mutat` | Mutations |
| `Fetch`, `Replic`, `Repl` | Replication |
| `IO`, `Disk`, `Read`, `Write` | IO |
| `Sched`, `BgSch` | Schedule |
| `HTTP`, `TCP`, `Handler` | Handlers |
| Everything else | Other |

Uses `trace_type = 'CPU'` only (not `'Real'`) because the CPU profiler (`CLOCK_THREAD_CPUTIME_ID`) samples only when a thread is actually consuming CPU. `Real` (`CLOCK_MONOTONIC`) samples wall-clock time, so sleeping background pools dominate and give a misleading picture.

The 180s window ensures at least one flush cycle is captured. Under heavy load, `SystemLogFlush` gets starved and actual flush intervals stretch to 30-60s+.

Top functions require `allow_introspection_functions = 1` for `demangle()` and `addressToSymbol()`.

## Core Timeline

A swimlane visualization showing what each physical CPU core was doing over time.

**Source:** `system.trace_log` with `trace_type IN ('CPU', 'Real')`
**Window:** 60s default (options: 30s, 60s, 180s)

Samples are bucketed into 100ms time slots per core. For each (core, slot), the dominant thread (highest sample count) is displayed.

**CPU vs Real modes:**

| Mode | Shows |
| --- | --- |
| CPU+Real (default) | Full core activity. Real-only slots rendered with diagonal stripes. |
| CPU only | Strict on-CPU execution. IO-bound/sleeping threads excluded. |

Each slot has `cpu_samples` and `real_samples` counts:
- **CPU only** = confirmed on-CPU execution (solid color)
- **Real only** = wall-clock activity, may include IO wait (striped)
- **Mixed** = both types (solid color, most common)

Both types matter because under heavy saturation, the kernel delays `SIGPROF` delivery, reducing CPU samples exactly when you need them most. Real samples fill the gaps, though they also fire for threads in `futex_wait` or `epoll_wait`.

**k8s core IDs:** `cpu_id` comes from `sched_getcpu()` and returns the **host** physical core number. Linux cgroups limit CPU time but don't virtualize core numbering. A 3-core pod on a 12-core host sees scattered non-contiguous core IDs (e.g. 1, 2, 5, 6, 10) as the scheduler migrates threads. You'll never see more than N cores active simultaneously (N = cgroup limit), but specific IDs change over time. The UI shows an amber "(host IDs)" indicator when cgroup limiting is detected.

**What this reveals:** core contention, NUMA locality, OS scheduler behavior, temporal patterns (merge bursts, query waves, replication storms).

## trace_log flush lag

`trace_log` data is buffered in memory and flushed by `SystemLogFlush` (default interval ~7.5s). Under heavy CPU load, this thread gets starved and flush intervals stretch to 30-60s+. You may see zero samples in the most recent 30s during sustained spikes. Once load drops, a burst of delayed samples appears.

Don't use `SYSTEM FLUSH LOGS` as a workaround: it blocks under load and makes things worse.

Mitigations: Core Timeline defaults to 60s window, CPU Sampling uses 180s window. The UI shows "No CPU samples" with a hint when no data is available.

## Memory Fragmentation

```
fragmentation_pct = (1 - MemoryAllocated / MemoryResident) × 100
```

From `system.asynchronous_metrics`. High values indicate allocator fragmentation causing higher-than-expected memory usage.

## Thread Pool Saturation

```
is_saturated = (active_tasks / max_threads) > 0.8
```

From `system.metrics` (`BackgroundPoolTask` / `BackgroundPoolSize`). >80% utilization indicates a potential bottleneck.

> **Tests:** `merge-tracker.integration.test.ts` → "getBackgroundPoolMetrics"
