# CPU Metrics

## Source: `system.asynchronous_metric_log`

All CPU timeseries (Overview trends, Time Travel server line) read from `asynchronous_metric_log`, which collects OS-level CPU via `/proc/stat` (host) or `/sys/fs/cgroup/cpu.stat` (container). This captures **all** process CPU: queries, merges, mutations, replication, internal scheduler, retry loops.

The SQL auto-detects the environment:

```sql
if(
  countIf(metric IN ('CGroupUserTime', 'CGroupSystemTime')) > 0,
  sumIf(value, metric IN ('CGroupUserTime', 'CGroupSystemTime')),
  sumIf(value, metric IN ('OSUserTime', 'OSSystemTime'))
) * 1000000 AS cpu_us
```

CGroup metrics are preferred when present (even if zero). OS metrics are the fallback.

## What you get per deployment

| Deployment | Numerator | Denominator | Accuracy |
| --- | --- | --- | --- |
| k8s / Docker | CGroupUserTime + SystemTime (container) | CGroupMaxCPU (e.g. 3 cores) | Correct |
| Dedicated Linux server | OSUserTime + SystemTime (host) | NumberOfCPUCores (e.g. 12) | Correct (host ≈ ClickHouse) |
| Shared Linux server | OSUserTime + SystemTime (all procs) | NumberOfCPUCores | Inflated, includes non-CH CPU |
| macOS (dev) | OSUserTime + SystemTime (entire Mac) | NumberOfCPUCores | Inflated, includes Chrome etc. |

## Formula

`asynchronous_metric_log` values are already per-second rates (seconds of CPU per second of wall time). The SQL converts to microseconds (`× 1,000,000`) with a fixed `interval_ms = 1000` for pipeline compatibility.

```
cpu_us_per_second = (CGroupUserTime + CGroupSystemTime) × 1,000,000
cpu_percentage = cpu_us_per_second / (effective_cores × 1,000,000) × 100
```

Where `effective_cores` = `CGroupMaxCPU` when available and less than host cores, otherwise `NumberOfCPUCores`. In code: `cgroupCpu > 0 && cgroupCpu < hostCores ? cgroupCpu : hostCores`.

> **Tests:** `metrics-collector.integration.test.ts` → "CPU usage calculation"
> Covers: 50% on 4 cores, clamping >100%, single core, CGroup preference over host cores, CGroup absent fallback, bogus CGroup ignored, zero cores default.

## Clamping

CPU is clamped to 100% in most paths (`Math.min(100, ...)` in `metrics-collector.ts`, `overview-service.ts`; `Math.min(v, cores × 1e6)` in `timeline-service.ts`). Exception: `getCpuSpikeAnalysis()` preserves raw values so spike detection can classify >100% anomalies.

## CPU core count in k8s

ClickHouse's `NumberOfCPUCores` reports **host** cores, not the pod's cgroup limit. A 4-core pod on a 96-core node reports 96.

Detection priority:
1. `CGroupMaxCPU` from `system.asynchronous_metrics` (CH >= 23.8). Returns the cgroup float limit directly (e.g. `4.0`). Returns `0` when no limit.
2. `max_threads` from `system.settings`. CH auto-detects cgroup quota at startup. Only trusted when ≤ 256 (can be overridden by config).
3. Fallback: `NumberOfCPUCores` / `NumberOfPhysicalCores` (bare metal / VM).

The Engine Internals CPU Core Map aggregates N host-level per-core metrics into M effective cgroup slots proportionally, and shows a `cgroup: N/M` badge.

## Overview Trends vs Snapshot vs Attribution

The app uses three different CPU calculations depending on context:

| Context | Source | What it is |
| --- | --- | --- |
| **Trends chart** | `asynchronous_metric_log` CGroup/OS metrics | Historical timeseries, OS-level |
| **Snapshot card** | `asynchronous_metrics.LoadAverage1 / PhysicalCores` | 1-minute smoothed average, capped at 100% |
| **Attribution bar** | `asynchronous_metrics.OSUserTimeNormalized + OSSystemTimeNormalized` | Instantaneous, pre-normalized to [0..1] per core |

The snapshot and attribution values can diverge during load transitions because LoadAverage1 is smoothed over 60s while `OSUserTimeNormalized` is instantaneous.

Snapshot fallback (if load average unavailable):
```
cpu_pct = (OSUserTime + OSSystemTime) / (OSUserTime + OSSystemTime + OSIdleTime) × 100
```

## Why we switched from metric_log (March 2026)

Previously, CPU timeseries used `metric_log.ProfileEvent_OSCPUVirtualTimeMicroseconds`. This captures query thread and completed merge CPU via ClickHouse's ProfileEvent instrumentation, but misses internal scheduler retry loops, replication coordination, ZooKeeper communication, and thread pool overhead.

On a k8s cluster with a merge retry loop, `metric_log` showed ~3% while `asynchronous_metric_log` showed ~34%. The new source reads from the OS kernel and captures all process CPU regardless of instrumentation. During normal heavy insert/merge workloads, both methods converge since most CPU is on instrumented paths. The gap appears in pathological cases (retry loops, replication churn).

| Source | Table | Captures | Misses |
| --- | --- | --- | --- |
| CGroupUser + SystemTime | `asynchronous_metric_log` | All container CPU | N/A |
| OSUser + SystemTime | `asynchronous_metric_log` | All host CPU | Can't distinguish CH from other procs |
| ProfileEvent_OSCPUVirtualTimeMicroseconds | `metric_log` | Instrumented threads only | Scheduler, retries, replication, ZK |
