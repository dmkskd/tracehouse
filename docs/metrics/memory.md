# Memory Metrics

## Overview Trends: Memory Usage (%)

**Source:** `system.metric_log.CurrentMetric_MemoryTracking` (point-in-time tracked memory)
**Reference:** `system.asynchronous_metric_log.OSMemoryTotal` (host RAM) or cgroup limit (container)

```
cgroup_mem = COALESCE(CGroupMemoryTotal, CGroupMemoryLimit)  -- CH 26+ renamed the metric
effective_ram = cgroup_mem > 0 && cgroup_mem < OSMemoryTotal ? cgroup_mem : OSMemoryTotal
memory_pct = (MemoryTracking / effective_ram) × 100
```

> **Tests:** `metrics-collector.integration.test.ts` → "Memory tracking"
> Covers: raw memory_used/memory_total, percentage derivation (25% = 4 GiB / 16 GiB).

## Container / k8s memory awareness

`OSMemoryTotal` reports the host node's RAM, not the pod's limit. A pod with `limits.memory: 4Gi` on a 64 GiB host shows 64 GiB, making memory % appear artificially low.

The `EnvironmentDetector` checks both `CGroupMemoryTotal` (CH 26+) and `CGroupMemoryLimit` (CH 23.8–25.x) from `system.asynchronous_metrics`. When a cgroup limit is detected and is less than host RAM, it becomes the effective ceiling. Values >= 1e18 are ignored (CH returns ~2^63 when no limit is set).

Applied in: `overview-service.ts` (server info + attribution), `timeline-service.ts` (Y-axis scaling), `metrics-collector.ts` (snapshot), `engine-internals.ts` (memory X-ray), `query-analyzer.ts` (per-host RAM).

## Time Travel: Server Memory

**Source:** `system.metric_log.CurrentMetric_MemoryTracking`

The server memory line is a direct point-in-time value (no rate conversion needed). Per-query memory uses `peak_memory` from `query_log` / `processes`.

## Gotchas

1. `MemoryTracking` is ClickHouse's internally tracked memory, not total system memory usage.
2. `MemoryResident` (RSS) can exceed `MemoryTracking` due to allocator overhead, fragmentation, and memory-mapped files.
3. `CGroupMemoryLimit` was renamed to `CGroupMemoryTotal` in CH 26+. Always check both.
4. Both cgroup metrics return ~2^63 when no limit is set. Filter values >= 1e18.
