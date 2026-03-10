# Metrics Calculations Reference

This document describes all metrics calculations used in the TraceHouse, organized by page/feature.

> **Integration test coverage:** The core formulas are validated against a real ClickHouse instance
> via testcontainers. Run with `just test-core-integration` (or `npx vitest run --config vitest.integration.config.ts` from `packages/core`).
> Test files are in `packages/core/__tests__/integration/`. Sections below note which test covers each formula.

## Page Structure

The application has the following main pages:
- **Overview** - Real-time monitoring with resource attribution, running queries, active merges/mutations (combines former "Live View" functionality)
- **Engine Internals** - Memory X-Ray, CPU core map, thread pools, PK index, dictionaries
- **Databases** - Database/table explorer with part inspector
- **Time Travel** - Historical timeline with CPU/memory/network/disk metrics and query/merge/mutation overlays
- **Queries** - Query history and analysis
- **Merges** - Merge tracker with background pool visualization

---

## Overview Page - Trends Mode

### CPU Usage (%)

**What we want:** ClickHouse process CPU utilization as a percentage

**Source:** `system.metric_log.ProfileEvent_OSCPUVirtualTimeMicroseconds`

**Reference value:** `system.asynchronous_metrics.NumberOfCPUCores` (logical cores)

**Formula:**
```
cpu_percentage = (OSCPUVirtualTimeMicroseconds / (NumberOfCPUCores × 1,000,000 × interval_seconds)) × 100
```

> **Test coverage:** `metrics-collector.integration.test.ts` → "CPU usage calculation"
> - 50% on 4 cores, >100% on 2 cores (containerized), single-core scenario
> - Zero CPU cores fallback, first-row interval defaulting

**Explanation:**
- `OSCPUVirtualTimeMicroseconds` = CPU virtual time used by the ClickHouse process in microseconds per collection interval
- `NumberOfCPUCores` = logical CPU cores available to ClickHouse
- `1,000,000` = microseconds per second
- `interval_seconds` = time between metric_log rows (typically 1 second)

**Important: Raw values can exceed 100% — we clamp to 100%**

`ProfileEvent_OSCPUVirtualTimeMicroseconds` measures "virtual CPU time" which can exceed the theoretical maximum (cores × 1M µs/s) because:
- Under heavy load, `metric_log` collection can be delayed, causing accumulated CPU time to exceed the reported wall-clock interval
- On virtualized environments (Docker on macOS, VMs, GCP), hypervisor scheduling jitter can inflate timing
- It includes time spent waiting in kernel calls attributed to the process
- Child processes spawned by ClickHouse may contribute to the total

The historical metrics calculation clamps CPU usage to 100% via `Math.min(100, ...)` to avoid misleading spikes in the Time Travel chart. The CPU Spike Analysis feature (`getCpuSpikeAnalysis`) intentionally preserves unclamped values for diagnostic purposes.

**Root cause — metric_log collection delay under load:**

When ClickHouse is under heavy CPU-bound load (all cores saturated), the `metric_log` collection
thread itself can be delayed by the OS scheduler — it simply can't get scheduled on time. When the
load drops (e.g. queries are cancelled), the collection thread catches up and writes a new row.
The `dateDiff` between this row and the previous one may report a normal interval (e.g. 1 second,
since `event_time` is truncated to seconds), but the `OSCPUVirtualTimeMicroseconds` value in that
row has accumulated CPU time from a longer real-world period (potentially several seconds of heavy
work batched into one row). The formula then divides a large numerator by a small denominator,
producing values like 300–800%.

This effect is amplified on virtual machines (GCP, AWS, Docker on macOS) because the hypervisor
performs its own CPU scheduling. The guest OS doesn't always receive perfectly accurate CPU time
accounting — "stolen time" can be reconciled in bursts, and vCPU scheduling jitter compresses
what appears to be a lot of CPU work into a short wall-clock window.

**Where clamping is applied:**
- `metrics-collector.ts` → `getHistoricalMetrics()`: `Math.min(100, ...)` — used by Overview page trends
- `timeline-service.ts` → `getTimeline()`: `Math.min(v, cpuCores × 1_000_000)` — clamps raw µs/s values before sending to the Time Travel chart
- `overview-service.ts` → `calculateResourceAttribution()`: `Math.min(100, ...)` — used by Overview page
- `metrics-collector.ts` → `calculateCpuUsage()`: `Math.min(100, ...)` — used by Overview snapshot

**Where clamping is intentionally NOT applied:**
- `timeline-service.ts` → `getCpuSpikeAnalysis()`: preserves raw values so the spike analysis
  feature can detect and classify >100% anomalies for diagnostic purposes

**Gotchas:**
1. **Collection interval varies.** The first row in a query window has an invalid interval. We filter: `if(interval_ms > 0 AND interval_ms < 10000, interval_ms, 1000)` to default invalid intervals to 1 second.

2. **This is ClickHouse process CPU, not system-wide.** The metric only measures the ClickHouse server process, not other processes on the host.

---

### Memory Usage (%)

**What we want:** Server memory utilization as a percentage

**Source:** `system.metric_log.CurrentMetric_MemoryTracking`

**Reference value:** `system.asynchronous_metric_log.OSMemoryTotal` (host RAM), or `CGroupMemoryTotal` / `CGroupMemoryLimit` (container limit)

**Formula:**
```
cgroup_mem = COALESCE(CGroupMemoryTotal, CGroupMemoryLimit)  -- CH 26+ renamed the metric
effective_ram = cgroup_mem > 0 && cgroup_mem < OSMemoryTotal
  ? cgroup_mem
  : OSMemoryTotal

memory_percentage = (MemoryTracking / effective_ram) × 100
```

> **Test coverage:** `metrics-collector.integration.test.ts` → "Memory tracking"
> - Raw memory_used/memory_total values, percentage derivation (25% = 4 GiB / 16 GiB)

**Important: Container / Kubernetes memory awareness**

`OSMemoryTotal` reports the host node's total RAM, not the pod's cgroup memory limit. A pod with `resources.limits.memory: 4Gi` on a 64 GiB host will report 64 GiB, making memory percentage appear artificially low (e.g. 6% instead of 95%).

The `EnvironmentDetector` resolves this by checking both `CGroupMemoryTotal` (ClickHouse 26+) and `CGroupMemoryLimit` (ClickHouse 23.8–25.x) from `system.asynchronous_metrics`. The metric was renamed in ClickHouse 26; all queries check for both names for backward compatibility. When a cgroup limit is detected and is less than host RAM, it becomes the effective memory ceiling for percentage calculations.

**Where cgroup memory is applied:**
- `overview-service.ts` → `getServerInfo()`: uses `EnvironmentDetector.effectiveMemoryBytes` for `totalRAM`
- `overview-service.ts` → `calculateMemoryAttribution()`: uses cached `EnvironmentDetector` result for `totalRAM`
- `timeline-service.ts` → `fetchTotalRam()`: queries `CGroupMemoryTotal`/`CGroupMemoryLimit` directly and uses it when less than `OSMemoryTotal`
- `metrics-collector.ts` → `getServerMetrics()`: checks both metric names from the memory metrics map
- `engine-internals.ts` → memory X-ray and server info: checks both metric names
- `query-analyzer.ts` → per-host RAM detection: checks both metric names in cluster-aware and local queries

**Gotchas:**
1. `MemoryTracking` is ClickHouse's tracked memory, not total system memory usage
2. `OSMemoryTotal` is the total RAM available to the OS (host-level in containers)
3. `CGroupMemoryLimit` was renamed to `CGroupMemoryTotal` in ClickHouse 26+ — always check for both
4. Both metrics return a very large number (~2^63) when no cgroup limit is set — we filter values >= 1e18
5. `MemoryResident` (RSS) can exceed `MemoryTracking` due to allocator overhead, fragmentation, and memory-mapped files

---

### Disk I/O (bytes/s)

**What we want:** Disk read/write rates in bytes per second

**Source:** 
- `system.metric_log.ProfileEvent_OSReadBytes`
- `system.metric_log.ProfileEvent_OSWriteBytes`

**Formula:**
```
disk_read_rate = OSReadBytes / interval_seconds
disk_write_rate = OSWriteBytes / interval_seconds
```

> **Test coverage:** `metrics-collector.integration.test.ts` → "Disk I/O rates"
> - 50 MiB read / 10 MiB write per interval

**Gotchas:**
1. These are OS-level disk operations, not just ClickHouse operations
2. Same interval normalization as CPU applies

---

### Network I/O (bytes/s)

**What we want:** Network send/receive rates in bytes per second

**Source:**
- `system.metric_log.ProfileEvent_NetworkSendBytes`
- `system.metric_log.ProfileEvent_NetworkReceiveBytes`

**Formula:**
```
network_send_rate = NetworkSendBytes / interval_seconds
network_recv_rate = NetworkReceiveBytes / interval_seconds
```

> **Test coverage:** `metrics-collector.integration.test.ts` → "Network I/O rates"
> - 1 MiB send / 2 MiB recv per interval

---

## Time Travel Page

Time Travel uses a **hybrid data model**: historical data from log tables combined with live in-flight data from virtual tables. When the visible time window includes "now" (within 30s of current time), both sources are fetched in parallel and deduplicated.

### Data Sources

| Data | Historical Source | Live Source | Dedup Key |
|------|------------------|-------------|-----------|
| Server CPU/Memory/IO | `system.metric_log` | — | timestamp |
| Queries | `system.query_log` | `system.processes` | `query_id` |
| Merges | `system.part_log` | `system.merges` | `part_name` |
| Mutations | `system.part_log` | `system.merges` (is_mutation=1) | `part_name` |

When both historical and live data exist for the same operation (e.g. a query that just finished and appears in both `processes` and `query_log`), the completed log entry takes priority. Running operations not yet in the logs are appended.

### Server CPU (µs/s)

**What we want:** CPU time in microseconds per second (raw value for stacking with queries)

**Source:** `system.metric_log.ProfileEvent_OSCPUVirtualTimeMicroseconds`

**Formula:**
```
cpu_us_per_second = OSCPUVirtualTimeMicroseconds / interval_seconds
```

**Display as percentage:**
```
cpu_percentage = (cpu_us_per_second / (NumberOfCPUCores × 1,000,000)) × 100
```

**Y-axis:** Shows percentage where 100% = all cores saturated. The 100% reference line is drawn at `NumberOfCPUCores × 1,000,000` µs/s.

**Gotchas:**
1. Time Travel shows raw µs values internally for stacking visualization, but the Y-axis labels display percentages (0–100% of all cores)
2. Same interval normalization applies as Overview Trends
3. Values can transiently exceed 100% due to `metric_log` collection interval jitter under heavy CPU load (the `lagInFrame` interval calculation becomes unreliable when the metric flush thread is starved)

---

### Query CPU Usage

**What we want:** CPU time consumed by individual queries

**Source (completed):** `system.query_log.ProfileEvents['OSCPUVirtualTimeMicroseconds']`
**Source (running):** `system.processes.ProfileEvents['OSCPUVirtualTimeMicroseconds']`

**Formula:**
```
query_cpu_us = ProfileEvents['OSCPUVirtualTimeMicroseconds']
```

**Visualization as a flat band:**

Each query/merge/mutation has two datapoints: total CPU consumed and wall-clock duration. Since we don't know the CPU profile within the operation, we convert the cumulative total to a per-second rate and display it as a flat band across the operation's time range:

```
band_height_us_per_sec = query_cpu_us / max(duration_ms / 1000, 0.001)
```

This same cumulative-to-rate conversion applies to **all cumulative metrics** (CPU, network, disk). Memory is the exception — it uses peak value directly (see below).

**Gotchas:**
1. This is the total CPU time for the query duration, not per-second
2. Running queries have live-updating ProfileEvents; completed queries have final values

---

### Memory Usage

**What we want:** Memory used by server and individual queries

**Sources:**
- Server: `system.metric_log.CurrentMetric_MemoryTracking`
- Queries (completed): `system.query_log.memory_usage` (peak memory)
- Queries (running): `system.processes.memory_usage` (current memory)

**Visualization:** Unlike CPU/network/disk, memory is an **instantaneous peak**, not a cumulative total. The band height is the raw `peak_memory` value — no division by duration.

---

### Network Usage

**What we want:** Network I/O by server and individual queries

**Sources:**

- Server send: `system.metric_log.ProfileEvent_NetworkSendBytes`
- Server recv: `system.metric_log.ProfileEvent_NetworkReceiveBytes`
- Queries: `system.query_log.ProfileEvents['NetworkSendBytes']` + `ProfileEvents['NetworkReceiveBytes']`

**Visualization:** Network bytes are cumulative totals, so the band uses the same rate conversion as CPU:

```
band_height_bytes_per_sec = (net_send + net_recv) / max(duration_ms / 1000, 0.001)
```

---

### Disk Usage

**What we want:** Disk I/O by server and individual queries

**Sources:**

- Queries: `system.query_log.ProfileEvents['ReadBufferFromFileDescriptorReadBytes']` + `ProfileEvents['WriteBufferFromFileDescriptorWriteBytes']`

**Visualization:** Disk bytes are cumulative totals, same rate conversion:

```
band_height_bytes_per_sec = (disk_read + disk_write) / max(duration_ms / 1000, 0.001)
```

---

### Band Metric Summary

| Mode | Band height | Reasoning |
| ---- | ----------- | --------- |
| memory | `peak_memory` (as-is) | Instantaneous high-water mark |
| cpu | `cpu_us / duration_s` | Cumulative → rate |
| network | `(net_send + net_recv) / duration_s` | Cumulative bytes → bytes/sec |
| disk | `(disk_read + disk_write) / duration_s` | Cumulative bytes → bytes/sec |

**Approximation limitations — bands are not precise measurements:**

The flat-band model is a best-effort visualization, not a precise reconstruction. Users should be aware of these trade-offs:

- **Memory overcounts when bands overlap.** Each query's band shows its *peak* memory for the entire duration, but that peak was likely a brief moment. When multiple queries overlap, their peaks are stacked as if they occurred simultaneously — in reality they probably didn't. The stacked bands will exceed the server memory line, and that gap is the overcount.
- **CPU/network/disk lose temporal shape.** The total resource consumed is correct (area under the curve is preserved), but the actual profile within the query is unknown. A query that burns 8 cores for 1s then waits on IO for 7s looks identical to one that uses 1 core steadily for 8s. Bursts are smoothed away, idle periods are filled in.
- **The server line is the closest reference.** The `metric_log`-based server line shows real per-second measurements, so it's generally more accurate than the flat bands. When stacked bands diverge significantly from the server line, it suggests the flat-band approximation is at its weakest. This is expected and informative — it's not a bug.

---

### Cluster-Wide Aggregation ("All" Mode)

When viewing all hosts in a cluster, the Time Travel chart needs to reconcile two data sources:
- **Server metric line** (CPU, memory, network, disk) — from `system.metric_log`
- **Stacked query/merge/mutation bands** — from `system.query_log`, `system.part_log`, `system.processes`, `system.merges`

The challenge: the metric line and the bands must tell the same story. If the bands represent total cluster work, the line must too — otherwise the bands tower over the line and the chart is misleading.

**Four options were considered:**

1. **Average the line, keep bands as-is.** The server line uses `avg()` across hosts; bands stack all items from all hosts. Problem: the line shows "average per-host load" while bands show "total cluster work." A single busy host's merges stack to 90% but the averaged line sits at 23% (90/4 hosts). The chart looks broken.

2. **Average both.** Divide each band's peak by the host count so bands also represent "average per-host contribution." Problem: a merge genuinely using 870ms/s of CPU on one host would visually appear as ~217ms/s. Misleading when you hover and the tooltip shows the real value.

3. **Sum both, scale ceiling to total cluster capacity.** The server line uses `sum()` across hosts. CPU cores and RAM are summed across hosts. The 100% line represents total cluster capacity (e.g., 16 cores for 4×4-core hosts). Both the line and the bands represent total cluster activity against total cluster capacity. Problem: the Y-axis goes to N×100% which is unusual, and the bands look disproportionately large relative to the line when work is concentrated on one host.

4. **Avg line + real bands + per-host tooltip.** ✅ **(Chosen)** The server line uses `avg()` across hosts. Bands show real (undivided) values — a merge using 900ms/s of CPU shows at 900ms/s regardless of host count. The Y-axis ceiling is single-host capacity. Bands can stack above the server line, which is intentional: it reveals that work is concentrated on specific hosts rather than evenly distributed. A per-host CPU breakdown in the tooltip (mini bar chart) shows exactly which hosts are hot.

**Why this approach:** It's the most honest representation. The server line shows average cluster health. The bands show real workload intensity. When bands tower over the line, it's a signal: "this work is concentrated, not spread." The per-host tooltip bars let you instantly identify hotspots. The split view provides full per-host detail when needed.

**Implementation:**
- SQL: all metric timeseries use `avg()` with `GROUP BY event_time` (CPU, memory, network, disk)
- Per-host CPU: `CLUSTER_CPU_TIMESERIES` fetched in "All" mode, returned as `per_host_cpu` map (hostname → timeseries)
- CPU cores: per-host value via `asynchronous_metric_log` (`NumberOfCPUCores`, min across hosts, capped at cgroup limit)
- RAM: per-host value via `asynchronous_metric_log` (`OSMemoryTotal`, min across hosts, capped at cgroup limit)
- Host count: returned as `host_count` in the `MemoryTimeline` response
- Bands: rendered at real values (no division). `peak === realPeak`
- Tooltip: "Cluster avg" label with per-host CPU bar chart (gradient bars, color-coded: green < 50%, orange 50-80%, red > 80%)
- Each query/merge/mutation in tooltip shows `[hostname]` tag
- CPU clamping: `Math.min(v, cpuCores × 1,000,000)` — uses per-host cores
- Y-axis labels: percentages derived from per-host cores/RAM
- Single-host mode: `per_host_cpu` is omitted, tooltip shows "Server:" instead of "Cluster avg"

**Split view:** An alternative "Split" view renders one chart per host, stacked vertically. Each chart fetches data filtered to its host, with its own Y-axis scaled to that host's capacity. This avoids the aggregation problem entirely and gives the clearest per-host picture.

**Decision log:**
- Initially implemented option 3 (sum both, scale ceiling). Problem: on a 4-node cluster with 3 cores each, the Y-axis went to 12 cores / 400%. Bands looked correct but the scale was unintuitive and the chart felt empty when load was moderate.
- Switched to option 4 with band division (divide each band's peak by host_count). Problem: bands became too small — a merge using 900ms/s of CPU on one host appeared as 225ms/s. The "All" chart looked like the system was barely working while split view showed hosts at 50-90%.
- Final approach: avg server line + real (undivided) bands + per-host CPU tooltip. Bands can exceed the server line, which is intentional — it signals concentrated work. The per-host bar chart in the tooltip is the key UX element that makes this work: you hover anywhere and instantly see which hosts are hot. This is the least misleading option we found, though it's still an imperfect compression of N-dimensional data into one chart. The split view remains the gold standard for per-host analysis.

---

## Overview Page - Snapshot Mode (Real-time)

### CPU Usage (%) — MetricsCollector (header card)

**What we want:** Current CPU utilization for the summary card

**Source:** `system.asynchronous_metrics`
- `LoadAverage1` - 1-minute load average
- `NumberOfPhysicalCPUCores` - for normalization

**Formula:**
```
cpu_percentage = min(100, (LoadAverage1 / NumberOfPhysicalCPUCores) × 100)
```

> **Test coverage:** `metrics-collector.integration.test.ts` → "getServerMetrics (snapshot mode)"
> - Valid shape, cpu_usage in [0, 100], memory_used ≤ memory_total

**Fallback formula (if load average unavailable):**
```
cpu_percentage = ((OSUserTime + OSSystemTime) / (OSUserTime + OSSystemTime + OSIdleTime)) × 100
```

**Gotchas:**
1. Load average can exceed number of cores (processes waiting)
2. We cap at 100% for display purposes
3. This is different from the resource attribution calculation below

### CPU Usage (%) — Resource Attribution (breakdown bar)

**What we want:** Total CPU utilization for the attribution breakdown (queries / merges / mutations / other)

**Source:** `system.asynchronous_metrics`
- `OSUserTimeNormalized` — user-mode CPU time, pre-normalized to [0..1] per core
- `OSSystemTimeNormalized` — kernel-mode CPU time, pre-normalized to [0..1] per core

**Formula:**
```
total_cpu_pct = min(100, (OSUserTimeNormalized + OSSystemTimeNormalized) × 100)
```

**Why this differs from the header card:** The header card uses `LoadAverage1` which is a 1-minute smoothed average. The attribution bar uses `OSUserTimeNormalized` which is an instantaneous measurement — more accurate for real-time breakdown but noisier.

**Gotchas:**
1. `OSUserTimeNormalized` is already divided by core count, so no manual normalization needed
2. Core count is derived: `cores = OSUserTime / OSUserTimeNormalized` (non-normalized / normalized)

---

## Query Monitor / Query Details

### Query Efficiency Score

**What we want:** How effectively the query used primary key indexes to skip unnecessary data

**Source:** `system.query_log.ProfileEvents`
- `SelectedMarks` - marks actually scanned
- `SelectedMarksTotal` - total marks in touched parts

**Formula:**
```
efficiency_score = ((SelectedMarksTotal - SelectedMarks) / SelectedMarksTotal) × 100
```

> **Test coverage:** `query-analyzer.integration.test.ts` → "getQueryHistory"
> - Returns finished queries with selected_marks, selected_marks_total from real query_log

**Interpretation:**
- 90%+ = Excellent — primary key pruned most data, only 10% of marks scanned
- 50–90% = Good — some pruning happening
- <50% = Poor — consider optimizing primary key or adding skip indexes
- `null` / `—` = No marks data available (e.g. `SELECT 1`, system queries, non-MergeTree tables)
- Higher is better (more data skipped)

**Why not `result_rows / read_rows`?**
ClickHouse is designed for analytical workloads that aggregate large datasets. A `GROUP BY` over 10M rows returning 5 results is perfectly efficient — the old row-ratio metric would incorrectly flag this as "poor". Marks pruning effectiveness measures what actually matters: how well the primary key helped avoid scanning irrelevant data.

**Display:**
- Table column: "Pruning" badge with percentage
- Detail view: "Index Pruning" card with percentage and label

---

### Index Selectivity (Parts)

**What we want:** How well the primary key pruned data parts

**Source:** `system.query_log.ProfileEvents`
- `SelectedParts` - parts actually scanned
- `SelectedPartsTotal` - total parts in touched tables

**Formula:**
```
parts_selectivity_pct = (SelectedParts / SelectedPartsTotal) × 100
```

**Interpretation:**
- Lower is better (more pruning)
- ≤10% = Excellent (green) - primary key is very effective
- ≤50% = Good (yellow) - some pruning happening
- >50% = Poor (red) - consider optimizing primary key

**Note:** Only available for MergeTree tables. S3/Parquet queries won't have this data.

---

### Index Selectivity (Marks)

**What we want:** How well the primary key pruned index granules (marks)

**Source:** `system.query_log.ProfileEvents`
- `SelectedMarks` - marks actually scanned
- `SelectedMarksTotal` - total marks in touched parts

**Formula:**
```
marks_selectivity_pct = (SelectedMarks / SelectedMarksTotal) × 100
```

**Interpretation:**
- Lower is better (more pruning)
- Same color coding as parts selectivity
- Marks are finer-grained than parts (default 8192 rows per mark)

---

### Index Pruning Effectiveness

**What we want:** Percentage of marks that were pruned (NOT scanned)

**Source:** Same as marks selectivity

**Formula:**
```
pruning_effectiveness = ((SelectedMarksTotal - SelectedMarks) / SelectedMarksTotal) × 100
```

**Interpretation:**
- Higher is better (more data skipped)
- 90% = excellent, only 10% of marks were scanned
- This is the inverse of marks selectivity

---

### Mark Cache Hit Rate

**What we want:** How often mark data was found in cache vs read from disk

**Source:** `system.query_log.ProfileEvents`
- `MarkCacheHits` - marks found in cache
- `MarkCacheMisses` - marks read from disk

**Formula:**
```
mark_cache_hit_rate = MarkCacheHits / (MarkCacheHits + MarkCacheMisses) × 100
```

**Interpretation:**
- Higher is better
- >80% = good cache utilization
- Low hit rate may indicate cache is too small or queries touch different data

---

### Query Parallelism Factor

**What we want:** How effectively the query used multiple threads

**Source:** `system.query_log.ProfileEvents` or `system.processes.ProfileEvents`
- `UserTimeMicroseconds` - CPU time in user mode
- `SystemTimeMicroseconds` - CPU time in kernel mode
- `RealTimeMicroseconds` - wall clock time

**Formula:**
```
parallelism = (UserTimeMicroseconds + SystemTimeMicroseconds) / RealTimeMicroseconds
```

**Interpretation:**
- 1.0 = single-threaded execution
- 4.0 = effectively using 4 CPU cores
- Higher values indicate better parallelization

---

### IO Wait Percentage

**What we want:** How much time the query spent waiting for I/O

**Source:** `system.query_log.ProfileEvents`
- `OSIOWaitMicroseconds` - time waiting for I/O
- `RealTimeMicroseconds` - total wall clock time

**Formula:**
```
io_wait_pct = (OSIOWaitMicroseconds / RealTimeMicroseconds) × 100
```

**Interpretation:**
- Lower is better
- >50% = I/O bound query (red)
- >20% = moderate I/O wait (yellow)
- ≤20% = CPU bound or well-cached (green)

---

## Query Details — Color Coding Policy

**Principle:** Metric values in the Query Details view (both the modal and the side panel) use the default text color unless the metric has a clear, unambiguous semantic state.

**Colored metrics (have clear semantic meaning):**
- **Status** — Success (green), Failed (red), Running (amber). This is the only metric where color directly communicates a known good/bad state.
- **Index Pruning / Selectivity** — These have well-defined thresholds (≤10% scanned = excellent, >50% = poor) based on ClickHouse primary key behavior. Color coding is justified because the thresholds are meaningful.
- **Efficiency Score** — Same rationale as selectivity: higher pruning = objectively better index usage.

**Neutral metrics (no color, use default text):**
- **Duration** — A 2-minute query isn't inherently "good" or "bad" without knowing the workload.
- **CPU Time** — Same reasoning. High CPU time may be expected for analytical queries.
- **Peak Memory** — Context-dependent. 200 MB could be fine or terrible depending on the query.
- **Network I/O** — No universal threshold for "too much" network traffic.
- **Disk I/O** — Same as network. Depends entirely on the workload.
- **Mark Cache Hit Rate** — Removed color coding. While higher is generally better, the thresholds (80%/50%) were arbitrary and context-dependent.
- **IO Wait** — Removed color coding. Whether 30% IO wait is concerning depends on the storage backend and query type.
- **Elapsed / Started / Ended / Threads / User** — Informational only, no state to convey.

**Rationale:** Arbitrary color coding (green for CPU time, orange for network, purple for disk) implies a judgment the system isn't qualified to make. It creates false confidence — a user might see green CPU time and assume the query is "fast" when it's actually consuming excessive resources for its workload. Only color-code when there's a clear, defensible threshold.

---

## Overview Page (Real-time Monitoring)

The Overview page combines system metrics with real-time monitoring features (formerly "Live View").

### CPU Resource Attribution

**What we want:** Break down total CPU usage by category (queries, merges, mutations, other)

**Source:**
- Total CPU: `system.asynchronous_metrics` → `OSUserTimeNormalized` + `OSSystemTimeNormalized`
- Query CPU: `system.processes.ProfileEvents` → `UserTimeMicroseconds` + `SystemTimeMicroseconds`
- Active merges: `system.merges` (count of rows where `is_mutation = 0`)
- Active mutations: `system.merges` (count of rows where `is_mutation = 1`)

**Formula:**
```
Total CPU % = (OSUserTimeNormalized + OSSystemTimeNormalized) × 100

Query CPU % = Per-query CPU cores summed:
  For each running query:
    query_cores = (UserTimeMicroseconds + SystemTimeMicroseconds) / (elapsed × 1,000,000)
  queryCoresUsed = sum of all query_cores
  Query CPU % = (queryCoresUsed / total_cores) × 100

Merge CPU % = Estimated only if merges are actually running in system.merges:
  - If no merges running: 0%
  - If merges running: estimate ~1.5 cores per active merge (capped at 50% of total)

Mutation CPU % = Estimated only if mutations are actually running:
  - If no mutations running: 0%
  - If mutations running: estimate ~1.0 cores per active mutation (capped at 30% of total)

Other CPU % = Total CPU % - Query CPU % - Merge CPU % - Mutation CPU %
```

**IMPORTANT — Per-query CPU cores, not averaged:**

The query attribution computes CPU cores per query individually (`cpuTime / elapsed` for each query), then sums them. This is critical for parallel queries: if 5 queries each use 2 cores for 85 seconds, the correct answer is 10 cores total, not `totalCpuTime / totalElapsed = 2 cores` (which would dilute by 5x).

**IMPORTANT - Why we don't use thread pool metrics:**

The metric `MergeTreeBackgroundExecutorThreadsActive` from `system.metrics` is **NOT** a reliable indicator of merge activity. This thread pool handles multiple types of background operations:
- Actual merges
- Part moves between disks
- Part fetches (replication)
- TTL operations
- Cleanup tasks
- Other background MergeTree operations

Using this metric for CPU attribution leads to incorrect results where 50-80% of CPU is attributed to "merges" when no merges are actually running. The thread pool may be active due to queries triggering background operations.

**Correct approach:** Only attribute CPU to merges/mutations when there are actual entries in `system.merges`. This table shows operations that are genuinely in progress.

**Gotchas:**
1. Query CPU is calculated from actual ProfileEvents, which is accurate
2. Merge/mutation CPU is estimated (no direct CPU metrics available for background operations)
3. The "other" category captures system overhead, background tasks, and any unattributed CPU
4. When no queries/merges/mutations are running, all CPU goes to "other"
5. **Merge vs mutation split uses `is_mutation` from `system.merges`:** ClickHouse exposes both regular merges and mutation merges in the same `system.merges` table. The `is_mutation` column distinguishes them — `0` for regular part merges, `1` for mutation merges (ALTER UPDATE/DELETE applying to parts). The code filters on this flag to count each type separately for the heuristic.
6. **Unused `part_log` CPU data:** The service also fetches actual CPU time for recently completed merges (`event_type = 'MergeParts'`) and mutations (`event_type = 'MutatePart'`) from `system.part_log.ProfileEvents`. These values (`recentMergeCPU`, `recentMutationCPU`) are passed into the attribution function but are **not currently used** — the heuristic count-based estimation is used instead. This is a potential improvement: using real CPU data from completed operations to calibrate the estimates for in-flight ones.

---

### CPU Cores Used (per query)

**What we want:** Effective CPU cores being used by a running query

**Source:** `system.processes.ProfileEvents`
- `UserTimeMicroseconds`
- `SystemTimeMicroseconds`
- `elapsed` (seconds)

**Formula:**
```
cpu_cores = (UserTimeMicroseconds + SystemTimeMicroseconds) / (elapsed × 1,000,000)
```

**Interpretation:**
- 2.5 = query is using ~2.5 CPU cores on average
- Useful for understanding resource consumption of running queries

---

### Query Progress

**What we want:** Estimated completion percentage of a running query

**Source:** `system.processes`
- `read_rows` - rows read so far
- `total_rows_approx` - estimated total rows to read

**Formula:**
```
progress = min(100, max(0, (read_rows / total_rows_approx) × 100))
```

**Gotchas:**
1. `total_rows_approx` is an estimate and may be inaccurate
2. Progress can appear to jump or go backwards as estimates are refined
3. Clamped to 0-100% range

---

### Merge Progress

**What we want:** Completion percentage of an active merge

**Source:** `system.merges`
- `progress` - reported progress (0.0 to 1.0)

**Formula:**
```
merge_progress_pct = progress × 100
```

---

### Merge Throughput (bytes/s)

**What we want:** How fast data is being processed by a merge operation

**Source (active merges — overview I/O attribution):** `system.merges`

- `bytes_read_uncompressed` - uncompressed bytes read so far
- `bytes_written_uncompressed` - uncompressed bytes written so far
- `elapsed` - seconds since merge started

**Formula (active merge — overview I/O attribution):**
```
read_rate  = bytes_read_uncompressed / elapsed
write_rate = bytes_written_uncompressed / elapsed
```

**Source (completed merges — merge tracker):** `system.part_log`
- `size_in_bytes` - compressed size of the result part
- `duration_ms` - total merge duration in milliseconds

**Formula (completed merge — on-disk throughput):**
```
throughput = size_in_bytes / (duration_ms / 1000)
```

**Aggregate throughput (all active merges):**
```
total_throughput = sum of per-merge read/write rates
```

**Gotchas:**

1. Overview uses uncompressed bytes (from `system.merges`) for I/O attribution; merge tracker uses compressed `size_in_bytes` (from `part_log`) for on-disk throughput
2. When `elapsed` is 0 (merge just started), throughput is 0
3. For completed merges, `size_in_bytes` is the result part size (compressed on-disk), which is the most meaningful measure of write throughput
4. Aggregate throughput sums individual merge rates — useful for understanding total merge I/O pressure on the system

---

### Mutation Progress

**What we want:** Completion percentage of an active mutation

**Source:** `system.mutations`
- `parts_to_do` - remaining parts to mutate
- Total parts calculated from initial state

**Formula:**
```
mutation_progress = ((total_parts - parts_to_do) / total_parts) × 100
```

---

### Stuck Mutation Detection

**What we want:** Identify mutations that may be stuck

**Source:** `system.mutations`
- `elapsed` (calculated from create_time)
- `parts_to_do`

**Formula:**
```
is_stuck = elapsed_seconds > 3600 AND parts_to_do > 0
```

**Interpretation:**
- Mutation running >1 hour with parts remaining is flagged as potentially stuck

---

### Parts Alert Threshold

**What we want:** Alert when a table has too many parts (merge pressure)

**Source:** `system.parts`
- Part count per table

**Formula:**
```
should_alert = part_count > 150
```

**Interpretation:**
- >150 parts indicates merge backlog
- May cause "too many parts" errors on inserts

---

### Disk Space Alert

**What we want:** Alert when disk space is running low

**Source:** `system.disks`
- `free_space`
- `total_space`

**Formula:**
```
should_alert = (free_space / total_space) < 0.15
```

**Interpretation:**
- Alert when <15% disk space remaining

---

## Engine Internals

### Memory Fragmentation

**What we want:** How fragmented the memory allocator is

**Source:** `system.asynchronous_metrics`
- `MemoryAllocated` - memory allocated by allocator
- `MemoryResident` - actual resident memory (RSS)

**Formula:**
```
fragmentation_pct = (1 - MemoryAllocated / MemoryResident) × 100
```

**Interpretation:**
- 0% = no fragmentation, all resident memory is allocated
- High values indicate memory fragmentation
- Can cause higher memory usage than expected

---

### Thread Pool Saturation

**What we want:** Detect if a thread pool is overloaded

**Source:** `system.metrics`
- `BackgroundPoolTask` (active tasks)
- `BackgroundPoolSize` (max threads)

**Formula:**
```
is_saturated = (active_tasks / max_threads) > 0.8
```

> **Test coverage:** `merge-tracker.integration.test.ts` → "getBackgroundPoolMetrics"
> - Pool metrics shape, active_parts > 0 after data insertion

**Interpretation:**
- >80% utilization indicates potential bottleneck
- May need to increase pool size or reduce workload

---

### CPU Core Count — Kubernetes / Container Awareness

**What we want:** The actual number of CPU cores available to the ClickHouse process, not the host node.

**The problem:** ClickHouse's `NumberOfCPUCores` and `NumberOfPhysicalCores` async metrics report the host node's core count, not the pod's cgroup CPU limit. In Kubernetes, a pod with `resources.limits.cpu: 4` on a 96-core node will report 96 cores. The `OSUserTimeCPU*` per-core metrics are also per host logical CPU. This means:
- The CPU Core Map shows 96 bars instead of 4
- CPU percentage calculations use the wrong denominator
- The visualization is meaningless for understanding the pod's actual CPU budget

**Detection strategy (priority order):**

1. **`CGroupMaxCPU`** from `system.asynchronous_metrics` (ClickHouse >= 23.8)
   - Directly reports the cgroup CPU limit as a float (e.g., `4.0` for a 4-core limit)
   - Returns `0` when no cgroup limit is set
   - Most reliable source

2. **`max_threads`** from `system.settings`
   - ClickHouse auto-detects cgroup `cpu.cfs_quota_us` / `cpu.max` at startup
   - Sets `max_threads` to the detected CPU count
   - Works on older ClickHouse versions that don't have `CGroupMaxCPU`
   - Caveat: can be overridden by user config, so only trusted when `<= 256`

3. **Fallback:** `NumberOfCPUCores` / `NumberOfPhysicalCores` / counting `OSUserTimeCPU*` metrics
   - Used when no cgroup limit is detected (bare-metal or VM deployments)

**Core map aggregation:** When `effectiveCores < hostCores`, the N host-level per-core metrics are aggregated into M effective core slots proportionally. For example, on a 96-core host with a 4-core cgroup limit, each of the 4 displayed bars aggregates 24 host cores' worth of CPU time. This gives a meaningful view of the pod's CPU budget utilization.

**Metadata surfaced to UI:** The `cpuCoresMeta` field on `EngineInternalsData` exposes:
- `effectiveCores` — the cgroup-limited count (or host count if no limit)
- `hostCores` — the host node's logical core count
- `isCgroupLimited` — `true` when a cgroup limit was detected

The CPU Core Map component displays a `cgroup: N/M` badge when container limiting is detected, showing the effective vs host core count.

**Where this applies:**
- `engine-internals.ts` → `getCPUCoreMetrics()` — core map visualization
- `timeline-service.ts` → `fetchCpuCores()` — CPU percentage clamping in Time Travel
- `metrics-collector.ts` → `fetchCpuCores()` — CPU percentage in Overview trends

**ClickHouse version notes:**
- `CGroupMaxCPU` was added in ClickHouse ~23.8
- cgroup v2 (`cpu.max`) is supported since ClickHouse 22.x
- cgroup v1 (`cpu.cfs_quota_us`) has been supported for longer
- `max_threads` auto-detection works across all supported versions

---

### CPU Sampling Attribution (from trace_log)

**What we want:** Real CPU consumption breakdown by thread pool, based on kernel profiler samples

**Source:** `system.trace_log` with `trace_type = 'CPU'`

**Window:** 180 seconds (to account for trace_log flush lag under load)

**Query:**
```sql
SELECT thread_name, count() AS cpu_samples
FROM system.trace_log
WHERE event_time >= now() - INTERVAL 180 SECOND
  AND trace_type = 'CPU'
GROUP BY thread_name
ORDER BY cpu_samples DESC
```

**Thread pool classification:**
| Thread name pattern | Pool category |
|---|---|
| `QueryPipelineEx`, `QueryPool`, `Parallel` | Queries |
| `Merge` (not `Mutat`) | Merges |
| `Mutat` | Mutations |
| `Fetch`, `Replic`, `Repl` | Replication |
| `IO`, `Disk`, `Read`, `Write` | IO |
| `Sched`, `BgSch` | Schedule |
| `HTTP`, `TCP`, `Handler` | Handlers |
| Everything else | Other |

**Why `trace_type = 'CPU'` only (not 'Real'):**
- `CPU` uses `CLOCK_THREAD_CPUTIME_ID` / `SIGPROF` — samples only when the thread is actually consuming CPU
- `Real` uses `CLOCK_MONOTONIC` — samples wall-clock time, so sleeping/idle threads get sampled too
- Using `Real` causes background pools like `BgSchPool` to dominate (they're always alive but mostly sleeping), giving a misleading picture

**Why 180s window:**
- `trace_log` is flushed to disk periodically (~every 60s by default)
- Under heavy CPU load, the flush thread (`SystemLogFlush`) gets starved and flush intervals stretch
- A 30s or 60s window may return 0 samples during sustained load because the data hasn't been flushed yet
- 180s reliably catches at least one flush cycle

**Gotchas:**
1. CPU profiler must be enabled: `query_profiler_cpu_time_period_ns > 0` (default: 1 billion ns = 1s)
2. Under extreme CPU saturation, the kernel delays `SIGPROF` delivery, reducing sample count — proportions remain correct but absolute counts are lower
3. `SYSTEM FLUSH LOGS` should NOT be used to force a flush — it blocks under heavy load and can make things worse
4. The card shows a note: "trace_log · may lag ~60s under load"
5. Top functions require `allow_introspection_functions = 1` setting

---

### Core Timeline (per-core physical view from trace_log)

**What we want:** A swimlane visualization showing what each physical CPU core was doing over time, colored by thread pool category.

**Source:** `system.trace_log` with `trace_type IN ('CPU', 'Real')`, using `cpu_id`, `thread_name`, `query_id`, and `event_time_microseconds`.

**Window:** 60 seconds default (options: 30s, 60s, 180s)

**Query:**
```sql
SELECT
    toUInt32(cpu_id) AS core,
    toStartOfInterval(event_time_microseconds, INTERVAL 100000 microsecond) AS slot,
    thread_name,
    query_id,
    count() AS samples,
    countIf(trace_type = 'CPU') AS cpu_samples,
    countIf(trace_type = 'Real') AS real_samples
FROM system.trace_log
WHERE event_time >= now() - INTERVAL 60 SECOND
  AND trace_type IN ('CPU', 'Real')
GROUP BY core, slot, thread_name, query_id
ORDER BY core, slot, samples DESC
```

**Trace type handling — CPU vs Real profiler:**

The query always fetches both trace types. The UI provides a toggle to control how they're displayed:

| Mode | Filter | What it shows |
|------|--------|---------------|
| **CPU+Real** (default) | All slots shown | Full core activity including IO-bound work. Real-only slots rendered with diagonal stripes. |
| **CPU only** | Hides slots where `cpu_samples = 0` | Strict on-CPU execution only. Threads blocked on IO/mutex/sleep are excluded. |

Each slot carries `cpu_samples` and `real_samples` counts, plus a derived `traceType` field:
- `'CPU'` — slot has only CPU profiler samples (thread was executing on-CPU)
- `'Real'` — slot has only Real profiler samples (thread was scheduled but may have been in IO wait, futex, epoll)
- `'Mixed'` — slot has both CPU and Real samples (most common under normal load)

**Why both types matter:**
- The CPU profiler (`CLOCK_THREAD_CPUTIME_ID`) only fires when a thread consumes CPU cycles. Under heavy saturation, the kernel coalesces/delays `SIGPROF` delivery, producing fewer samples exactly when you need them most.
- The Real profiler (`CLOCK_MONOTONIC`) fires on wall-clock time regardless of CPU state. For threads actively running (not sleeping), a Real sample is effectively a CPU sample.
- The tradeoff: Real samples also fire for threads blocked on IO, mutex waits, or sleeping. A thread in `futex_wait` or `epoll_wait` generates Real samples but not CPU samples. On the timeline it appears "active" when the core was actually idle from a CPU perspective.
- This is most visible for handler threads (HTTP/TCP) which are mostly waiting for connections — they show as Real-only striped slots.

**Visual cues:**
- Solid color = CPU or Mixed samples (confirmed on-CPU execution)
- Diagonal stripes = Real-only samples (wall-clock activity, may include IO wait)
- The stripe pattern uses `repeating-linear-gradient(135deg)` at 2px intervals over the pool color
- In CPU-only mode, Real-only slots are hidden entirely (not just visually different)

**Tooltip shows:**
- Core number, thread pool, thread name, sample count, query/background type
- Trace type indicator: `● CPU` (green), `◐ Real` (amber), `● Mixed` (neutral)
- For non-CPU slots: breakdown of cpu/real sample counts

**Comparison with CPU Sampling Attribution card:**
The CPU Sampling card (`GET_CPU_SAMPLES_BY_THREAD`) uses `trace_type = 'CPU'` only — it's strict CPU attribution. The Core Timeline defaults to both types because it's showing "what was this core doing" (broader activity view), not just "what was burning CPU." Users can switch to CPU-only mode when they want the strict view.

**Slot bucketing:**
- Samples are bucketed into 100ms time slots per core using `toStartOfInterval(event_time_microseconds, INTERVAL 100000 microsecond)`
- For each (core, slot), only the dominant thread (highest sample count) is displayed
- This keeps the visualization clean while preserving the most important signal

**Thread pool classification:** Same as CPU Sampling Attribution (see table above).

**Visualization:**
- One horizontal swimlane per physical CPU core
- Each cell = one 100ms time slot, colored by the dominant thread pool
- Solid fill = CPU or Mixed trace type (confirmed on-CPU execution)
- Diagonal stripe fill = Real-only trace type (wall-clock activity, may include IO wait)
- Opacity scales with sample density (more samples = more opaque)
- Empty cells = core was idle (no profiler samples in that slot)
- Tooltip shows core number, thread pool, thread name, sample count, trace type breakdown, and whether it was query-driven
- Toggle between CPU+Real (default, shows everything with visual distinction) and CPU-only (hides Real-only slots)

**What this reveals that other views don't:**
- Core contention: multiple hot thread pools fighting for the same cores
- NUMA locality issues: work concentrated on specific core ranges
- Scheduler behavior: how the OS distributes ClickHouse threads across physical cores
- Temporal patterns: merge bursts, query waves, replication storms visible as colored bands

**Container / Kubernetes core IDs:**
The `cpu_id` column in `trace_log` comes from `sched_getcpu()` at the moment the profiler signal fires. This returns the host node's physical core number, not a virtualized container core. Linux cgroups limit CPU time but don't virtualize core numbering. A pod with a 3-core cgroup limit on a 12-core host will see core IDs like 1, 2, 5, 6, 10 — non-contiguous, scattered across the host — because the kernel scheduler migrates threads across physical cores freely.

Implications:
- You'll never see more than N cores active simultaneously (where N = cgroup CPU limit), but the specific core numbers change over time
- Core IDs from different time windows may not overlap at all
- NUMA locality is visible: if activity clusters on cores 0–5 (same NUMA node) vs scattered across 0–47, that affects memory access latency
- The CPU Core Map card aggregates host cores into effective cgroup slots, but the Core Timeline shows raw host core IDs to preserve this scheduler/NUMA insight

The UI shows an amber "(host IDs)" indicator in the header when cgroup limiting is detected, with a tooltip explaining the discrepancy between the pod's vCPU count and the visible core numbers.

**trace_log flush lag — critical behavior under load:**

The `trace_log` table is a system log table backed by MergeTree. Profiler samples are first collected into an in-memory buffer, then flushed to disk by the `SystemLogFlush` background thread. This flush mechanism has two layers of delay under heavy CPU load:

1. **Flush thread starvation:** The `SystemLogFlush` thread runs on the `BackgroundSchedulePool`. Under heavy CPU-bound load (all cores saturated), the OS scheduler cannot schedule this thread on time. The default `flush_interval_milliseconds` is 7500ms, but actual flush intervals can stretch to 30–60+ seconds when the system is under sustained load. This is the same mechanism that causes `metric_log` collection delays (see "CPU Usage %" section above).

2. **Profiler signal delivery delay:** The CPU profiler uses POSIX per-thread timers (`timer_create` with `CLOCK_THREAD_CPUTIME_ID`) that deliver signals to threads. Under extreme CPU saturation, the kernel delays or coalesces these signal deliveries, reducing the number of samples collected. The Real-time profiler (`CLOCK_MONOTONIC`) is less affected since it doesn't depend on CPU time accounting.

**Combined effect:** During a sustained CPU spike, you may see zero samples in the most recent 30s because: (a) the profiler collected fewer samples than normal, and (b) the samples that were collected haven't been flushed from the in-memory buffer to the MergeTree table yet. Once the load drops, the flush thread catches up and a burst of delayed samples appears.

**ClickHouse source references:**
- System log flush mechanism: `src/Common/SystemLogBase.cpp` — the `SystemLog` template class manages the in-memory buffer and schedules periodic flushes via `BackgroundSchedulePool::TaskHolder`
- Profiler timer setup: `src/Common/QueryProfiler.cpp` — creates per-thread POSIX timers with `timer_create(CLOCK_THREAD_CPUTIME_ID, ...)` for CPU profiling and `timer_create(CLOCK_MONOTONIC, ...)` for real-time profiling
- Trace collector: `src/Common/TraceCollector.cpp` — receives profiler signals via a pipe and writes `TraceLogElement` entries to the in-memory buffer
- Default flush interval: `<flush_interval_milliseconds>7500</flush_interval_milliseconds>` in the `<trace_log>` config section ([docs](https://clickhouse.com/docs/operations/system-tables/overview))
- Profiler settings: `query_profiler_cpu_time_period_ns` (default 1e9 = 1s) and `query_profiler_real_time_period_ns` (default 1e9 = 1s) ([docs](https://clickhouse.com/docs/en/operations/optimizing-performance/sampling-query-profiler))

**Mitigation in the UI:**
- Default window is 60s (not 30s) to account for flush lag
- 180s option available for sustained high-load scenarios
- The card shows "No CPU samples" with a hint about idle/profiler-disabled when no data is available
- The CPU Sampling Attribution card uses a 180s window for the same reason

---

## Part Lineage - Space Savings

### Size Reduction Calculation

**What we want:** The compression achieved by merging L0 parts into a higher-level part.

> **Test coverage:** `lineage-builder.integration.test.ts`
> - L0 nodes are leaves, statistics match tree structure, tree is finite, original_total_size ≥ 0

**CRITICAL:** `read_bytes` in `system.part_log` is the **uncompressed** bytes read during the merge operation, NOT the compressed on-disk sizes of source parts. Using `read_bytes` will give incorrect (inflated) original size values.

**Source:**
- L0 part sizes: `system.part_log` WHERE `event_type = 'NewPart'` → `size_in_bytes`
- Active part sizes: `system.parts` → `bytes_on_disk`

**Correct Formula:**
```
Original Size = sum of all L0 part sizes (size_in_bytes from NewPart events)
Final Size = current part size (bytes_on_disk)
Space Saved % = (Original - Final) / Original × 100
```

**Algorithm:**
1. For L0 leaf nodes: use their `size_in_bytes` (compressed on-disk size)
2. For L1+ merges: recursively sum children's `size_in_bytes`
3. Do NOT use `read_bytes` - it represents uncompressed data read during merge

**Example:**
```
6 L0 parts × 24.7 MB each = 148.2 MB (original compressed size)
L1 result part = 149.94 MB
Space Saved = (148.2 - 149.94) / 148.2 = -1.2% (slight expansion, not compression)
```

**Gotchas:**
1. **`read_bytes` is NOT compressed size:** Using `read_bytes` (e.g., 331.88 MB) instead of summing L0 `size_in_bytes` (148.2 MB) will show incorrect savings like "+54.8%" when the actual result is slight expansion.

2. **L0 parts may be purged:** If `part_log` TTL has expired, L0 `NewPart` events may be missing. In this case, fall back to summing children's `size_in_bytes` from the lineage tree.

3. **Negative savings is normal:** When compression ratio is poor or data is already compressed, the merged part may be larger than the sum of inputs.

---

## Data Sources Reference

### system.metric_log
- Stores periodic snapshots (default: every 1 second)
- `ProfileEvent_*` columns contain **delta values** (change since last snapshot)
- `CurrentMetric_*` columns contain **point-in-time values**

### system.asynchronous_metric_log
- Stores async metrics history
- Contains system-level metrics like `OSMemoryTotal`, `NumberOfCPUCores`

### system.asynchronous_metrics
- Current values of async metrics (no history)
- Fallback when log tables don't have data

### system.query_log
- Per-query statistics after completion
- `ProfileEvents` map contains detailed counters
- `memory_usage` is peak memory during query

### system.processes
- Currently running queries
- `ProfileEvents` map contains live counters (updated during execution)
- `elapsed` is current runtime in seconds

### system.merges
- Currently active merge operations
- `progress` is 0.0 to 1.0

### system.mutations
- Active and completed mutations
- `parts_to_do` shows remaining work

### system.parts
- All data parts (active and inactive)
- `bytes_on_disk` is compressed size

### system.trace_log
- Stack trace samples collected by the CPU and Real-time profilers
- `trace_type = 'CPU'` — sampled when thread is consuming CPU (`CLOCK_THREAD_CPUTIME_ID`)
- `trace_type = 'Real'` — sampled on wall-clock time (`CLOCK_MONOTONIC`), includes sleeping threads
- `thread_name` identifies the thread pool (e.g. `QueryPipelineEx`, `BgSchPool`)
- `query_id` is non-empty for query-associated threads, empty for background threads
- `trace` array contains instruction pointer addresses (use `addressToSymbol` + `demangle` for function names)
- Data is buffered in memory and flushed periodically — may lag ~60s+ under heavy load
- Requires `query_profiler_cpu_time_period_ns > 0` to collect CPU samples

### system.part_log
- History of part operations (create, merge, mutate, drop)
- `size_in_bytes` is compressed size for NewPart events
- `read_bytes` is UNCOMPRESSED bytes read (don't use for size calculations!)

---

## ProfileEvents Reference

### CPU Events
| Event | Description | Unit |
|-------|-------------|------|
| `OSCPUVirtualTimeMicroseconds` | Total CPU time (user + system + wait) | µs |
| `OSCPUWaitMicroseconds` | Time waiting for CPU | µs |
| `UserTimeMicroseconds` | CPU time in user mode | µs |
| `SystemTimeMicroseconds` | CPU time in kernel mode | µs |
| `RealTimeMicroseconds` | Wall clock time | µs |

### I/O Events
| Event | Description | Unit |
|-------|-------------|------|
| `OSReadBytes` | Bytes read from disk (OS level) | bytes |
| `OSWriteBytes` | Bytes written to disk (OS level) | bytes |
| `ReadBufferFromFileDescriptorReadBytes` | Bytes read via file descriptors | bytes |
| `WriteBufferFromFileDescriptorWriteBytes` | Bytes written via file descriptors | bytes |
| `OSIOWaitMicroseconds` | Time waiting for I/O | µs |
| `DiskReadElapsedMicroseconds` | Time spent reading from disk | µs |
| `DiskWriteElapsedMicroseconds` | Time spent writing to disk | µs |

### Network Events
| Event | Description | Unit |
|-------|-------------|------|
| `NetworkSendBytes` | Bytes sent over network | bytes |
| `NetworkReceiveBytes` | Bytes received over network | bytes |
| `NetworkSendElapsedMicroseconds` | Time spent sending | µs |
| `NetworkReceiveElapsedMicroseconds` | Time spent receiving | µs |

### Query Processing Events
| Event | Description | Unit |
|-------|-------------|------|
| `SelectedParts` | Data parts scanned | count |
| `SelectedPartsTotal` | Total parts in touched tables | count |
| `SelectedMarks` | Index marks scanned | count |
| `SelectedMarksTotal` | Total marks in touched parts | count |
| `SelectedRanges` | Index ranges scanned | count |
| `SelectedRows` | Rows selected for processing | count |
| `SelectedBytes` | Bytes selected for processing | bytes |

### Cache Events
| Event | Description | Unit |
|-------|-------------|------|
| `MarkCacheHits` | Mark cache hits | count |
| `MarkCacheMisses` | Mark cache misses | count |
| `UncompressedCacheHits` | Uncompressed cache hits | count |
| `UncompressedCacheMisses` | Uncompressed cache misses | count |
| `QueryCacheHits` | Query cache hits | count |
| `QueryCacheMisses` | Query cache misses | count |

---

## Common Gotchas

1. **Interval normalization:** `metric_log` collection interval is typically 1 second but can vary. Always calculate actual interval using `lagInFrame()` window function.

2. **First row in window:** The first row's interval calculation references data outside the window, producing invalid values. Filter with: `interval_ms > 0 AND interval_ms < 10000`.

3. **Logical vs Physical cores:** Always use `NumberOfCPUCores` (logical) for CPU percentage calculations, not `NumberOfPhysicalCPUCores`. **However**, in Kubernetes/containerized environments, neither metric is correct — both report the host node's count. See "CPU Core Count — Kubernetes / Container Awareness" in the Engine Internals section.

4. **Timestamp handling:** ClickHouse returns timestamps without timezone. Always append 'Z' or handle as UTC.

5. **Server restart:** After restart, counters reset. This can cause anomalies in the first few data points.

6. **S3/Parquet queries:** External table queries (S3, HDFS, URL) won't have MergeTree-specific ProfileEvents like `SelectedParts`, `SelectedMarks`, etc.

7. **ProfileEvents availability:** Running queries (`system.processes`) have ProfileEvents that update live. Completed queries (`system.query_log`) have final ProfileEvents values.

8. **Thread pool metrics are misleading for attribution:** `MergeTreeBackgroundExecutorThreadsActive` does NOT mean merges are running. This thread pool handles many background operations (moves, fetches, TTL, cleanup). Always check `system.merges` for actual merge activity.

9. **Background executor threads vs actual operations:** High `MergeTreeBackgroundExecutorThreadsActive` with zero entries in `system.merges` is normal - queries can trigger background work that uses these threads without starting actual merge operations.

10. **trace_log flush lag:** `system.trace_log` data is buffered in memory and flushed periodically. Under heavy CPU load, the `SystemLogFlush` thread gets starved, causing flush intervals to stretch well beyond the default ~60s. Use a 180s query window to reliably capture flushed data. Do NOT use `SYSTEM FLUSH LOGS` as a workaround — it blocks under load.

11. **Time Travel hybrid data:** When the time window includes "now", Time Travel fetches both historical log data and live in-flight data from virtual tables (`system.processes`, `system.merges`). Completed entries from logs take priority; running operations not yet flushed to logs are appended. This means the right edge of the chart shows real-time data even though the rest is historical.

12. **Overview CPU: two different calculations:** The header summary card uses `LoadAverage1 / cores` (smoothed 1-minute average). The resource attribution bar uses `OSUserTimeNormalized` (instantaneous). These can diverge significantly during load transitions.

13. **Container/Kubernetes CPU core count:** `NumberOfCPUCores` and `NumberOfPhysicalCores` report the host node's cores, not the pod's cgroup limit. A pod with 4 CPU cores on a 96-core node will show 96 cores everywhere unless cgroup detection is used. The codebase detects this via `CGroupMaxCPU` (ClickHouse >= 23.8) or `max_threads` from `system.settings`. See "CPU Core Count — Kubernetes / Container Awareness" in the Engine Internals section for the full detection strategy.

14. **Container/Kubernetes memory:** `OSMemoryTotal` reports the host node's RAM, not the pod's cgroup memory limit. A pod with 4 GiB memory on a 64 GiB host will show 64 GiB, making memory percentage appear artificially low. The codebase detects this via `CGroupMemoryTotal` (ClickHouse 26+) or `CGroupMemoryLimit` (ClickHouse 23.8–25.x) from `system.asynchronous_metrics`. The metric was renamed in CH 26; all queries check both names. When a cgroup limit is detected and is less than host RAM, it becomes the effective memory ceiling. Values >= 1e18 are ignored (ClickHouse returns ~2^63 when no limit is set).

15. **Time Travel cluster "All" mode — avg line + real bands:** In cluster-wide view, the server metric line (CPU, memory, network, disk) is averaged across hosts. Stacked query/merge/mutation bands show real (undivided) values. This means bands can exceed the server line, which is intentional — it signals work concentrated on specific hosts rather than evenly distributed. The Y-axis 100% represents single-host capacity. A per-host CPU breakdown in the tooltip shows which hosts are hot. See "Cluster-Wide Aggregation" in the Time Travel section for the design rationale and alternatives considered.
