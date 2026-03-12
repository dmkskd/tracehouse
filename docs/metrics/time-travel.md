# Time Travel

## Hybrid data model

Time Travel combines historical log data with live in-flight data. When the visible window includes "now" (within 30s), both sources are fetched in parallel and deduplicated.

| Data | Historical | Live | Dedup key |
| --- | --- | --- | --- |
| Server CPU | `system.asynchronous_metric_log` | — | timestamp |
| Server Memory/IO | `system.metric_log` | — | timestamp |
| Queries | `system.query_log` | `system.processes` | query_id |
| Merges | `system.part_log` | `system.merges` | part_name |
| Mutations | `system.part_log` | `system.merges` (is_mutation=1) | part_name |

Completed log entries take priority. Running operations not yet in logs are appended. This means the right edge of the chart shows real-time data while the rest is historical.

## Server CPU line

Same OS-level metrics as Overview Trends (see [cpu.md](cpu.md)). Values are in µs/s internally for stacking with query bands, but Y-axis labels display percentages (100% = all effective cores saturated). In k8s, 100% = cgroup limit, not host cores.

No interval normalization needed: `asynchronous_metric_log` values are already per-second rates.

## Flat-band approximation

Each query/merge/mutation has a total resource consumed and a wall-clock duration. Since we don't know the resource profile within the operation, we display a flat band:

| Metric | Band height | Reasoning |
| --- | --- | --- |
| Memory | `peak_memory` (as-is) | Instantaneous high-water mark |
| CPU | `cpu_us / duration_s` | Cumulative → rate |
| Network | `(net_send + net_recv) / duration_s` | Cumulative → rate |
| Disk | `(disk_read + disk_write) / duration_s` | Cumulative → rate |

Division uses `max(duration_ms / 1000, 0.001)` to avoid divide-by-zero.

### Limitations

**Memory overcounts when bands overlap.** Each band shows peak memory for the entire duration, but peaks were likely brief moments. Stacked bands will exceed the server line when peaks didn't actually coincide.

**CPU/network/disk lose temporal shape.** Area under the curve is preserved, but bursts are smoothed and idle periods filled. A query burning 8 cores for 1s then waiting 7s looks identical to 1 core steady for 8s.

**In-flight merges have no CPU data.** `system.merges` does not expose ProfileEvents, so currently-running merges show `cpu_us = 0`. Once a merge completes and appears in `part_log`, its band gets real CPU from ProfileEvents. During heavy merge workloads, this creates a visible gap: the server CPU line is high but merge bands contribute nothing on the CPU tab. Use the Engine Internals CPU Sampling Attribution panel (see [engine-internals.md](engine-internals.md)) to identify what's consuming CPU in these situations. TODO: merges are single-threaded (`max_merge_threads` default 1), so `elapsed` from `system.merges` could approximate CPU as `elapsed × 1 core` — much better than 0.

**The server line is the ground truth.** It shows real per-second OS-level measurements. When bands diverge significantly from the server line, the flat-band approximation or the in-flight merge gap is usually the cause. This is expected, not a bug.

## Cluster "All" mode aggregation

In cluster view, the server metric line and stacked activity bands need to tell a consistent story.

**Chosen approach:** avg server line + real (undivided) bands + per-host tooltip.

- Server line: `avg()` across hosts, representing average cluster health
- Bands: real values, not divided by host count. A merge using 900ms/s of CPU shows at 900ms/s
- Y-axis: single-host capacity (100% = one host's cores)
- Bands can exceed the server line. This is intentional: it signals work concentrated on specific hosts

The per-host CPU tooltip (mini bar chart, color-coded green/orange/red) shows which hosts are hot on hover.

**Implementation details:**
- Per-host CPU: `CLUSTER_CPU_TIMESERIES` fetched in "All" mode, returned as `per_host_cpu` map
- CPU cores: per-host via `asynchronous_metric_log`, min across hosts, capped at cgroup limit
- Tooltip: "Cluster avg" with per-host bars; single-host mode shows "Server:" instead
- CPU clamping uses per-host cores: `Math.min(v, cpuCores × 1,000,000)`

**Split view** renders one chart per host, stacked vertically, each with its own Y-axis. This avoids the aggregation problem entirely.

### Why this approach

Three alternatives were considered and rejected:

1. **Avg line + avg bands** — merge using 900ms/s appears as 225ms/s (÷4 hosts). Misleading.
2. **Sum both, scale to cluster capacity** — Y-axis goes to 400% on 4 nodes. Unintuitive.
3. **Avg line + stacked bands (undivided)** with band division — bands too small, split view showed 50-90% while "All" looked idle.

The current approach is the least misleading compression of N-dimensional data into one chart. Split view remains the gold standard for per-host analysis.
