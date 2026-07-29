# Time Travel

See [time-travel-events.md](time-travel-events.md) for the operational event
taxonomy, ClickHouse sources, capability handling, timestamp semantics, and
planned event-source backlog.

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

**In-flight merges use estimated CPU.** `system.merges` does not expose ProfileEvents. Merges are single-threaded by default (`max_merge_threads = 1`), but on busy clusters with many concurrent merges/queries competing for CPU, a merge rarely gets a full core. In-flight merge CPU is estimated as `elapsed × RUNNING_MERGE_CPU_CORES` (currently **0.5**, defined in `timeline-queries.ts`). Once a merge completes and appears in `part_log`, its band switches to real CPU from ProfileEvents. The UI marks estimated values with `~` and "est." in the tooltip. For precise attribution, use the Engine Internals CPU Sampling Attribution panel (see [engine-internals.md](engine-internals.md)).

**Server line vs. bands.** The server line shows real per-second OS-level measurements. Bands use flat-band approximations and (for in-flight merges) estimated CPU, so they won't always match the server line — especially during bursty workloads or when many merges overlap.

## Overall aggregation

For CPU and memory, Overall shows total usage across the selected hosts against
their total capacity:

- CPU line: sum of per-host CPU usage
- CPU capacity: sum of the selected hosts' effective cores
- Memory line: sum of per-host memory usage
- Memory capacity: sum of the selected hosts' RAM
- Activity bands: real values, not divided by host count

Therefore, 100% means all selected capacity is in use. This is equivalent to
`sum(usage) / sum(capacity)` and remains correct when hosts have different CPU
or memory capacities. Network and disk retain their existing average-across-host
aggregation.

Per-host `OSMemoryTotal` values support heterogeneous host RAM. The legacy
container-memory fallback still applies one locally observed cgroup memory
limit to every selected host; heterogeneous per-container RAM limits require a
future per-host cgroup-capacity query and are not claimed as exact here.

The per-host CPU tooltip (mini bar chart, color-coded green/orange/red) still
shows which hosts are hot. Each bar uses that host's own CPU capacity.

**Implementation details:**

- Per-host CPU: `CLUSTER_CPU_TIMESERIES` fetched for the all-host tooltip and
  returned as `per_host_cpu`
- Per-host CPU capacity: returned as `per_host_cpu_cores`
- Capacity source priority and completeness behavior are documented in
  [cpu.md](cpu.md#time-travel-capacity-resolution)
- CPU clamping uses total selected capacity:
  `Math.min(v, totalCpuCores × 1,000,000)`
- If any selected host lacks capacity, CPU percentage labels, the capacity
  summary, clamping, and the 100% reference line are omitted rather than using
  a partial denominator
- RAM metadata follows the same completeness rule: if any selected host still
  lacks RAM capacity after cache validation and refetch, raw memory usage
  remains visible but the RAM summary, percentages, and capacity line are
  omitted
- Cached host metadata is revalidated when cluster-aware CPU data observes a
  different host set, preventing pre-detection one-host metadata from defining
  an All-host capacity
- Host-selection requests are latest-wins: changing the selection starts a new
  request, and an older response cannot overwrite the newer selection
- CPU spike analysis uses the same total/total model as the chart:
  summed selected-host CPU usage divided by summed effective cores
- Per server renders one chart per selected host, each with its own capacity
