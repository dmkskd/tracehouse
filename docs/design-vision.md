# Design Vision — TraceHouse

> Consolidated from the original design documents. This captures the conceptual model
> and architectural decisions. For implementation details, see:
> - [metrics-calculations.md](metrics-calculations.md) — actual formulas used
> - [dashboard-data-sources.md](dashboard-data-sources.md) — UI element → system table mapping
> - [clickhouse-observability-tiers.md](clickhouse-observability-tiers.md) — system table architecture

---

## The Three Actors

A running ClickHouse system is made of three interacting actors:

### Actor 1: The Server (The Machine)

The physical substrate: CPU cores, memory, disks, network. It exists whether or not
anyone is talking to it. It has resource ceilings (total RAM, cores, disk) and consumption
patterns that shift over time.

### Actor 2: The Users (The Demand)

Clients submitting queries — SELECTs, INSERTs, DDL. Each session consumes resources.
Users create demand: they read data (scan parts, decompress blocks, evaluate filters),
write data (create new parts, buffer async inserts), and compete for shared resources.

### Actor 3: The Background Processes (The Housekeeping)

ClickHouse's architecture (VLDB paper, Schulze et al. 2024) is built around asynchronous
background work:

- **Merges** — continuously combine smaller parts into larger ones (the LSM-tree heartbeat)
- **Mutations** — ALTER UPDATE/DELETE rewriting parts (async, can run for days)
- **Replication** — fetching parts, replaying the replication log, coordinating through Keeper
- **Materialized Views** — triggered on insert, transform and route data
- **TTL Processing** — moving data to cheaper tiers, re-compressing, deleting expired data

These three actors interact: users create parts (INSERT), merges consolidate them,
replication distributes them, queries read them. Monitoring means watching all three
simultaneously and understanding how they compete for the same physical resources.

---

## The Core Problem

Standard monitoring shows symptoms, not causes:
- "CPU is at 200%" — but not WHY
- "Memory is at 85%" — but not WHO
- "Disk IO is 800 MB/s" — but not WHAT

This tool shows attribution:
- "CPU is at 200%. Of that: 120% is queries (query X alone uses 45%), 60% is merges,
  15% is a stuck mutation on table Y, 5% is other"
- "Memory is 85%. Breakdown: 40% query buffers, 15% mark cache, 10% uncompressed cache,
  8% primary keys, 12% jemalloc overhead, 15% OS page cache"

---

## Attribution Model

### CPU Attribution

```
Total CPU = Query CPU + Merge CPU + Mutation CPU + Replication CPU + Other
```

| Category | Live source | Historical source |
|---|---|---|
| Total process CPU | `system.asynchronous_metrics` → `OSCPUVirtualTimeMicroseconds` | `system.metric_log` |
| Per-query CPU | `system.processes` → ProfileEvents | `system.query_log` → ProfileEvents |
| Per-merge CPU | NOT available from `system.merges` | `system.part_log` → ProfileEvents (MergeParts) |
| Per-mutation CPU | NOT available | `system.part_log` → ProfileEvents (MutatePart) |
| Materialized views | NOT available | `system.query_views_log` → ProfileEvents |

The merge CPU gap: `system.merges` has no ProfileEvents. We use heuristic estimation
for in-flight merges (count × estimated cores per merge) and actual CPU data from
`system.part_log` for completed merges. See [metrics-calculations.md](metrics-calculations.md)
for the full formula.

### Memory Attribution (instantaneous)

```
RSS = Query buffers + Merge buffers + Mark cache + Uncompressed cache
    + Primary keys + Dictionaries + jemalloc overhead + Other
```

All components are directly queryable from virtual tables (zero-cost).

### IO Attribution

Same pattern as CPU — live query IO from `system.processes` ProfileEvents,
merge/mutation IO from `system.part_log` for completed operations.

---

## Data Collection Strategy

| Tier | Interval | Tables | Cost |
|---|---|---|---|
| Tier 1 (live state) | 2–5s | `asynchronous_metrics`, `metrics`, `processes`, `merges`, `mutations` | All virtual — near-zero |
| Tier 2 (recent completed) | 30s | `query_log`, `part_log`, `events` (deltas) | MergeTree reads, small time windows |
| Tier 3 (structural) | 60s | `parts`, `disks`, `replicas`, `dictionaries` | Virtual or light aggregation |

Total overhead: ~10 lightweight queries per 30-second cycle. All read-only SELECTs
against `system.*` tables.

---

## System Table Taxonomy

| Category | Table | Type | Actor |
|---|---|---|---|
| Instantaneous gauges | `system.metrics` | Virtual | All |
| Background-computed gauges | `system.asynchronous_metrics` | Virtual | Server |
| Cumulative counters | `system.events` | Virtual | All |
| Time-series of gauges+counters | `system.metric_log` | MergeTree | All |
| Time-series of async metrics | `system.asynchronous_metric_log` | MergeTree | Server |
| Running queries | `system.processes` | Virtual | Users |
| Query history | `system.query_log` | MergeTree | Users |
| Per-thread query details | `system.query_thread_log` | MergeTree | Users |
| MV execution | `system.query_views_log` | MergeTree | Background |
| Running merges | `system.merges` | Virtual | Background |
| Part lifecycle events | `system.part_log` | MergeTree | Background |
| Active mutations | `system.mutations` | Virtual | Background |
| Replication status | `system.replicas` | Virtual | Background |
| Replication queue | `system.replication_queue` | Virtual | Background |
| Data parts | `system.parts` | Virtual | Storage |
| Column statistics | `system.columns` | Virtual | Storage |
| Disk volumes | `system.disks` | Virtual | Server |
| Tables metadata | `system.tables` | Virtual | Storage |
| Cluster topology | `system.clusters` | Virtual | Server |
| Dictionaries | `system.dictionaries` | Virtual | Storage |
| Server text logs | `system.text_log` | MergeTree | Server |
| Stack trace profiler | `system.trace_log` | MergeTree | Server |
| OpenTelemetry spans | `system.opentelemetry_span_log` | MergeTree | All |
| Crash records | `system.crash_log` | MergeTree | Server |

Virtual tables are computed from in-memory state — free to query. MergeTree tables
(the `*_log` tables) are persisted and grow over time — they need TTL management.

---

## Key Caveats

1. **The merge CPU gap**: `system.merges` has no ProfileEvents. Live merge CPU is estimated.
2. **ProfileEvents in processes**: Requires CH 22.x+. Older versions lack this.
3. **Per-operator memory breakdown**: Not available. ClickHouse reports total `memory_usage`
   per query, not per-operator (hash table vs sort buffer vs JOIN).
4. **Core-to-owner mapping**: Per-core utilization is available from async_metrics, but
   attributing which core runs which query/merge requires OS-level `/proc` introspection.
5. **Log table flush lag**: `*_log` tables flush every ~7.5s. Under heavy load, flush
   intervals stretch. Use 30s+ query windows for reliable data.
6. **MV double-counting**: INSERT query_log ProfileEvents may include MV work. Don't
   add `query_views_log` on top — use it to break down the INSERT's CPU.
7. **Thread pool metrics ≠ merge activity**: `MergeTreeBackgroundExecutorThreadsActive`
   handles many background operations, not just merges. Always check `system.merges`
   for actual merge activity.
