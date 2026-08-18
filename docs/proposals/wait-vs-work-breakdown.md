# Proposal: Wait vs work breakdown

**Status:** Phases 1 through 4.5 implemented and validated on a live cluster,
2026-08-18. Phase 5 not started. Prompted by an Altinity write-up of a Kafka→ClickHouse
pipeline where the fix hinged on a signal TraceHouse collects but never surfaces.

Phases 1–2 gave X-Ray a per-second answer to "where did the time go". Phases 3–4
make that answer a *summary* — one bar, wherever a duration is already shown.
Phase 4.5 takes it from one query to the whole workload, somewhere a user can
pull the numbers apart themselves.

## The failure mode we are blind to

A cluster with idle CPU and 8.6s ingestion lag. Two chained materialized views
both wrote through `Distributed` tables with `distributed_foreground_insert = 1`.
`system.query_views_log` showed ~3.3s wall clock per view execution, of which
~3.3s was network wait and *milliseconds* were CPU. The cost stayed flat across
a 60× throughput swing (13 → 778 rows/s) — a fixed per-insert round trip, not
saturation. Pointing one MV at its `_local` table halved end-to-end lag.

Nothing in TraceHouse today would have led an operator to that conclusion:

- Every duration we render is **undifferentiated**. The Distributed tab shows a
  node took 8.53s; X-Ray shows 2.1 peak cores. Neither says whether the time was
  spent working or waiting.
- X-Ray samples network as **bytes only**, so a query blocked on a socket while
  transferring almost nothing draws a flat near-zero network line. That is worse
  than a gap — it actively suggests the network is fine.
- The synchronous Distributed path has no coverage at all. Replication tooling
  (`system.distribution_queue`, `DistributedFilesToInsert`) assumes the *async*
  failure mode; under `distributed_foreground_insert = 1` those all read zero
  while the cluster stalls.

The generalizable question is not about Distributed inserts specifically. It is
**"where did this second go?"** — and it applies equally to S3, remote disks,
replication acks, and lock contention.

## The signal is already collected

`tracehouse.processes_history` stores `ProfileEvents Map(String, UInt64)` in full
(`infra/scripts/setup_sampling.sh:431`), and `DISTRIBUTED_TOPOLOGY_EXECUTIONS`
selects the whole `ProfileEvents` map per node
(`packages/core/src/queries/query-queries.ts:199`).

Everything below reads keys out of maps we already store. **No sampler change,
no re-deploy, and it works retroactively against history already collected.**

## Phase 1 — X-Ray: network wait as concurrency  ✅ implemented

The highest-value change, and the one the sampler is already shaped for.

`d_cpu_cores` is `Δ OSCPUVirtualTimeMicroseconds / 1e6 / dt`
(`packages/core/src/queries/process-queries.ts:115`) — thread-summed µs divided
by wall seconds. That normalization is what makes "2.1 peak cores" meaningful
rather than nonsense, and it is exactly what the raw ProfileEvents totals cannot
give us elsewhere.

Applying the identical formula to `NetworkReceiveElapsedMicroseconds` yields
**threads-worth of blocking on network during that second** — same unit
(concurrency), same axis, directly stackable against cores:

```
t=4s   cpu ██▊ 2.1        ← work
       net ▏  0.1
t=7s   cpu ▎  0.2
       net ███████▊ 7.8   ← 8 threads parked on a socket
```

A corridor whose width comes from waiting rather than working, and the moment it
flips is visible.

**Changes.** In both SQL builders in `process-queries.ts` (the `sum()` blocks at
`:89` and `:164`): pull `NetworkReceiveElapsedMicroseconds` and
`NetworkSendElapsedMicroseconds` in the inner projection alongside the existing
`pe_*` bindings, add the matching `raw_d_*` lag lines, emit two columns. Then
extend the `ProcessSample` interface (`:20`) and add entries to the metric
registry (`:291`).

## Phase 2 — correct the mislabelled I/O wait  ✅ implemented

Independent of the above but the same edit surface, and a genuine correctness
bug.

`pe_io_wait` reads `OSCPUWaitMicroseconds` (`process-queries.ts:129`, `:202`),
stores it as `io_wait_us`, exposes it as `d_io_wait_s`, labels it **"I/O Wait"**
in the metric registry (`:297`), and titles the analytics chart **"Query I/O
Wait"** (`frontend/src/components/analytics/queries/xray.ts:125`).

But `OSCPUWaitMicroseconds` is CPU *scheduling* wait — thread runnable, not
scheduled. Our own reference already draws the distinction correctly
(`docs/clickhouse-profile-events-classification.md:197-198`). The disk metric is
`OSIOWaitMicroseconds`, which we do not sample at all.

The two point at opposite remedies: high runqueue wait means CPU
oversubscription (add cores, reduce concurrency); high I/O wait means disk-bound
(different fix entirely). Today the chart labelled "I/O Wait" will send an
operator the wrong way.

This is a missed call site rather than a considered trade-off. Analytics already
treats the two as distinct metrics — `io_wait` from `OSIOWaitMicroseconds` and
`cpu_wait` from `OSCPUWaitMicroseconds`
(`frontend/src/components/analytics/queries/advancedDashboard.ts:119,130`) — and
commit `0cce39d` explicitly fixed this same confusion there ("IO wait to use
`OSIOWaitMicroseconds`"). X-Ray predates that fix (`bbeab8a`) and was not swept
into it.

**Sample both, do not swap one for the other.** `OSIOWaitMicroseconds` depends on
procfs/taskstats access and can read zero in some containerized deployments,
where `OSCPUWaitMicroseconds` still populates — so dropping the latter would lose
signal on exactly the environments where the former is unavailable. Name them
`d_cpu_wait_s` and `d_io_wait_s` respectively and fix the chart title.

With Phases 1 and 2 done, X-Ray answers "where did the second go" as a stacked
composition of CPU / network wait / runqueue wait / disk wait. The residual
(elapsed minus everything accounted) is itself informative — that is where lock
contention and scheduling gaps hide.

Note this is sampled at the sampler's REFRESH interval, not exact. The signal in
question was a 3.3s-out-of-3.3s ratio; it survives sampling comfortably.

## Phase 3 — `<TimeBreakdownBar>` in Overview: where a query's time went  ✅ implemented

X-Ray answers this per second, for one query, on a page you have to go looking
for. Phases 3 and 4 answer it as a *summary*, everywhere a duration is already
rendered.

### Where it belongs

Overview, and Overview first — because it is the only placement that works on
*every* query. Of the modal's tabs only `overview`, `sql`, `details` and
`analytics` are unconditional; Distributed is always rendered but greyed out via
`unavailable` when there is no fan-out, and X-Ray needs `processes_history`.

| Placement | What it shows | Available |
|---|---|---|
| **Overview → Time bar** | the query's whole-life composition | always |
| Distributed → timeline | per-node composition + coordinator split | only when distributed |
| X-Ray → corridor | per-second evolution | needs the sampler |

Overview is the floor; the other two are progressive enhancement.

There is already an exact home for it. `ResourcePressurePreview`
(`OverviewTab.tsx:714-727`) renders a `MetricBar` stack whose first row *is*
Time:

```
Time    ████████░░░░░░  9.66s     ← today: pressure ratio vs other queries
Memory  ██████░░░░░░░░  1.15 GB
CPU     ███░░░░░░░░░░░  12.4s
```

That bar currently answers "how heavy was this relative to peers". Segmenting it
makes it answer "what was that time made of" — same row, no new real estate:

```
Time    ██▓▓▓▓░░░░▒▒▒▒  9.66s
        cpu 12% · net 34% · queue 8% · unacct 46%
```

The CPU and I/O bars beneath it gain meaning by contrast: `CPU 12.4s` next to a
Time bar that is 46% unaccounted reads very differently than CPU alone does.

### The rule the whole design rests on

There are two clocks and they must never be mixed:

- **Wall clock** (`query_duration_ms`) — what the user waited. 8.53s.
- **Thread time** (ProfileEvents) — summed across threads, so always ≥ wall
  clock when parallel. 24 threads × 8.53s ≈ 205 thread-seconds.

The breakdown only exists in thread time. Therefore:

> **Bar length = wall clock (truth). Bar fill = thread-time composition (ratio).**

Nothing thread-summed ever touches the wall-clock axis, so no bar overflows and
no number claims more precision than it has. The bar keeps showing real elapsed
time while answering "of the effort spent here, how much was waiting?"

This is what makes the earlier trap tractable rather than blocking: a
100%-composition fill is a *ratio*, and ratios compose onto a wall-clock bar
safely. Dividing thread time by thread count to approximate wall clock remains
the wrong answer — it is an estimate that will be read as a measurement.

### The decomposition

Denominator is `RealTimeMicroseconds` (also thread-summed, so self-consistent):

| Segment | Counter |
|---|---|
| CPU | `OSCPUVirtualTimeMicroseconds` |
| Disk wait | `OSIOWaitMicroseconds` |
| CPU queue | `OSCPUWaitMicroseconds` |
| Network wait | `NetworkReceiveElapsedMicroseconds` + `NetworkSendElapsedMicroseconds` |
| **Unaccounted** | `RealTimeMicroseconds − Σ(above)` |

```
Query 9.66s   ██▓▓▓▓▓▓░░░░░░░░░░░░░▒▒▒▒
              cpu 12% · net 34% · queue 8% · unacct 46%
```

**The residual is the reason to build one composed bar** rather than four
separate charts: lock contention, mutex waits, sleeps and scheduling gaps have
no counter of their own and only ever appear as the gap.

But it must not be over-read. `RealTimeMicroseconds` is thread *lifetime*, not
busy time, so an alive-but-idle thread lands in the residual too. Measured on a
live cluster: a 12ms query with 8 threads showed 70% unaccounted, which was
simply threads waiting for the pipeline to hand them work — over-parallelisation
on a tiny query, not contention. Short queries will routinely look this way.

So the segment names the *state* ("thread alive but idle") rather than
diagnosing a cause. A large residual is a prompt to open the Threads tab or the
pipeline, not a finding on its own.

### Guards

Learned from the Phase 1–2 clamp work — these counters do not respect the bounds
you would expect:

- Segments can sum to more than `RealTimeMicroseconds` (overlapping accounting).
  Normalize proportionally when Σ > 100% rather than letting a segment overflow.
- Only render the residual when positive.
- `OSIOWaitMicroseconds` reads 0 without procfs/taskstats, so an absent disk
  segment must not be presented as "no disk wait".

## Phase 4 — the same bar in the distributed timeline  ✅ implemented

One component, three placements. This is where the payoff is highest.

```
Coordinator  ██▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓░░░░░  9.66s
   └ overhead 1.13s: ▓ waiting on shards 0.9s · █ merging 0.2s
s1r2         ███████▓▓▓▓░░░░░         7.57s
s2r2         ████▓▓▓▓▓▓▓▓▓▓░░         8.53s   ← net-heavy, the outlier
```

Skew stops being "which node took longest" and becomes **"which node waited
differently"**. A shard that is CPU-red while its peer is network-blue is a
completely different diagnosis from one that is merely slower — and the current
flat bars cannot express that difference at all.

**Coordinator overhead split.** "Coordinator overhead: 1.13s"
(`DistributedQueryTopology.tsx:383`) is currently pure wall-clock arithmetic —
coordinator duration minus slowest child. The intent is to split it into
*waiting on shards* vs *merging results locally* using
`NetworkReceiveElapsedMicroseconds` from the coordinator's own `profileEvents`.

**That counter does not work for this. Measured, on a clean case.** A 2-node
distributed SELECT whose coordinator did no local reading — both children
remote — waited 5.63s of its 5.64s for the slower shard:

```
real_s   28.19   (5 threads × 5.64s, all alive throughout)
cpu_s     0.23  →  0.8%
queue_s   5.77  → 20.5%
net_recv  0.01  →  0.04%   <- the 5.63s wait is not here
residual 22.18  → 78.7%    <- it is all here
```

`NetworkReceiveElapsedMicroseconds` recorded **10ms of a 5.63s wait**. The
counters time actual socket read/write syscalls, and `RemoteQueryExecutor` waits
on shard results with async polling (epoll) rather than blocking reads, so the
idle time between packets is never attributed to network.

**The rule this gives:**

- *Pushing* to shards (distributed INSERT, synchronous) → blocking writes →
  captured by `NetworkSendElapsedMicroseconds`. This is why the Altinity case
  surfaced as network wait.
- *Waiting* for shard results (distributed SELECT) → async epoll → **not
  captured**, lands in the residual.

So the network segment is meaningful on the write path and near-useless on the
read path, and the coordinator-overhead split needs a different source:
`processors_profile_log` wait stages, or text_log send/receive events. Note the
wall-clock arithmetic already on screen (coordinator duration minus slowest
child) is honest and needs no counter — in the case above it read 9ms, correctly
saying the coordinator added almost nothing of its own.

**Per-node skew columns.** `DistributedSkewMetric` is a closed union
(`distributed-query-topology.ts:393`) consumed by `distributedReadMetricValue`
(`:1335`) and the metrics array (`:1381`). Adding `cpu_time_us` and
`network_wait_us` there, plus label/format/colour in
`DistributedTab.tsx:320/332/368`, yields two more columns in the existing grid —
and skew detection comes free, since `buildSkewMetricSummary` is metric-generic.

### What it costs

Most of the data is already selected: `cpu_time_us`, `io_wait_us` (already
correctly `OSIOWaitMicroseconds` on this path), `real_time_us`, `user_time_us`,
`system_time_us` — `query-queries.ts:110-125`. `QueryDetail.tsx:170-175` already
computes `parallelism` and `ioWaitPct` from `real_time_us`, so both the data and
the precedent exist.

Missing: `OSCPUWaitMicroseconds` and the two `Network*Elapsed` — three lines.
`DISTRIBUTED_TOPOLOGY_EXECUTIONS` already selects the whole `ProfileEvents` map
(`query-queries.ts:199`), so the distributed side needs no query change at all.

**Gap in the degraded path.** `SUB_QUERIES`
(`packages/core/src/queries/query-queries.ts:145`) does not select CPU or network
events, and the fallback constructs executions with `profileEvents: false`
(`DistributedQueryTopology.tsx:169`). When topology inference fails and the tab
falls back to "Query log only", the bars render unsegmented. Two extra
`ProfileEvents[...]` lines in `SUB_QUERIES` close it; worth doing in the same
pass so the fill does not silently vanish.

## Phase 4.5 — an analytics dashboard to explore the numbers  ✅ implemented

Phases 3–4 answer "where did *this* query's time go". This phase answers "where
is time going *across the workload*", and lets a user pull the numbers apart
themselves rather than reading a fixed verdict.

The modal is per-query and reactive — you already have to suspect a query to
open it. A dashboard is where you go *without* a suspect, and where the shape of
a whole workload becomes visible.

Analytics is the right host: the catalog already supports `stacked_bar` (14
existing uses), `{{time_range}}`, `{{drill_value:db}}` / `{{drill_value:tbl}}`
filters, `@query_link` for click-through to the modal, and `@drill` for
cross-panel navigation. So "playing with the numbers" is mostly existing
machinery, not new UI.

Panels worth having, roughly in value order:

1. **Wait composition by query shape** — `stacked_bar` over
   `normalized_query_hash`, segments as in Phase 3. Which *shape* of query wastes
   the most time waiting, aggregated over every run rather than one sample.
   `@query_link` opens a representative execution.
2. **Composition over time** — the same segments as `area`, bucketed. Shows a
   cluster changing character: a rising network band means fan-out cost growing,
   a rising queue band means CPU contention arriving.
3. **The flat-cost detector** — per-insert (or per-query) latency against
   rows/s. A slope means saturation; a horizontal line means fixed round-trip
   overhead. This is the single most differentiated panel in the proposal: it
   delivers an insight a number genuinely cannot, and it is the reasoning that
   solved the article's case. Both axes already exist in `part_log` and
   `query_log`.
4. **Wait by user / by table** — same composition grouped differently, for
   attributing waiting to a tenant or a hot table.

All four read the same segment accessor Phase 3 defines, so this phase should
not start before Phase 3 lands — otherwise the composition logic gets written
twice and drifts.

Two caveats carried from Phases 1–2 and worth restating in dashboard form, where
a stray value is far less scrutinised than in a modal:

- Aggregating thread-summed counters across queries is only meaningful as a
  *ratio*. Summed raw microseconds across a workload mean nothing on a wall-clock
  axis.
- A zero disk-wait band may mean "no disk wait" or "counter unavailable"
  (procfs/taskstats). At dashboard scale that ambiguity spreads silently — the
  panel needs to say which.

## Three layers of explaining "Parked"

The composition is always the same five segments. What changes by deployment is
how far you can subdivide the parked one. Each layer is capability-probed and
degrades to the layer above it, so the bar never depends on a source being
present.

| Layer | Source | Availability | What it says about Parked |
|---|---|---|---|
| **1** | `query_log` ProfileEvents | always | that it happened, and how much |
| **2** | `processors_profile_log` | usually — no special privilege | *which pipeline stage*, and which side (starved on input vs back-pressured on output) |
| **3** | `trace_log` Real + introspection | often blocked, especially on SaaS | *the actual blocking call*, by proportion |

**Layer 1** is the floor. It gives CPU / Queue / Disk / Network and a residual it
cannot open. Measured below: on distributed coordinators that residual reaches
99%, and no amount of ProfileEvents arithmetic reduces it.

**Layer 2** ranks the stalled stages. `input_wait_elapsed_us` and
`output_wait_elapsed_us` per processor distinguish *starved waiting for upstream
data* from *back-pressured because downstream could not consume*. Per-processor
waits overlap heavily — measured at 3x-100x the thread-time residual — so this
layer ranks stages and never apportions a duration.

**Layer 3** is the real answer, and the least available. `Real` traces fire on a
per-thread wall-clock timer, so they sample threads while they are blocked and
capture the stack. Walking past the unresolved kernel frame names the call:

```
ExecutionThreadContext::wait          48,255   parked pipeline thread
Epoll::getManyReady                   37,234   waiting for a remote shard
ConcurrentBoundedQueue<Chunk>::pop    29,792   waiting on an async queue
WriteBufferFromPocoSocket::send        7,686   blocked writing to the client
```

That is every hypothesis in this document, measured. Note the second row: the
async shard wait that `NetworkReceiveElapsedMicroseconds` misses entirely is
plainly visible here.

Layer 3 gates on three things, all probeable: `allow_introspection_functions`
(a privilege, commonly denied to read-only users and on hosted offerings),
symbols in the binary, and `query_profiler_real_time_period_ns > 0`. Sampling
is also incomplete — coverage measured at 24-65% of `RealTimeMicroseconds` at a
10ms period — so this layer gives proportions among sampled blocked stacks, not
an absolute apportionment.

**The rule that holds across all three layers:** every source here is usable as
a ratio and none as a duration on a wall-clock axis.

### Layers 2 and 3 compared, same queries

They are not two resolutions of one answer — they answer different questions,
and neither implies the other.

```
query b5e19c26
  L2  MarshallBlocks    22.1s in-wait    starved
      MergeTreeSelect   17.4s out-wait   back-pressured
  L3  pipeline 32.3% · client 31.7% · remote_shard 24.4% · queue 9.1%

query 9a1d7fa0
  L2  MarshallBlocks    21.4s out-wait   back-pressured
      ConvertingAggregated... 19.8s out-wait   back-pressured
  L3  pipeline 31.1% · client 28.6% · remote_shard 26.1% · queue 10.7%
```

- **Layer 2 locates the choke point in the plan.** "MergeTreeSelect is
  back-pressured" tells you reads are being held up by something downstream —
  actionable against the query shape.
- **Layer 3 names the cause outside the plan.** "client 32%" says the consumer
  cannot keep up. Layer 2 cannot see that: it reports back-pressure without
  saying what is applying it.
- Conversely layer 3 cannot see plan structure — "MarshallBlocks starved" is
  invisible there, folded into `pipeline` and `queue`.

So the useful reading is both together: *reads back-pressured (L2) because the
client is not consuming (L3)*. Neither sentence is derivable from one layer.

Practical consequence for the UI: layer 3 is not a drill-down of layer 2 and
should not be rendered as one. Two sentences, not a tree.

## Layer 1 detail: where the floor is

Measured on the dev cluster, this is the limit of what `query_log` can explain.
A distributed GROUP BY over `nyc_taxi.trips` (168ms wall):

| row | RealTime | CPU | named waits | residual |
|---|---|---|---|---|
| coordinator | 834ms | 7.6ms | ~1ms | **99%** |
| shard child A | 814ms | 486ms | ~78ms | ~31% |
| shard child B | 703ms | 420ms | ~54ms | ~31% |

Two structurally untimed contributors dominate it:

- **Coordinators blocked on shards.** Remote reads go through async epoll, so
  blocking never hits the receive timer — `NetworkReceiveElapsedMicroseconds`
  read 305µs on that coordinator. Nothing else in its ProfileEvents map is
  nameable: no disk read, no IO wait, no thread-pool wait.
- **Pipeline threads starved on ports.** Threads attached to the query thread
  group accrue RealTime while waiting on input/output ports, and no `query_log`
  counter names it.

`system.processors_profile_log` names both, per plan step. For the query above,
`ExpressionTransform` showed 937ms of `input_wait_elapsed_us` against 208ms of
real work in `AggregatingTransform`. Cluster-wide the pattern is stark —
`ExpressionTransform` totals 1,570s of input wait against 9.6s of work.

That turns "80% unaccounted" into something actionable: *starved waiting on the
read pool* vs *blocked pushing to the coordinator*. It is the only source found
that decomposes the residual.

**Constraints for whoever builds it:**

- `log_query_threads` defaults to 0 (verified `changed = 0` on a live cluster),
  so per-thread data is not a portable fallback. Do not design around it.
- `log_processors_profiles` was enabled on the dev cluster (`changed = 1`), so
  treat it as a capability to detect, not to assume.
- `input_wait`/`output_wait` are summed across processors and threads, so they
  are subject to the same rule as everything else here: usable as ratios, never
  as durations on a wall-clock axis.
- `OSIOWaitMicroseconds` is 0 on every pod in this deployment (no taskstats in
  the containers), so the Disk segment reads zero there regardless.

**Layer 1 alone should not present a residual it cannot explain.** Where neither
layer 2 nor 3 is available and the named segments do not account for most of the
denominator, fall back to the existing CPU bar rather than showing a user "80%
parked" with no way to open it.

Build order follows availability, not precision: layer 2 first, since it works
everywhere and turns the residual from a dead end into "which stage". Layer 3
layers on top for the deployments that allow it — the same segment, subdivided
further, no different UI.

## Phase 5 — materialized view coverage

`system.query_views_log` is granted to the demo read-only user, configured in
infra, and documented (`docs/clickhouse-observability-tiers.md:338`,
`docs/design-vision.md:154`) — but the only analytics query touching it is the
RAM-retrospection one (`frontend/src/components/analytics/queries/altinityKb.ts:235`).
There is no MV group.

This is the missing tier between "the insert was slow" and "which part was
written". Worth adding: slowest MVs by `view_duration_ms`, MV chain fan-out per
`initial_query_id`, failures by `exception_code`, and read-vs-written row
amplification.

Two further ideas that follow from it, both larger:

- **Flat-cost detector.** Plot per-insert latency against rows/s. A slope means
  saturation; a horizontal line means fixed round-trip overhead. Both axes exist
  in `part_log` and `query_log` already — a derived metric, not new
  instrumentation. This is the most differentiated idea in the proposal: a chart
  that delivers an insight a number cannot.
- **MV → target topology.** A graph of MV → target annotated with target engine
  (`Distributed` vs `ReplicatedMergeTree`) makes chained cross-shard fan-out
  visible as *shape* rather than something inferred from DDL.
  `distributed-query-topology.ts` and `DistributedQueryTopology.tsx` already do
  this for SELECT fan-out; this is the same rendering on the write path.

## What building it changed

Five claims in the sections above were wrong, and are corrected here rather
than quietly edited out — each was disproven by measurement, and the reasoning
is worth keeping.

**1. The composition must not be painted onto a timeline.** Phase 4 originally
filled the Gantt bars with the composition, on the argument that bar length
stays wall clock so only the paint changes. In practice a horizontally
segmented bar on a time axis reads as a *sequence* — "CPU, then queue, then
parked" — when the parts have no order. The composition now appears only in the
hover panel, which has no time axis. Stacking vertically was tried first and
still implied phases.

**2. The coordinator-overhead split cannot use `Network*Elapsed`.** Already
noted below, but confirmed on a clean case: a coordinator that waited 5.63s of
its 5.64s for a shard recorded 10ms. The existing wall-clock arithmetic
(coordinator duration minus slowest child) is honest and needs no counter.

**3. The residual is not mainly lock contention.** `RealTimeMicroseconds` is
thread *lifetime*, so idle-but-alive threads dominate it. Three
indistinguishable causes: over-parallelised short queries, coordinators on
async epoll, and genuine contention. Renamed from "Unaccounted" to **Parked**,
which names the thread state instead of implying a measurement failure.

**4. `query_thread_log` is not a portable fallback.** `log_query_threads`
defaults to 0 (verified `changed = 0`), so the exact per-thread denominator is
unavailable on most deployments. The connection-handler discount is an
approximation instead — and it must be *skipped* when it would not fit rather
than clamped, or it reports a fabricated 100% accounting. That clamp fired on 5
of 6 queries before it was caught.

**5. `processors_profile_log` cannot apportion the residual.** Per-processor
waits overlap across concurrently blocked processors — measured at 3x to 100x a
query's thread-time residual. It ranks stalled stages; it never divides a
duration.

One further constraint found while wiring the dashboard: `@query_link` opens a
query via a lookup that filters `is_initial_query = 1`, so any panel listing
shard children produces clicks that silently do nothing. Panels that link must
filter to coordinators.

## Suggested order

Phases 1 through 4.5 are done. What remains:

1. **Grant `allow_introspection_functions`** to the connection user. Layer 3
   works when queried directly but reports "not permitted" in the app, so a
   third of the explanation is currently invisible. No code; it decides whether
   layer 3 earned its place.
2. **Phase 5** — materialized view coverage, below. Still the blind spot the
   original article was actually about.
3. **The X-Ray analytics panels** do not filter `is_initial_query`, so ~3% of
   their rows (shard children) are dead clicks. One line per panel.

## What Phases 1–2 established

Implemented 2026-08-18 and validated against a live 2-shard cluster. Three
findings worth carrying into Phases 3–5:

1. **The mislabelling was doing real damage.** On the test cluster a query
   showed `CPU 0.47 cores` against `CPU Wait 5.228` — 24 threads queueing behind
   a `CGroupMaxCPU=3` cap on a 14-core node. Under the old code that 5.228
   rendered as *"I/O Wait"*, pointing at disks instead of the CPU limit.

2. **Thread counters are discontinuous.** A thread's accumulated counters merge
   into the query total when it *detaches*, and pipeline threads come and go
   throughout execution — so any interval catching a detach absorbs that
   thread's whole history. Observed: `OSCPUWaitMicroseconds` jumping +75s inside
   0.9s on a 4-thread process. Rates are therefore clamped to the interval's
   thread bound and flagged (`rate_clamped`). Measured at ~1.3% of intervals on
   a live cluster, more often mid-query than at teardown.

3. **Zero is ambiguous.** `OSIOWaitMicroseconds` needs procfs/taskstats and reads
   0 where unavailable, which is indistinguishable from "no disk wait" unless
   said explicitly. Both wait counters are sampled rather than substituted.

Points 2 and 3 apply directly to any future breakdown UI: a segment can be
absent because nothing happened, or because the counter was never populated, and
those must not look alike.

## Reference

- [Pipeline Optimization for ClickHouse Distributed Tables with Synchronous Inserts](https://altinity.com/blog/pipeline-optimization-for-clickhouse-distributed-tables-with-synchronous-inserts) — Altinity
