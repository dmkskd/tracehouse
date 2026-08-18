# Proposal: Wait vs work breakdown

**Status:** Phases 1–2 implemented, 2026-08-18. Phases 3–5 proposed. Prompted by
an Altinity write-up of a Kafka→ClickHouse pipeline where the fix hinged on a
signal TraceHouse collects but never surfaces.

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

## Phase 3 — Distributed tab: coordinator overhead split

The smallest change of everything here, applied to a number already on screen.

"Coordinator overhead: 1.13s" (`DistributedQueryTopology.tsx:383`) is currently
pure wall-clock arithmetic — coordinator duration minus slowest child. With
`NetworkReceiveElapsedMicroseconds` from the coordinator's own `profileEvents`
it splits into *waiting on shards* vs *merging results locally*. That is the
article's diagnostic, on data already rendered.

## Phase 4 — Distributed tab: per-node wait columns

`DistributedSkewMetric` is a closed union (`distributed-query-topology.ts:393`)
consumed by `distributedReadMetricValue` (`:1335`) and the metrics array
(`:1381`). Adding `cpu_time_us` and `network_wait_us` there, plus
label/format/colour in `DistributedTab.tsx:320/332/368`, yields two more columns
in the existing bar grid — and skew detection comes free, since
`buildSkewMetricSummary` is metric-generic. Answers "is one shard CPU-bound
while the others idle?"

**The trap.** These are `query_log` final totals, **summed across threads** —
`OSCPUVirtualTimeMicroseconds` routinely exceeds `query_duration_ms` on a
multi-threaded query. They cannot be laid on the wall-clock axis the timeline
uses; the bars would overflow and read as broken. X-Ray escapes this only
because it divides deltas by `dt`.

If the flat per-node duration bar is ever segmented into CPU / network / I/O, it
must be a **100%-composition** bar normalized against `RealTimeMicroseconds`
(also thread-summed, so self-consistent), labelled "thread time" and visually
distinct from the wall-clock timeline above it. Dividing by thread count to
approximate wall clock is the tempting alternative and should be avoided — it is
an estimate that will be read as a measurement.

**Gap in the degraded path.** `SUB_QUERIES`
(`packages/core/src/queries/query-queries.ts:145`) does not select CPU or network
events, and the fallback constructs executions with `profileEvents: false`
(`DistributedQueryTopology.tsx:169`). When topology inference fails and the tab
falls back to "Query log only", any new columns render empty. Two extra
`ProfileEvents[...]` lines in `SUB_QUERIES` close it; worth doing in the same
pass so the columns do not silently blank out.

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

## Suggested order

Phase 1 and 2 together — same edit surface, validatable against existing
history, and they close the blind spot that motivated this. Phase 3 next as a
cheap standalone win. Phase 4 only with the composition-bar caveat respected.
Phase 5 is its own body of work and should not block the rest.

## Reference

- [Pipeline Optimization for ClickHouse Distributed Tables with Synchronous Inserts](https://altinity.com/blog/pipeline-optimization-for-clickhouse-distributed-tables-with-synchronous-inserts) — Altinity
