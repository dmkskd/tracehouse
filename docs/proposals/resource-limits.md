# Proposal: Resource limits - "why did this wait, why did this fail"

**Status:** draft, 2026-11-11. Nothing built.

Reference for the mechanisms themselves: `clickhouse-patterns/patterns/resource-limits/guide.yaml`
(diagnose by error code / enforce by resource, scope, action).

**Mockup:** https://claude.ai/artifact/3sEk2gXV3H42Dptudg5igi (Limits page with Impact / Plan /
Configuration tabs, and the Query Detail Limits section; sample data, private until shared).

## Why

Understand the impact of limits and workload scheduling on overall performance, from two sides:

- **Q1 - "I want to enforce restrictions: what can I add?"** (before). Answered in order:
  1. Is there contention at all? Workloads only reallocate when demand exceeds supply; with
     no contention a restriction only adds waiting. Source: `metric_log`.
  2. Who competes at the peaks? Consumption and concurrency split by what a workload can
     classify on: user, profile, query kind, client, query shape. Source: `query_log`.
     Extends what the Workload Breakdown dashboard does for Select/Insert/Merge.
  3. What would a cap have done? Replay `query_log` (start, end, `peak_threads_usage`) against
     a hypothetical cap. Always labelled an estimate: it cannot model how a cap changes a
     query's own runtime. Plus which mechanisms this server version supports (guide data).
- **Q2 - "Are workloads greatly affecting my queries?"** (after).
  1. How much query time was held back, by workload, user, query shape, over time.
  2. Is that more than before? Same query shape before vs after a workload definition changed.
     This is why definitions must be captured with timestamps.
  3. Did workloads cause failures? `RESOURCE_ACCESS_DENIED`, admission timeouts, reservation kills.

Q1 is mostly for self-managed admins, plus Cloud users adding workloads under the operator's
root. Q2 is for everyone, including Cloud users whose ceiling is not in their own DDL, so Q2
comes first.

## Premise

Two families of limits, with different audiences:

1. **Classic limits** - server/user/query memory, `max_concurrent_queries*`, quotas,
   settings profiles, timeouts, scan limits, bandwidth throttlers, spill thresholds.
   Every install has them; users meet them as errors or as unexplained slowness.
2. **Workload scheduling** (`CREATE RESOURCE` / `CREATE WORKLOAD`). The docs' cloud recipe
   has the operator define the root in server config (immutable from SQL) and lets users
   create *additional* workloads inside it. Users only subdivide a share they do not
   control. Public adoption outside ClickHouse is thin (~8 external reporters on GitHub).

So TraceHouse **explains, never authors**. The value is making the ceiling visible
("this query waited 2.1s for a CPU slot under `/all`, defined in server config") and
showing how close a query came to each limit. No UI to create workloads, profiles or quotas;
for Q1 it gives evidence and estimates, and links to the guide entry for each mechanism.

**Naming.** TraceHouse already uses "Workload" for the Workload Breakdown dashboard
(`workloadBreakdown.ts`, group `Workload`). Call this feature **Limits**, and say
"ClickHouse workload" when referring to the scheduler entity.

## What data exists

Checked against ClickHouse 26.10.2 (`clickhouse local`, `system.columns` / `system.events`).

### A. Per query, historical, no sampler - `query_log`

Per-query ProfileEvents already record time spent blocked by a limit:

| Mechanism | ProfileEvents |
|---|---|
| CPU slots (concurrency control / workload CPU) | `ConcurrencyControlWaitMicroseconds`, `ConcurrencyControlPreemptedMicroseconds`, `ConcurrencyControlQueriesDelayed`, `ConcurrencyControlSlotsDelayed`, `ConcurrencyControlPreemptions` |
| Workload IO scheduling | `SchedulerIOReadWaitMicroseconds`, `SchedulerIOWriteWaitMicroseconds` (+ `*Requests`, `*Bytes`) |
| Workload memory reservation | `MemoryReservationAdmitMicroseconds`, `MemoryReservationIncreaseMicroseconds`, `MemoryReservationKilled`, `MemoryReservationFailed`, `MemoryReservationSpilledBytes` |
| Bandwidth throttlers (server/user/query, local/remote/S3/Azure/backup) | `ThrottlerSleepMicroseconds` (total) + one `*ThrottlerSleepMicroseconds` per throttler |
| Memory overcommit | `MemoryOvercommitWaitTimeMicroseconds` |
| `priority` setting | `QueryPreempted` |
| Query slots | `ConcurrentQuerySlotsAcquired` (count only, no wait time found) |

Limit *failures* are the exception codes: 158 `TOO_MANY_ROWS`, 159 `TIMEOUT_EXCEEDED`,
160 `TOO_SLOW`, 201 `QUOTA_EXCEEDED`, 202 `TOO_MANY_SIMULTANEOUS_QUERIES`,
241 `MEMORY_LIMIT_EXCEEDED`, 307 `TOO_MANY_BYTES`, 396 `TOO_MANY_ROWS_OR_BYTES`,
673 `RESOURCE_ACCESS_DENIED`, 745 `SERVER_OVERLOADED`, 776 `RESOURCE_LIMIT_EXCEEDED`.

Which limits applied to the query: `Settings` map (`max_memory_usage`, `max_execution_time`,
`workload`, ...), plus server settings for anything not overridden.

This layer works retroactively on any install with `query_log`, and it answers the per-query
questions on its own.

### B. Server level over time, no sampler - `metric_log`

CurrentMetrics: `ConcurrencyControlScheduled` (CPU slot requests waiting), `ConcurrencyControlAcquired`,
`ConcurrencyControlSoftLimit`, `ConcurrencyControlPreempted`, `ConcurrentQueryScheduled` (query
slot requests waiting), `MemoryReservationApproved` / `Demand` / `Reclaimable`,
`SchedulerIOReadScheduled` / `WriteScheduled`. Plus the ProfileEvent columns above as
per-interval deltas. This gives "was the server contended at 14:03", but not per workload.

### C. Per ClickHouse workload - point in time only

- `system.workloads` (`name`, `parent`, `create_query`), `system.resources` (`name`, disks,
  `unit`, `create_query`). Definitions; change rarely. A `source` column
  (`implicit`/`disk`/`keeper`/`configuration`) is proposed in ClickHouse #122761 (open, not in 26.10).
- `system.scheduler`: one row per scheduling node (path like `/all/semaphore/p0_fair/etl/queue`)
  with cumulative counters (`dequeued_requests`, `dequeued_cost`, `canceled_*`, `rejected_*`,
  `busy_periods`, `throttling_us`, `admits`, `killers`, `victims`, `spills`, ...) and gauges
  (`queue_length`, `queue_cost`, `inflight_requests`, `inflight_cost`, `max_requests`,
  `tokens`, `budget`, `allocated`, `reclaimable`, `pending`, ...).

No `*_log` table keeps this. It is the only layer that needs capture.

## Capture: is a `tracehouse.scheduler_history` worth it?

A and B already cover "why did this query wait" and "was the server contended". A sampler
adds only **per-workload state over time**:

- gauges (queue length, in-flight, allocated memory) are meaningless unless sampled;
- per-workload rates (CPU/IO cost dequeued per second, rejects, kills) need two counter readings;
- "was the operator's root ceiling saturated, or only my sibling workload?" needs both levels at once.

**Recommendation:** worth it, but optional and last. Same mechanism as `processes_history`:
a new `--target scheduler` in `infra/scripts/setup_sampling.sh`, refreshable MV every N seconds
selecting `system.scheduler` plus `hostname()` and `sample_time`, same TTL. Only offered when
`system.workloads` is non-empty. Sizing: one row per node per sample. The 3-workload test tree
above is 8 nodes for one resource; a realistic tree of 30-100 nodes at 1s is 2.6-8.6M narrow
rows per host per day. 5s is probably the right default.

Definitions (`system.workloads` / `resources`) should be captured alongside (on change, keyed
by `cityHash64(create_query)`), so a historical view shows the tree as it was.

Derivation rule (core, not TSX): rates from counter deltas per `(hostname, resource, path)`,
clamping negative deltas to "reset" since nodes are rebuilt on DDL (to verify).

## UI

### 1. Query Detail - "zoom out" from one query

A **Limits** section (sub-tab of Details, or a section in Overview; to decide) with three parts:

- **Headroom.** One row per limit that applied, as usage vs limit: peak memory vs
  `max_memory_usage`, duration vs `max_execution_time`, rows read vs `max_rows_to_read`, result
  vs `max_result_rows`, and so on. Rows only for limits that were set. Failed queries get the
  row that tripped highlighted, with the guide's explanation for that error code.
- **Time held back.** Wait counters from layer A: CPU slot wait, preemption, IO scheduler wait,
  memory reservation wait, throttler sleep, overcommit wait. Same discipline as
  `utils/time-breakdown.ts`: these are thread-summed, so show shares or raw counters, never
  durations on the wall-clock axis. Open question: fold them into the existing time-breakdown
  bar as new segments, or keep them separate. They probably overlap `cpu_wait` and the residual.
- **Where it sits.** The query's ClickHouse workload (`Settings['workload']`, else `default`),
  its ancestor chain from `system.workloads` with each level's settings, and which levels
  come from server config. Then "zoom out": other queries in the same workload that overlapped
  this one, from `query_log` alone (same `Settings['workload']`, overlapping interval). With
  the sampler, also that workload's queue and in-flight load over the query's lifetime.

### 2. Time Travel

- **Filter by ClickHouse workload**, alongside the existing host and `nqh` filters.
  Works from `query_log` without a sampler.
- **Limit hits as events.** Queries failing with the codes above become a "limit" event
  category on the timeline (Events already has query failures; this classifies them).
- **Contention overlay** from `metric_log` (layer B): CPU slot requests waiting and query slot
  requests waiting, drawn under the CPU strip. Shows "CPU looked free but queries were
  queued for slots", which the current strips cannot show.
- With the sampler: per-workload lanes (in-flight / queue) for the selected resource.

### 3. Limits page (new)

Top strip, always visible: contention summary and how many workloads exist. Opens on
**Impact** (Q2) when workloads exist, **Plan** (Q1) when none do. Third tab, **Configuration**,
is the "who sets my ceiling" view, cluster-wide:

- effective server limits (`system.server_settings`), profiles and constraints, quotas with
  current usage (`system.quotas_usage`);
- the ClickHouse workload tree per resource, configuration-defined levels marked;
- recent limit hits from `query_log`, grouped by code / user / workload, each linking to the
  query (same-tab, per the link-journey rule).

Plus an Analytics preset dashboard for limit hits over time (literal SQL per preset).

## Phasing

1. Query Detail Limits section (headroom + wait counters + workload chain). `query_log` only. (Q2)
2. Limit-hit event category in Time Travel / Events, and the workload filter. (Q2)
3. Limits page: Impact and Configuration tabs, and the analytics preset. (Q2)
4. `metric_log` contention overlay in Time Travel, and the contention strip. (Q1 step 1)
5. Plan tab: competing consumers and the labelled what-if estimate. (Q1 steps 2-3)
6. Optional `scheduler_history` sampler and per-workload lanes.

1-5 need no setup and work on Cloud, OSS and older versions as far as the counters exist.

## Open questions (verify before building)

- **Cloud visibility.** Can a Cloud customer user read `system.scheduler` / `system.workloads`?
  If not, layer C (and the sampler) is OSS-only, and Cloud gets A and B.
- **Is `workload` in `query_log.Settings` when it comes from a profile**, not the query?
  Determines whether the workload filter works for the common case.
- **Counter semantics on a server.** In `clickhouse local` the test tree appeared in
  `system.scheduler` but all counters stayed 0 for a query run with `workload='etl'`.
  Repeat on a server: confirm counters move, and whether they reset on `CREATE OR REPLACE WORKLOAD`.
- **Are the wait counters thread-summed** (like `OSCPUWaitMicroseconds`) or per query?
- **Version availability** of each counter, for capability detection (the compat matrix is
  run rarely, so detect at runtime rather than gate on versions).
- **`query_metric_log` columns.** If it carries these ProfileEvents per interval, X-Ray can
  plot scheduler wait per second for one query without the sampler.
