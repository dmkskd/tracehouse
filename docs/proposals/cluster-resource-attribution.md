# Proposal: Cluster resource usage and constraints

Status: design exploration, 2026-11-16. No application changes.

Placement revision: [Time Travel integration alternatives](time-travel-resource-attribution.md)
replaces the new Analytics Resources destination explored below. It provides
inline expansion and side-inspector mockups within the existing page. This
document retains the original weaknesses, visualization choices and accounting contract.

Question: **What is the cluster doing? Am I short of CPU or memory, and what is consuming those resources?**

Interactive mockup: [Cluster resources](../prototypes/cluster-resources.html).
Static CPU preview: [PNG](../prototypes/cluster-resources.png).
Open the HTML file directly in a browser; it has no dependencies or network requests.
Switch CPU / Memory, node scope, interval, and Flow / Ranked breakdown. Select an
activity to inspect its consumers. All values and diagnostic statements are synthetic.

Related proposals: [resource limits](resource-limits.md),
[wait versus work](wait-vs-work-breakdown.md),
[UI redesign](ui-redesign.md).

## Current weaknesses

The existing Analytics → Surfaces → Resource Usage view was intended to explain
resource consumption by table over time. In the screenshot discussed on
2026-11-16, Stress and Load are selected, with ten table lanes. The following
design assessment is an interpretation of that screen and its implementation,
not a usability measurement.

| Current behavior | Consequence for the question |
| --- | --- |
| Default stress combines CPU 35%, memory 25%, reads 25%, marks 15%. | A high value does not identify the constrained resource. The weights are not a model of capacity or contention. |
| Share uses logged-operation totals; Load uses the observed maximum; Contrast rescales peaks. | None establishes remaining physical capacity. A 30% label is not evidence of 70% CPU headroom. |
| Perspective separates table lanes along a third axis. | Occlusion and perspective make magnitudes and intervals difficult to compare. |
| Tables are the primary entities; merge costs fold into their lanes. | Reading, inserting, and maintaining a table are not immediately distinguishable. |
| CPU, memory, and other channels have separate counters but share a surface metaphor. | Time-integrated CPU and concurrent memory can appear more comparable than their accounting permits. |
| Attribution is separate from policy and waiting evidence. | The view cannot explain unused CPU while queries queue, or distinguish a busy server from a constrained workload. |

Implementation evidence:

- [Resource processor](../../packages/core/src/utils/resource-lanes-processor.ts):
  weights, scaling, merge aggregation, and normalized shares.
- [Resource queries](../../packages/core/src/queries/surface-queries.ts): query
  counters are repeated through `ARRAY JOIN tables`; initial queries only;
  operations are bucketed by completion time, not interval CPU execution.
- [Overview service](../../packages/core/src/services/overview-service.ts): live
  query CPU uses lifetime CPU / elapsed; merge and mutation CPU use heuristic
  core estimates. These are not aligned interval measurements.
- [Workload dashboard](../../frontend/src/components/analytics/queries/workloadBreakdown.ts):
  already separates SELECT, INSERT, other queries, merges, mutations. Its memory
  time series correctly treats per-operation peaks as footprints rather than total RAM.

The existing components provide useful data and drill paths. Changing the renderer
alone would preserve their accounting limitations.

## Proposed information order

1. **Capacity and pressure.** CPU and memory usage against known, scope-matched
   ceilings; per-node values; waiting, throttling, spills, and limit failures.
2. **Activity.** SELECT, INSERT, merges, mutations, other identified activity,
   and unattributed consumption. CPU also retains unused capacity.
3. **Consumers.** Query patterns for query costs; destination tables for merges
   and mutations; individual operations for investigation.

In production, the selected time interval and node scope apply to every layer.
The mockup scopes usage and attribution, while retaining explicitly labelled
cluster context for diagnostic readouts. CPU uses average
cores over the interval. Memory uses a named snapshot, or an explicitly defined
average of concurrent samples. Changing the resource changes the unit and accounting
boundary, not just the label.

## UI alternatives

### Capacity timeline with Sankey

CPU: `effective capacity → activity / unused → consumers`.
All widths use average cores, with percentages of capacity and percentages of used
CPU distinguished. A large merge branch can be inspected without losing its
relationship to total capacity. Clicking a branch selects the consumer detail.

Memory: `tracked memory → concurrent activity → consumers` in the mockup.
The diagram excludes free RAM, RSS, and caches because they do not share this
tracking boundary. A separate indicator compares tracked bytes with a hypothetical
matching server tracking limit. This is a conditional design: a production version
must first establish that the measurements and limit use the same tracker.

Advantages: shows the relationship between operation type and consuming entities;
keeps capacity visible in CPU mode. Costs: crossings, long labels, and aggregation
can make exact comparison harder; conservation requires stricter accounting than
the existing surface. Show only the largest consumers and retain an explicit
remainder. Do not imply that CPU or memory physically flows into a table.

### Capacity timeline with ranked breakdown

The same data becomes activity rows with absolute values, capacity shares, and
expandable consumers. This is implemented as the second mockup mode.

Advantages: exact ordering and values, simpler keyboard use, compact narrow-screen
layout, easier representation of incomplete attribution. Cost: relationships across
multiple grouping dimensions are less immediately visible.

### Capacity timeline with activity stacks

An alternative for later exploration: a two-dimensional stacked time series for
activity, plus a ranked detail panel for a selected interval. It makes transitions
and sustained saturation easier to inspect than a single aggregated Sankey, but
adds chart density and does not show activity-to-consumer relationships directly.
Not separately rendered in this mockup.

Initial recommendation: evaluate Flow and Ranked against the same questions and
data before choosing a default. Retain the timeline in either design. A composite
stress score and scale-mode controls are unnecessary for these questions.

## Consumption is not proof of a bottleneck

| Observation | Interpretation allowed | Additional evidence needed |
| --- | --- | --- |
| High CPU use | Little CPU capacity was unused in that scope and interval. | Runnable waits, throttling, duration and throughput trends to establish contention and impact. |
| Low CPU, requests queued | Work is waiting despite unused observed CPU. | Admission/slot limits, I/O waits, workload ancestry, query parallelism. Do not infer idle CPU caused waiting. |
| Tracked memory approaches its limit | Little tracking budget remains. | Reservation/overcommit waits, memory failures, spilling, and matching effective settings. |
| One replica busy, another idle | Load is uneven. | Shard ownership, routing and workload requirements before claiming the spare capacity is usable. |
| Merges dominate consumption | Background maintenance is a major consumer. | Backlog, part creation, throughput and foreground impact before describing it as excessive. |

The HTML includes a low-CPU queued-work interval, an uneven interval, and a busy
interval. These are deliberately distinct synthetic examples, not diagnoses of
the user's cluster. Waiting is a separate readout; it is never a consumed-capacity
branch or an additive part of CPU utilization.

## Accounting contract and feasibility

- **Scope:** identify node, timestamp, resource, measurement boundary, coverage and
  source for every value. Preserve per-node capacity and pressure alongside cluster
  aggregates. Account for missing hosts and capacity changes.
- **CPU:** interval deltas of appropriate CPU counters / elapsed seconds give
  average cores. Capacity is effective cores, including fractional cgroup quotas
  where available. Host CPU and ClickHouse process CPU are distinct boundaries;
  external work cannot be labelled ClickHouse background activity.
- **Execution time:** completion logs measure total operation cost. They cannot
  reconstruct when a long-running operation consumed CPU. Use aligned interval
  samples where available; otherwise show a separately labelled completed-work
  accounting view, without subtracting it from an interval utilization total.
- **Merge CPU:** active merge counts do not measure CPU. Historical `part_log`
  counters can account for completed operations; interval attribution needs further
  instrumentation or an explicitly labelled estimate.
- **Memory:** never sum peaks as concurrent usage. Query/merge tracker bytes, RSS,
  allocator memory, cache allocations and container usage are distinct quantities.
  Do not subtract overlapping trackers from RSS and call the residual exact.
  Missing concurrent samples disable historical memory attribution rather than
  falling back to peaks. Remaining tracker budget is not free physical RAM.
- **Table costs:** multi-table query counters are not a per-table allocation.
  Group these queries by pattern/table set, or use an explicitly labelled model.
  Never repeat their full cost into each Sankey branch. Insert source reads,
  destination writes and materialized-view work also need separate attribution rules.
- **Distributed work:** use host-local accounting whose semantics have been verified.
  Root aggregate and child execution counters must not both be charged as local
  work. `is_initial_query = 1` alone is not a cluster CPU attribution rule.
- **Conservation:** children reconcile to their parent within a documented tolerance.
  Unattributed usage stays visible. If observations exceed the parent, show a
  reconciliation error/coverage state instead of silently normalizing or clamping.
  Separate identified other work from an unexplained residual.
- **Limits:** weights are sharing policies, not reserved cores. Query, workload,
  server and container limits have different scopes. Missing settings/history
  remain unknown. Detect available counters and permissions at runtime.
- **Evidence:** distinguish measured counters, derived values, estimates, incomplete
  coverage and interpretation. Clicking a production readout should expose its
  source, formula, interval and native evidence links.

Sources for mechanism semantics: [ClickHouse workload scheduling](https://clickhouse.com/docs/operations/workload-scheduling)
(time-shared versus space-shared resources), [query_log](https://clickhouse.com/docs/operations/system-tables/query_log),
and the repository's [resource-limits proposal](resource-limits.md), whose unresolved
version, permission and counter-semantics questions still apply.

## Integration and decisions still open

Original candidate placement: Analytics → Resources, beside the existing experimental
Surfaces. Following review, use the Time Travel integration alternatives linked
above; a separate resource destination is not the current recommendation.

Reuse the Workload Breakdown categories, existing per-node CPU/memory collectors,
and query/merge detail destinations. Resource limits supplies queue, ceiling and
wait evidence; this proposal supplies the cluster-level resource attribution view.
The view must remain useful when no ClickHouse workload scheduler is configured.

Before production implementation, validate interval accounting on a controlled
cluster with foreground-only, merge-heavy, mixed, and admission-limited workloads.
Record counter coverage and discrepancies; do not assume the current sources can
produce the mockup's fully reconciled branches. Decide whether CPU can be accurate
enough without new sampling, and which memory boundary is supportable per deployment.

Design acceptance: from the initial view, identify the busiest resource, the
largest consuming activity, and whether evidence supports contention. Then locate
the responsible node and consumer without changing normalization modes. Validate
against idle, uneven, saturated, capped, missing-history and unattributed states.

## Mockup validation

Checked in headless Chromium: all 24 resource/node/interval combinations, branch
reconciliation, SVG label bounds, keyboard branch selection, view switching, and
320/390/768/1024 px layouts. Inspected desktop CPU and memory and narrow-screen
memory renders. No runtime page errors occurred. This validates the prototype
behavior and layout, not ClickHouse counter semantics or production feasibility.
