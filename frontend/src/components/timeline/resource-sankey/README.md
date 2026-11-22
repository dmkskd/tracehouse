# Time Travel resource Sankey views

## Calculation and presentation

Core provides two attribution inputs. `utils/timeline-attribution.ts` attributes the
already-loaded timeline rows used by the ordinary activity list.
`services/resource-attribution-service.ts` executes the Sankey's whole-population
aggregation SQL. Its totals include every matching logged execution; timeline
ranking, memory thresholds, and top-N limits do not restrict this population.
There is no separate page or backend endpoint. Both interfaces expose the same
instant/range units, lifetime-average fallback, and missing-memory semantics.

`resource-exploration-model.ts` converts core attribution into explicit primary
and secondary presentation groups. `resource-flow-layout.ts` calculates geometry,
leaf limits, colours, and failure portions without React or DOM access.
`ResourceAttributionFlow.tsx` renders those nodes and handles expansion and scrolling.
`components/common/InfoPopover.tsx` owns the shared help-popover interaction and dismissal.

The modal supports two presentation orders. Activity (default) groups by operation
type, then table set. Table groups by table set first, ranks by total attributed
usage across all operation types, and expands into types and individual operations.
Both orders use the same attributed consumers and conserve the same total. Table
ranking happens before presentation limits; multi-table costs remain in one table-set
branch rather than being duplicated or divided between tables.
Colours identify operation types in both grouping orders. A table containing
multiple types has matching stacked colour segments on its incoming ribbon and
node, including when collapsed. Expansion preserves those colours and widths
through activity branches and individual operations. Failed portions are hatched
within each segment. Labels identify tables; tables have no separate colour palette.

Initially, five table groups and five expanded individual operations are displayed.
Each remaining branch offers **Show 5 more** (or the exact smaller remaining count).
The count is local to that branch. Additional table groups and operation values
remain in the aggregate remainder, preserving the total and failed-operation share.
Activity types below 0.1% of attributed usage, and types beyond the largest five,
are initially combined; the same progressive expansion reveals them.

The shared calculation accepts an explicit selection:

```ts
{ selection: { kind: 'instant', atMs }, mode: 'cpu' }
{ selection: { kind: 'range', startMs, endMs }, mode: 'cpu' }
```

Instant values are CPU usage (µs/s, displayed as cores), I/O bytes/s, and sampled
concurrent memory bytes. Missing concurrent memory remains unavailable. Execution
memory peaks are not used as concurrent memory attribution.

Range values accumulate contributions over the intersection of each operation's
lifetime and the selected interval. CPU is CPU time in µs; disk/network are bytes;
memory is byte-seconds of sampled occupancy. These are duration-weighted integrals,
not unweighted sums of snapshots. `meanValue` remains available to other consumers;
the range Sankey uses totals. Peak sampled memory is computed from simultaneous
contributions, never by adding each operation's execution peak.

Range Sankey labels show each accumulated contribution divided by the attributed
total. **100% means all attributed work from matching operations**, not all cluster
capacity or all actual resource consumption. Header comparisons explicitly use
capacity where available, otherwise the attributed total. Absolute CPU time and
coverage remain available from the shared attribution result.

Query-rate samples apply to their containing one-second bucket. Missing rate
intervals use explicitly marked lifetime-average estimates; merge-history CPU is
always estimated. Disk I/O uses execution OS read/write counter averages; logical
progress bytes are not substituted for physical I/O. Memory is supported by nearby captured samples only. Sampled
coverage describes available reconstructed sample buckets, not raw sampler uptime.
Missing sample periods use explicit estimates for rates; memory remains unknown. User filters restrict the population; presentation limits do not.

## Loading and controls

`PinnedResourceActivity` owns one materialized dataset for a pin. It loads available
interval samples while the pin is visible, then passes that result into the modal
as its initial snapshot. Expanding, closing/reopening, or resetting to that pin does
not independently refetch its initial data. Unpinning and repinning creates a new
snapshot. Changing resource, scope, or range inside the modal still loads that
explicitly requested selection.

Active operations with a valid zero resource value remain visible in a separate
zero-contribution note: a Sankey cannot draw a zero-width flow. A cumulative-counter
baseline without a predecessor is different: `interval_valid = false` becomes
`rate_sampled = false`, retaining the explicit rate estimate while preserving the
valid memory gauge. Unknown rates are not reclassified as measured zeros.

`useResourceData` keys results by resource, host set, selection kind/window, and loader
identity. It discards superseded responses and debounces range dragging. Cleanup does
not abort queries already submitted: the core adapter currently has no abort API.

`createResourceDataLoader` uses:

- `ResourceAttributionService.getRange`: one SQL statement unions queries, merges,
  and mutations, computes interval contributions, and groups by table set/type.
  No per-execution rows or top-N restriction are used for totals.
- `getSnapshots`: one request prefetches one-second grouped snapshots. Windows up
  to ten minutes are prefetched together; longer windows use two-minute chunks.
  Cursor changes inside a chunk perform local Map lookups. Eight cached windows
  deduplicate in-flight requests and support returning to a previous chunk.
- `getDetails`: only an expanded branch requests individual execution rows. Each
  Show more raises that branch's detail limit by five. Totals remain unchanged.
  Details have an explicit 10,000-row safety bound; exceeding it reports an error.
- `TimelineService.getTimeline`: supplies server series and capacity only. Its
  limited operation rows never supply production Sankey totals.
- `getNavigatorMetric`: broader server context outside a selected range.

The aggregation SQL reads the operation population once. Running/logged execution
membership is reconciled by a window count, rather than repeated CTE subqueries.
Sample identities use local query IDs and hosts; root-query rollup remains unchanged
for ordinary X-Ray callers. A compressed bitmap identifies captured sample buckets
without a self-join that would reread the sampling source. The query does not depend
on experimental materialized CTE support or introduce a population limit.

Aggregation errors are shown, rather than silently falling back to limited timeline
rows. Missing optional sampler tables produce a visible estimation warning.
Instant snapshots have one-second resolution: a sub-second execution between ticks
can be absent from the instant view while contributing to the range integral.

The time-mode and duration controls use `TimeTravelSegments.styles` and
`TimeTravelControls.styles`. Server selection reuses `TimeTravelServerSelector`,
including multiple-host selection. There are no parallel control implementations.
The available cluster hosts are separate from a pin's initial host selection.
Reset restores that initial selection; selecting all hosts explicitly overrides
the originating page's filter.

Events are disabled by default. The Events toggle controls the shared `TimelineEventOverlay` on the navigator. Time Travel
passes its deduplicated, filtered window and overview events through the chart and
pin; the Sankey does not fetch events. The modal filters host-specific events to
its server selection and retains events without a hostname. Markers highlight on
hover; clicking opens an event summary or a group list without changing the slider
or originating pin. Only one popup is open at a time. Popups are portalled outside
the scrolling content, into the enclosing modal's focus scope, and positioned within
the viewport. Event coverage is limited
to the data already loaded by Time Travel, including when exploring another server
or a wider interval. Existing event clustering, severity colours, summaries, and
detail navigation are reused.

## Disable or remove the Sankey

Set `RESOURCE_SANKEY_ENABLED = false` in the public `index.ts` to disable the pinned
preview, modal, and additional resource requests. Ordinary Time Travel attribution
continues through core. Application imports use only this directory's public index;
ESLint restricts deep imports.

For permanent removal:

1. Remove the Sankey imports, feature props, pinned activity branch and sticky-pin-specific integration from `TimelineChart.tsx`. Keep its
   normal `attributeTimeline` calls, activity rows, and operation hit testing.
2. Remove the Sankey loader construction, related props, and split-chart preview
   ownership from `TimeTravelPage.tsx`.
3. Remove the optional resource-attribution service, SQL builder, types, and exports
   if no other caller uses them. Keep ordinary timeline attribution.
4. Delete this directory and its import-boundary rule in `frontend/eslint.config.js`.
5. Optionally remove the navigator's `selectionLabel` prop when there are no other
   point-selector callers. Its ordinary range behavior remains the default.
6. Run frontend typecheck, tests, style checks, and build.

Keep the shared attribution module, interval-resampling correction, optional query
metadata, ordinary hover-card improvements, and shared modal keyboard fixes. None
requires the Sankey. `operation-instant.ts` is a compatibility re-export; its instant
breakdown now delegates to the same attribution rules.

## Remaining measurement constraints

The aggregate covers operations retained in the queried system logs and matching the
selected filters. Log retention, disabled logs, and missing samples constrain coverage.
Server usage and operation accounting have different instrumentation coverage. Operation estimates are never subtracted from server usage to invent
an idle or unattributed branch. Distributed root/child execution rollups still need
an explicit multi-host integration fixture before claiming complete allocation.

Range capacity uses supplied selected-host capacity × interval duration and assumes
that capacity stayed constant. Historical autoscaling within the interval is not
integrated. Current-capacity fallbacks remain disclosed. Sample intervals assume
uniform work within each interval; they cannot repair deferred OS counter updates.

Failure hatching shows the recorded final outcome, including when inspecting time
before the failure. Mixed groups hatch only the failed operations' attributed share.

Tests cover instant/range consistency, overlap integration and partitioning, hourly
CPU totals, simultaneous memory peaks, missing coverage, host selection, request
races/retry, full navigator context, percentage labels, controls, disable behavior,
operation details, and proportional failure hatching. These tests do not replace a
production load test or a full distributed ClickHouse attribution integration test.

The real ClickHouse regression suite substitutes deterministic log subqueries into
the production SQL. It verifies 250 executions without truncation, clipped ranges,
CPU sample intervals and fallback, signed snapshot joins, unknown memory and observed
occupancy, detail limits, and filters. Frontend tests verify local scrubbing across
100 cursor positions, chunk caching, progressive expansion with invariant totals,
range selection, and events disabled by default.
