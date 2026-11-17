# Time Travel resource Sankey views

## Calculation and presentation

Activity attribution belongs to `packages/core/src/utils/timeline-attribution.ts`.
The normal Time Travel activity list, pinned preview, and this modal all consume
`attributeTimeline`. This directory owns presentation: grouping attributed rows into
activity/table branches, rendering the flows, and opening existing detail modals.
There is no separate page or backend service.

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
total. **100% means all attributed work from loaded operations**, not all cluster
capacity or all actual resource consumption. Header comparisons explicitly use
capacity where available, otherwise the attributed total. Absolute CPU time and
coverage are available in Measurement details.

Query-rate samples apply to their containing one-second bucket. Missing rate
intervals use explicitly marked lifetime-average estimates; merge-history CPU is
always estimated. Memory is supported by nearby captured samples only. Sampled
coverage describes available reconstructed sample buckets, not raw sampler uptime.
Missing periods and operations omitted by filters/top-N remain unknown.

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

`createResourceDataLoader` uses existing core services:

- `getTimeline`: metric-ranked operations and server data for the selected interval.
- `getZoomData`: per-operation samples across that interval, including a full hour
  when requested from a shorter originating timeline.
- `getNavigatorMetric`: the broader server context outside the selection. This
  series is separate from the selected-range data and has a bounded per-loader cache.

The time-mode and duration controls use `TimeTravelSegments.styles` and
`TimeTravelControls.styles`. Server selection reuses `TimeTravelServerSelector`,
including multiple-host selection. There are no parallel control implementations.

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
3. Delete this directory and its import-boundary rule in `frontend/eslint.config.js`.
4. Optionally remove the navigator's `selectionLabel` prop when there are no other
   point-selector callers. Its ordinary range behavior remains the default.
5. Run frontend typecheck, tests, style checks, and build.

Keep the shared attribution module, interval-resampling correction, optional query
metadata, ordinary hover-card improvements, and shared modal keyboard fixes. None
requires the Sankey. `operation-instant.ts` is a compatibility re-export; its instant
breakdown now delegates to the same attribution rules.

## Remaining measurement constraints

This is a breakdown of loaded operations. It is not a reconciled allocation of total
server usage. Operation estimates are never subtracted from server usage to invent
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
