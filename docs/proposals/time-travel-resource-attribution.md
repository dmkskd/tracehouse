# Proposal: Resource attribution within Time Travel

Status: initial sticky-tooltip and Sankey implementation, 2026-11-17.
Earlier mockups below remain design references.

Latest implementation: [pinned mini Sankey and table drilldown](#pinned-mini-sankey-and-table-drilldown).

Previous interaction: [sticky activity tooltip workflow](#sticky-activity-tooltip-workflow)
implements hover → pin → explore for any activity, with actions directly in the
pinned tooltip.

Previous iteration: [anchored snapshot alternatives](#anchored-snapshot-alternatives)
uses an OOM investigation to compare pinned tooltips, chart-edge inspectors,
activity hover actions and a modal with local timeline context. The earlier
inline and side layouts below remain comparison references.

This revises the placement in [Cluster resource usage and constraints](cluster-resource-attribution.md):
the investigation belongs in Time Travel, without a new Resources destination.
The earlier standalone Analytics mockup remains a reference for accounting and
visualization, not the proposed navigation.

[Interactive HTML: both alternatives](../prototypes/time-travel-attribution.html).
Static previews: [inline expansion](../prototypes/time-travel-attribution-inline.png)
and [side inspector](../prototypes/time-travel-attribution-side.png).
Open the HTML directly in a browser. It has no external dependencies or cluster
connection. All values, coverage and diagnostic evidence are synthetic. The UI
demonstrates CPU only; other resource labels establish existing page context.

## Problem and existing context

Time Travel already provides server scope, resource choice, a measured server line,
operation bands, events, an overview navigator and an operations table. Its pinned
tooltip already shows total consumption, per-server usage and active operations.
The missing connection is persistent attribution that can be inspected without
obscuring the chart or navigating to another page.

A pin is a stable selection, not a new resource measurement. For CPU, display the
measurement interval represented by that sample; a nominal timestamp does not
mean an instantaneous CPU measurement. For memory, use concurrent sampled bytes.
Operations active at the nominal pin and operations contributing during the CPU
measurement interval can differ. Name the operation-table scope explicitly.

## Alternative: pinned sample with inline expansion

Entry: existing click-to-pin behavior. A small `Explain resource use` action opens
an attribution section below the overview navigator and above the operations table.
The mockup starts expanded for review; `Collapse attribution` demonstrates the
compact state. Unpin removes the attribution section and clears its activity filter.

The pinned tooltip can remain a short preview. The expanded section contains:

- The pinned timestamp, actual CPU measurement interval and server scope.
- CPU usage against effective capacity, including explicit unused capacity.
- A consumed-CPU Sankey from activity to contributing operations/consumers.
- An explicit residual and accounting/coverage information.

Selecting an activity filters the existing operation list. Clearing the activity
filter restores all contributing operations. Hovering elsewhere previews another
timestamp but does not replace the pinned attribution.

Advantages: retains full chart width; reuses an established interaction; full-width
flow and long labels fit. Costs: pushes the operation list down; expanding a large
flow for every pin would be disruptive. Persist the user's expansion choice, not
an automatic expansion on each hover. Collapse should leave the pin intact.

The mockup implements sample changes by chart click and previous/next sample buttons,
pin/unpin, expansion, and activity filtering. It does not implement the existing
app's drag-to-zoom behavior. Existing band/event click actions must retain their
meaning; only background/server-line selections should move a pin in production.

## Alternative: selected interval with side inspector

Entry: an explicit `Select interval` action, separate from zoom. In the mockup,
choose start and end with two chart clicks, or use the keyboard-accessible preset
and `Use visible window`. The blue interval does not change the zoom window.

The inspector sits beside the chart and navigator. It shows interval-average CPU,
capacity, activity rows, and expandable consumers. Choosing an activity filters the
operation table below. `Expand flow below chart` opens the same larger Sankey only
when requested. A narrow side panel should not force a tiny flow diagram.

Advantages: chart and attribution remain visible together; intervals support a
burst or sustained-load investigation; compact activity rows precede the larger
diagram. Costs: narrows the chart; introduces a second time range distinct from
the zoom viewport; needs explicit selection and cancellation affordances.
At narrow widths the inspector stacks below the navigator, rather than covering
the chart. A production interaction could use a separate brush mode, but must not
silently change what existing drag-to-zoom does.

The chart selection remains temporal: it does not select a query or event. If the
user then opens operation details, preserve the interval so returning does not
discard the investigation context. Hover is always transient; changing the server
or resource re-evaluates the persistent selection against available data.

## Shared visual and data decisions

The flow starts from **consumed CPU**, with a separate capacity composition strip.
At low utilization, drawing unused capacity as the dominant Sankey branch would
make the relevant consumers too small. The denominator is explicit in both places:
the strip is out of 64 effective cores; activity percentages are of consumed CPU.
Widths throughout the flow use average cores. Unattributed consumption is not
treated as identified background work.

The operation table is scoped to contributors in the measurement interval, rather
than claiming all rows were active at the nominal timestamp. Production must
reconcile this with the existing `Active at pin` scope and disclose loaded/top-N
coverage. Sampling can miss short operations. Missing samples, missing hosts and
estimated lifetime rates must remain visible; do not force a conserved Sankey
by subtracting estimates from a measured server line.

For CPU without interval attribution, show measured usage and a separately labelled
estimate of the composition of shown operations. Disable the reconciled flow or
label it as a model with an explicit independent basis. For memory history without
concurrent samples, show unavailable attribution rather than summing operation peaks.
The complete accounting contract in the parent proposal continues to apply.

## Implementation context

The repository already has a parked [OperationInspector](../../frontend/src/components/timeline/OperationInspector.tsx).
It can show an instant breakdown or operation context and explicitly distinguishes
sampled rates from lifetime-average estimates. It is currently unmounted; the
comment near the navigator/operations boundary in
[TimeTravelPage](../../frontend/src/pages/TimeTravelPage.tsx) describes restoring
it through a two-column layout. This is a reuse opportunity for the side option,
not evidence that the proposed interval accounting already exists.

Both alternatives should share selection, scope, attribution derivation and
operation filtering. Layout is the design choice; a second independent set of
resource calculations is unnecessary.

## Evaluation and initial recommendation

Start with inline expansion to test the existing pin as an entry point. Its smaller
interaction change makes it easier to evaluate whether attribution adds value to
the current workflow. Keep interval selection as a distinct alternative until
users demonstrate a need to explain bursts rather than individual samples.
This is a design judgment, not a measured preference.

Evaluate both with the same tasks: identify the largest consuming activity, locate
its operations, explain the gap between total and attributed CPU, and inspect an
adjacent time without losing context. Also test clearing the selection, changing
server/resource, missing samples, a top-N truncation, and a mostly idle cluster.
Measure whether the panel obscures or displaces information needed for those tasks.

## Prototype verification

Verified in Chromium: pin/unpin, inline collapse/expansion, previous/next sample,
explicit two-click interval selection, preset interval, activity filtering and
clearing, and optional flow expansion. Both layouts were checked for page overflow
at 320, 390, 768, 1024 and 1500 px. Desktop PNGs were visually inspected; no runtime
page errors were reported. This verifies mockup behavior, not real counter coverage.

## Anchored snapshot alternatives

### Problem and benefit

Time Travel is useful for recognizing overall system behavior, finding large
consumers and locating failures. The question after selecting an OOM event is:
**What was happening at that time, how was memory being used, and what evidence
explains this failure?** A detached attribution row makes users reconstruct the
relationship between the selected event, sample, host and consumers.

The proposed benefit is a stable, visibly attached resource snapshot that connects
the event to its measurement context and contributing operations. It should make
the distinction between concurrent usage and failure cause explicit. An OOM label
alone does not identify a query limit, workload reservation, server tracker limit,
or a container/kernel OOM. The mockup's MEMORY_LIMIT_EXCEEDED event represents a
query limit failure, not a kernel process kill.

### Mockups

[Interactive HTML: all four anchored designs](../prototypes/time-travel-snapshot.html).
Each includes Summary, Consumers and Sankey states. No navigation destination is
added. Narrow layouts substitute activity rows for an unreadably small flow.

| Design | Entry and time connection | Benefits and costs | PNG previews |
| --- | --- | --- | --- |
| Anchored pinned tooltip | An expanded version of the current pinned tooltip; a connector joins the selected sample to its header. | Strong spatial attachment and familiar entry. Obscures chart data and needs placement/size limits. | [Summary](../prototypes/time-travel-snapshot-tooltip-summary.png), [Sankey](../prototypes/time-travel-snapshot-tooltip-sankey.png) |
| Inspector inside chart edge | Attribution occupies the chart's right edge, connected to the selected sample. | Stable position and readable activity rows. Consumes chart width; a distant connector can be hard to follow. | [Summary](../prototypes/time-travel-snapshot-edge-summary.png), [Sankey](../prototypes/time-travel-snapshot-edge-sankey.png) |
| Activity quick-action popover | Hover or focus `Explain memory` beside the OOM marker to preview its Sankey; click or `Keep open` stabilizes it. | Starts from the observed failure; no permanent attribution chrome. Needs accessible click/focus entry, a reliable pointer path into the preview, and clear transient/persistent behavior. | [Sankey](../prototypes/time-travel-snapshot-activity-sankey.png) |
| Modal with timeline context | Open the OOM snapshot from the activity action. Repeat the host, event time and nearby memory timeline inside the dialog. | More space for attribution and evidence. Covers the main chart and weakens spatial attachment; local timeline context and return focus mitigate that cost. | [Sankey](../prototypes/time-travel-snapshot-modal-sankey.png) |

The mockup initially shows the pinned tooltip. Select another design with the
review controls above the Time Travel surface. Use the OOM activity action to open
the popover/modal; switch presentation states inside the inspector. Before/after
controls move between two captured snapshots without changing the event time.
Selecting an activity scopes the existing operation table. Escape closes the
popover/dialog; the dialog traps Tab focus and returns focus to its trigger.

### Event time is not sample time

The example event is at 17:55:32 on ch-03. The available samples are 17:55:31.800
and 17:55:32.200. The header says which is selected and its offset from the event;
the chart has a separate event marker and selected sample marker. The connector
targets the selected sample, not an assumed measurement at the event timestamp.

In the earlier sample, the failed query holds 7.8 GiB. In the later sample, its
allocations have been released. A view opening on the later sample alone would
omit an important consumer. For a production event entry, prefer an available
pre-event sample for this question, expose the timestamp and gap, and permit
before/after comparison. If no sufficiently close sample exists, say so; do not
reconstruct precise concurrent bytes from query peaks.

The illustrative error reports a query tracker request taking usage from 7.8 to
8.2 GiB against an 8 GiB query limit. The selected host's tracker holds 26 GiB
against a hypothetical matching 28 GiB server tracking limit. The failure evidence
identifies the query limit; the server's concurrent usage is context. The 32 GiB
container limit is labelled separately and is not used as a tracker denominator.
Values assume disjoint tracking scopes for the illustration; real tracker overlap
and historical settings must be verified before drawing conserved memory flows.

### Interaction decisions still open

- Pinning stabilizes a selection; it is not the only entry point. An event action
  can create the same selection, retaining its event association and sample gap.
- Hover should be a preview. It must not move an existing pin or replace an open
  investigation. Click/focus/touch must offer an equivalent entry. The HTML uses
  a short exit delay so the pointer can enter the popover before it disappears.
- Opening Summary versus Sankey is a product choice. The activity action opens
  Sankey in this study; the existing pin starts compact. Evaluate both with users.
- The mockup grows the chart container to avoid clipping its inspector. A production
  tooltip needs a viewport size budget, alternative placement and possibly a
  scrollable detail area; this growth is a disclosed prototype tradeoff.
- Event selection, operation selection and sample selection are different states.
  Highlight concurrent consumers without labelling every one a cause of failure.
- No layout preference has been established. These designs are alternatives for
  visual comparison, not an implementation commitment.

Anchored mockup verification: all 12 design/detail combinations, before/after
allocation release, the pointer path from activity action into the popover,
keep-open, operation filtering, and modal Escape/focus return were checked in
Chromium. All four designs were checked at 320, 390, 768, 1024 and 1500 px for
page overflow and inspector clipping. Six PNG previews are included. Desktop
popover, chart-edge and modal renders were visually inspected. This does not
validate real ClickHouse sampling coverage or attribution semantics.

## Sticky activity tooltip workflow

Added 2026-11-17 following review of the current Activity tooltip.

[Interactive HTML](../prototypes/time-travel-sticky-activity.html) ·
[Hover preview](../prototypes/time-travel-sticky-activity-hover.png) ·
[Pinned activity](../prototypes/time-travel-sticky-activity-pinned.png) ·
[Exploration modal](../prototypes/time-travel-sticky-activity-modal.png).

This iteration keeps the existing tooltip's information order: timestamp, total
CPU, host distribution and active operations. It does not require an event or
introduce a separate action menu. The two footer actions appear only after pinning:

1. Hover previews activity at the cursor time, with a hint to click the chart.
2. Clicking the chart freezes the timestamp and content. The header changes to
   `Pinned at 11:30:55`; moving into the tooltip no longer changes the time.
3. `Explore resource usage…` opens a Sankey dialog initialized from the pin's time,
   resource and host scope. `Unpin` restores transient hover previews.
4. The dialog permits changing resource, host scope and sample time. It retains
   a local timeline and marks the original pin. Selecting a branch exposes consumers.
5. Closing the dialog restores focus to Explore and keeps the original pin. Escape
   closes the dialog; a subsequent Escape unpins the activity.

The prototype starts pinned to make the footer reviewable. The three controls above
the depicted app can also show Hover, Pinned and Modal states directly. Its CPU
tooltip reproduces the discussed 14.7% / 9.43 CPU-seconds per second example with
synthetic host-local values that reconcile. The tooltip uses the current s/s units;
the dialog labels the equivalent average cores and the CPU measurement interval.
Memory is a separate concurrent tracker snapshot, never a sum of operation peaks.

The depicted chart's zoom controls and host/resource chrome provide context; drag
zoom and main-page resource switching are not implemented in this prototype. Modal
resource/host/time controls are functional. Activity rows retain their detail role;
the resource exploration action concerns all consumers in the selected scope, not
only one operation's lifetime totals. Missing per-interval samples and estimated
production operation rates remain constraints, as described above.

Verified in Chromium: hover-to-pin transition, pinned content stability, modal
opening/return, unpin/Escape, branch selection, sample/host/resource controls, and
40 resource/host/time combinations with parent-child reconciliation. Checked page
overflow and tooltip bounds at 320, 390, 768, 1024 and 1500 px. Pinned and modal
PNGs were visually inspected. All values are illustrative; no application code changed.

## Initial implementation

The 2D Time Travel chart now keeps the activity tooltip interactive after pinning,
with Explore resource usage and Unpin actions. Exploration opens a portal dialog
with resource, host and loaded-window time controls, a measured context strip,
activity-to-consumer Sankey and a consumer table. Closing restores focus and the
original pin. Only the originating server row owns the sticky tooltip in split view.

Unlike the synthetic mockups, the production flow accounts for **loaded operations**,
not all server resource consumption. Measured server usage is displayed independently.
Interval samples are labelled; CPU, IO and network fall back to labelled lifetime
averages. Memory excludes unsampled peaks and reports missing concurrent coverage.
No unexplained residual or unused-capacity branch is inferred from estimates. Host
filtering never reuses an aggregate measured memory line for a single server.
The controls use already-loaded data and do not fetch new history or alter the
original Time Travel viewport. Narrow screens use the consumer table instead of
shrinking the flow below legible size.

## Pinned mini Sankey and table drilldown

Updated 2026-11-17 after live review. Hover retains the existing detailed activity
readout. Pinning now replaces that verbose list with a compact activity Sankey,
the independently measured server value, sampled/estimated contribution counts,
and Unpin beside the timestamp. Clicking the diagram opens resource exploration.
For memory without concurrent samples the preview explicitly offers exploration
to load available history; operation peaks are not rendered as concurrent usage.

The modal flow is now activity → table/table set → query or background operation.
Selecting a table reveals the operation column and smoothly scrolls the flow's
own horizontal viewport, preserving readable type size and the modal position.
Back to tables collapses the expansion. Reduced-motion preference disables the
smooth transition. Large lists retain explicit remainder branches. The consumer
table supports the same drilldown at narrow widths.

Historical QuerySeries now retains query_log.tables. Table sets are normalized
and queries counted once. A multi-table query displays its actual table names
and “shared cost”; its counters are not duplicated or arbitrarily divided across
the tables. Running-query records without table metadata, explicitly empty table
arrays, and unknown metadata have distinct labels. Merge/mutation destinations
continue to use their native table field. Query SQL/comments are not parsed to
invent table attribution.

The pinned preview now uses exact operation start/end membership at the pin,
matching Operations / Active at pin. It no longer adopts membership from a nearby
chart bucket, which caused short queries and merges to appear in the pinned card
while absent from the table. Hover's drawn-band inspection remains unchanged.
Explicit operation-table searches or a different table scope can still intentionally
narrow the visible table.

Resource changes in exploration load the selected metric's server history and
request available operation samples, without changing the main Time Travel
viewport. Sample-load errors retain the measured history and disclose the error.
Large windows sample near the original pin rather than requesting unbounded
per-operation history. Missing concurrent memory is labelled unavailable, not 0 B.
Time exploration is directly on the context timeline: click/drag the marker,
use arrow keys (Shift for ten seconds), or Home/End. The separate range input
has been removed.

Background CPU filled into merges_history samples from lifetime averages remains
labelled estimated. It is not counted as interval-sampled CPU merely because
the same record contains sampled memory or IO.

Verification includes regression tests for exact pin membership, normalized table
sets and conservation, resource loading and stale responses, timeline pointer/keyboard
interaction, mini-Sankey click-through and horizontal scroll behavior. Actual React
components were checked in Chromium with fixture data, including reduced motion and
390/768/1440 px layouts. Production build, type checking and shared-style lint pass.

## Compact exploration layout and operation details

The modal now uses a compact header and a single wrapping toolbar for resource,
server scope, totals, contribution coverage and capacity. The longer explanation
and sample offsets live in collapsed Measurement details. The bottom consumer
table has been removed; the flow is the primary inspection surface.

The actual shared Time Travel TimelineNavigator is used directly above the
Sankey, replacing the custom purple marker. Its blue draggable window selects
the exploration instant; arrow keys and Home/End remain available. A point-time
caption shows seconds without changing the standard navigator's default display.

An expanded table uses Collapse queries or Collapse operations to hide its last
column. Clicking an individual query, merge or mutation leaf opens the existing
detail modal over the resource flow. The flow stays mounted so the selected time,
expansion and horizontal scroll survive. Closing details returns to that exact
context. Shared modal Escape handling closes only the topmost modal and maintains
scroll lock until the final modal closes.


## Implementation ownership and removal

The implementation now lives in `frontend/src/components/timeline/resource-sankey/`.
Its README records the architecture review, disabled path, removal steps, shared/core
changes to retain, and remaining measurement limitations. Application code uses the
public directory index; `RESOURCE_SANKEY_ENABLED` controls the experiment.


## Shared instant and range attribution

The normal activity list and Sankey views now use `attributeTimeline` in
`packages/core/src/utils/timeline-attribution.ts`. `operation-instant.ts` is a
compatibility re-export. The Sankey UI lives beside Time Travel in
`frontend/src/components/timeline/resource-sankey/`, rather than `experimental/`.

The UI labels the modes **Selected instant** and **Time range**. Range attribution
accumulates CPU time, I/O bytes, and sampled memory occupancy over the selected
interval. Range flows display shares of that attributed total (100% = loaded
attributed work). Capacity percentages use an explicit separate denominator.
The range selector supports an hour even from a short originating pin. Navigator
context is loaded separately from selected-range attribution so the resource line
remains visible outside the blue selection. See the UI directory README for units,
coverage limitations, architecture, and removal instructions.
