# Analytics: events as a dashboard layer

Status 2026-11-11: phase 1 viewer side built and in use (uncommitted). Built: time axis, shared
events fetch, markers and episode bands, hover and click-through, Events toggle with saved choice,
shared event filter, drag-to-zoom, events on the correlation overlay. Not built: dashboard schema
and import/edit persistence, source pruning, host filter. Next: table history or the X-Rays (see
"Where events go next"); phase 3 leaning to SQL annotation queries for Grafana (option B).
Details in "Current state" below. Two reviews applied (see the end).

## Current state (2026-11-11)

### Built

| Area | What | Where |
|---|---|---|
| Time axis | Line, area, dual-axis and grouped-line panels plot UTC rows on a numeric time axis over the dashboard window, so panels line up and events land at their own time. Falls back to the label axis for anything else. | core `utils/time-axis.ts` (`timeAxisValues`, `timeAxisDomain`, `timeAxisTicks`, `resolveDashboardWindow`), `charts.tsx` |
| Fetch | One `EventsService.getEvents` per dashboard window, origin `analytics` (`TAB_ANALYTICS`), only while events are on. Returns events and coverage. | `analytics/useDashboardEvents.ts` |
| Chart layer | Point markers as count bubbles (same size scale as the Events page), episode bands with open and clipped edges, drawn above the series with recharts `ZIndexLayer`. Hover card (severity, title, time, host, type, detail) takes priority over the data tooltip. Works on any recharts chart with an epoch-ms x axis. | `events/ChartEventLayer.tsx`, `events/event-layer-model.ts` |
| Click-through | Marker or band click opens the event on the Events page in the same tab with `state.from`; Events shows `← Analytics`, which returns to the dashboard URL (range, toggle, filter). Clusters open their most severe event. | `buildAnalyticsEventUrl` in `events/event-model.ts`, `pages/Events.tsx`, `EventsDashboard.tsx` |
| Toggle | `Events` button in the Focus / Crosshair / Correlate group. Off by default; the last choice is saved per browser (`analyticsEventsVisible`). A link with `dashboard_events=1` turns it on. Shows the shown/total count and a dot for active filters or coverage problems. | `DashboardViewer.tsx`, `stores/userPreferenceStore.ts` |
| Filter | Hover menu on the Events button: count, All / Errors+ / Critical presets, severity / category / event type checkboxes, source coverage, fetch error. Category to type cascade with a partial state, per-option counts of shown events, section `all · none`, row `only`. Kept in the URL (`dashboard_event_severity`, `_category`, `_kind`). The same panel serves Time Travel's inline bar. | `events/EventLayerControls.tsx` (`EventLayerControls`, `EventLayerMenu`, `EventFilterPanel`), rules in `events/event-model.ts` |
| Correlation overlay | Same time axis and filtered events. | `OverlayChart` in `charts.tsx` |
| Drag-to-zoom | Drag across a time chart sets a `CUSTOM:` range; less than 5px stays a click (drill, markers). Zoom-out icon inside the time picker steps back through earlier ranges. The custom range label sits under the picker so the header does not move. | core `zoomSelection`, `shared/ChartTimeZoom.tsx`, `TimeRangePicker` (`onZoomOut`, `customRangeLabel`) |

### Changed from the plan

- No severity cutoff on the dashboard. The plan drew warning and above; the viewer now picks with
  the filter, and the layer fetches every severity and filters in the browser. Section 4's
  severity pushdown is dropped; scope pushdown stays for record pages (see follow-ups).
- The toggle lives on the `Events` button with a hover menu, instead of an inline bar in the
  header (the bar did not fit).
- `buildEventsUrl` keeps `from=timetravel`; Analytics has its own `buildAnalyticsEventUrl` and
  passes the origin in navigation state, like `@query_link`.
- Section 1's dashboard `events` settings are not built. Every time-series panel shows the layer
  when the viewer turns it on.

### Not built yet (phase 1)

- Dashboard schema `events` and per-panel opt-out (section 1), with import/edit round trip (section 2).
- Source pruning in `EventsOptions` by kind and category (section 4).
- Host filter from a dashboard `host` filter.

### Follow-ups found while building

1. Custom ranges ignore the end time in about 16 time filters. `resolveTimeRange` adds an end bound
   only for `event_time > {{time_range}}` and `event_date >= toDate({{time_range}})`. Prefixed
   columns (`m.`, `q.`, `p.`, `a.event_time`), `event_time >=` and two other columns get only the
   start, so after a zoom those panels show data up to now. Small, query text only.
2. Fixed buckets. 118 presets use `toStartOfInterval(event_time, INTERVAL 1 MINUTE)` (plus some 5
   minute, hourly and 15 second). A zoom below about 10-15 minutes shows few points. Fix: a bucket
   placeholder set from the range, like Grafana `$__interval`. Up to about 150 presets; separate
   decision.
3. Stale events on a relative range. The window is fixed when the range is picked and events are
   fetched once; re-running a panel later does not refetch them.
4. Zoom-out history is not in the URL, so returning from Events keeps the zoomed range without the
   steps back.
5. The filter menu opens on hover only; keyboard users can toggle events but not reach the filter.
6. `event-layer-model.ts`, `clusterEvents` and `isStateEpisode` live in frontend `.ts` model files
   (tested), like the other event models. Moving them to core is a separate refactor.

## The gap

Grafana annotations are a dashboard-level layer: defined once, drawn on every time-series panel of
the dashboard. A restart annotation shows up on the connections chart, the slow-reads chart and the
memory chart at the same time, which is how a reader connects a dip to its cause.

TraceHouse events are richer (22 kinds in 8 categories, severity, precision, host, entity fields,
and a context view with overlapping workload, host metrics and server logs) but only two screens
consume them:

- the **Events** page (`frontend/src/components/events/*`, `frontend/src/pages/events-page-model.ts`)
- **TimeTravel** (`TimelineChart`, `TimelineEventOverlay`, `TimelineNavigator`, mounted only in
  `TimeTravelPage.tsx`)

Analytics dashboards, Query Monitor, Merge Tracker and Overview never fetch events.
`EventsOptions.origin` in `packages/core/src/services/events-service.ts` allows only
`'events' | 'timeTravel'`.

Where events come from is not part of the gap. Detectors could read Prometheus as well as system
tables, and retention is a table configuration choice.

## Phase 1: built-in events on dashboards

### 1. Dashboard schema

Add an optional event layer to `Dashboard` in `packages/core/src/analytics/dashboard-types.ts`:

```ts
events?: {
  enabled: boolean;
  kinds?: EventKind[];          // omitted = all kinds
  categories?: EventCategory[];
  minSeverity?: EventSeverity;  // e.g. 'warning' to keep charts readable
  hostFilter?: string;          // DashboardFilter.param; its current value scopes events by host
}
```

`DashboardPanel` gets `events?: false` to opt a panel out.

Built-in dashboards ship with the layer on and `minSeverity: 'warning'`. This is only a default:
the viewer's header toggle (section 7) turns it off or on, so it does not drive the design.

### 2. Persistence: import, export, edit

The layer does not survive on its own. Two places rebuild dashboards field by field and drop
anything they do not name:

- `parseDashboardJson` in `packages/core/src/analytics/dashboard-import.ts` reconstructs the
  dashboard (lines 59-70) and each panel (lines 37-41). It must parse and validate `events` and the
  panel `events` opt-out, rejecting unknown kinds, categories and severities the same way it rejects
  malformed filters.
- The dashboard editor in `frontend/src/components/analytics/DashboardViewer.tsx` holds panels in
  local state (line 1500) and saves an explicit object (line 1730). It must carry `events` through,
  and needs a control to edit it.

Acceptance: an import/export round trip and an edit/save round trip both preserve the layer and the
panel opt-outs. Each is one test that executes the real code path.

### 3. One fetch per dashboard

- A core function resolves the dashboard window into absolute UTC start and end. It covers every
  value the `TimeRangePicker` can produce: all presets and `CUSTOM:` absolute ranges. The existing
  `resolveTrackerTimeRange` (`frontend/src/utils/trackerTimeRange.ts`) is not enough: it lives in
  the frontend and maps only the intervals in its `INTERVAL_MS` table.
- `timeRangeOverride` is `null` when no dashboard window is set; each panel then uses its own
  `@meta: interval`. `DashboardViewer` currently always initialises it (default `'1 HOUR'`) and the
  picker emits only presets and custom ranges, so this case is not reachable today. If it becomes
  reachable, the layer is off, because there is no single window to draw on.
- A relative window re-resolves `now` on every fetch. The layer refetches when the window, the host
  filter value or the layer settings change. Panels only have a per-panel refresh, so the events
  indicator (section 4) gets its own refresh action.
- `DashboardViewer` calls `EventsService.getEvents` once and passes the result to every panel, the
  same way `hoveredTimestamp` already reaches every panel.
- `origin` gains `'analytics'`, mapped to the existing `TAB_ANALYTICS`
  (`packages/core/src/queries/source-tags.ts`) where `getEvents` picks `sourceTab`
  (events-service.ts:220).
- `getEvents` reads about 10 system tables, so one shared fetch per dashboard is a requirement, not
  an optimisation.

### 4. Coverage and limits

`getEvents` returns `coverage: EventSourceCoverage[]` with a per-source `status`, `event_count`,
`truncated` and `detail`. Unavailable, failed and truncated sources usually do not throw. Without a
visible signal, an empty chart reads as "no incidents" when it may mean "not looked".

- The dashboard header shows an events indicator: events shown, plus counts of sources that were
  unavailable, failed or truncated. Opening it lists each source with its status and detail.
- When any source is truncated, the indicator says so and names the source. Charts never imply a
  complete set they do not have.
- Limit policy: `EventsOptions` gains `kinds`, `categories` and `minSeverity`. `getEvents` skips
  every source whose possible kinds, categories and severities (from `EVENT_KIND_DEFINITIONS`)
  cannot match, so a `lifecycle`-only or `warning`-and-above layer does not run the other queries.
  The remaining sources use the existing service limit (default 1000, max 10000).
- Most sources assign severity while mapping rows, not in SQL, so a source that can produce both
  info and warning events still returns both up to the limit. Where info rows crowd out higher
  severities, the truncation indicator says so. Filtering by severity inside a source query is a
  follow-up per source, done only where truncation shows up.

### 5. Placement on the chart: time axis

#### The two kinds of x-axis

The Analytics time-series charts in `frontend/src/components/analytics/charts.tsx` use a **label
axis** today (`dataKey="name"`; recharts calls it categorical). Each returned sample is a label in a
list, and the labels are spaced evenly whatever the time between them:

```
samples: 13:00  13:01  13:02  13:08  13:09     (server down 13:02-13:08)

label axis:  |13:00|13:01|13:02|13:08|13:09|
             the 6 minutes between 13:02 and 13:08 take the width of 1 minute
```

A **time axis** (`type="number" scale="time"`) places each sample at its timestamp, so screen
distance is proportional to time:

```
time axis:   13:00 13:01 13:02 ............ 13:08 13:09
```

For events this matters in two ways:

- **Exact position.** A restart at 13:05:23 has no "13:05:23" label on a label axis, so recharts
  `ReferenceLine` and `ReferenceArea` do not render it. On a time axis it is just a position.
- **Alignment across panels.** On label axes, two panels with different sample counts or a
  different first sample put the same event at different x positions. With a time axis whose range
  is the dashboard window, every eligible panel uses the same scale, and an event is one vertical
  line down the dashboard. That alignment is what makes Grafana annotations readable.

#### First implementation: time axis on eligible charts

Eligible panels:

- Chart type passes `isTimeSeriesChartType` (line, area, grouped_line). The renderers are
  `LineChart2D`, `AreaChart2D`, `GroupedLineChart2D`, and `DualAxisAreaChart2D` (used for line or
  area with two value columns).
- And the x-axis is time. `ChartConfig` does not record the type of the group-by column, so a `.ts`
  model checks the data: every x value parses with `parseAnalyticsTimestamp` and the values
  increase. Charts that fail the check keep the label axis and get no events.
- Excluded, listed so the omission is deliberate: `stacked_bar` and `grouped_bar` (default
  orientation is horizontal, with categories on the y-axis), `grouped_stacked_bar`, and the
  dashboard `OverlayChart` (`dashboard_overlay=1`). Bars on a time axis need bar-width handling;
  later.

Changes per eligible renderer, all in `charts.tsx`:

| Today | Time axis |
|---|---|
| Rows are `{ name: label, value }` | Add `ts`: epoch ms from `parseAnalyticsTimestamp(label)`; `grouped_line` joins series on `ts` |
| `<XAxis dataKey="name" interval="preserveStartEnd" tickFormatter={formatXTick}>` | `<XAxis type="number" scale="time" dataKey="ts" domain={[windowStartMs, windowEndMs]}>`, tick formatter takes ms |
| `onClick` finds the drill row by `e.activeLabel` | `activeLabel` is now a number: find the row by `ts` and pass its original label, so `@drill` and links are unchanged |
| `onMouseMove` broadcasts `activeLabel` to other panels | Broadcast the row's label, so the cross-panel hover contract stays a string |
| Tooltip prints the label | Format `ts` |

`SyncedCrosshair` (`charts.tsx:565`) reads the domain as a list of labels (`xDomain as string[]`,
`indexOf`) and interpolates between the first and last timestamps. On a time axis the domain is
`[min, max]` in ms and the crosshair becomes one time-to-x conversion.

Events then draw with recharts `ReferenceLine x={ms}` and `ReferenceArea x1 x2`. The axis domain
clips anything outside the window. Sample timestamps go through `parseAnalyticsTimestamp`, so the
timezone handling from #5 is not bypassed.

Tests execute the renderers' data path: time-to-row lookup for drill and hover, the label-axis
fallback for non-time data, and events inside, at the edges of and outside the window.

#### Limitations of the first implementation

- **Missing time is drawn as a sloped line, not a gap.** Recharts connects consecutive points, so
  the 13:02-13:08 outage becomes one long straight segment. It occupies the right width (unlike the
  label axis), but it is not a break. A break needs empty (`null`) rows at the missing buckets,
  which needs the bucket size; it can be inferred from the median sample spacing. Follow-up.
- **Existing charts change.** Every eligible time-series panel moves to the new axis, including
  dashboards that never show events. Risk is in interactions (drill, linked hover, tooltip), which
  is why the tests above click through them.
- **Bar and overlay charts get no events** (see exclusions).

#### Alternative: overlay on the label axis

Keep the label axis and draw events in an overlay inside the chart SVG, through the hooks
`SyncedCrosshair` already uses (`useXAxisDomain`, `usePlotArea`). A `.ts` model maps an event
timestamp to a fractional label index by interpolating between the two samples around it, and
clips to the plot edges.

Why it may still be the right call: existing charts are untouched. If the time axis causes problems
in drill, hover or tooltips that are not worth fixing, the overlay gives events without changing how
any chart draws.

What it does not fix: events do not line up across panels with different samples, missing time
stays invisible, and clustering cannot reuse `clusterEvents` (which assumes a linear time axis).

#### Effort, and switching between them

- Time axis: about 150-250 lines in `charts.tsx` plus tests. It can land one renderer at a time,
  because each renderer chooses its axis independently.
- Overlay: about 200-250 lines (placement, clipping, pixel clustering) plus tests.
- Everything else in phase 1 (schema, persistence, fetch, coverage, toggle, interaction) is the same
  for both. Switching later replaces only the placement part.

### 6. Episodes, including open ones

- Point events draw as a vertical marker. Episodes draw as a shaded band from `occurred_at` (or
  `started_at`) to `ended_at`.
- An episode with no `ended_at` is open: read-only replica episodes have no `ended_at` when recovery
  was not observed in the range (`fetchReplicaReadonlyEpisodes`, events-service.ts). It shades to the
  window end, has no closing edge, and its tooltip keeps the existing detail text ("no recovery was
  observed in the selected range"). It never renders as a point event.
- An episode that started before the window starts at the left edge with a marker showing it
  continues off-chart.
- Colour by severity, reusing `EVENT_SEVERITY_COLORS`.
- Nearby point markers are grouped with `clusterEvents`
  (`frontend/src/components/events/event-model.ts:112`), using the window as the range and
  `usePlotArea().width` as the pixel width. It assumes a linear time-to-pixel scale, which the time
  axis provides.

All of this is a `.ts` model with tests; the TSX only draws what the model returns.

### 7. Interaction, same as TimeTravel

- Hovering a marker shows kind, host, severity, precision and detail.
- Clicking opens the Events page context for that event in the same tab with `state.from`; closing
  it returns to the dashboard, following the existing link-journey convention. Built-in events
  resolve by `event_id`, which the Events page already supports.
- `buildEventsUrl` hardcodes `from: 'timetravel'` (event-model.ts:174). It takes the origin as a
  parameter so the return breadcrumb names the dashboard.
- The line, area and grouped-line charts attach `onClick` to the whole chart for `@drill` and
  links, using `activeLabel`. Clicks on markers and cluster popovers stop propagation so they do
  not also fire the panel's drill.
- A header toggle next to the existing Overlay and Correlate controls turns the layer on and off
  for the viewer, persisted as `dashboard_events` in `ANALYTICS_DASHBOARD_SHARE_SCHEMA` like
  `dashboard_overlay` and `dashboard_crosshair`. The dashboard's `events` settings are the default;
  the toggle does not edit the dashboard.

## Phase 2: user-defined event queries

The equivalent of a Grafana annotation query. A new directive on any query in the dashboard's
`queries`:

```sql
-- @events: time=col end=col title=col host=col severity=col
```

Its rows become events in the same layer. This lets a dashboard draw events from any source: any
table, a long-retention copy of system tables, or Prometheus through a remote table function, all as
literal SQL in the existing preset style. Parsing goes in `metaLanguage.ts`; row mapping in core.

An `@events:` query is part of the dashboard definition, like `filters[].query`, and no panel
references it. `exportDashboardJson` (`frontend/src/components/analytics/dashboards.ts:175`)
deletes `queries` for the "Dashboard only" export, so it must keep `@events:` queries in that mode.
The "Include queries" export and `listEmbeddedSql` already handle unreferenced queries.

Prerequisite, to design before building phase 2: a context-resolution contract. The Events page
selects an event by `event_id` among the events it reloads from the built-in detectors, so it cannot
find a custom SQL event. The contract needs:

- **Stable identity**: derived from the originating query identity plus the row's time, host and
  title, so the same row gets the same id on every run.
- **Taxonomy defaults**: a kind, category and severity for rows that do not supply them, and a rule
  for unknown values.
- **Context resolution from the event itself**: `EventContextService.getContext` needs only
  `eventTime`, `hostname`, `queryId`, `initialQueryId` and `exceptionName`. A custom event can carry
  these in the link (or be re-resolved by re-running its originating query) instead of being looked
  up by id among built-in events.

## Phase 3: TraceHouse events as Grafana annotations

Goal: any Grafana dashboard, including ones TraceHouse did not create, can add "TraceHouse events"
as an annotation source, and every time-series panel shows them next to annotations such as a
Prometheus restart query. This closes the gap this proposal started from. Fits the
[Grafana last mile](grafana-last-mile.md) proposal.

The plugin today is one `app` (`grafana-app-plugin/src/plugin.json`). It runs core services,
`EventsService` among them, through `GrafanaAdapter` on the ClickHouse datasource the user picked.
Grafana takes annotations only from a datasource, so the app cannot supply them as it is.

**Option A: a TraceHouse Events datasource bundled with the app.** Frontend-only;
queries through the user's ClickHouse datasource like the app and runs the same `EventsService`.
Its annotation editor offers the shared filter (severity, category, event type, host). Gives the
same 22 kinds from the same detectors, episodes as start/end regions, and annotation text that
links to the event on the TraceHouse Events page.

**Option B (recommended, 2026-11-11): SQL annotation queries for the official ClickHouse
datasource.** Less disruptive: no change to the plugin package, no signing or catalog question,
and users paste the queries into the datasource they already have. Costs, checked in
`events-service.ts`:

- Classification done in TypeScript must be repeated as SQL `CASE`: `classifyQueryFailure` (OOM
  241/173, timeout 159, rejected 201/202/252/692, otherwise resource limit),
  `classifyOperationalError` (error-burst category by error-name regex), restart detection
  (Uptime reset threshold of 5 s).
- One annotation query per source (12 in `event-queries.ts`). Their `{{cluster_aware:...}}` and
  `{param}` placeholders become Grafana macros (`$__timeFilter`, a host variable).
- Drift when a detector changes.

Scope and safeguards:

1. First set: restarts, crashes, query OOMs and timeouts, merge failures, read-only episodes.
   Error bursts later.
2. Generate the Grafana SQL from the same core query constants, with a test that runs the
   generated SQL and the app detector on the same data and compares the events.
3. Annotation text links to the event on the TraceHouse Events page.

Option A stays the fallback if the duplication grows.

To check before building A (if revisited):

- Whether a datasource bundled in the app needs its own signing or catalog step.
- Volume: Grafana draws every annotation; the fetch caps at 1000 per source. Default the editor to
  warning and above or similar.
- The Grafana versions the annotation API needs, against the versions the app supports.

Earlier phase 3 (superseded): carry events in a Grafana export. There is no whole-dashboard Grafana
export (`toGrafanaDashboard`, `packages/core/src/services/grafana-export/render.ts:26`, wraps one
Query Explorer query in a one-panel dashboard), and option A makes it unnecessary for events.

## Where events go next

Reusable now: `ChartEventLayer` (any recharts chart with an epoch-ms x axis), `EventLayerMenu` /
`EventFilterPanel`, `ChartTimeZoom`, and the click-through with `state.from`.

Ranked by value against reuse:

1. **Table history in Database Explorer** (recommended next). DDL, merge / mutation / part-move
   failures, read-only episodes and replication task failures for one `database.table`, as a list.
   Needs an `EventScope` on `EventsOptions` (`database`, `table`, later `query_id`, `part_name`)
   pushed into the source queries in `packages/core/src/queries/`; filtering in the browser would
   hit the 1000-row cap on busy clusters. Medium, mostly core.
2. **Query X-Ray and Merge X-Ray.** Events on the same host while the operation ran (Keeper
   disconnects, restarts, other OOMs, disk move failures). Their x axis is seconds since start, so
   the layer needs a `toX` offset from event time to axis value. Small to medium.
3. **Overview, System Overview and Metrics time-series charts.** `TimeSeriesChart` puts `time` on a
   label axis; it needs the same time-axis switch. Not yet checked: whether its timestamps carry a
   timezone. Small if they do.
4. **Query and merge detail.** The query's own event (OOM, timeout, rejection), events on the host
   while it ran, failures of the same `normalized_query_hash`; merge and mutation failures for a
   part. Uses the scope from item 1.
5. **Status badges.** Open read-only episodes and recent critical events on the Overview alert
   banner and host cards. Small, separate query.
6. **Notebooks.** An event or event query as evidence, with the SQL-backed evidence plan.
7. **Grafana annotations.** Phase 3 below.

Not a fit: Live View (seconds-scale), Engine Internals (static), Lineage, Query comparison (two
different times).

## Size

| Phase | Scope | Est. lines |
|---|---|---|
| 1 | Schema, import/editor persistence, window resolution, source pruning, shared fetch, coverage indicator, time axis on eligible charts, event and episode rendering, header toggle, tests | ~850-1050 |
| 2 | Context-resolution contract, `@events:` directive, row mapping, "Dashboard only" export | ~450 |
| 3 | TraceHouse Events annotations datasource (option A): not estimated until the signing and API checks are done | open |

## Open decisions

1. **Phase 3 approach.** Leaning B (SQL annotation queries, first set of kinds, generated from
   core with a parity test), per the user's preference for the less disruptive option. A stays
   the fallback.
2. **Next screen.** Table history (recommended) or the X-Rays.
3. **Bucket placeholder** for presets (follow-up 2): build it, or accept coarse zoom.

## Review log

**Review 1 (2026-11-09).** Five findings, all confirmed against the code and applied:

1. [P1] Categorical x-axis cannot place arbitrary timestamps. Section 5 added.
2. [P2] Import and editor rebuild dashboards field by field and would drop the layer. Section 2
   added, with round-trip acceptance.
3. [P2] Open read-only episodes have no `ended_at`. Section 6 defines open and clipped episodes.
4. [P2] Coverage, failures and truncation were not surfaced. Section 4 added.
5. [P2] Custom events cannot be resolved by the Events page. Recorded as a phase 2 prerequisite.

**Review 2 (2026-11-09).** Partly written against the pre-review-1 text (ReferenceLine, "no extra
work" for import), which review 1 had already changed. New points, checked against the code:

- Applied: `hostFilter` names `DashboardFilter.param` (section 1); window resolution for presets,
  `CUSTOM:` ranges, `null` and refresh (section 3); `TAB_ANALYTICS` mapping (section 3); source
  pruning by kind, category and severity in `EventsOptions` (section 4, moved from follow-up into
  phase 1); explicit eligible and excluded chart renderers and the domain-based time check
  (section 5); `stopPropagation` against drill clicks, `buildEventsUrl` origin, header toggle
  (section 7); "Dashboard only" export keeps `@events:` queries (phase 2); no whole-dashboard
  Grafana export exists (phase 3, new decision). The built-in default was dropped as a decision: the viewer toggle covers it (section 1).
- Not applied:
  - "Follow the `SyncedCrosshair` pattern exactly." Its first-to-last linear interpolation
    misplaces events with irregular samples on a categorical axis. Kept neighbour interpolation
    (section 5).
  - "Use `usePlotArea().width` for `clusterEvents`." `clusterEvents` assumes a linear time axis.
    Clustering is done in pixel space after placement instead (section 6).
  - "Per-source / global limit = 300." Not found: the service default is 1000, max 10000
    (events-service.ts:193, 221).
  - "`timeRangeOverride` null means Default." Not reachable in `DashboardViewer` today; handled
    defensively (section 3).
- Found while checking: the review did not cover it, but `clusterEvents` cannot be reused unchanged
  on categorical axes (section 6).

**Decisions after review 2 (2026-11-09).** The built-in default is not a decision: the viewer
toggle covers it (section 1). Placement: the time axis is the first implementation; the label-axis
overlay stays documented as the alternative, to revisit if the time axis causes problems
(section 5). Correction to an earlier claim: a time axis alone draws missing time as a sloped line,
not a gap; a real break is a follow-up.
