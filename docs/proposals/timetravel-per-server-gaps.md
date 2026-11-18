# TimeTravel: per-server view gaps

Status: open. Observed 2026-11-13, mostly in the Grafana plugin.

## Gaps

### 1. No zoom or pin controls in Per server

The "Reset zoom" and "Unpin" chips (`chartStatusChips`) render only in the single-chart branch (`frontend/src/pages/TimeTravelPage.tsx`, around line 1850). The per-server branch (around lines 1763-1814) passes the same `zoomRange` and `pinnedMs` to every row but renders no chips. A zoom is invisible there, and the only ways out are Esc or a double-click on a chart.

Proposal: render `chartStatusChips` once at the top right of the per-server block, above the first row. All rows share one zoom and one pin, so one chip set covers them. About 5 lines, no new component.

### 2. Tooltips do not name the server

In Per server, the "Activity at" tooltip does not say which server it describes, and it can overlap the row below.

Proposal: optional `hostLabel` prop on `TimelineChart`, shown after the time in the tooltip header (`Activity at 14:39:32 · 27a20386aedd`), passed only by the per-server rows. A working-tree change doing this exists, not yet reviewed. Not covered yet: the single-band tooltip (query, merge, mutation card) and the `Server:` line under CPU usage.

### 3. Rows go blank sporadically: failed CPU query drawn as "no data" (Grafana)

Two kinds of blank seen:

- Zoomed (14:33:46-14:34:34, and 14:39:42-14:39:46): every row at 0%, header `0/…` counts. The zoom pointed at a range whose data had not loaded yet. Moving the window fixed it.
- Not zoomed, demo cluster, 20:26-20:31: `01af3448bfcb` and `86e65f72dbc5` blank (no CPU line, no operations, axis all 0%, events still drawn), `9574f44a0c81` and `c30cc496067b` normal. Moving the window fixed it.

Evidence for the second case (demo cluster `query_log` via `clusterAllReplicas(tracehouse, …)`, refresh at 20:31:06-20:31:08, initial queries only):

- All four hosts have a full `metric_log` record for 20:25-20:32 (52-60 rows a minute each). Hostnames did not change across the 20:27 restart.
- The per-host `serverCpu` query is present for the two drawn rows and absent for the two blank rows: no `QueryStart`, no `QueryFinish`, no exception. It never reached ClickHouse.
- The other per-host queries for the blank rows reached ClickHouse and succeeded (`queries` returned 100 rows, `merges` 100 rows). Operation bands are drawn on the CPU sample buckets, so no CPU samples means an empty row.
- Grafana server log has no datasource errors at that time. The requests failed or were dropped before Grafana forwarded them, most likely in the browser. Grafana `requestId` de-duplication is ruled out: the adapter makes it unique per call (`grafana-app-plugin/src/ServiceProvider.tsx`).
- `fetchServerCpu` (`packages/core/src/services/timeline-service.ts`, around line 804) catches the error, logs `[TimelineService] cpu metric_log error:` to the browser console, and returns `[]`. Most other `fetch*` helpers in the service do the same. A failed host is drawn exactly like a host with no data.

Related: the demo cluster runs near its memory limit. `query_log` has code 241 (memory limit exceeded) on TimeTravel queries at 19:56 and 20:31:07, and the screenshot shows OOM ×4 events.

Load: each per-server row runs about 13 queries in parallel (`TimelineService.getTimeline`), so 4 servers send about 50 requests in one burst, plus the main chart, the events and the navigator.

Open: why the two requests were dropped. Next time, read the `[TimelineService]` line in the browser console.

Proposal:

1. Stop presenting a failed fetch as empty data. `getTimeline` reports which parts failed (for example `failed: ['serverCpu']`) and the row shows "CPU data failed to load" with a retry, instead of 0%.
2. Lower the burst: fetch per-server rows one after another, or one query grouped by host in place of N per-host queries.

### 4. Dragging on a chart selects the row

`77b083bd22d2` rendered as a solid green block with every axis label at 0% while the other three rows looked normal (zoomed, 14:39:42-14:39:46). The block was the browser text selection (a later drag reproduced it: whole row box highlighted, axis labels and host chip selected). The 0% axis was that row having no samples in the 4s zoom (see gap 3). No chart bug.

A mouse drag on a chart, the drag-to-zoom gesture, can select the row's text and paint the row with the selection color.

Proposal: `userSelect: 'none'` on the per-server rows, and on the Overall chart container. Cost: the row text can no longer be selected; the host label is a button, which browsers already make hard to select.

### 5. Drag-to-zoom is cancelled when the pointer crosses an event badge

Dragging to zoom and passing over an event badge (`OOM ×22`, `Restart`, `36 events`) drops the drag rectangle. Dragging below the badges works. Applies to the Overall chart too.

Cause (`frontend/src/components/timeline/TimelineChart.tsx`): the drag lives in the `<svg>` handlers (`onMouseDown`/`onMouseMove`/`onMouseUp`). The event badges are HTML elements from `TimelineEventOverlay` stacked over the svg with `pointerEvents: 'auto'`, as siblings of the svg. Moving onto a badge fires the svg's `mouseleave`, and `handleMouseLeave` (around line 429) clears `dragStart` and `dragCurrent`. The badge also takes hover focus.

Expected behavior: a drag that starts on the chart owns the pointer until the button is released. Badges passed over during the drag do not take focus or open; they respond again after release.

Proposal: pointer capture. Move the svg's drag handlers from mouse events to pointer events (`onPointerDown`/`onPointerMove`/`onPointerUp`), and call `svg.setPointerCapture(event.pointerId)` on pointer down. While captured, the browser sends every pointer event to the svg and treats the pointer as over it, so the badges get no hover and the svg gets no leave until release; capture ends on its own at `pointerup`. Contained in `TimelineChart.tsx`, no overlay change.

## Order

1. Gap 1 (small, removes the hidden-zoom confusion behind gap 3).
2. Gap 2 (review the existing change, decide on the band tooltip).
3. Gap 4 (one style line per container) and gap 5 (drag survives event badges).
4. Gap 3: proposal 1 first (makes failures visible, and the console error then names the cause), then proposal 2.
