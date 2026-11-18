# Analytics: open a merge on the Merges page

Status 2026-11-04: passes 1, 2 and 4 built (uncommitted); passes 3 and 5 proposed.

## The journey

```
Analytics panel ─click a merge─► Merges page, merge detail open ─close─► back on the Analytics panel
```

- Same tab. The Merges page shows "← Analytics" (kept visible after the page's own URL updates).
- Closing the merge detail returns to the Analytics URL the click came from. Same for links from
  the Overview, for finished and running merges.
- Analytics keeps drill state in memory, not in the URL. If the panel was reached by drilling,
  the return lands on the panel the drill started from.

## Meta-language: one directive, `@merge_link`

```sql
-- @merge_link: on=<col> database=<col> table=<col> part=<col> host=<col> time=<col>
```

Each value names a column of the panel's result rows.

| Field | Meaning |
| --- | --- |
| `on` | Clickable column: chart `group_by` and the table cell |
| `database`, `table`, `part` | The merge's result part |
| `host` | `hostName()` of the replica: replicas merge the same part name independently |
| `time` | `toString(event_time_microseconds)` of the `part_log` row: picks that exact record |

The link opens exactly one `MergeParts` record. It carries the microsecond time as `md_us`, and the
Merges page looks it up with `GET_MERGE_HISTORY_BY_EXACT_TIME`:
`event_time_microseconds = toDateTime64(<md_us>, 6)`, no nearest-time fallback. If the row is gone
(TTL) or not flushed yet, the page shows its not-found / pending-flush message instead of a
neighbouring retry, a later `DownloadPart` / `MovePart` of the same part name, or a `NotAMerge` row.

The single-record lookups also return the microsecond time (`event_time_us`, mapped to
`MergeHistoryRecord.event_time_microseconds`), so the detail modal's second lookup (it re-fetches
the full record on open) stays on the same row. Without it the modal re-fetched by the
second-resolution `event_time` and could switch to another retry in the same second.

`md_time` (nearest time, written when a merge is opened from the Merges list) keeps its behaviour.

Not the same as `@part_link`: that opens the Part Inspector (the part on disk now: size, rows,
columns). `@merge_link` opens the merge that produced the part (duration, CPU, memory, source
parts, logs).

## Pass 1: Merge Attempts (built)

Bar click or `attempt` cell click opens that attempt. Merge Attempts is reached from Top Failing
Merges, or opened directly from the query list (then it lists every merge attempt in the range).

| Where | Change |
| --- | --- |
| `queries/mergeCost.ts` | Merge Attempts: the directive, plus `database`, `table`, `part_name`, `event_time` columns (last, so the table view starts as before) |
| `core/analytics/meta-language.ts` | Parse `@merge_link`; `mergeLinkUrl(link, row)` → URL, or null when a value is missing |
| `core/utils/deep-link-resolution.ts` | `buildMergeRecordLinkUrl(db, table, part, { hostname, eventTimeMicroseconds, eventType })` → `md_db/md_tbl/md_part/md_host/md_us/md_type` |
| `core/queries/merge-queries.ts` | `GET_MERGE_HISTORY_BY_EXACT_TIME`; the three single-record lookups return `toString(event_time_microseconds) AS event_time_us` (not aliased as the column name: the alias would shadow the column in `WHERE` / `ORDER BY`) |
| `core/services/merge-tracker.ts`, `core/mappers/merge-mappers.ts`, `core/types/merge.ts` | `getMergeHistoryByPartName` uses the exact query when it gets `event_time_microseconds`; the record carries it |
| `DashboardViewer.tsx`, `QueryExplorer.tsx` | Chart click and cell click → `navigate(url, { state: { from: { path, label: 'Analytics' } } })`; only when the panel has no `@drill` |
| `ResultsTable.tsx`, `charts.tsx` | Clickable cell; tooltip "Click to open this merge on the Merges page" |
| `merge/MergeTracker.tsx` | `md_us` URL param; `from` captured on mount (URL updates on this page drop navigation state); closing the linked merge, finished or running, navigates back to `from.path`; other merges opened later close as before |
| `common/BackLink.tsx` | Optional `from` prop, so the page passes the captured value and the link stays visible |
| `GrafanaExportDialog.tsx` | `@merge_link` listed as "Not exported" |

Grafana plugin: no plugin-specific code. `navigate` and `state` go through the plugin's
`useNavigate` / `LocationContext`, as the Overview links do.

Tests:
- Unit: directive parsing; the real Merge Attempts SQL plus a row → expected URL (`md_us`); null
  when a value is missing. QueryExplorer component tests render inside `MemoryRouter`.
- Integration (`link-resolver.integration.test.ts`, real ClickHouse): a real merge's microsecond
  time returns that record; looking it up again with the returned record (what the modal does)
  returns the same row; the time plus 1 µs returns nothing. Running it caught the alias-shadowing
  bug above.

Verified by hand: click → modal → close → back on the panel, standalone app (before the review
changes). Not verified: Grafana plugin; the review changes in the browser.

Missing (from the review): navigation tests for click → correct detail → close → originating URL,
standalone and Grafana. There is no MergeTracker component harness yet (only
`merge-activity-model.test.ts`); building one with mocked services is roughly 150 lines, and a
Grafana plugin navigation test roughly 60 more.

## Inventory: every Analytics panel that touches merges or parts

Scan of all presets (2026-11-04): 108 panels read `system.part_log`, `system.merges`,
`system.mutations` or `system.parts`. Most aggregate (per table, hour, bucket, host) and have no
single merge to open. The ones below do, or are one drill away from it.

### A. One finished part_log record per row: link directly

| Panel | Group / dashboard | Rows | Needs |
| --- | --- | --- | --- |
| Merge Attempts | Merge Analytics (drill target) | one `MergeParts` attempt | done (pass 1) |
| Merge Errors | Merges / Merge Monitoring | one failed `MergeParts` | `part_name`, `hostName()`, `event_time_microseconds` |
| Part Operation Failures | Events / Operational Events Explorer | one failed record of any type (merge, fetch, mutate, move) | `type=<col>` field (see below); has every other column already |
| Peak Memory by Part | Inserts / Insert Health | per day, the record with the highest peak memory (`argMax`) | `argMax` of part name, host, time, type; `type=<col>` |

### B. One running merge per row: link with `start=`

| Panel | Group / dashboard | Needs |
| --- | --- | --- |
| Active Merges | Merges / Merge Monitoring | `result_part_name`, `hostName()`, start ms |
| Merge Throughput (bytes/sec) | Merges / Merge Monitoring | same; chart label `db.table #Np` is not unique, so `on=` a unique column |
| Merges (who ate my CPU) | Knowledge Base / Altinity KB | same; it is a KB-sourced query, so the added columns change the KB text |

### C. Per-part rows where the merge is known

| Panel | Rows | Link |
| --- | --- | --- |
| Part Wait Timeline | one source part consumed by a merge | `@merge_link` to the consuming merge: add the merge's `part_name`, host, `event_time_microseconds` |
| Part Merge Estimates | one active part, not a merge | `@part_link` (Part Inspector), not `@merge_link` |

### D. Drill chains that end before a single merge (the main gap)

| Chain | Ends at |
| --- | --- |
| Merge Throughput by Table → by Size Bucket | bucket |
| Merge Duration by Table → by Size Bucket | bucket |
| Merge CPU / Elapsed by Table → Table Merge Cost Breakdown | outcome x level x size x reason |
| Merge CPU / Elapsed by Level Transition, by Result Size, Cost by Reason, JSON Merge Cost | no drill |

These are most of the Merge Analytics and Merge Monitoring dashboards. Today a user can see "merges
of 1-5 GB on this table are slow" but cannot click through to one of those merges.

Proposal: give each chain a last level that lists merges, and put `@merge_link` on it. One literal
SQL per source panel (bucket definitions differ between panels, so no shared panel), e.g.
Merge Duration by Size Bucket `--drill size_bucket-->` "Slowest Merges in Size Bucket": the top 50
`MergeParts` records of that table and bucket by duration, with the link columns.

### E. Out of reach for now

- Mutations panels (`system.mutations`: Active / Failed / Recent Mutations, KB Mutations): the
  Merges page has no deep link for a mutation (no URL param). Needs Merges page work first.
- Insert panels on `NewPart` (KB Parts per Insert, Rows per Insert, ...): grouped; the detail is the
  insert query, which `@query_link` covers where `query_id` is a column.

## Meta-language changes for the rest

Both are optional fields on `@merge_link`, not new directives.

- `start=<col>` (B): running merge. The link carries `md_start`; the Merges page resolves it with
  `resolveOverviewMerge` (running merge, else its `part_log` record), as for the Overview links.
  Exactly one of `time` / `start` (agreed in review).

  `resolveOverviewMerge` (`merge-activity-model.ts:414`) does not identify one attempt today, which
  also affects the Overview links:
  - the running merge is matched by host and result part only, not by start; if the attempt fails
    and a retry starts before the page loads, the link opens the retry;
  - the history fallback takes the latest record of any type for that part on that host
    (`MergeParts`, `MutatePart`, `MovePart`, `DownloadPart`) not older than the start, so a later
    fetch or move can be opened.

  Pass 4 strengthens it: running merge only if its start (`now - elapsed`) is within 1 s of
  `md_start`; history fallback only `MergeParts` with `event_time_microseconds - duration_ms`
  within 1 s of `md_start` (the attempt that started then). Anything else: not found. If that is
  judged too costly, the alternative is to label running-merge links best effort in the UI.
- `type=<col>` (A): the record's `event_type` when it is not `MergeParts`. The identity lookup
  already matches any type, and the Events page already opens `DownloadPart` / `MutatePart` records
  on the Merges page this way (`pages/events-page-model.ts`). Default stays `MergeParts`.

## Passes 2 and 4 (built)

| Panel | Link | SQL added |
| --- | --- | --- |
| Merge Errors (Merges) | `time=event_time_us`, `on=part_name` | `part_name`, `hostName() AS host`, `toString(event_time_microseconds) AS event_time_us` |
| Active Merges (Merges) | `start=started_ms`, `on=result_part_name` | `result_part_name` (after `table`), `hostName() AS host`, `started_ms` |
| Merge Throughput (bytes/sec) (Merges) | `start=`, `on=merge` | chart label now includes the result part, so each bar is one merge; link columns at the end |
| Merges (Knowledge Base, who ate my CPU) | `start=`, `on=result_part_name` | link columns appended after the KB columns, with a comment saying they are added |

`started_ms = toInt64(toUnixTimestamp64Milli(now64(3))) - toInt64(elapsed * 1000)`, evaluated on the
node running the merge.

Running-merge resolution (`resolveOverviewMerge`, also used by the Overview links), review P2:

- Running merge: same host and result part, and its start (`now - elapsed`) within
  `MERGE_START_TOLERANCE_MS` (1.5 s) of the link's start. A retry started later is not opened.
- Finished since: `GET_MERGE_HISTORY_BY_START`, `MergeParts` / `MutatePart` only, attempt start
  (`event_time_microseconds - duration_ms`) within 1.5 s of the link's start. No fetch, move or
  later retry. Nothing found: not-found message.
- Without a host (no `md_host`): the previous behaviour (latest record not older than the start).
- Assumption: browser and server clocks agree within the tolerance. Overview link starts are
  computed with the browser clock and Analytics link starts with the server clock; the previous
  fallback already compared the two.

Tests: unit (directive with `start`, both fields rejected, Active Merges SQL + row → `md_start`
URL; resolver: running attempt opens, later retry rejected, finished lookup by host and start);
integration on real ClickHouse (`GET_MERGE_HISTORY_BY_START` finds the merge by its start, not by
a start 5 s off, not on another host); the preset smoke test runs all four changed panels.

## Query links (built 2026-11-04)

`@query_link: on=<query_id col>` now follows the same journey as `@merge_link`: same tab to the
Queries page (`/queries?qd_id=<id>&qd_tab=xray`), "← Analytics" kept visible, closing the linked
query returns to the Analytics URL (and to the Overview for its links).

- Before: in place, chart clicks only, `QUERY_BY_ID` (finished initial queries only); shard
  executions fell back to opening the coordinator on the Distributed tab; table cells and the
  query explorer had no query links.
- Now: chart clicks and table cells, dashboards and query explorer. The Queries page lookup
  (`QUERY_DETAIL`) accepts finished, failed and shard queries, and its link resolver handles a
  running query ("still running", then opens when `query_log` has it). A shard execution opens as
  itself, not its coordinator.
- Panels: Current Processes (KB) gets `@query_link: on=query_id`; the 9 existing `@query_link`
  panels (Events x7, Wait Breakdown, X-Ray) change from in-place to the Queries page.
- `buildQueryLinkUrl(id, endedAt?, tab?)` sets `qd_tab`, which the query detail modal already reads.

## Proposed order (remaining)

| Pass | Panels | Change | Size |
| --- | --- | --- | --- |
| 3 | Part Operation Failures, Peak Memory by Part | `type=`; SQL | ~15 lines + tests |
| 5 | D: one per-merge list per drill chain, plus Part Wait Timeline | new drill-target panels (literal SQL) with `@merge_link` | ~40 SQL lines per panel, 4-6 panels |

Pass 5 is the one that makes "click from Analytics to the merge" true across the merge dashboards.

## Not in scope

- Restoring the drill path on return (needs drill state in the Analytics URL).
- Exporting `@merge_link` to Grafana dashboards.
- Unifying `@part_link` / `@query_link` / `@merge_link`.
- Tooltip text chosen by matching the string passed as `drillIntoQuery` (`'Merges page'`, as
  `'Part Inspector'` today). A `linkHint` prop would replace this.

## Review outcome (2026-11-04)

- P1, exact attempt: fixed in pass 1 (`md_us`, exact query, microseconds through the modal re-fetch).
- P2, running merges: fixed in pass 4 (start-matched live and history lookups).
- `start=` with exactly one of `time` / `start`: agreed.
- Return-on-close for Overview links, finished and running: done; back link kept visible: done.
- Pass 2 (Merge Errors): built.
- KB "Merges" query: link columns added (decided 2026-11-04).
- Navigation tests (standalone and Grafana): not built, see pass 1 "Missing".

## Still open

1. `type=` on `@merge_link` (pass 3).
2. Pass 5: one per-merge drill target per chain (several new panels) vs fewer, broader ones.
3. Navigation tests (click → detail → close → origin), standalone and Grafana.
