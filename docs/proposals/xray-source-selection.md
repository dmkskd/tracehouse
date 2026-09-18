# Proposal: X-Ray source selection

**Status:** Implemented, 2026-09-18. Builds on the dual-source Query X-Ray shipped
the same day (`selectQueryXRaySource()` in `packages/core/src/types/xray-source.ts`),
which reads either `tracehouse.processes_history` or `system.query_metric_log`.
That work is complete and tested; what follows is about making the choice
*consistent*, *correctly scoped*, and *administrable*.

## Before this change

One selector, `selectQueryXRaySource({ availability, preference, queryState })`, pure and
tested. Two consumers go through it: the Query X-Ray tab (via
`useProcessSamples`) and `QueryComparisonPanel`. The preference lives in
`userPreferenceStore.xraySource` and persists to localStorage.

Four problems.

### 1. The preference is scoped to the browser, not the server

Whether `tracehouse.processes_history` exists is a property of a *ClickHouse
server*. The preference is a single global string. Pin `query_metric_log` while
investigating server A, connect to server B where it is absent, and the selector
falls back correctly while the settings UI still claims the log is pinned. There
is no way to express "log on A, sampler on B", which is the normal state of
affairs when one sampler install has not been rolled out everywhere.

### 2. Nothing prevents a screen from bypassing the selector

`QueryComparisonPanel` called `buildProcessSamplesSQL()` directly for the whole
life of the feature. It was found by reading the diff, not by any test. Other consumers read sampler data directly: Time Travel zoom and Series overlays
use different time-window query shapes, while self-monitoring intentionally
measures sampler health. `trace-queries.ts` reads trace logs and is outside this
setting's scope.

Wiring individual consumers fixes today and does nothing about tomorrow. A setting that
*most* screens honour is worse than one whose scope is stated, because the gaps
are invisible.

### 3. The type conflates queries and merges

`XRaySource` is a single union covering only query tables, yet the merge X-Ray is
an X-Ray too. Nothing in the type says merges are out of scope; it is a
convention held up by the fact that no one has wired merges to it.

### 4. There is no org-level default

Every user rediscovers the setting on every connection. An operator who knows the
sampler is not installed anywhere cannot express that once.

## Proposal

### A. Split the source types by subject

```ts
type QueryXRaySource = 'processes_history' | 'query_metric_log';
type MergeXRaySource = 'merges_history';
```

Query selection uses `selectQueryXRaySource()`. Merges have a separate source
type, but no persisted preference or selector while only one source exists.

**Merges have exactly one viable source, and that is a finding, not an
oversight.** `system.query_metric_log` is query-keyed, so background merges never
appear in it. `system.part_log` does record merges, but as one row per completed
merge (`duration_ms`, `rows`, `size_in_bytes`, `peak_memory_usage`) with no
per-interval samples, so it cannot drive a corridor over time. The merge X-Ray
needs a time series and `tracehouse.merges_history` is the only thing that
produces one.

The split still earns its place:

- the type stops implying a query-source setting governs merges;
- the settings UI can say *why* merges have no choice instead of silently not
  applying;
- if ClickHouse ever adds a per-merge metric log, the seam already exists.

### B. Make the selector the only door

Add one façade in core and stop exporting the raw builders from the barrel:

```ts
buildXRaySamplesSQL(selection: SelectQueryXRaySourceInput, ids: string[], opts): string
```

The façade runs the selector internally. Callers provide availability, preference,
and query lifecycle rather than a concrete source. It supports comparison samples
and single-query host-tagged samples (`opts.perHost`). Time-window consumers use
`buildXRayWindowSamplesSQL()` and `buildSelectedXRayOverlaySQL()`, which also
select internally. They keep local query/host partitions separate, normalize
counters to intervals, retain predecessors before clipping the viewport, and
average sub-second samples before summing distributed executions.

Back it with an architecture test asserting no frontend module imports the raw
builders, including the hook and Grafana modules. This codebase already relies on hand-maintained
conventions of exactly this shape - `CAPABILITY_REGISTRY` carries a "when you add
a `useCapabilityCheck()`, add an entry here too" comment with nothing enforcing
it. A test is strictly better than a comment.

### C. Scope the preference per connection

```ts
queryXraySource: Record<ConnectionId, QueryXRaySourcePreference>  // absent = inherit
```

`connectionStore.activeProfileId` identifies the connection (a datasource UID
in Grafana). Absent entries inherit the runtime admin default, or `auto` in
standalone mode. Explicit `auto` overrides an admin pin; choosing Default removes
the entry. Controls are disabled while no connection is selected.

This needs the repo's first zustand `persist` `version` / `migrate` pair, to lift
the existing scalar into the map rather than silently dropping whatever a user
has already pinned. The migration copies non-auto legacy pins to existing saved
standalone profiles and the saved Grafana datasource. Legacy `auto` stays unset;
new connections inherit the current default.

### D. Three layers, explicit precedence

```ts
const preference = connectionOverride ?? adminDefault ?? 'auto';
```

The Grafana provider supplies a runtime default through context. It is never
copied into localStorage, so changing admin defaults affects inherited values
without replacing explicit user choices. This deliberately differs from the
boot-time `killQueriesEnabled` sync.

Add to `AppPluginSettings`:

```ts
queryXraySource?: 'auto' | 'processes_history' | 'query_metric_log';
```

Follow the **refresh-rate model** (admin sets a default, users may still switch)
rather than the **kill-query model** (admin-only, no override). Switching source
is an investigative action, not a permission.

**No lock flag.** The only arguments for one are speculative: the `query_log`
identity join costs more on a large cluster, or an admin wants the sampler
unused. Neither has been raised as a real constraint. Add it when someone asks,
with their reason attached.

The plugin already probes capabilities (`ServiceProvider.tsx:259`), so `auto`
resolves correctly there.

### E. Surface it in two places, named honestly

- **Per-query badge** (exists) stays. It writes the same per-connection value, so
  switching source while investigating persists, which is what makes the setting
  consistent rather than a per-screen toggle.
- **Settings controls** in standalone `Layout.tsx` and Grafana `App.tsx`.
- **Grafana AppConfig** exposes the admin default and preserves other jsonData
  fields when saving.

Label it **"Query X-Ray source"**, not "data source". A setting that reads as
global must state its supported consumers and fidelity limits. The
capabilities card should list which screens honour it; `CAPABILITY_REGISTRY`
already maps capability to consumers, so that is a data change, not new UI.

## Original sizing estimate

| Piece | Lines |
|---|---|
| Split query/merge source types and selectors | ~70 |
| Per-connection preference plus persist migration | ~60 |
| Core façade, barrel change, architecture test | ~90 |
| Settings popover control | ~40 |
| Grafana `AppPluginSettings`, AppConfig field, runtime default | ~70 |
| Time-window support for zoom / overlay | ~180 |

The implementation includes all rows. Self-monitoring and trace-log queries stay
outside source selection because they diagnose different sources.

## Validation

- Selector, façade, architecture, preference migration, and admin-default tests.
- ClickHouse fixtures represent the same executions as sampler counters and
  metric-log deltas, asserting equal CPU, memory, network, and wait intervals.
  They cover distributed children, sub-second samples, viewport predecessors,
  and host filtering.
- The existing real-query metric-log integration suite remains intact; an added
  test checks the window path against the same authoritative query-log CPU totals.
- Dashboard tests check source changes and the sampled read-bytes limitation.

## What this does not fix

- **The merge X-Ray gains no alternative source.** See A.
- **Progress bytes remain source-specific.** Query metric logs have SelectedBytes,
  not the sampler's read/write progress counters. Series read throughput explains
  the unavailable metric instead of drawing zeros. Time Travel disk mode retains
  the query-log average while CPU, memory, and network use sampled values.
- **Self-monitoring stays sampler-specific**, because it diagnoses sampler health.
- **`clampFidelity` was removed from the selection metadata.** It predicted whether a thread ceiling would
  exist; the SQL now reports the truth as `rate_unclamped`. Two sources of truth
  for one fact, one of them proven wrong, is what caused the longest debugging
  detour in the original work.
