# Proposal: dependable link sharing

Status: draft, assessment complete; implementation not started  
Author: repository assessment, 2026-08-01

---

## 1. Product definition

In TraceHouse, **Share** should mean:

> From any meaningful screen state, I can send something to a colleague that
> opens the same investigation: the same TraceHouse surface, data source and
> cluster, stable selected object, filters, ordering, view mode, and bounded
> time context, subject to the colleague's access and the continued existence
> of the underlying data.

This is stronger than "the URL opens the same page" and slightly weaker than
"the pixels are identical." Live data, viewport size, theme, hover state, and
permissions can legitimately differ. If the underlying data may disappear or
change, exact reproduction requires a stored snapshot rather than a permalink.

The feature should therefore expose two related outcomes:

1. **Link to view**: a compact URL that reconstructs the investigation by
   querying the recipient's authorized data source.
2. **Snapshot**: an immutable, access-controlled capture for cases where the
   data or state cannot be reconstructed later. This can be a snapshot link
   once server-side storage exists, and an exported file/image before then.

The product promise is "Share is available everywhere." It does not mean every
piece of transient UI state belongs in a URL.

## 2. Executive assessment

Copied links work for some investigations, but they do not consistently
reproduce the screen the sender was viewing.

- Every top-level page has an address, so page-level links generally work.
- Queries and Merges have the strongest deep links: their filters, sort order,
  tabs, and selected detail records are substantially represented in the URL.
- Events and Analytics preserve selected parts of an investigation.
- Most other pages preserve only the route. A copied browser URL often opens a
  visibly different state for the recipient.
- Only Analytics has an explicit Share button. Elsewhere the user must infer
  that copying the browser address might work.
- The standalone and Grafana builds use different URL implementations. They
  already have different schemas and behavior.
- Neither build has an immutable snapshot mechanism.

The result does not meet the definition above. It is a collection of useful
deep-link features, not yet a product-wide sharing capability.

## 3. Current implementation

### 3.1 What exists

The standalone app uses `HashRouter`, producing URLs such as
`/#/queries?...` (`frontend/src/App.tsx`). It has a schema-based URL hook and a
specialized Analytics codec (`frontend/src/hooks/useUrlState.ts`). Query detail
modals use `qd_id`, and Merges has a compound identity for historical merge
details.

The Grafana plugin mounts pages below
`/a/dmkskd-tracehouse-app/<page>`. It aliases shared frontend imports to
plugin-specific location and URL hooks. Search parameters are mainly managed
with Grafana's `locationService`, while frontend code that imports
`react-router-dom` directly is redirected to a local stub.

The existing URL coverage is:

| Surface | What a URL currently restores | Material state not restored | Assessment |
|---|---|---|---|
| Overview | Page only | snapshot/trend/map mode, selected metrics and host, split modes, history range, map search and selected node/detail | Poor |
| Queries | Tab, filters, quick filter, limit, sorting, selected query ID | some navigation-only state; exact result set is live and unbounded in time | Good foundation |
| Merges | Tab, merge/mutation filters, time range, sorting, selected part, compound merge-detail identity | preview/selection modes and exact result set; some preferences remain local | Good foundation |
| Time Travel | query hash, selected event/time, selected query ID | live/frozen state, exact viewport and pin, window size, navigator range, zoom, metric, hosts, per-server mode, event filters, category visibility, 2D/3D mode, sorting | Poor for its main use case |
| Events | investigation range/center, selected event, origin, selected query ID | text search, grouping, auto-refresh and event-context window | Partial |
| Analytics — Query Explorer | tab, preset or SQL, table/chart mode and basic chart mapping, database/lookback, fullscreen, selected query ID | drill stack, result sorting, time override, several analysis/chart controls | Partial |
| Analytics — Dashboards | Analytics page and sometimes a `fromDashboard` return ID | opening a dashboard directly, dashboard filters/time override, focused/fullscreen panel, overlay/correlation state; custom dashboards exist only in the sender's local storage | Poor |
| Analytics — Surfaces | Analytics tab only | surface sub-tab, database/table/time, lane count, drill target, scale and visualization mode | Poor |
| Database Explorer | Page only | database/table/partition/part breadcrumb and inspector, performance mode | Poor |
| Replication | Page only | selected table/topology and expanded queue | Poor |
| Engine Internals | Page only | selected host and card-specific time/view controls | Poor |
| Cluster | Page only | data context is implicit; the main surface has little durable selection state | Route is adequate, context is not |

This table intentionally omits hover, cursor position, open menus, loading
spinners, animation phase, and scroll offsets. Those are ephemeral rather than
shareable investigation state.

### 3.2 Important failure modes

#### Data context is not part of the link

The screen is a function of both view state and data context. Today:

- Standalone connection profiles and the active profile are browser-local.
  Another person does not have the sender's profile ID or credentials.
- Grafana datasource UID and cluster override are stored in `localStorage` by
  `grafana-app-plugin/src/ServiceProvider.tsx`.
- The selected cluster can therefore differ even when two people open the same
  URL.

A link can reconstruct all filters and still show different data. Credentials
must never be put in a URL, but a non-secret, resolvable source identity must be.

#### Relative time is not reproducible

Values such as `1 HOUR`, "live," and "last 15 minutes" move with the clock. A
recipient opening the link tomorrow sees another investigation. "Link to view"
must freeze a live range to absolute `from` and `to` timestamps at share time.
The UI may also offer an explicit "keep live" option, but it cannot be the
default meaning of "same thing I am seeing."

#### Grafana has split URL ownership

There are currently three mechanisms in the plugin:

1. local route state in `grafana-app-plugin/src/App.tsx`;
2. Grafana `locationService` in plugin URL hooks;
3. a `react-router-dom` stub that calls `window.history` directly.

The third mechanism does not subscribe to Grafana location changes and can
bypass consumers of `locationService`. This is especially relevant to shared
frontend features such as query-detail deep links. Browser back/forward,
programmatic navigation, and copying a URL can consequently disagree about the
current state.

#### Standalone and Grafana schemas have drifted

`frontend/src/hooks/useUrlState.ts` and
`grafana-app-plugin/src/hooks/useUrlState.ts` duplicate parsing, defaults,
serialization, and Analytics fields. The Grafana Analytics type currently lacks
fields present in standalone, including `noAutoExecute` and the legacy event
fields. Fixes made in one build are not automatically fixes in the other.

#### The current Share action can discard context

Analytics constructs a fresh query string containing only its known fields.
That can drop unknown but important parameters. In Grafana it can also discard
Grafana-owned parameters such as organization context. In standalone it can
drop a query-detail `qd_id` even though generic URL updates otherwise preserve
unknown keys.

#### Stable identity is uneven

`qd_id` is a useful stable key while the query remains available. Merge detail
is stronger because it uses database, table, part, host, time, and event type.
Other screens often retain only an in-memory row object or array index. Array
indices and display labels are not durable identities. A recipient also needs a
clear "no longer retained / no access / not found on this source" state rather
than a silently different screen.

#### Long and sensitive URLs are unmanaged

Analytics can place encoded SQL in the URL. Encoding is not encryption: SQL,
object names, query IDs, usernames, and filters can leak through browser
history, chat previews, reverse-proxy logs, screenshots, and referrers. There is
no URL length budget, sensitivity warning, or fallback when state becomes too
large.

## 4. Required sharing contract

Every surface should declare a versioned `ShareDescriptor`, independent of the
standalone or Grafana URL transport:

```ts
interface ShareDescriptorV1 {
  v: 1;
  route: string;
  source?: {
    kind: 'standalone-alias' | 'grafana-datasource';
    id: string;                 // never a credential
    cluster?: string | null;
    grafanaOrgId?: string;
  };
  time?: {
    from: string;               // absolute UTC timestamp
    to: string;                 // absolute UTC timestamp
    mode?: 'frozen' | 'live';
  };
  state: Record<string, unknown>;
}
```

This is a conceptual interface; the serialized URL can remain readable query
parameters. The important boundary is that pages own state capture/restore,
while one platform adapter owns URL construction and navigation.

### State precedence

On opening a link, precedence must be deterministic:

1. share descriptor;
2. recipient's saved preferences for fields absent from the descriptor;
3. product defaults.

Link state should temporarily override preferences when needed to reconstruct
the shared view, without permanently changing the recipient's preferences.

### Source resolution

- In Grafana, preserve `orgId`, include datasource UID, and include the selected
  cluster. If the recipient cannot access the datasource, show a source picker
  with the requested source identified; do not silently use their last source.
- In standalone, introduce administrator-configured, non-secret source aliases.
  Never serialize host credentials or a browser-local profile ID. If no alias
  can be resolved, open the correct page/state and ask the recipient to choose a
  source, then explicitly report whether it matches the shared source.
- Authorization is never transferred by a link. The recipient uses their own
  permissions.

### What should and should not be captured

Capture state that changes the question being answered or the visible evidence:

- route, tab, filter, sort, grouping, limit;
- stable selected entity and open detail tab;
- absolute time range, viewport/zoom, pin, host and cluster scope;
- view mode, visible metrics/categories, panel focus/fullscreen;
- dashboard ID/version and dashboard filter values.

Do not capture incidental interaction state:

- hover and tooltip state;
- open dropdowns, context menus, toasts and loading indicators;
- animation phase;
- scroll offset, unless a future long-form investigation surface has a concrete
  need for anchored sections.

## 5. User experience

Add one Share control to the common application header in both builds. Page
components supply the descriptor; they do not implement their own clipboard
logic.

The initial menu should offer:

- **Copy link to this view** — default; freezes relative time and reports
  success or a useful failure.
- **Keep time range live** — optional when the current view is live/relative.
- **Export snapshot** — file/image fallback until stored snapshot links exist.

Before copying, the menu should state which source and cluster are included and
whether the link contains SQL or other potentially sensitive fields. If the URL
would exceed the supported budget, offer snapshot/export instead of producing a
fragile link.

When opening a link, render an explicit degraded-state banner when exact restore
is impossible, for example:

- requested datasource is unavailable;
- requested cluster is unavailable;
- object has expired from system logs;
- feature/capability is disabled;
- descriptor version is newer than this TraceHouse build.

The banner must distinguish "not found" from "not authorized" wherever the
backend permits that distinction safely.

## 6. Architecture

### One codec, two transports

Move schemas, codecs, defaults, migrations, and descriptor types into shared
code. Keep only thin platform adapters:

- standalone adapter: reads/writes the HashRouter location;
- Grafana adapter: reads/writes only through `locationService` and preserves
  Grafana-owned parameters.

Remove direct `window.history` mutation from the Grafana router stub. Shared
frontend code should use the router-agnostic location API, including query
detail deep links. Navigation and search state must have one observable source
of truth per build.

Each page should implement the equivalent of:

```ts
interface ShareableSurface<S> {
  capture(): S;
  restore(state: S): void;
  normalizeForShare(state: S, now: Date): S;
}
```

Normalization removes defaults, freezes relative time, replaces unstable row
indices with stable identities, and rejects secrets or unsupported values.

### Schema evolution

Include `v=1` from the first product-wide release. Decoders must:

- ignore unknown fields;
- validate enum and numeric bounds;
- migrate known older versions;
- fall back visibly rather than throw on malformed state.

Existing unversioned `qd_id`, merge, event, and Analytics links should remain
valid through compatibility decoders.

### Permalink versus snapshot

A permalink stores intent and re-runs queries. It cannot guarantee identical
rows if data changes or expires.

A snapshot stores the rendered evidence (structured result data plus the share
descriptor, not necessarily a screenshot). A future snapshot service requires:

- unpredictable IDs;
- explicit viewer authorization;
- expiry and deletion controls;
- size and rate limits;
- redaction rules and an audit trail;
- a clear indication that the view is a snapshot and when it was captured.

Until that service exists, do not call permalinks "snapshots."

## 7. Delivery plan

### Phase 0 — correctness and guardrails

1. Create the shared descriptor/codec package and platform adapters.
2. Make Grafana search state use `locationService` exclusively.
3. Preserve Grafana-owned and unknown parameters when sharing.
4. Add schema versioning, validation, URL-size checks, and sensitive-state
   classification.
5. Add a common Share control that can initially report incomplete coverage.

This phase fixes the foundation without pretending every screen is shareable.

### Phase 1 — complete the investigation-critical surfaces

Implement and verify, in order:

1. Time Travel: frozen range, viewport, pin, metric, hosts, split mode, filters,
   visible categories, view mode, sort, and selected detail.
2. Analytics dashboards and Query Explorer: dashboard identity/version,
   dashboard time/filters/focus, query drill state, and selected detail.
3. Database Explorer: database/table/partition/part identity and inspector.
4. Events: search/grouping and event-context window in addition to current
   range/selection.

### Phase 2 — remaining surfaces

Add source context everywhere, then cover Overview, Replication, Engine
Internals, Cluster, and remaining secondary controls. Query and Merge schemas
should be migrated to the shared codec and checked for missing detail-tab/time
state rather than rewritten.

### Phase 3 — snapshots

Start with downloadable structured snapshots or a self-contained report. Add a
server-side snapshot-link service only after its authorization, retention, and
redaction model is approved.

## 8. Acceptance criteria

The feature is ready when all of the following hold:

1. Every top-level surface has the common Share control.
2. Every surface documents its captured, deliberately omitted, and unsupported
   state.
3. Opening a copied link in a clean browser profile restores the same route,
   source/cluster request, stable selection, filters, sorting, visible modes,
   and absolute time window.
4. The test above passes in standalone and in Grafana, including a non-default
   Grafana organization and datasource.
5. Reload and browser back/forward preserve the same state in both builds.
6. Unknown query parameters and Grafana-owned parameters survive updates and
   sharing.
7. Existing unversioned query, merge, event, and Analytics deep links remain
   valid.
8. Missing source, permission, capability, retained data, or descriptor version
   produces an explicit degraded state; no silent source substitution occurs.
9. No credentials or known secrets are serialized. SQL-bearing links warn the
   sender. Oversized state falls back to export/snapshot.
10. Automated round-trip tests prove `decode(encode(state))` for every schema,
    and end-to-end tests share from one browser context and open in another.

## 9. Decisions required

1. Is "same investigation" (re-query authorized data) the default promise, with
   exact historical evidence delegated to snapshots? This proposal recommends
   yes.
2. Should standalone deployments support configured source aliases? Without
   them, cross-user source selection cannot be automatic or safe.
3. What URL size budget should trigger snapshot/export fallback? A conservative
   product budget should be chosen and tested through supported proxies rather
   than relying on browser maximums.
4. Are raw SQL and object names acceptable in warned permalinks, or must any
   SQL-bearing share use protected snapshot storage?
5. What retention and authorization policy should a future snapshot service
   use?

## 10. Recommendation

Approve phases 0 and 1 before adding more isolated deep links. The most valuable
first user-visible result is a trustworthy Share action on Time Travel, because
that page currently has both the highest investigation value and one of the
largest gaps between a copied URL and the screen being viewed. Build it on one
shared URL contract so the Grafana plugin and standalone app cannot drift again.
