# Proposal: dependable link sharing

Status: draft; Grafana Queries proof of concept implemented, product-wide coverage incomplete
Author: repository assessment, 2026-08-01

Updated: 2026-09-10 — Grafana implementation recommendations (section 11)

---

## 1. Product definition

In TraceHouse, **Share** should mean:

> From any meaningful screen state, I can send something to a colleague that
> opens the same investigation: the same TraceHouse surface, data source and
> cluster, stable selected object, filters, ordering, view mode, and bounded
> time context, subject to the colleague's access and the continued existence
> of the underlying data.

This is stronger than "the URL opens the same page," but it does not promise
identical rows or pixels. The link carries the screen's coordinates and queries
the recipient's authorized ClickHouse datasource as it exists when opened.
ClickHouse data and metadata may legitimately change or expire between the two
visits; preserving that evidence is outside the scope of this feature.

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
- The browser address is not yet a dependable representation of every screen.
- The standalone and Grafana builds use different URL implementations. They
  already have different schemas and behavior.

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

Time controls are coordinates too. Absolute windows remain absolute; relative
or live windows remain relative or live. The recipient may therefore see newer
rows while using the same time mode, which is consistent with this feature's
coordinate-sharing contract.

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

#### Some URL writers can discard context

The Analytics Share action constructs a fresh query string containing only its known fields.
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

The address bar is the sharing interface. Every meaningful state change must
update it immediately, including modal open/close state and the selected modal
tab. Users share a view with the browser's ordinary copy-URL behavior; pages and
modals must not need their own Share controls.

Before copying, the menu should state which source and cluster are included and
whether the link contains SQL or other potentially sensitive fields. If the URL
would exceed the supported budget, report that the current state cannot yet be
represented by a dependable link.

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

### An explicit code-level contract

Every surface declares its durable URL state with `defineShareSchema` beside
the component that owns it. An entry in that schema means "this coordinate must
survive copy/paste, reload, and browser navigation." State kept only in local
`useState` is intentionally ephemeral. The shared codec owns parsing and
serialization; standalone React Router and Grafana `locationService` are
transport adapters for the same contract.

Defaults that must remain visible in copied URLs use `persistDefault: true`.
For example, Time Travel declares its activity limit this way, so `Show 100`
produces `tt_limit=100` rather than relying on an implicit recipient default.
Query Details declares `qd_id` and `qd_tab` in the existing deep-link hook used
by both the modal and its record loader.

Each page should implement the equivalent of:

```ts
interface ShareableSurface<S> {
  capture(): S;
  restore(state: S): void;
  normalizeForShare(state: S, now: Date): S;
}
```

Normalization removes defaults, preserves the selected time mode, replaces unstable row
indices with stable identities, and rejects secrets or unsupported values.

### Schema evolution

Include `v=1` from the first product-wide release. Decoders must:

- ignore unknown fields;
- validate enum and numeric bounds;
- migrate known older versions;
- fall back visibly rather than throw on malformed state.

Existing unversioned `qd_id`, merge, event, and Analytics links should remain
valid through compatibility decoders.

### Coordinate contract

A shared link stores intent and re-runs queries. Changed rows, expired system-log
entries, and refreshed metadata do not make the link incorrect as long as the
same source and screen coordinates were applied. The UI should distinguish a
selection that no longer exists from a failure to restore its coordinates.

## 7. Delivery plan

### Phase 0 — correctness and guardrails

1. Create the shared descriptor/codec package and platform adapters.
2. Make Grafana search state use `locationService` exclusively.
3. Preserve Grafana-owned and unknown parameters when sharing.
4. Add schema versioning, validation, URL-size checks, and sensitive-state
   classification.
5. Resolve source/organization/cluster before page queries start in both builds;
   link context takes precedence over browser-local defaults.
6. Make the address bar continuously represent the current coordinates,
   including nested views and modals.

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

With source context established in phase 0, cover Overview, Replication, Engine
Internals, Cluster, and remaining secondary controls. Include every registered
route, including hidden/experimental Notebooks and nested detail views, in the
coverage inventory; hidden navigation does not exempt a reachable page. Query and Merge schemas
should be migrated to the shared codec and checked for missing detail-tab/time
state rather than rewritten.

## 8. Acceptance criteria

The feature is ready when all of the following hold:

1. Every top-level surface and meaningful nested view continuously exposes its
   coordinates in the browser URL.
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
   sender. Oversized state produces an explicit unsupported-state message.
10. Automated round-trip tests prove `decode(encode(state))` for every schema,
    and end-to-end tests share from one browser context and open in another.

## 9. Decisions required

1. Should standalone deployments support configured source aliases? Without
   them, cross-user source selection cannot be automatic or safe.
2. What URL size budget should reject link creation? A conservative
   product budget should be chosen and tested through supported proxies rather
   than relying on browser maximums.
3. Are raw SQL and object names acceptable in warned permalinks?

## 10. Recommendation

Approve phases 0 and 1 before adding more isolated deep links. The most valuable
first user-visible result is a trustworthy address-bar URL on Time Travel, because
that page currently has both the highest investigation value and one of the
largest gaps between a copied URL and the screen being viewed. Build it on one
shared URL contract so the Grafana plugin and standalone app cannot drift again.


## 11. Grafana-focused implementation recommendations

### 11.1 Assessment and scope

Grafana is the highest-risk **integration** work because the host owns routing,
organization context, datasource access, and deployment prefixes. It is not
necessarily the largest implementation effort: capturing all page controls
remains substantial shared work. Prove the Grafana
foundation first, while keeping page schemas shared with standalone.

The recommendations below follow inspection of the current repository. They
are proposed changes, not claims that sharing is already implemented.

### 11.2 Concrete issues confirmed in the code

| Location | Current behavior | Recommended change |
|---|---|---|
| `grafana-app-plugin/src/App.tsx` and `src/hooks/useAppLocation.ts` | App location context and Grafana location are separate mechanisms; navigation updates context as well as the host. | Derive route and search from one subscribed host location. Keep only genuinely transient navigation hints in memory. |
| `grafana-app-plugin/src/stubs/react-router-dom.tsx` | Search writes call `window.history` directly; several router hooks are no-ops. Links construct a hard-coded plugin path. | Make compatibility exports delegate to the same platform adapter; remove independent history writes and migrate shared consumers to the app location API. |
| `frontend/src/hooks/useQueryDeepLink.ts` | Imports `useSearchParams` directly from `react-router-dom`, reaching the stub in Grafana. | Use the common adapter and test detail open/close with back/forward and a fresh browser. |
| `grafana-app-plugin/src/hooks/useAppLocation.ts` | Search replacement clears all existing parameters and converts repeated values into a scalar record. | Update only explicitly owned keys, preserve unrelated host parameters, and retain repeated values such as multiple hosts. |
| `grafana-app-plugin/src/hooks/useUrlState.ts` | Generic schema updates preserve unrelated keys, but Analytics Share constructs a new query string; Analytics fields differ from standalone. | Retain the generic ownership behavior, move codecs into shared code, and build shares from the current host URL plus the normalized descriptor. |
| `grafana-app-plugin/src/ServiceProvider.tsx` | Datasource initialization, single-source auto-selection, and cluster overrides depend on local defaults. | Resolve explicit link context first and prevent default selection or cluster detection from overwriting it. |
| `grafana-app-plugin/.config/webpack/` | Multiple build configurations alias shared hooks and router imports. | Verify development and production builds use the same adapter; a passing standalone test does not verify the plugin aliases. |

### 11.3 One host adapter and explicit parameter ownership

Use one Grafana adapter for reading location, subscribing to changes, building
URLs, and performing push/replace navigation. Its consumers include header
navigation, page state, and modal deep links. All writes go
through `locationService`; location context reflects that source rather than
optimistically maintaining a second durable route.

The adapter must:

- Preserve Grafana organization and other unrelated host parameters on updates
  and Share. On a page change, remove the previous page's owned state while
  retaining global source context and host context.
- Reserve a TraceHouse namespace for new global fields, for example `th_v`,
  `th_ds`, `th_cluster`, `th_from`, and `th_to`. Existing `from` already has
  page-specific meaning; do not silently reinterpret legacy parameters.
- Distinguish an omitted cluster (use defaults) from an explicit connected-node
  scope (no cluster). The schema must encode both without ambiguity.
- Merge against the latest host location, preserving repeated query values and
  preventing concurrent modal/page updates from overwriting one another.
- Respect the configured Grafana deployment subpath when building absolute
  links. Test both `/a/...` and a deployment such as `/grafana/a/...`; do not
  assume Grafana is mounted at the origin root.
- Preserve normal anchor behavior for new tabs and modifier clicks. Navigation
  hints stored in memory cannot be required to restore a shared view.
- Use replace for frequent control adjustments and push for meaningful
  navigation, with tested back/forward semantics and no feedback loops.

Verify the precise host API and subpath behavior against the supported Grafana
versions during implementation. Treat this as a compatibility requirement,
not an assumption based only on mocked location services.

### 11.4 Restore source context before executing page queries

Implement an explicit restore sequence:

1. Decode and validate the descriptor without querying ClickHouse.
2. Establish or validate the requested Grafana organization through supported
   host navigation. Carry the complete target through login/organization
   navigation; never assume adding an `orgId` parameter grants access.
3. Resolve the requested datasource UID within that organization using the
   recipient's permissions. Names are display labels, not stable identity.
4. Discover available clusters through that datasource and validate the exact
   requested cluster or connected-node scope.
5. Apply page selection, filters, absolute time, and refresh mode; only then
   enable investigation queries and polling.

Datasource discovery may require host/backend requests; the gate applies to
page investigation queries that could otherwise run against the wrong source.
While resolving, show a restoring state. On failure, preserve the requested
view and show a specific unresolved-source state with a source picker. Do not
fall back silently to the recipient's saved datasource or first available one.

Keep link overrides separate from saved preferences: opening a colleague's
link should not permanently change the recipient's default source or refresh
settings. On subsequent URL/source changes, cancel or ignore stale requests,
clear source-dependent selections/caches as appropriate, and repeat resolution.
Late cluster discovery must not replace the requested scope.

### 11.5 Browser links and Share links

Keep meaningful view state synchronized to the browser URL so copying the
address works for selections and controls. Preserve the displayed time
coordinate: absolute intervals remain absolute, and relative or live intervals
remain relative or live. The recipient re-queries ClickHouse;
different returned data is expected and does not mean coordinate restoration
failed.

TraceHouse custom dashboards are app content, not automatically Grafana
Dashboards. Sharing them requires a portable definition/version or a shared
repository. Likewise, inventory Notebooks and their portable definitions before
choosing reusable infrastructure; the page-coverage table above predates that
route. A browser-local artifact ID is never sufficient.

Default scope is sharing within the same Grafana deployment, or within the same
standalone deployment. The common schema supports equivalent behavior in both;
a Grafana URL does not automatically become a standalone URL. Cross-deployment
or cross-build transfer requires an explicit import/source-mapping flow.

### 11.6 Recommended implementation slices and release gates

1. **Grafana adapter and compatibility fixes.** Unify host location ownership,
   repair query-detail navigation, preserve host/repeated parameters, and verify
   subpath handling in the real plugin. Migrate shared codecs without breaking
   existing URLs. Gate: route, detail selection, reload, and back/forward agree.
2. **Source-aware restoration.** Add organization/datasource/cluster identity and
   the initialization gate. Add standalone alias resolution in the same shared
   contract. Gate: a conflicting saved source never receives page queries when
   an explicit shared source is requested.
3. **Complete Time Travel in both builds.** Capture all investigation state,
   time mode, and selected detail in the live address-bar URL. Gate:
   browser B restores browser A's normalized descriptor without local storage
   from A.
4. **Every remaining surface.** Maintain a checklist derived from the route
   registry plus nested views, with captured/omitted/unsupported fields. Cover
   portable dashboard/notebook content as needed. Gate: no reachable page is
   silently considered complete merely because its route opens.
Run the following Grafana acceptance matrix in actual browser contexts, in
addition to shared codec and adapter unit tests:

| Scenario | Required outcome |
|---|---|
| Clean recipient and recipient with conflicting local preferences | Same requested source, cluster, controls, and time mode; saved defaults remain unchanged. |
| Non-default organization; unavailable datasource/cluster; insufficient access | Correct host context or explicit unresolved state; no silent source substitution. |
| Root and subpath deployment; development and production plugin builds | Correct absolute URL, route restoration, and adapter wiring. |
| Copy address, Share, reload, back/forward, detail open/close, new tab | Consistent coordinates and time mode across each entry path. |
| Multiple hosts, unknown host parameters, legacy URLs, malformed/newer versions | Repeated values and host context survive; compatibility or visible degradation works. |
| Slow source resolution and switching sources during requests | No investigation query starts on the wrong source; stale responses cannot repaint the new view. |
| Login redirect and recipient opening the link later | Target coordinates survive authentication; missing or expired selected objects are reported. |

Use at least two provisioned datasources and a non-default organization so
accidental reliance on defaults cannot pass. Run against supported Grafana
versions; decide the version matrix before declaring the adapter complete.

### 11.7 Proof-of-concept status

The initial Grafana Queries proof of concept now keeps `th_v`, `th_ds`, and
`th_cluster` coordinates in the address bar, restores an explicit datasource
ahead of browser-local preferences, and gates page services until cluster
resolution. Query filters, sorting, `qd_id`, and the active query-detail tab use
the page URL. The Grafana router compatibility layer now sends search changes
through `locationService` and preserves repeated parameters.

The Analytics proof now keeps dashboard identity, time mode, dashboard filter
values, focused/fullscreen panel, focus expansion, crosshair/overlay modes,
Tables system-database visibility, Surfaces controls, and query-detail state in
the address bar. Query Explorer continues to use its existing URL-backed query
and chart coordinates.

This proves the implementation direction, not the product-wide acceptance
criteria. It still needs real two-browser Grafana validation, explicit
organization-transition testing, richer missing-source recovery, and coordinate
coverage for the other registered surfaces.
