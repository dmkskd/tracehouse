# Grafana Plugin

TraceHouse ships as a Grafana **app plugin** (`dmkskd-tracehouse-app`). It reuses the standalone frontend (same page components, same shared packages) and wraps it with the plugin scaffolding needed to run inside Grafana's sandbox: `plugin.json`, `module.ts`, webpack config, and adapter shims.

The key difference from the standalone app is how ClickHouse connectivity works. The standalone app connects directly (via the built-in proxy or user-provided credentials), while the Grafana plugin routes all queries through Grafana's ClickHouse datasource plugin. ClickHouse connectivity is managed entirely by Grafana's datasource configuration, not by TraceHouse itself.

All pages are available as Grafana navigation entries: Overview, Engine Internals, Cluster, Explorer, Time Travel, Events, Queries, Merges, Replication, and Analytics.

## Running with Docker

The easiest way to run TraceHouse inside Grafana is with the full Docker Compose profile:

```bash
just docker-start-full
```

This builds the plugin first (`just grafana-plugin-build`), then starts ClickHouse, Prometheus, and Grafana together. Grafana is available at `http://localhost:3001`.

The Grafana service mounts the built plugin from `grafana-app-plugin/dist/` and auto-installs the ClickHouse datasource plugin. Anonymous auth is enabled for local development.

To start the same stack without installing or mounting the TraceHouse app plugin, use:

```bash
just docker-start-full-no-plugin
```

This still starts ClickHouse, Prometheus, Grafana, and Tempo, installs the Grafana ClickHouse datasource plugin, and provisions the local ClickHouse datasource. TraceHouse is left absent so it can be installed separately from the Grafana plugin catalog.

### Testing against different Grafana versions

`just docker-start-full` accepts an optional Grafana image tag:

```bash
just docker-start-full 11.6.3
just docker-start-full 13.0.3
just docker-start-full latest
```

The default is `13.0.3`. The background and refresh recipes accept the same argument:

```bash
just docker-start-full-bg 11.6.3
just docker-refresh-grafana-plugin 11.6.3
just docker-refresh-grafana-plugin-fast 11.6.3
```

When switching across major versions or downgrading Grafana, clear the persisted Grafana data volume to avoid database migration conflicts:

```bash
just docker-stop
docker volume rm tracehouse_tracehouse-grafana-data
just docker-start-full 11.6.3
```

### Running the exact release plugin locally

`just docker-start-full` mounts your local `grafana-app-plugin/dist/` directory. To reproduce what users are testing from a GitHub release, run Grafana with the packaged release ZIP instead:

```bash
just docker-start-full-release v0.16.12
```

This downloads the versioned plugin ZIP, such as `dmkskd-tracehouse-app-0.16.12.zip`, from the release, extracts it under `/private/tmp`, and mounts that extracted plugin directory into Grafana. It does not rebuild the plugin locally.

You can also run a ZIP you already have:

```bash
just docker-start-full-plugin-zip /path/to/dmkskd-tracehouse-app-0.16.12.zip
```

Both commands accept the optional Grafana version argument:

```bash
just docker-start-full-release v0.16.12 13.0.3
just docker-start-full-plugin-zip /path/to/dmkskd-tracehouse-app-0.16.12.zip dev-preview-react19
```

### Development with watch mode

For iterating on the plugin:

```bash
# Terminal 1: start infra (builds the plugin once, then starts everything)
just docker-start-full

# Terminal 2: watch mode, rebuilds on file changes
just grafana-plugin-dev
```

Reload the Grafana page after each rebuild.

## How the Integration Works

- **Plugin type:** Grafana `app` plugin, registered via `AppPlugin` from `@grafana/runtime` in `module.ts`
- **Pages:** Declared in `plugin.json` under `includes[]`. Each page gets a Grafana nav entry at `/a/dmkskd-tracehouse-app/<slug>`
- **Data flow:** Queries go through Grafana's `/api/ds/query` backend endpoint via `GrafanaAdapter` (in `packages/core`), using whichever ClickHouse datasource the user selects
- **Configuration:** Admin settings (refresh rates) stored in Grafana plugin `jsonData`, accessed at runtime via `PluginConfigContext`
- **Theme bridge:** Reads `config.theme2.isDark` from Grafana and sets the `data-theme` attribute on `<html>` so the frontend's CSS variables match Grafana's theme
- **Shared code:** Pages and utilities come from workspace packages (`@tracehouse/core`, `@tracehouse/ui-shared`) and from `frontend/src/` via webpack aliases. The plugin reuses the same page components as the standalone app

### Analytics result types and metadata lookup

**Potential performance cost: each time-bearing Analytics execution adds one
sequential datasource request. There is currently no metadata cache.** A dashboard
with many time-bearing panels, or frequent refreshes, can therefore incur extra
latency and ClickHouse load. This does not imply that every query takes twice as
long: the additional cost depends on network latency and metadata analysis time.

The installed ClickHouse datasource represents both calendar `Date`/`Date32`
columns and `DateTime`/`DateTime64` instants as Grafana `time` fields containing
epoch milliseconds. They need different treatment: a calendar date must retain
its day, while an instant must retain its absolute time and localize for display.
Treating a calendar date as an instant can shift it to the preceding day.

`InteractiveQueryService` requests `utcDateTimeOutput`. In GrafanaAdapter:

1. The main query runs with datasource target `refId: q`.
2. If its returned frames contain a `time` field, a second request runs
   `DESCRIBE TABLE (<query>)` with `refId: types` to obtain native column types.
3. Calendar dates are reconstructed as date strings; timestamp instants become
   UTC ISO strings. Native types are resolved once per column, not per cell.

The metadata request starts after the main query completes and is repeated on
each qualifying execution, including refreshes and repeated identical SQL. It
performs type analysis; scalar subqueries may evaluate during analysis, so it
must not be assumed to be free or incapable of running expressions. Queries
without returned time fields skip the lookup. Other service calls that do not
request typed UTC output skip it too. Direct browser, HTTP and proxy transports
use ClickHouse's ISO output setting and do not make this metadata request.

Calendar-date decoding depends on the datasource request using
`GRAFANA_QUERY_TIMEZONE = 'browser'`, shared with ServiceProvider. Changing that
setting requires revisiting decoding and its timezone tests. Grafana time frames
retain milliseconds; exact microsecond identities must remain SQL string
projections rather than pass through a Grafana time field.

#### Diagnosing slowness or lookup failures

- In browser DevTools, inspect `/api/ds/query` requests. Compare the main `q`
  target with the following `types` target containing `DESCRIBE TABLE (...)`.
  Measure their durations separately; panel completion includes both. Count the
  metadata requests during a dashboard refresh to assess the extra traffic.
- If the main request succeeds but the panel fails with
  `Cannot safely interpret Grafana time fields: native column-type lookup failed`,
  inspect the `types` response. Permissions, datasource behavior or metadata SQL
  analysis can fail independently of the main query. The adapter reports that
  failure rather than silently returning potentially incorrect dates.
- Metadata SQL is constructed in `packages/core/src/queries/result-type-queries.ts`.
  Its scanner removes outer output clauses and terminators while respecting
  quotes and comments. When reporting a failure, include the main SQL, generated
  metadata SQL, error, and ClickHouse/Grafana/datasource versions.
- Conversion lives in `packages/core/src/adapters/grafana-adapter.ts`; datasource
  request construction lives in `grafana-app-plugin/src/ServiceProvider.tsx`.
  Relevant checks are `just test-core`, `just test-timezone-integration` and,
  with the development services running, `just test-timezone-transports`.

Caching is a possible future optimization, not current behavior. A cache design
must account for datasource/database identity and schema changes; SQL text alone
does not guarantee stable result types. Native type information in the main
datasource response would be another way to remove the extra request.

## Workarounds

- **No React Router:** Grafana's plugin sandbox doesn't provide a Router context. The plugin stubs out `react-router-dom` entirely (see `src/stubs/react-router-dom.tsx`) and replaces navigation hooks with a custom `LocationContext`-based implementation (`src/hooks/useAppLocation.ts`)
- **Store aliasing:** The standalone app's `connectionStore` and `ClickHouseProvider` are aliased at webpack level to Grafana-compatible shims that source the connection from the selected datasource instead of user-entered credentials
- **`useUrlState` via `locationService`:** Analytics state syncs to URL query params using Grafana's `locationService`, so links like `/a/dmkskd-tracehouse-app/analytics?tab=misc&preset=3` are fully shareable, same as in the standalone app
- **Swapped modules keep the frontend API.** The webpack config (`.config/webpack/webpack.config.cjs`, `resolve.alias`) replaces these frontend modules with plugin versions:

  | Frontend module | Plugin module |
  |---|---|
  | `frontend/src/stores/connectionStore` | `src/stores/connectionStore` |
  | `frontend/src/providers/ClickHouseProvider` | `src/ServiceProvider` |
  | `frontend/src/hooks/useAppLocation` | `src/hooks/useAppLocation.ts` |
  | `frontend/src/hooks/useUrlState` | `src/hooks/useUrlState.ts` |
  | `frontend/src/utils/urlParams` | `src/utils/urlParams.ts` |
  | `react-router-dom` | `src/stubs/react-router-dom.tsx` |

  Pages are typechecked against the frontend module but run the plugin one, so the frontend typecheck and tests cannot catch a difference. **When you change one of these frontend modules, change its plugin version in the same change:** every exported name, and every field and option of what the hooks return. A missing field fails only at runtime in Grafana (for example `undefined is not an object (evaluating 'e.get')`).

  `src/aliasParity.typecheck.ts` checks `useUrlState`, `useAppLocation` and `urlParams`: the plugin module exports every frontend name, and its hooks are assignable to the frontend ones. It runs with `npm test` in `grafana-app-plugin` (`tsc -p tsconfig.alias-parity.json`). `connectionStore`, `ServiceProvider`, the router stub, and the `useSearchParams` setter (the plugin accepts a `URLSearchParams` or an updater function, not every `react-router` init form) are not covered.
- **AMD output format:** Grafana's plugin loader requires AMD modules. The webpack config sets `output.library.type: 'amd'` with `uniqueName: 'dmkskd-tracehouse-app'` for namespace isolation
- **Node.js polyfill stubs:** Webpack config stubs all Node.js core modules (`stream`, `zlib`, `crypto`, etc.) to `false` since the plugin runs in the browser but some transitive dependencies reference them
- **Unsigned plugin allowlist:** For local development, `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=dmkskd-tracehouse-app` is set in the Docker Compose config

## Building the Plugin

```bash
# One-off build
just grafana-plugin-build

# Output: grafana-app-plugin/dist/ (module.js, plugin.json, README.md)
```
