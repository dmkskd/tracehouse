# Grafana Plugin

TraceHouse ships as a Grafana **app plugin** (`dmkskd-tracehouse-app`). It reuses the standalone frontend (same page components, same shared packages) and wraps it with the plugin scaffolding needed to run inside Grafana's sandbox: `plugin.json`, `module.ts`, webpack config, and adapter shims.

The key difference from the standalone app is how ClickHouse connectivity works. The standalone app connects directly (via the built-in proxy or user-provided credentials), while the Grafana plugin routes all queries through Grafana's ClickHouse datasource plugin. ClickHouse connectivity is managed entirely by Grafana's datasource configuration, not by TraceHouse itself.

All pages are available as Grafana navigation entries: Overview, Engine Internals, Cluster, Explorer, Time Travel, Queries, Merges, Replication, and Analytics.

## Running with Docker

The easiest way to run TraceHouse inside Grafana is with the full Docker Compose profile:

```bash
just docker-start-full
```

This builds the plugin first (`just grafana-plugin-build`), then starts ClickHouse, Prometheus, and Grafana together. Grafana is available at `http://localhost:3001`.

The Grafana service mounts the built plugin from `grafana-app-plugin/dist/` and auto-installs the ClickHouse datasource plugin. Anonymous auth is enabled for local development.

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

## Workarounds

- **No React Router:** Grafana's plugin sandbox doesn't provide a Router context. The plugin stubs out `react-router-dom` entirely (see `src/stubs/react-router-dom.tsx`) and replaces navigation hooks with a custom `LocationContext`-based implementation (`src/hooks/useAppLocation.ts`)
- **Store aliasing:** The standalone app's `connectionStore` and `ClickHouseProvider` are aliased at webpack level to Grafana-compatible shims that source the connection from the selected datasource instead of user-entered credentials
- **`useUrlState` via `locationService`:** Analytics state syncs to URL query params using Grafana's `locationService`, so links like `/a/dmkskd-tracehouse-app/analytics?tab=misc&preset=3` are fully shareable, same as in the standalone app
- **AMD output format:** Grafana's plugin loader requires AMD modules. The webpack config sets `output.library.type: 'amd'` with `uniqueName: 'dmkskd-tracehouse-app'` for namespace isolation
- **Node.js polyfill stubs:** Webpack config stubs all Node.js core modules (`stream`, `zlib`, `crypto`, etc.) to `false` since the plugin runs in the browser but some transitive dependencies reference them
- **Unsigned plugin allowlist:** For local development, `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=dmkskd-tracehouse-app` is set in the Docker Compose config

## Building the Plugin

```bash
# One-off build
just grafana-plugin-build

# Output: grafana-app-plugin/dist/ (module.js, plugin.json, README.md)
```
