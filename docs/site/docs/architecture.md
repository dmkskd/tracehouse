# Architecture

## Client-Side Only - Static HTML, No Backend

TraceHouse is a fully static, client-side application. There is no backend server, no API layer, no database of its own. The entire app is a set of HTML, CSS, and JavaScript files that run in your browser and query ClickHouse directly over its HTTP interface.

This means:

- You can serve it from any static file host (Nginx, S3, `file://`, etc.)
- There is nothing to deploy, scale, or keep running on the server side
- Connection credentials are managed in the browser. In direct mode they're sent straight to ClickHouse; in proxy mode they're forwarded via request headers through the local CORS proxy (which is stateless and stores nothing)
- You can even build the whole app as a [single self-contained HTML file](./guides/deployment) that works from `file://`

The browser uses [`@clickhouse/client-web`](https://github.com/ClickHouse/clickhouse-js), the official ClickHouse JavaScript client for browser environments, to send SQL queries over HTTP or HTTPS and receive JSON results. When you enter a remote host in the connection form, the app defaults to HTTPS (port 8443). For localhost connections it defaults to plain HTTP (port 8123). You can override both the protocol and port manually.

### CORS and the Proxy

Because the browser makes HTTP requests directly to ClickHouse, the ClickHouse server must allow cross-origin requests (CORS). Without CORS headers, the browser will block the responses.

| ClickHouse setup | CORS built-in | Proxy needed |
|---|---|---|
| Local / Docker Compose (this repo) | ✓ (configured) | No |
| Kubernetes / Kind (this repo) | ✓ (configured) | No |
| ClickHouse Cloud | ✓ | No |
| Aiven for ClickHouse | ✗ | Yes |
| Self-managed (default config) | ✗ | Yes (or enable CORS in config) |

There are two ways to handle this:

**Option 1 - ClickHouse has CORS enabled (simplest)**

If your ClickHouse instance returns the right `Access-Control-Allow-*` headers, the browser can talk to it directly. This is the case for:

- ClickHouse Cloud (CORS enabled by default)
- The Docker Compose and Kubernetes setups in this repo (configured via `http_options_response` in the ClickHouse config)
- Any self-managed instance where you've added CORS headers to the config

```
Browser ──HTTP──▶ ClickHouse (with CORS headers)
```

**Option 2 - Use the CORS proxy**

When connecting to a ClickHouse instance that doesn't have CORS enabled (common with self-managed clusters), the app routes requests through a lightweight proxy server.

```
Browser ──HTTP──▶ proxy ──HTTP──▶ ClickHouse
```

The proxy is a small Express server (`packages/proxy/`) that:
- Receives the query from the browser
- Forwards it to ClickHouse server-side (where CORS doesn't apply)
- Streams the response back to the browser with permissive CORS headers

The proxy is completely stateless - connection details (host, port, user, password) are passed in request headers on every call. It holds no configuration and stores nothing.

**In development**, the proxy starts automatically with `just frontend-start` on `localhost:8990`. It can also be started separately with `just proxy-start`.

**In the Docker image**, the proxy is bundled alongside the frontend and served from the same port. The app automatically uses it - no user configuration needed. See [Deployment → Docker Image](./guides/deployment) for details.

## Grafana App Plugin

TraceHouse can be deployed as a Grafana app plugin (`grafana-app-plugin/`). The same static frontend is packaged and served inside Grafana, giving you the full monitoring experience without a separate deployment.

Instead of connecting to ClickHouse directly via `@clickhouse/client-web`, the app routes all queries through the [Grafana ClickHouse data source plugin](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) - an external plugin maintained by Grafana. This means:

- **No connection setup** - users pick an existing Grafana ClickHouse data source instead of entering host/port/credentials
- **Grafana handles auth and permissions** - access control, team permissions, and credential management are all delegated to Grafana
- **No CORS concerns** - queries go through the Grafana backend, so there's no need for a CORS proxy or ClickHouse CORS configuration
- **Same app, different transport** - the UI, query logic, and attribution models are identical to the standalone version; only the connection layer is swapped

The app plugin shares the same `packages/core` library as the standalone frontend.

## Data Collection

:::caution Work In Progress
The tiered polling model described below is planned but not yet fully implemented.
:::

The app collects data from ClickHouse system tables in three tiers:

| Tier | Interval | Tables | Cost |
|------|----------|--------|------|
| Tier 1 (live state) | 2–5s | `asynchronous_metrics`, `metrics`, `processes`, `merges`, `mutations` | Near-zero (virtual tables) |
| Tier 2 (recent completed) | 30s | `query_log`, `part_log`, `events` | Light MergeTree reads |
| Tier 3 (structural) | 60s | `parts`, `disks`, `replicas`, `dictionaries` | Virtual or light aggregation |

Total overhead: ~10 lightweight read-only queries per 30-second cycle.

## Attribution Model

The core innovation is resource attribution - breaking down aggregate metrics into per-actor contributions:

```
Total CPU = Query CPU + Merge CPU + Mutation CPU + Replication CPU + Other
```

Each resource dimension (CPU, memory, I/O) is attributed to specific actors using data from `system.processes` (live) and `system.query_log` / `system.part_log` (historical).
