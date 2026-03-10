# Connecting to ClickHouse

TraceHouse connects to any ClickHouse instance over its HTTP interface. You bring your own cluster - the app just needs HTTP(S) access and read permissions on system tables.

## Supported Environments

| Environment | CORS | Proxy needed | Notes |
| --- | --- | --- | --- |
| ClickHouse Cloud | Built-in | No | Works out of the box |
| Self-managed (on-prem / VM) | Needs config | Usually yes | Enable CORS in ClickHouse config, or use the bundled proxy |
| Kubernetes (Altinity Operator, ClickHouse Operator) | Needs config | Usually yes | Same as self-managed; ensure the HTTP interface is exposed |
| Aiven for ClickHouse | No CORS | Yes | Use the bundled proxy and configure credentials in `.env` |
| Docker Compose / Kind (this repo) | Pre-configured | No | For local development and testing |

## Connection Setup

The primary way to connect is through the **app's connection UI** - enter your host, port, and credentials directly in the browser. The app defaults to HTTPS (port 8443) for remote hosts and HTTP (port 8123) for localhost.

### `.env` file (for scripts only)

The `.env` file configures connection details for CLI scripts (`just load-data`, `just run-queries`, etc.) - it is **not** used by the frontend app itself. See [Loading Test Data - Configuration](./test-data#configuration) for details.

## CORS Proxy

When connecting to a ClickHouse instance that doesn't return CORS headers (common with self-managed and Aiven), the browser will block requests. The app routes these through a lightweight proxy that forwards them server-side. See [Deployment](./deployment.md) for the different ways to run TraceHouse with the proxy included.

**Docker image** - the proxy is bundled and always active. No setup needed - just enter your ClickHouse host in the connection form and the proxy handles the rest.

**Development / standalone** - the proxy starts automatically with `just frontend-start` on `localhost:8990`. To run it separately:

```bash
just proxy-start
```

Then enable "Use CORS proxy" in the connection form and point it at `http://localhost:8990/proxy`.

The proxy is a small Express server (`packages/proxy/`) that forwards requests to ClickHouse server-side and streams responses back with permissive CORS headers. It's completely stateless - connection details are passed in request headers on every call.

:::tip Docker networking
When running the Docker image and connecting to ClickHouse on your host machine, use `host.docker.internal` instead of `localhost` as the host. Inside Docker, `localhost` refers to the container itself.
:::

## Cluster Topology

TraceHouse automatically detects cluster topology by querying `system.clusters`. For multi-shard or multi-replica setups, the app uses a host-targeted adapter to query individual nodes.

## Required Permissions

The monitoring user needs read access to system tables:

```sql
GRANT SELECT ON system.* TO monitoring_user;
```

For full functionality, the following system tables are queried:

- `system.metrics`, `system.asynchronous_metrics`, `system.events`
- `system.processes`, `system.merges`, `system.mutations`
- `system.query_log`, `system.part_log`
- `system.parts`, `system.columns`, `system.tables`
- `system.clusters`, `system.replicas`, `system.disks`
- `system.dictionaries`, `system.replication_queue`

### Automatic capability detection

On connect, the app probes the server to determine what is available and degrades gracefully when features are missing. The detection covers:

- **Server version** — via `version()`
- **System log tables** — checks which log tables exist in `system.tables` (e.g. `query_log`, `trace_log`, `part_log`, `metric_log`)
- **Introspection functions** — tests `demangle('')` to see if flamegraphs and stack trace demangling will work
- **CPU profiler** — reads `query_profiler_cpu_time_period_ns` / `query_profiler_real_time_period_ns` from `system.settings`
- **Cluster topology** — queries `system.clusters`, falls back to `system.replicas` if inaccessible
- **ZooKeeper / Keeper** — checks if `system.zookeeper` is present
- **ClickHouse Cloud** — detects cloud via `cloud_mode_engine` setting and `system.build_options`
- **Container environment** — reads cgroup CPU/memory limits, detects Kubernetes by hostname pattern

Screens that depend on a missing capability are either hidden or show a message explaining what is needed. See `packages/core/src/services/monitoring-capabilities.ts` for the full probe logic.

## Environment-Specific Notes

| | ClickHouse Cloud | Self-Managed | Aiven | Kubernetes |
| --- | --- | --- | --- | --- |
| **CORS** | Built-in | Needs config | No CORS - use proxy | Needs config |
| **Proxy needed** | No | Usually yes | Yes | Usually yes |
| **System log tables** | All available | All available | Many restricted ([details](./aiven.md#known-limitations)) | All available |
| **Log TTL** | Configurable | Configurable | 1 hour (hard limit) | Configurable |
| **`allow_introspection_functions`** | Off by default - must enable | Off by default | Not available | Off by default - must enable |
| **Query profiler** | Available | Needs config | Not available | Needs config |
| **Flamegraphs** | Requires introspection functions | Requires introspection functions + profiler | Not supported (no `trace_log`) | Requires introspection functions + profiler |

:::tip Aiven
Aiven has significant system table restrictions that affect several features. Aiven also enforces a short TTL (typically 1 hour) on most system log tables — their recommended workaround is to create materialized views that copy log data into regular tables for longer retention. We are planning to support configurable system table locations so the app can read from those materialized views instead. See the dedicated [Aiven for ClickHouse](./aiven.md) guide for full details on limitations and workarounds.
:::

## Recommended Settings

### `allow_introspection_functions`

This setting enables functions like `demangle`, `addressToLine`, and `addressToSymbol` which are required for flamegraph visualization and stack trace demangling. **ClickHouse Cloud disables this by default.**

There are three ways to enable it:

**Per session** (current session only):

```sql
SET allow_introspection_functions = 1;
```

**Per user** (persistent):

```sql
ALTER USER my_user SETTINGS allow_introspection_functions = 1;
```

**Per role** (persistent, applies to all users with the role):

```sql
ALTER ROLE my_role SETTINGS allow_introspection_functions = 1;
```

**Via config file** (self-managed only - add to user profile in `users.xml` or `users.d/`):

```xml
<profiles>
    <default>
        <allow_introspection_functions>1</allow_introspection_functions>
    </default>
</profiles>
```

:::note
The config file change requires `SYSTEM RELOAD CONFIG` or a server restart to take effect. The `SET` approach only applies to the current session. For a monitoring user that needs this permanently, prefer `ALTER USER` or `ALTER ROLE`.
:::

### Query Profiler

For flamegraphs to collect data, the query profiler must be active. On self-managed and Kubernetes deployments, ensure these settings are configured:

```xml
<profiles>
    <default>
        <query_profiler_cpu_time_period_ns>40000000</query_profiler_cpu_time_period_ns>
        <query_profiler_real_time_period_ns>40000000</query_profiler_real_time_period_ns>
    </default>
</profiles>
```

On ClickHouse Cloud, the query profiler is already active - you only need to enable `allow_introspection_functions`.
