# TraceHouse — Polling & Refresh Rate Reference

This document is intended for Grafana administrators who manage the TraceHouse plugin.
It explains every automatic polling interval in the application, what each one queries,
and the relative load it places on your ClickHouse cluster.

---

## How Polling Works

The plugin uses client-side `setInterval` polling to keep dashboards up to date.
There is no server-side push (WebSocket or SSE) — every refresh is a new SQL query
routed through the Grafana ClickHouse datasource proxy.

Polling starts when a user opens a page and stops when they navigate away.
Multiple users viewing the same page will each generate their own queries.

---

## Polling Intervals by Page

### Overview

| Default Rate | What It Polls | System Tables Hit | Load |
|---|---|---|---|
| 5 s | Live view bundle: resource attribution, running queries, active merges, mutations, alerts | `system.processes`, `system.merges`, `system.mutations`, `system.metrics`, `system.asynchronous_metrics` | Moderate — multiple lightweight queries bundled into one cycle |
| 5 s | Core server metrics (CPU, memory, disk I/O) | `system.metrics`, `system.asynchronous_metrics` | Low–moderate |
| 2 s | Background pool utilisation | `system.metrics` | Low — single narrow query |

### Live View

| Default Rate | What It Polls | System Tables Hit | Load |
|---|---|---|---|
| 5 s | Same live view bundle as Overview | `system.processes`, `system.merges`, `system.mutations`, `system.metrics`, `system.asynchronous_metrics` | Moderate |

### Engine Internals

| Default Rate | What It Polls | System Tables Hit | Load |
|---|---|---|---|
| 5 s | Thread pools, memory trackers, PK index stats, dictionaries | `system.metrics`, `system.dictionaries`, `system.parts` (index stats) | Moderate — several system table reads per cycle |
| 10 s | CPU flame-graph sampling | `system.trace_log` | **Higher** — `trace_log` can be large; query scans recent rows with aggregation |

### Database Explorer

| Default Rate | What It Polls | System Tables Hit | Load |
|---|---|---|---|
| 2 s (default, user-adjustable) | Parts, active merges, mutations — scope depends on drill level (database → table → partition) | `system.parts`, `system.merges`, `system.mutations` | **Moderate–High** — `system.parts` can return many rows on large clusters |

User-selectable options (current defaults): **0.5 s, 1 s, 2 s, 5 s, 10 s**

> **Admin note:** The 0.5 s and 1 s options are the most aggressive intervals in the
> entire plugin. On clusters with thousands of parts, each poll of `system.parts` can
> take tens of milliseconds and generate non-trivial I/O. Consider disabling these
> via the plugin configuration page.

### Merge Tracker

| Default Rate | What It Polls | System Tables Hit | Load |
|---|---|---|---|
| 5 s | Active merges, mutations, background pool metrics | `system.merges`, `system.mutations`, `system.metrics` | Moderate |
| 5 s | Per-table merge tracking (via `useMergeTracking` hook) | `system.merges` (filtered to one table) | Low |

### Query Monitor

| Default Rate | What It Polls | System Tables Hit | Load |
|---|---|---|---|
| 2 s | Running queries | `system.processes` | Low–moderate — process list is typically small |

Query history is fetched on-demand (not polled).

### Time Travel

| Default Rate | What It Polls | System Tables Hit | Load |
|---|---|---|---|
| User-selected (default: Off) | Timeline data, memory snapshots, query aggregations | `system.query_log`, `system.parts`, `system.asynchronous_metrics` | **Varies** — heavier aggregation queries over time ranges |

User-selectable options: **Off, 5 s, 10 s, 30 s, 1 m**

---

## Non-Query Intervals (No ClickHouse Load)

These timers exist in the UI but do not generate any ClickHouse queries:

| Rate | Purpose |
|---|---|
| 1 s | Layout header clock display |
| 1 s | Fade-out cleanup for completed merges (local state) |

---

## Load Impact Summary

Sorted from most to least aggressive:

| Risk | Interval | Page | Why |
|---|---|---|---|
| 🔴 High | 0.5 s | Database Explorer (user-selected) | `system.parts` every 500 ms on large tables |
| 🔴 High | 1 s | Database Explorer (user-selected) | Same as above, slightly less frequent |
| 🟡 Moderate | 2 s | Database Explorer (default), Overview (pools), Query Monitor | Multiple system tables at high frequency |
| 🟡 Moderate | 5 s | Overview, Live View, Engine Internals, Merge Tracker | Bundled queries, reasonable cadence |
| 🟢 Low | 10 s | Engine Internals (CPU sampling) | `trace_log` is heavy but 10 s is manageable |
| 🟢 Low | 30 s / 60 s | Time Travel (user-selected) | Infrequent, acceptable for aggregation queries |

---

## Scaling Considerations

- **Per-user multiplier**: Every open browser tab runs its own polling loops.
  10 users on the Overview page = 10× the query volume at each interval.
- **Cluster size**: `system.parts` row count grows with the number of tables and partitions.
  A cluster with 50K+ parts will feel the 0.5 s / 1 s Database Explorer intervals.
- **trace_log volume**: CPU sampling queries `system.trace_log` which can grow quickly
  if `query_profiler_real_time_period_ns` is set aggressively. The 10 s interval is
  a reasonable default but monitor `trace_log` size.
- **Read-only impact**: All queries are `SELECT` against system tables. They do not
  acquire locks or block writes, but they do consume CPU and memory on the ClickHouse
  server handling the query.

---

## Admin Configuration

Grafana admins can restrict which refresh rates are available to users from the
plugin configuration page:

**Grafana → Administration → Plugins → TraceHouse → Configuration**

From there you can:

1. **Disable aggressive rates** (e.g. uncheck 0.5 s, 1 s, 2 s) to prevent users
   from overloading the cluster.
2. **Set a default rate** that applies when a user first opens a page.

These settings are stored in Grafana's plugin `jsonData` and apply globally to all
users of the plugin instance.

The configuration is consumed by every page in the app:

- Pages with user-selectable dropdowns (Database Explorer, Time Travel) filter
  their options to only show admin-allowed rates.
- Pages with fixed polling intervals (Overview, Live View, Engine Internals,
  Query Monitor, Merge Tracker) clamp their interval to the nearest allowed rate.

> Settings are read at page load. Users who already have a tab open will pick up
> changes on their next page navigation or browser refresh.
