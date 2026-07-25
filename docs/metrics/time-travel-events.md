# Time Travel Events

Time Travel events are discrete operational occurrences that change how the
surrounding resource timeline should be interpreted. They answer questions such
as:

- Did the server restart or crash?
- Did a query hit a memory or admission limit?
- Was a table changed just before workload behaviour changed?
- Did storage, replication, or another background operation fail?
- What queries, merges, and mutations were active when it happened?

Events complement the continuous CPU, memory, disk, and network series described
in [time-travel.md](time-travel.md). They are not another metric-alerting system.

## Event inclusion policy

An occurrence is a good Time Travel event when it is:

1. **Discrete** — it has a meaningful point in time.
2. **Interpretive** — knowing it happened changes how someone reads nearby
   workload.
3. **Actionable or correlatable** — it can lead to a query, host, table, part, or
   subsystem that can be investigated.
4. **Historically recoverable** — ClickHouse persists enough evidence to place
   it in the requested time range.

Examples include restarts, crashes, query OOMs, DDL, background-task failures,
and Keeper connectivity changes.

Ordinary CPU or memory threshold crossings are not events by default. Those
conditions are already visible in the continuous graph, and inventing markers
for them would duplicate the chart while adding threshold noise.

## Generating events for development

The data-utils event workload can safely generate the three most useful
query-log-backed event patterns:

```bash
just run-events --once
just run-events
just run-events --types oom --oom-interval 900
```

It capability-checks `system.query_log`, labels generated SQL with
`tracehouse-demo-event`, and supports independent DDL, query-OOM, and timeout
cadences. DDL is confined to a disposable database and cleans up its tables.
The OOM is query-scoped through `max_memory_usage`; it does not exhaust server
or process memory.

Server restarts must be initiated outside ClickHouse. Crashes, full disks,
corrupt parts, and background-network failures are deliberately not generated
by the normal load tooling because they require isolated fault injection.
See the user-facing [test-data guide](../site/docs/guides/test-data.md#time-travel-events)
for commands and safety boundaries.

## Data contract and filtering

Every event has stable machine-readable dimensions:

| Field | Purpose |
| --- | --- |
| `occurred_at` | Best estimate of when the occurrence itself happened |
| `ended_at` | Recovery/end time for a state episode; omitted for point and still-open events |
| `observed_at` | When ClickHouse recorded or sampled it, if different |
| `hostname` | Host that produced the evidence |
| `kind` | Specific filter key, such as `query_oom` or `server_restart` |
| `category` | Broad filter group: lifecycle, queries, replication, coordination, storage, changes, or maintenance |
| `severity` | `critical`, `error`, `warning`, or `info` |
| `precision` | Whether timing is `exact`, `sampled`, or `inferred` |
| `source` / `capability` | System table and capability required to read it |

Source-specific correlation fields are retained when available: `query_id`,
`initial_query_id`, `normalized_query_hash`, user, query kind/text, exception
code/name, represented occurrence count, remote-error flag, duration, memory,
crash signal, and server version.

These fields are deliberately separate from presentation labels. The eventual
UI can therefore support combinations such as:

- critical events only;
- lifecycle events only;
- query OOMs, including non-fatal query memory-limit failures;
- one host or all hosts;
- all executions of one `normalized_query_hash`;
- storage and coordination failures while hiding routine query failures.

## Implemented sources

The first implementation lives in:

- `packages/core/src/queries/timeline-event-queries.ts`
- `packages/core/src/services/timeline-events-service.ts`
- `packages/core/src/types/timeline.ts`

Each source is queried independently. One missing or inaccessible table does not
prevent other event sources from loading.

### Query resource failures

| Property | Value |
| --- | --- |
| Source | `system.query_log` |
| Capability | `query_log` |
| Event category | `queries` |
| Timing | Exact query failure time |
| Current kinds | `query_oom`, `query_timeout`, `query_rejected`, `query_resource_limit` |

Only terminal failure rows are considered:

```sql
type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
```

The marker belongs at query failure, not query start:

```sql
query_start_time_microseconds
    + toIntervalMillisecond(query_duration_ms)
```

The current resource/admission allowlist is:

| Code | Name | Event kind | Default severity |
| ---: | --- | --- | --- |
| 159 | `TIMEOUT_EXCEEDED` | `query_timeout` | warning |
| 173 | `CANNOT_ALLOCATE_MEMORY` | `query_oom` | error |
| 201 | `QUOTA_EXCEEDED` | `query_rejected` | warning |
| 202 | `TOO_MANY_SIMULTANEOUS_QUERIES` | `query_rejected` | warning |
| 241 | `MEMORY_LIMIT_EXCEEDED` | `query_oom` | error |
| 243 | `NOT_ENOUGH_SPACE` | `query_resource_limit` | error |
| 252 | `TOO_MANY_PARTS` | `query_rejected` | warning |
| 692 | `TOO_MANY_MUTATIONS` | `query_rejected` | warning |

Occurrences remain individual. Two OOMs 15 minutes apart are two events, even
when they have the same normalized query hash. This preserves scheduled-job and
retry-loop patterns. Visual collision grouping must not destroy the underlying
timestamps.

TraceHouse's own tagged queries are excluded with the shared
`APP_SOURCE_LIKE` filter.

> **Query OOM is not a server OOM.** A `query_oom` means a query-scoped
> exception was recorded in `query_log`. It commonly indicates a query memory
> limit, and the ClickHouse process may continue normally. Conversely, an OS
> OOM-killer `SIGKILL` may leave no query failure or crash-log record at all.

### Server restarts

| Property | Value |
| --- | --- |
| Source | `system.asynchronous_metric_log` |
| Capability | `asynchronous_metric_log` |
| Event category | `lifecycle` |
| Event kind | `server_restart` |
| Timing | Inferred |
| Default severity | warning |

A restart is detected from the `Uptime` metric in two cases:

1. Uptime drops between consecutive samples for the same hostname.
2. A newly observed hostname starts with a low Uptime value, as happens when a
   container is recreated and receives a new hostname.

The collector reads 15 minutes before the requested range so a sample at the
left edge can be compared with earlier state. The inferred occurrence time is:

```text
sample event_time - sampled Uptime
```

The sample time is retained as `observed_at`, while the estimated boot time is
`occurred_at`.

This evidence proves that the ClickHouse process restarted. It does not yet
distinguish a planned restart, crash, operator action, container replacement, or
OOM-killer termination. A restart must not be labelled “process OOM” without
additional evidence.

### Server crashes

| Property | Value |
| --- | --- |
| Source | `system.crash_log` |
| Capability | `crash_log` |
| Event category | `lifecycle` |
| Event kind | `server_crash` |
| Timing | Exact |
| Default severity | critical |

The event records the crash time, signal, related query ID when present, host,
and ClickHouse version.

`system.crash_log` is a **persisted, node-local system log table**, not a live
virtual table. Its rows survive a normal ClickHouse restart, subject to the
table's configured storage and TTL. Like other persisted system logs it is
normally MergeTree-backed, although the system-log engine can be configured.

The table commonly does not exist until the first fatal error has occurred.
This is expected behaviour, not evidence of a capability-probe bug. Some managed
services also do not expose it. The query only runs when the capability probe
confirms the table is available. See the official
[`system.crash_log` documentation](https://clickhouse.com/docs/operations/system-tables/crash_log).

An OS `SIGKILL`, including an OOM-killer termination, may not give ClickHouse an
opportunity to write `crash_log`. In that case the only surviving evidence may
be a later Uptime reset and a data gap.

### DDL changes

| Property | Value |
| --- | --- |
| Source | `system.query_log` |
| Capability | `query_log` |
| Event category | `changes` |
| Event kind | `ddl` |
| Timing | Exact successful query end |
| Default severity | info |

Only successful `QueryFinish` rows are emitted. The implemented `query_kind`
allowlist is:

```text
Create, Alter, Drop, Rename, Truncate, Optimize, Undrop
```

The event retains query ID, initial query ID, normalized query hash, user, query
text, affected database/table arrays, duration, memory, and host. As with query
failures, the marker is placed at:

```sql
query_start_time_microseconds
    + toIntervalMillisecond(query_duration_ms)
```

DDL is intentionally informational. A schema change may be operationally
important without indicating an error. Because it has both `category: changes`
and `kind: ddl`, users can hide all change events or hide only DDL while keeping
other future change kinds visible.

### Part operation failures

| Property | Value |
| --- | --- |
| Source | `system.part_log` |
| Capability | `part_log` |
| Event category | `storage`, or `replication` for replicated fetch/download work |
| Event kind | `part_failure`, or `replication_task_failure` |
| Timing | Exact operation record time |
| Default severity | error |

Only rows with `error != 0` are emitted. Successful NewPart, merge, mutation,
download, remove, and move records remain normal Time Travel activity and do not
become events.

The event retains the operation type, database, table, part, partition, disk,
query ID, duration, error code, and exception. Replicated fetch/download
operations are classified as replication; other failed part operations remain
storage events.

`system.part_log` is node-local and only exists when the server's `part_log`
setting is configured and MergeTree part activity has created it. See the
official [`system.part_log`
documentation](https://clickhouse.com/docs/reference/system-tables/part_log).

### Background task failures

| Property | Value |
| --- | --- |
| Source | `system.background_schedule_pool_log` |
| Capability | `background_schedule_pool_log` |
| Event category | `maintenance`, or `replication` for replicated tasks |
| Event kind | `background_task_failure`, or `replication_task_failure` |
| Timing | Exact task execution record time |
| Default severity | error |

Only rows with `error != 0` are emitted. The event retains the background task
name, database, table, query ID, host, duration, error code, and exception.

Background schedule pools execute more than storage maintenance: they include
periodic distributed sends, Buffer flushes, and message-broker work. These
events therefore use the broader `maintenance` category instead of `storage`.
Tasks identifiable as replicated-table work use the `replication` category.
Users can still hide only `background_task_failure` without hiding other
maintenance events.

Retrying background work may emit many individual failures. Collection preserves
those occurrences so retry cadence remains visible; marker clustering and
deduplication belong in presentation. The per-source limit and coverage status
prevent a retry loop from being presented as complete history when truncated.

The table is optional and capability-gated. See the official
[`system.background_schedule_pool_log`
documentation](https://clickhouse.com/docs/reference/system-tables/background_schedule_pool_log).

### Operational error bursts

| Property | Value |
| --- | --- |
| Source | `system.error_log` |
| Capability | `error_log` |
| Event category | `replication`, `coordination`, `storage`, or `maintenance`, based on error type |
| Event kind | `error_burst` |
| Timing | Sampled system-log flush time |
| Default severity | error; critical for selected corruption signals |

`system.error_log` persists deltas from ClickHouse's global error counters. One
row can represent multiple occurrences during a log flush interval, so the
event retains `value` as `count`. Its `event_time` is reported as sampled
timing, not as the exact time of every represented failure. The `remote` flag is
also retained.

This source is intentionally allowlisted. The first version includes:

- storage capacity, file I/O, checksum/corruption, and unexpected-part errors;
- Keeper/ZooKeeper and replica-active errors;
- failure to connect after all connection attempts.

It intentionally excludes query memory limits, timeouts, cancellations,
admission limits, SQL mistakes, and broad `NETWORK_ERROR` rows. Query resource
failures already have better per-query evidence in `system.query_log`, while
the other excluded classes are too noisy to be useful as default timeline
markers.

Corruption, checksum mismatch, and suspiciously many unexpected parts are
classified as critical. The other allowlisted types are errors. Keeper and
ZooKeeper signals use `coordination`; replica-specific signals use
`replication`; disk, file, corruption, and part signals use `storage`;
remaining allowlisted signals use `maintenance`.
Both category and kind remain independently filterable.

Newer ClickHouse versions expose `last_error_time`, `last_error_message`, and
`last_error_query_id`, but the baseline query does not use them because those
columns are version-dependent. A later column capability can enrich the event
and improve its timestamp without making the entire source unavailable on older
servers.

### Replication state episodes

| Property | Value |
| --- | --- |
| Source | `system.metric_log.CurrentMetric_ReadonlyReplica` |
| Capability | `metric_log_replication_state` |
| Event category | `replication` |
| Event kind | `replica_readonly` |
| Timing | Sampled interval |
| Default severity | error |

`CurrentMetric_ReadonlyReplica` records how many replicated tables on a host are
currently read-only after Keeper session loss or while reinitializing. The
collector turns each contiguous positive run into one episode:

- `occurred_at` is the first positive sample in the requested history;
- `ended_at` is the first later zero sample, when recovery is observed;
- an episode without a recovery sample remains open;
- `count` is the maximum number of read-only replicated tables observed in the
  episode.

This is a host-level aggregate. `metric_log` does not retain the database/table
identity from the original `system.replicas` rows, so the event must not claim a
specific table. Per-table historical episodes require a future sampler-owned
table. A live `system.replicas` snapshot can enrich the present state, but it
cannot reconstruct the past.

### Replication failure counters

| Property | Value |
| --- | --- |
| Source | `system.metric_log` replication ProfileEvents |
| Capability | `metric_log_replication_failures` |
| Event category | `replication` |
| Event kinds | `replication_data_loss`, `replication_task_failure` |
| Timing | Sampled point |
| Default severity | critical for data loss; error for fetch/check failures |

The source reads persisted deltas for:

- `ProfileEvent_ReplicatedDataLoss`;
- `ProfileEvent_ReplicatedPartFailedFetches`;
- `ProfileEvent_ReplicatedPartChecksFailed`.

Each non-zero sample becomes a point event and retains the delta as `count`.
These events remain visible after the live `system.replication_queue` entry has
cleared. They are server-level counters, so they do not provide table identity.

Both replication sources have feature-level capability probes against
`system.columns`. The collector only queries them when the required flattened
metric columns are present; merely having `system.metric_log` is not enough.

## Capability and coverage semantics

The global `MonitoringCapabilitiesService` is authoritative. Time Travel passes
the IDs it has confirmed available to the event collector. The collector does
not optimistically query absent tables.

`Promise.allSettled` is still used as a fallback because availability can change
after probing, permissions can differ across replicas, and schemas may vary by
version. It is not a substitute for the capability check.

Every response includes `event_coverage`:

| Status | Meaning |
| --- | --- |
| `loaded` | Source was queried successfully; zero events genuinely means none were found in the range |
| `unavailable` | Capability probe reported that the source could not be used |
| `failed` | Capability was available, but this particular read failed |
| `not_requested` | Caller deliberately did not request event collection |

The default limit is 1,000 rows per event group/source, with a hard service cap
of 10,000. Query-log DDL and query-resource failures use `LIMIT BY` so a burst of
DDL cannot consume the allowance needed to reveal recurring query OOMs (or vice
versa). Coverage is marked `truncated` when a source reaches its limit. The UI
must not present truncated data as complete.

## Cluster and host behaviour

Persisted ClickHouse system logs are local to each server. Event queries use the
same `{{cluster_aware:...}}` adapter mechanism as the rest of Time Travel, which
resolves to `clusterAllReplicas()` when a cluster is configured.

Events retain the producing hostname. Exact duplicates caused by refresh overlap
or cluster reads are removed using deterministic event IDs, but distinct hosts
and distinct query executions are preserved.

Selecting one Time Travel host applies the host filter inside every event-source
query. In split view, the cluster-wide event collection should be shared instead
of running the same event queries once per rendered host.

## Retention and arrival delay

Event history cannot exceed the retention of its source table. Retention and
availability differ by deployment:

- `query_log` determines query-failure history;
- `asynchronous_metric_log` determines restart-detection history;
- `crash_log` determines exact crash history.
- `part_log`, `background_schedule_pool_log`, `error_log`, and `metric_log`
  determine their
  respective failure histories.

System log tables are buffered and flushed periodically, so an event near “now”
may arrive later than the continuous live metrics. Collection and flush
intervals are configurable. The UI should expose source coverage rather than
promising immediate or complete history.

## “What was running when this happened?”

Selecting an event should first pin its `occurred_at` timestamp. Existing
queries, merges, and mutations whose execution interval contains that timestamp
can then be highlighted immediately.

The normal Time Travel activity lists are limited and metric-ranked, so they are
not an authoritative answer. A later focused event-context query should:

1. remove the normal top-N and minimum-memory filters;
2. return every query/merge/mutation overlapping a narrow event window;
3. correlate `query_id` and `initial_query_id`;
4. use `tracehouse.processes_history` when available;
5. for restarts/crashes, include unmatched `QueryStart` records as
   **possibly interrupted**, not definitely killed.

## Planned event sources

These sources are candidates, not currently implemented. Each must have an
explicit capability, timestamp policy, severity mapping, and noise policy before
it is enabled.

| Priority | Event | Candidate source | Notes |
| --- | --- | --- | --- |
| Later | Backup/restore lifecycle | `system.backup_log` | Capability already exists; retain operation ID and status |
| Later | Async insert failures | `system.asynchronous_insert_log` | Capability already exists; correlate query and flush IDs |
| Later | Keeper connectivity | `system.zookeeper_connection_log`, selected `text_log` records | Version/deployment dependent; connection logs have known shutdown gaps |
| Later | Fatal/critical server messages | `system.text_log` | Strict levels and deduplication; never ingest all errors by default |
| Later | Per-table replica state and queue-stall episodes | TraceHouse sampler over `system.replicas` / `system.replication_queue` | Current virtual tables are not historical; persist transitions with table identity |
| Later | Keeper connection episodes | persisted connection metrics/logs | Separate coordination failures from their downstream replica impact |

### Noise rules for additional sources

- Do not display raw `error_log` or `text_log` rows indiscriminately.
- Prefer allowlisted operational meanings over generic “Error” messages.
- Aggregate bursts only when raw occurrences remain recoverable.
- Collapse the same cluster-wide incident across hosts in presentation, not at
  collection time.
- Mark inferred relationships and timing honestly.
- Avoid adding events that merely restate a metric already visible on the graph.

## Visualization contract

The first visualization is implemented as a dedicated event rail between the
detailed chart and the draggable overview. Keeping it outside the chart renderer
means the same event interaction is available in 2D, 3D, 3D-surface, and
split-host views.

Implemented interaction:

- The detailed rail displays events in the current chart or zoom range.
- Markers that would overlap are clustered visually. Opening a cluster reveals
  its individual source events; collection data is never aggregated away.
- The draggable overview contains lightweight event ticks using the same active
  filters.
- Clicking a detailed event pins its exact timestamp. Existing activity tables
  then show queries, merges, and mutations whose execution interval contains
  that timestamp.
- Clicking an overview event outside the detailed window recentres the window
  and restores the event pin after data loads.
- Quick presets show all events, errors and critical events, or critical events
  only.
- The filter popover can independently include or exclude severity, category,
  and individual event kind. This allows users to hide DDL while retaining
  failures, or isolate recurring query OOMs.
- The rail exposes loaded-source coverage and flags failed or truncated
  collection as partial.
- Selecting an event uses the existing pinned cursor in the 2D graph. The event
  rail and activity-table correlation remain available in experimental 3D
  views.
- The selected-event footer links to the top-level **Events** page, carrying the stable
  event ID, exact occurrence time, and investigation range in the URL.

The detailed and overview-range Time Travel requests already collect events.
Per-host split requests do not repeat event collection.

Source, host, and normalized-query-pattern filters remain future extensions.
The underlying event fields are already retained, so adding those controls does
not require changing collection.

## Events investigation page

The top-level **Events** page uses the same `TimelineEventsService` and
capability set as Time Travel; it does not maintain a separate event definition
or run the continuous metric queries.

A Time Travel deep link opens a one-hour range centred on the selected event.
The URL retains:

- `event_id` — deterministic source event identity;
- `event_time` — exact, sampled, or inferred occurrence timestamp;
- `event_range` — investigation range in hours;
- `from=timetravel` — enables return navigation.

The page adds a category-lane distribution over time above the searchable
master/detail view. Point events render as markers; state episodes render as
spans. Dragging the distribution narrows and recentres the investigation range.
Selecting an event exposes an **Open in Time Travel** link that opens the
timeline centred and pinned at the event timestamp.
Events carrying a `query_id` also expose the standard **Query Details** modal;
the modal fetches the authoritative query record from `system.query_log`.

The dashboard provides severity summaries, event search, severity/category/kind
filters, nearby events, per-source coverage, and the full source-specific event
record. Shareable `/events` URLs preserve the selected event and time range.
