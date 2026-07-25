# Distributed Query View: Working Proposal

This is a working proposal and basis for ongoing discussion, not user-facing documentation and not implemented yet.

Goal: give TraceHouse a dedicated way to explain what ClickHouse is *actually* doing when a query spans more than one node, not just how long each child query took. The current **Distributed Query Topology** panel in the query-details Overview tab is a useful compact Gantt, but it hides the execution hierarchy (initiator → shard leaders → parallel-replica readers), the dynamic work distribution between replicas, and the shape of data movement through the query.

This should become two related surfaces:

- **Overview summary.** Enhance the existing compact Gantt in the Select Details / Overview tab so it remains a quick summary and entry point.
- **Dedicated Distributed tab.** Add a new top-level query-details tab (working name: **Distributed** or **Topology**) for richer distributed-query inspection: timeline swim lanes, execution tree, data-flow view, parts/marks view, and eventually spatial/3D representations.

For this proposal, a **distributed query** means an observed query execution that spans more than one ClickHouse node. That includes queries over the `Distributed` engine, `cluster()` / `clusterAllReplicas()`, parallel replicas, distributed joins/subqueries, and other remote execution paths when they show up as multiple correlated executions by `initial_query_id`. It should not be detected purely by SQL syntax or table engine.

Worked example throughout: `initial_query_id = 28ee001b-63d9-4539-b2c1-6a862d0f8749`.

```sql
/* run-queries type:pk table:synthetic_data.events */
SELECT event_type, count(), avg(duration_ms)
FROM synthetic_data.events
WHERE country_code = 'MX' AND device_type = 'mobile'
GROUP BY event_type
ORDER BY count() DESC
SETTINGS use_query_cache = 0
```

## Product scope

The existing Overview panel should stay compact. It should answer "did this query fan out, where did time go, and which child execution should I inspect next?" It should not try to carry every detail.

The new Distributed tab should be the detailed analysis surface. It can use the same underlying topology model as the Overview panel, but render it through multiple lenses:

- **Timeline / Gantt swim lanes** — one lane per role, shard, replica, or node, with clear start/end timing.
- **Execution tree** — initiator, shard coordinators, replica readers, and independent fan-out children.
- **Data-flow view** — rows/bytes/partial aggregate states moving from readers to coordinators to initiator.
- **Parts / marks view** — selected parts, mark ranges, assigned marks, stolen marks, and per-replica balance.
- **Object-storage work view** — files, manifests, partitions, Parquet row groups, metadata pruning, and object-storage workers when the query runs over Iceberg/S3/Parquet rather than MergeTree parts.
- **Evidence / logs view** — the raw `query_log` / `text_log` breadcrumbs behind the inferred model, useful when parser confidence is partial.

Spatial or 3D representations are future-facing, but the data model should preserve the dimensions needed for them: time, node/shard/replica, query role, data volume, parts, marks, and coordination events. One possible 3D view is time on one axis, node/replica swim lanes on another, and parts/marks scanned on the third.

## What actually happens

`synthetic_data.events` is a **Distributed** table [[1]](#references) over `events_local`, on cluster `tracehouse` = **2 shards × 2 replicas**. This query ran with **parallel replicas** [[2]](#references) enabled. That combination produces a 3-level execution tree, which is why the current Overview panel header reads "6 child queries · 4 nodes".

```
Initiator / Coordinator   983d5697f201   69ms · 1.22 MB · 32.5M rows   (merges everything, final ORDER BY)
│
├─ Shard 1 leader          9c87394eb34d   58ms · 16.2M   (ParallelReplicasReadingCoordinator, replicas_count=2)
│   ├─ replica 0  ch-s1r1  9c87394eb34d   51ms ·  5.3M   (leaf read — same node as the leader)
│   └─ replica 1  ch-s1r2  914b0f94691c   32ms · 10.9M   (leaf read, stole 355 marks)
│
└─ Shard 2 leader          32131174193f   46ms · 16.2M   (ParallelReplicasReadingCoordinator, replicas_count=2)
    ├─ replica 0  ch-s2r1  32131174193f   35ms ·  7.6M   (leaf read — same node as the leader)
    └─ replica 1  ch-s2r2  fcefdf781d9e   34ms ·  8.7M   (leaf read, stole 44 marks)
```

Arithmetic that ties it together:

- 5.3M + 10.9M = 16.2M (shard 1); 7.6M + 8.7M = 16.3M (shard 2); both shards ≈ 32.5M total → matches the coordinator bar.
- A node id appears twice (`9c87394eb34d`, `32131174193f`) because **the shard leader is also one of its own replicas** doing reads. It shows up as two `query_log` rows: one coordinating, one reading. That is the gap between "6 child queries" and "4 nodes".

### The mechanic the current view misses

Within a shard, work is assigned dynamically, not split in fixed halves. The `DefaultCoordinator` slices the selected parts into 128-mark segments and assigns them to whichever replica requests work next; a replica that finishes early takes segments from the shared pool (task stealing) [[2]](#references). That is the `mine_marks / stolen_by_hash / stolen_rest` accounting, summarized at the end of each shard's coordination:

```
Coordination done: replica 0 - {requests: 4, marks: 699,  stolen_by_hash: 0}
                   replica 1 - {requests: 6, marks: 1462, stolen_by_hash: 355}
```

Shard 1's load was uneven: replica 1 read ~2× the marks of replica 0. Surfacing this imbalance is a goal of the redesign.

### Execution stages

Leaf reads run to `stage: WithMergeableState` — each node applies the `WHERE` (moved to PREWHERE, which reads the filter columns first to skip blocks before reading the rest) [[8]](#references), reads its assigned mark ranges, and builds a **partial GROUP BY aggregation state**. Those partial states flow up: leaf → shard leader → initiator. The initiator merges all partial states to `stage Complete` and runs the final `ORDER BY count() DESC`. The 32.5M scanned rows collapse to a 1.22 MB merged result.

### UI example: shard scheduler opportunity

Another useful fixture is the fast aggregate over `nyc_taxi.trips`:

```sql
/* run-queries type:fast table:nyc_taxi.trips */
SELECT min(pickup_date), max(pickup_date)
FROM nyc_taxi.trips
```

The Distributed tab summary for this run reads:

- Execution type: `Parallel replicas`
- Topology detail: `Shard mapping`
- Child queries: `6`
- Nodes: `4`
- Shards: `2`

The timeline shows the important shape:

```
Coordinator       56ms · 1.16 MB · 82.6M rows
├─ Shard 1 leader 46ms · 1.72 MB · 37.4M rows
│  ├─ reader       30ms · 4.67 MB · 18.7M rows
│  └─ reader       34ms · 4.14 MB · 18.7M rows
└─ Shard 2 leader 40ms · 1.21 MB · 45.2M rows
   ├─ reader       30ms · 3.74 MB · 30.9M rows
   └─ reader       26ms · 3.30 MB · 14.3M rows
```

This is a good example of the product value: the UI can tell the user that the shard scheduler detected an opportunity to optimize the query by turning a single shard-local read into a parallel read across both replicas of that shard. The shard coordinator is not merely forwarding the query to a preferred replica; it is running `ParallelReplicasReadingCoordinator`, building a work queue from selected MergeTree parts and marks, and serving non-overlapping mark ranges to replica readers as they ask for work.

The trace evidence is explicit:

```text
Parallel replicas query in shard scope: shard_num=1 cluster=tracehouse
Creating parallel replicas coordinator with replicas_count=2
Addresses to use: ch-s1r1:9000, ch-s1r2:9000
Replica number 0 assigned to address ch-s1r1:9000
Replica number 1 assigned to address ch-s1r2:9000
Using snapshot from replica num 1
Initial request: replica 1, mode Default, 34 parts: [...]
Initial request: replica 0, mode Default, 34 parts: [...]
```

Both replicas announce the same candidate part inventory for the shard. The coordinator then responds with disjoint ranges in `part_name[(mark_begin, mark_end)]` form. The clearest line pair in this run is:

```text
replica 1: 1782126000_0_99_3[(0, 70)]
replica 0: 1782126000_0_99_3[(70, 569)]
```

That single physical part is split by mark range, so the two replicas collaborate on one shard without double-counting rows. The assignment is dynamic and chunked around the requested mark size:

```text
Handling request from replica 1, minimal marks size is 1232, request count 1
Going to respond to replica 1 with 3 parts: [...] Finish: false; mine_marks=1232, stolen_by_hash=0, stolen_rest=0

Handling request from replica 0, minimal marks size is 1232, request count 1
Going to respond to replica 0 with 3 parts: [...] Finish: false; mine_marks=1232, stolen_by_hash=0, stolen_rest=0
```

Later requests drain the remaining work, including partial ranges from already-started parts and small tail fragments:

```text
Going to respond to replica 1 with 22 parts: [...] mine_marks=1176, stolen_by_hash=56, stolen_rest=0
Going to respond to replica 0 with 8 parts: [...] mine_marks=1232, stolen_by_hash=0, stolen_rest=0
Going to respond to replica 0 with 2 parts: [...] mine_marks=8, stolen_by_hash=0, stolen_rest=0
```

The final coordinator accounting explains what the reader bars mean:

```text
Coordination done: Statistics:
replica 0 - {requests: 5 marks: 2472 assigned_to_me: 2472 stolen_by_hash: 0 stolen_unassigned: 0}
replica 1 - {requests: 4 marks: 2464 assigned_to_me: 2408 stolen_by_hash: 56 stolen_unassigned: 0}
```

The same shape is visible in `system.query_log.ProfileEvents`, and those counters are useful when `text_log` evidence is missing or too expensive to read. In this run the root coordinator has no `ParallelReplicas*` profile events; it only sees the distributed query as a normal fan-out/fan-in. The shard coordinator and shard reader rows carry different event signatures:

| Role | Example ProfileEvents | What it means |
|---|---:|---|
| Root coordinator | no `ParallelReplicas*` events | The top-level query merged shard results, but did not coordinate mark-range handouts itself. |
| Shard coordinator | `ParallelReplicasReadAssignedMarks = 4880`<br>`ParallelReplicasHandleRequestMicroseconds = 1.4ms`<br>`ParallelReplicasHandleAnnouncementMicroseconds = 509us`<br>`ParallelReplicasStealingByHashMicroseconds = 93us`<br>`ParallelReplicasProcessingPartsMicroseconds = 83us`<br>`ParallelReplicasReadAssignedForStealingMarks = 56`<br>`ParallelReplicasCollectingOwnedSegmentsMicroseconds = 40us`<br>`ParallelReplicasNumRequests = 9`<br>`ParallelReplicasUsedCount = 2`<br>`ParallelReplicasAvailableCount = 2`<br>`ParallelReplicasQueryCount = 1` | This row hosted the parallel-replicas scheduler for one shard: two replicas were available and used, nine reader requests were handled, 4,880 marks were assigned, and 56 marks came through the stealing path. |
| Shard reader | `ParallelReplicasReadRequestMicroseconds = 6.4ms`<br>`ParallelReplicasReadMarks = 2464`<br>`ParallelReplicasAnnouncementMicroseconds = 71us` | This row participated as a reader: it announced its available parts/segments, requested assigned work from the shard coordinator, and read 2,464 marks. |

This gives the inference layer a cheap Tier 1.5 detector:

- no `ParallelReplicas*` events on the initial row means "root coordinator only";
- `ParallelReplicasHandle*`, `ParallelReplicasReadAssigned*`, `ParallelReplicasUsedCount`, and `ParallelReplicasNumRequests` identify a shard coordinator;
- `ParallelReplicasReadRequestMicroseconds`, `ParallelReplicasReadMarks`, and `ParallelReplicasAnnouncementMicroseconds` identify a shard reader.

At a high level, these events describe the control plane for parallel reading:

- `ParallelReplicasUsedCount`, `ParallelReplicasAvailableCount`, and `ParallelReplicasQueryCount` describe the scheduler shape: how many replicas could participate, how many actually participated, and whether this query used the parallel-replicas path.
- `ParallelReplicasHandleAnnouncementMicroseconds` and `ParallelReplicasAnnouncementMicroseconds` measure replica discovery. Readers announce the parts/segments they can read; the shard coordinator receives those announcements and builds the shared work view.
- `ParallelReplicasProcessingPartsMicroseconds` and `ParallelReplicasCollectingOwnedSegmentsMicroseconds` measure coordinator-side preparation: turning the selected parts into mark-range work units, including the ownership/hash-locality view used to prefer cached/local work where possible.
- `ParallelReplicasNumRequests`, `ParallelReplicasHandleRequestMicroseconds`, and `ParallelReplicasReadRequestMicroseconds` measure the work-pull loop. Readers ask for more work; the coordinator chooses mark ranges and responds.
- `ParallelReplicasReadAssignedMarks` is coordinator-side accounting for the total marks handed out. `ParallelReplicasReadMarks` is reader-side accounting for the marks actually read by that reader.
- `ParallelReplicasReadAssignedForStealingMarks` and `ParallelReplicasStealingByHashMicroseconds` describe rebalancing. When a reader exhausts its preferred/owned work or another reader is slower, the coordinator can assign work that was originally hash-local to another replica.

The UI should make this discoverable from the Distributed tab:

- The shard coordinator row should explain why two readers exist inside the same shard.
- Hovering or clicking a reader should show assigned marks, stolen marks, requests, rows, bytes, and host.
- The parts/marks view should show that work is divided by mark ranges such as `[(0, 70)]` and `[(70, 569)]`, not by hash partitions, rows, or whole replicas.
- The evidence view should link the timeline rows back to `ParallelReplicasReadingCoordinator`, `ReadFromParallelRemoteReplicasStep`, `DefaultCoordinator`, and the final `Coordination done` stats.

For aggregate queries like `min/max`, each reader returns a partial aggregate state for its assigned ranges. The shard coordinator merges those states, then the root coordinator merges the shard-level states. That is why the timeline can show tens of millions of rows read at the leaves while only small aggregate-state payloads move upward.

## Taxonomy of distributed query shapes

The worked example above is **one cell** of a larger matrix. The topology a query produces depends on two independent axes:

1. **How many shards** the data is spread across (the *scatter* width).
2. **Whether parallel replicas is on** — i.e. whether multiple replicas of a single shard collaborate *within* one query, versus one replica per shard doing all the work.

Replicas serve two distinct purposes: **availability** (any replica can answer, used for failover and for spreading *different* queries) [[1]](#references) versus **intra-query parallelism** (replicas of the same shard split the work of *one* query) [[2]](#references). The second only occurs when parallel replicas is enabled.

| Topology | Parallel replicas OFF | Parallel replicas ON |
|---|---|---|
| **Single node** (no Distributed table) | One local query. Overview can show the coordinator bar only; the Distributed tab should hide, disable, or explain that there is no multi-node execution. | n/a — no replicas to spread across. |
| **1 shard, R replicas** | Initiator picks one replica (per `load_balancing`); the other R-1 do no work for this query. Single-query latency unchanged by adding replicas. [[1]](#references) | Coordinator distributes one query's granules across all R replicas (hash-locality + task-stealing), then merges. Topology used by ClickHouse Cloud. [[2]](#references) |
| **S shards, 1 replica each** | Initiator sends one remote query per shard, each reads locally to `WithMergeableState`, initiator merges. | No effect within a shard — one replica per shard means nothing to spread. Same shape as OFF. |
| **S shards, R replicas** | Initiator picks one replica **per shard**; S parallel leaf queries; merge. | The worked example: initiator → per-shard coordinator → R replica readers each. |

### Single-node queries

A query that touches only a local table (a plain `MergeTree`, or a `Distributed`/`cluster` reference that resolves to just the local node) has no distribution. There is one execution, `is_initial_query = 1` [[7]](#references), and zero child queries — nothing correlates by `initial_query_id` because nothing was fanned out. Parallelism is intra-node only (thread-level across mark ranges within the one process), which `query_log` summarises but does not break out per child.

UI behaviour: there is no distributed topology to draw. The Overview panel should detect an empty sub-query set and collapse to a single coordinator bar, or hide the topology section and show node-local thread/stream stats instead. The Distributed tab should not imply distribution; it can be hidden, disabled with a reason, or repurposed as a local execution summary if we decide that is useful.

### Single shard, multiple replicas

With a single shard there is no shard fan-out; only intra-shard replica coordination remains. This is the topology used by ClickHouse Cloud (see below).

- **Parallel replicas ON.** There is no separate shard-leader layer — the initiator hosts the `ParallelReplicasReadingCoordinator` and treats all replicas (possibly including itself) as readers [[2]](#references). The unit of work is granules / mark-range segments, not whole replicas: the coordinator selects parts and granules, then assigns segments to whichever replica requests work next, using hash-based locality (a replica tends to re-read parts it already has cached) with task stealing when a replica falls behind — the `mine_marks / stolen_by_hash` accounting in the worked example [[2]](#references). Each replica reads its assigned granules, builds a partial aggregate, and sends it to the initiator to merge [[3]](#references). Topology: `coordinator → R replica reader bars`, one level shallower than the multi-shard tree. Adding replicas increases the parallelism applied to a single query.

- **Parallel replicas OFF.** When the feature is off, the initiator resolves the shard to one replica (chosen by `load_balancing`) and forwards the whole query there; the other R-1 replicas do no work for this query [[1]](#references). In this mode replicas provide availability and concurrency headroom only, and do not reduce single-query latency.

#### ClickHouse Cloud topology

ClickHouse Cloud uses one logical shard with data in object storage (S3/GCS) and the SharedMergeTree engine; replicas are stateless compute nodes that read the same data via shared storage and a local SSD cache, coordinating metadata through ClickHouse Keeper [[4]](#references). Because there is no data locality to preserve (every node can read every part), one query's granules are distributed across the replicas via parallel replicas, and replica count can be changed without resharding or data movement [[4]](#references). This is the single-shard, R-replica cell of the matrix, and the mechanism by which a single query scales across many replicas and cores [[5]](#references). It uses the same `ParallelReplicasReadingCoordinator` and granule-assignment path described above.

> Unverified on our cluster: whether `enable_parallel_replicas` is enabled by default, and whether `synthetic_data.events_local` is `SharedMergeTree` or classic `ReplicatedMergeTree`. The worked example is a parallel-replicas run, so the feature is at least enabled for that path. Confirm the default before stating it in user-facing copy.
>
> How to verify: the 4-node `tracehouse` cluster (ch-s1r1 … ch-s2r2) is the `infra/demo/docker-compose.yml` stack (`clickhouse:26.3`). With it running, query any node:
> - `SELECT value, changed FROM system.settings WHERE name = 'enable_parallel_replicas'`
> - `SELECT engine FROM system.tables WHERE database = 'synthetic_data' AND name = 'events_local'` (or `SHOW CREATE TABLE synthetic_data.events_local`)
>
> Without starting the stack, the same answers are in-repo: the table DDL in the demo sampling/init SQL gives the engine, and the profile config XML mounted into the demo nodes gives the setting default.

Relevant settings (confirm names/availability on our build before relying on them):

- `enable_parallel_replicas` — master switch; values `0` disabled, `1` enabled, `2` force [[2]](#references).
- `max_parallel_replicas` — number of replicas one query is spread across.
- `parallel_replicas_for_non_replicated_merge_tree` — allow it on non-replicated tables.
- `load_balancing` — which single replica is chosen when parallel replicas is off [[1]](#references).

### `cluster()` and `clusterAllReplicas()` table functions

These are an ad-hoc alternative to a `Distributed` table: the same scatter/gather engine, but the node set is named inline rather than defined by a table. No `Distributed` table needs to exist [[6]](#references).

- **`cluster('name', db.table)`** — queries one replica per shard, like a `Distributed` table without parallel replicas. For an S-shard cluster, S child queries [[6]](#references).
- **`clusterAllReplicas('name', db.table)`** — queries every replica of every shard. For 2 shards × 2 replicas, 4 child queries, one per physical node [[6]](#references).

The difference from the cases above: each addressed node runs the query independently against its own local table and the results are concatenated (`UNION ALL` semantics). There is no parallel-replicas coordinator, no mark-range work-stealing, and no deduplication. This is why `clusterAllReplicas` is used for per-node system tables — each node has its own `system.query_log`, `system.parts`, `system.text_log`, and the union of all of them is required. Pointing `clusterAllReplicas` at a regular data table returns each row R times (once per replica), because replicas hold identical data.

Topology shape: `initiator → one independent child query per addressed node → concatenate`. It correlates by `initial_query_id` the same way, so the UI can render it as a flat fan-out with no shard-leader layer and no stealing, distinguishable from a parallel-replicas query by the absence of the coordination breadcrumbs.

> Note: TraceHouse reads its own data this way. The `{{cluster_aware:...}}` wrapper in `packages/core/src/queries` expands to a `clusterAllReplicas` fan-out (`packages/core/src/services/cluster-service.ts:194`) so that observability queries gather `system.*` rows from every node.

#### Forced all-replicas marker

The topology model should carry an explicit marker for `clusterAllReplicas()` queries, for example `fanout_mode = all_replicas` or `routing_marker = forced_all_nodes`.

This marker means the query was intentionally routed to **every configured replica in every shard**. It should not be interpreted as:

- one load-balanced replica per shard, which is the usual `cluster()` / plain `Distributed` shape when parallel replicas are off;
- a parallel-replicas scheduler splitting one shard's marks across replicas;
- an accidental duplicate set of child rows.

In UI terms, this should show as something like `Forced all replicas` or `All nodes targeted`, especially for TraceHouse self-monitoring queries over `system.*` tables. The expected participant count is the configured cluster width, not the number of remote `is_initial_query = 0` rows:

```
expected_participants = shard_count * replica_count
remote_children = expected_participants - local_participants_on_initiator
```

For a 2 shard × 2 replica cluster where the initiator is also a cluster replica, the expected shape is therefore **4 node-local participants**, often visible as **1 compound coordinator/local-reader row + 3 remote child rows**.

#### Co-located initiator and local reader

When the node that receives a `clusterAllReplicas()` query is also one of the addressed replicas, that node plays two roles in the same initial query:

- **Initiator / coordinator**: receives the client request, opens remote connections, waits for remote results, concatenates or merges, and returns the response.
- **Local reader**: scans the local table for the coordinator node's own replica, for example `system.metric_log`.

This local reader usually does not appear as a separate child row in `system.query_log`, because it is executed inside the initial query rather than over a remote `TCPHandler`. The evidence is in the coordinator's own `text_log`: the initial handler contains local `SelectExecutor` lines for the target table, followed by `Connection (host:9000)` lines for the remote children. For a 2 shard × 2 replica cluster, the UI may therefore show **3 remote children + 1 coordinator row**, while the logical `clusterAllReplicas()` read still touched **4 physical replicas**.

The current compact Gantt is misleading for this case because the coordinator bar is not pure coordination. Its duration, rows, bytes, memory, and profile events can include:

- dispatch and fan-in overhead;
- local scan/filter/aggregate work from the coordinator's own replica;
- final merge/concatenate work over remote and local results.

The topology model should represent this as a **compound initiator row** with an optional nested `local_read` segment. In UI terms, the coordinator lane should be able to show stacked or annotated components such as `coordinating`, `local read`, and `final merge`, instead of treating the whole blue bar as coordination. Metrics should preserve separate fields where evidence allows it:

- `coordinator_total_rows` / `coordinator_total_bytes`: the initial query row as ClickHouse reports it today.
- `local_read_rows` / `local_read_bytes` / `local_selected_parts` / `local_selected_marks`: work done by the coordinator node's local table read.
- `remote_child_rows` / `remote_child_bytes`: sum of remote child query rows.
- `coordination_overhead_ms`: elapsed time not explained by visible local or remote reader spans, with confidence noted.

This distinction matters most for TraceHouse's own `system.*` queries. A `clusterAllReplicas()` query over `system.metric_log` is intentionally collecting per-node local metrics; the local contribution is real data, not coordinator overhead. The Overview panel should make that visible enough that users do not read "Coordinator · 524 rows" as "the coordinator moved 524 rows without doing a local read."

### Secondary axis: query shape

Orthogonal to shard/replica layout, the **query shape** can add distribution stages the Distributed tab should eventually recognize:

- **Plain scan / aggregate** (the worked example) — single scatter, single gather.
- **`GLOBAL IN` / `GLOBAL JOIN`** — the initiator runs the subquery once, stores the result in a temporary table, and sends it to every shard before they read. Adds a build + broadcast phase ahead of the scatter [[9]](#references).
- **Distributed `JOIN`** (non-global) — the right-hand table is read per shard; can fan out further or pull data to the initiator depending on settings.
- **Two-level / external aggregation** — large `GROUP BY` switches to two-level merge or spills to disk (`Adjusting memory limit before external aggregation` appears in the logs); affects the gather cost, not the scatter shape.

These map onto the Distributed tab as extra phases or sub-trees rather than new top-level cells, and are out of scope for the first iteration. They are listed here so the data model (the typed `ShardCoordination` / topology shape) is designed to not preclude them.

#### Nested distributed subqueries

Some queries contain a distributed table function inside a remote child query. A representative TraceHouse shape is:

```sql
SELECT ...
FROM system.query_log AS q
INNER JOIN (
  SELECT ...
  FROM clusterAllReplicas('all-clusters', system.query_log)
  ...
) AS top_patterns
ON q.normalized_query_hash = top_patterns.normalized_query_hash
```

In `system.query_log`, this can look like one root query plus several child query shapes under the same `initial_query_id`: an outer child that scans a local `system.query_log` table and an inner `clusterAllReplicas()` branch that fans out again. The inner branch can multiply quickly: for example, 8 outer participants × 8 all-replica inner participants can produce 64 remote child rows. That is not one flat root fan-out and it is not necessarily a parallel-replicas scheduler.

This is where the inference needs a second pass over `system.text_log`. The first pass from `query_log` can say:

- these are the observed execution rows;
- these rows share the initial query id;
- these rows have different `normalized_query_hash` values, so they are different query shapes / branches;
- this non-initial row's SQL contains a nested `cluster()` or `clusterAllReplicas()` call.

The text-log pass can then add stronger role evidence for the outer child:

```text
Connection (chi-dev-cluster-dev-1-0:9000) ... Sent data for 2 scalars
Connection (chi-dev-cluster-dev-1-1:9000) ... Sent data for 2 scalars
```

Those lines are direct evidence that the child query opened remote connections and acted as a nested remote coordinator. They are **not**, by themselves, a direct edge to a specific receiving `query_id`. The receiving side usually appears as a separate `query_log` row with the same root `initial_query_id`, a matching host, start time, and query shape, but not an explicit `parent_query_id = <nested coordinator query_id>` field.

The model should therefore represent two levels of certainty:

- **Observed fact:** query `A` opened remote connections to hosts `H1..Hn`, sourced from `text_log`.
- **Inferred edge:** query `B` on host `H1` likely belongs to query `A`'s nested fan-out because host, timing, initial query id, and query shape line up.

The inferred edge should carry confidence and provenance, for example:

```ts
{
  fromQueryId: 'outer-child-query-id',
  toQueryId: 'inner-child-query-id',
  confidence: 'medium',
  evidence: [
    { source: 'text_log', message: 'outer child opened connection to chi-dev-cluster-dev-1-0:9000' },
    { source: 'query_log', message: 'receiver ran on chi-dev-cluster-dev-1-0-0 under the same initial_query_id' },
    { source: 'query_log', message: 'receiver start time followed the sender connection event' },
    { source: 'query_log', message: 'receiver query shape matched the nested clusterAllReplicas branch' }
  ]
}
```

The UI should label this as **inferred nested fan-out** or **nested remote coordinator**, not as a hard parent-child relationship unless a direct receiving-side breadcrumb is found. This prevents the timeline from losing useful hierarchy while still being honest about what ClickHouse actually recorded.

### Tertiary axis: storage and work unit

Distributed execution is not only a question of shards and replicas. The storage layer changes the unit of work and the evidence we should look for:

| Storage / execution fabric | Work unit | Typical evidence | UI implication |
|---|---|---|---|
| MergeTree / ReplicatedMergeTree | parts, marks, granules | `SelectedParts`, `SelectedMarks`, `ReadFromMergeTree`, primary-key pruning logs | parts/marks view |
| SharedMergeTree / shared object storage | shared parts / marks, cache locality | engine metadata, shared-storage settings, parallel-replica counters | replica readers over shared data, less fixed data ownership |
| Iceberg / Parquet / S3 | manifests, partitions, files, row groups, object reads | table engines/storages (`Iceberg`, `IcebergS3`, `S3`, `Parquet`), object-storage settings, processor phases, metadata-cache/bloom/pruning counters where available | file/manifest/row-group work view instead of marks-only view |
| Swarm/object-storage cluster | object files assigned to stateless workers | `object_storage_cluster` setting, swarm/worker child executions, object-storage table engines | initiator -> object-storage workers -> merge |
| Hybrid table | hot MergeTree segment + cold Iceberg/object-storage segment | `Hybrid` engine/storage metadata, child executions against different segments, segment-pruning evidence | show hot/cold segment branches and skipped segments |

Altinity Project Antalya is a concrete example of why this matters: its swarm clusters are stateless ClickHouse servers used for parallel queries over S3 files or Iceberg tables; the initiator dispatches subqueries to swarm nodes for individual Parquet files, then merges streamed results. Hybrid tables can query hot MergeTree data and cold Iceberg data in one logical query, with segment pruning based on query predicates. This should be modeled through capabilities and storage/work-unit detectors, not as a hard-coded vendor path.

## Breadcrumbs: what each log line tells you

All lines below carry `initial_query_id = 28ee001b…`, so they are correlatable across nodes [[7]](#references). In execution order:

| Step | Logger / message | What it tells you |
|---|---|---|
| Routing | `executeQueryWithParallelReplicas … shard scope: shard_num=N cluster=tracehouse` | which shard this leader owns |
| Replica set | `ReadFromParallelRemoteReplicasStep … Replica number K assigned to address ch-sXrY:9000` | replica index → physical host |
| Coordinator init | `ParallelReplicasReadingCoordinator … replicas_count=2` | parallel reading is on, how many replicas |
| Work denominator | `DefaultCoordinator … Total rows to read: 16238896` | total work in the shard |
| Part inventory | `Initial request: replica N … 29 parts: […]` | what parts each replica sees |
| Dynamic assignment | `Going to respond to replica N with K parts … mine_marks=…, stolen_by_hash=…` | each work handout + stealing, over time |
| Per-replica result | `Coordination done: Statistics: replica N - {requests, marks, assigned_to_me, stolen_by_hash}` | final per-replica load balance |
| Leaf read | `executeQuery … Read N rows, M MiB in T sec` | actual rows/bytes/time per node |
| Filtering | `Moved 2 conditions to PREWHERE` / `PK index dropped 0/2161 granules` | how the WHERE got pushed down |
| Aggregation | `AggregatingTransform … Aggregated X to Y rows` + `Merging aggregated data` | partial GROUP BY state built |
| Stage | `(stage: WithMergeableState)` vs initiator `to stage Complete` | leaves produce partial states; initiator merges + sorts |

## Two data tiers

| Tier | Source | Carries | Status |
|---|---|---|---|
| Tier 1 | `system.query_log` [[7]](#references) | per-node duration, memory, rows, bytes | **already used** by the Overview panel |
| Tier 1.5 | `system.query_log.ProfileEvents`, `system.processors_profile_log`, `system.clusters` | role hints, scan/mark counters, processor phases, static host→shard/replica mapping | **available, not wired into topology** |
| Tier 2 | `system.text_log` (Trace level) | runtime shard scope, replica→host map, remote connection breadcrumbs, total rows, per-handout assignment, stolen marks | **partially used for execution events; should enrich topology** |

The handout-level stolen-marks story exists **only in Tier 2**. Shard grouping and role classification may be possible before Tier 2 by combining:

- `system.clusters`: static `host_name → shard_num / replica_num` for configured clusters.
- `system.query_log.ProfileEvents`: parent/leader/reader hints.
- `system.processors_profile_log`: processor-level phase names such as `Remote`, `ReadFromMergeTree`, `MergingAggregated`, `BlocksMarshalling`, and descriptions like `MergeTreeSelect(pool: ReadPoolParallelReplicas, algorithm: Thread)`.

The plumbing for `system.text_log` already exists in the codebase (the tracer tab uses it), it is just not wired into topology.

Tier 2 should be treated as an enrichment layer rather than a replacement for `query_log`. It can promote a row from "remote child" to "nested remote coordinator" when that row's own log shows outgoing `Connection (...)` fan-out, and it can add inferred edges to receiving child rows when the host, time window, initial query id, and query shape match. Those inferred edges must keep their evidence and confidence; missing or ambiguous `text_log` should degrade to a flat branch view instead of inventing a tree.

Observed on local ClickHouse 26.3:

- Plain distributed read: initiator row has `DistributedConnection*` / `SuspendSendingQueryToShard`; remote child rows have `SelectedMarks` / `SelectedParts` and `MergeTreeSelect(pool: ReadPool, algorithm: Thread)` in `processors_profile_log`.
- Parallel-replicas read: shard leader rows have `ParallelReplicasHandleRequestMicroseconds`, `ParallelReplicasHandleAnnouncementMicroseconds`, `ParallelReplicasReadAssignedMarks`, `ParallelReplicasReadAssignedForStealingMarks`, `ParallelReplicasNumRequests`, `ParallelReplicasUsedCount`, `ParallelReplicasAvailableCount`, and `ParallelReplicasQueryCount`; replica reader rows have `SelectedMarks`, `SelectedParts`, `ParallelReplicasAnnouncementMicroseconds`, `ParallelReplicasReadRequestMicroseconds`, `ParallelReplicasReadMarks`, and `MergeTreeSelect(pool: ReadPoolParallelReplicas, algorithm: Thread)`.
- The global `system.events` catalog did not list these `ParallelReplicas*` event names on the tested build, even though the per-query `ProfileEvents` maps contained them. Detection should inspect map keys on the query rows, not only `system.events`.
- `processors_profile_log` is useful for phase classification, but not sufficient for shard grouping by itself.

### Current data flow (for reference)

```
packages/core/src/queries/query-queries.ts   SUB_QUERIES (system.query_log, by initial_query_id, is_initial_query=0)
  → packages/core/src/services/query-analyzer.ts   getSubQueries() → SubQueryInfo[]
    → frontend/.../modal/hooks/useQueryTopology.ts   { coordinator, subQueries }
      → frontend/.../modal/shared/DistributedQueryTopology.tsx   flat Gantt (TopologyBar)
```

## Proposed representation

Evolve the existing Gantt, but do not make it the only destination. The Overview panel should be a compact summary; the new Distributed tab should carry the detailed representations.

### Overview summary

Keep the current Gantt shape, but make it more informative:

- Preserve the coordinator bar on top.
- Show child query bars as today when only Tier 1 data exists.
- When Tier 2 coordination data exists, group bars by shard and role: shard leader / parallel-replica reader / independent child.
- For `clusterAllReplicas()` and other co-located local reads, annotate the coordinator bar as compound when evidence shows the initial handler also scanned the local target table.
- Add lightweight labels for role, node, rows, bytes, and duration.
- Link to the full Distributed tab for the same query.

This means true shard grouping should be evidence-backed. With only basic `system.query_log` fields, the Overview panel can still show a useful fan-out timeline, but it should not pretend to know shard boundaries. With Tier 1.5 data, `system.clusters` plus `ProfileEvents` may be enough to group configured-cluster queries by shard before falling back to Tier 2 `text_log`.

### Distributed tab

The dedicated tab can expose multiple coordinated views over the same inferred model:

#### Timeline / swim lanes

A richer Gantt with lanes grouped by query role, shard, replica, or physical node. It should make the execution order clear: initiator start, remote child execution, shard coordination, leaf reads, fan-in, final merge/sort.

TODO: keep the compact timeline responsible for the grouped timing view, but make the detailed execution-flow view strictly chronological. For parallel replicas, do not imply parent/child hierarchy through row adjacency or indentation when shard groups interleave by time. Instead, show explicit shard/role/parent labels on each event, e.g. "Shard 2 reader started · via f535...", while the timeline continues to show the shard coordinator → reader relationship graphically.

The clearest near-term visual model is a **2D fan-out / fan-in swim lane**, not a 3D scene and not a pure metric table. The execution story is:

```
client → coordinator → remote readers / shard coordinators → coordinator merge/finalize → client
```

The UI should make that movement visible:

```
time →

Client          request ─────────────────────────────────────────────── result

Coordinator     accept ━ dispatch ━━━━━━━━━━━━━━━━━━━ merge/finalize ━ done
                       │       │           ▲        ▲        ▲
                       │       │           │        │        │
Shard 1 / r1           └────── read ───────┘
Shard 1 / r2           └──────── read ─────┘

Shard 2 / r1           └──────────── read ──────────┘
Shard 2 / r2           └────── read ─┘
```

Suggested encoding:

- **Lane** = actor: client, coordinator, shard coordinator, replica reader, local folded reader, or independent `clusterAllReplicas()` child.
- **Horizontal position/length** = observed start time and duration.
- **Outgoing connector** = coordinator dispatching work to a child.
- **Incoming connector** = child feeding rows/bytes/aggregate states back to the coordinator.
- **Bar height/fill/intensity** = selected metric (`rows`, `bytes`, `duration`, `memory`, `parts`, `marks`).
- **Coordinator overlay** = merge/finalize span and, when applicable, a nested local-read segment for co-located `clusterAllReplicas()` reads.
- **Shard grouping** = lanes nested under shard headers; inactive replicas can be ghosted when they were available but not chosen.

This view answers "how did the work move?" The resource matrix answers "how much work did each node do?" They should be coordinated but not collapsed into one visualization. A reasonable first cut is:

- Top: fan-out / fan-in swim lane for the selected branch/query shape.
- Middle or side: compact node × metric matrix for skew.
- Bottom: evidence and detailed chronological events.

For `UNION ALL` and other multi-branch plans, the swim lane and matrix must be scoped by **query shape**. A `UNION ALL` query may contain several `clusterAllReplicas()` branches over different tables such as `system.metric_log`, `system.query_log`, `system.metrics`, and `system.server_settings`. Those branches are not one comparable cohort. The model should group child executions by `normalized_query_hash` (or a stronger branch identifier when available), label each group from a representative `query_preview` (`system.metric_log`, `system.query_log`, etc.), and render one branch at a time. An "All branches" overlay can exist later, but it should be explicitly marked as mixed workload and should not drive skew conclusions.

Branch chips should therefore avoid meaningless labels like `Shape 1`. Prefer:

```
system.metric_log · 8
system.query_log  · 8
system.metrics    · 1
```

with the full SQL and `normalized_query_hash` available in tooltip/evidence. This is especially important for TraceHouse self-monitoring queries, where a single top-level query can union small local settings reads with large all-node log scans.

#### Execution tree

A tree such as:

`initiator → shard coordinator → replica readers`

or, for `clusterAllReplicas()`:

`initiator → independent child query per addressed node`

When the initiator is also an addressed node, the tree should show the local participant explicitly:

`initiator → local reader on initiator node + remote child query per other addressed node`

The tree should distinguish coordinated parallel-replica reads from independent fan-out children.

#### Data-flow strip

A small Sankey/funnel: `2 shards → 4 replicas → 32.5M rows read → partial aggregates → 1.22 MB merged → final sort`. Arrow widths weighted by rows/bytes. Answers "what data moved where" and makes the fan-in (32.5M scanned collapsing to a tiny result) obvious.

#### Parts / marks view

For parallel replicas, show the work denominator and per-replica contribution: selected parts, total rows/marks, assigned marks, stolen marks, and requests. Clicking a shard leader opens a per-replica mini-timeline of the work handouts: each `Going to respond to replica N` as a segment colored by `mine` vs `stolen`, ending in the `Coordination done` summary (replica 0: 699 marks / replica 1: 1462 marks · 355 stolen). This is the literal "what is each node doing at each step" view, and where load imbalance shows up.

#### Evidence / confidence

Show the raw breadcrumbs used to infer the topology, or at least expose parser confidence and missing data. This matters because `text_log` can be absent, sampled by log level, expired by TTL, or incomplete for failed/running queries.

### Write-side distributed queries

The Distributed tab should cover write paths too, not only SELECT scatter/gather. Inserts have a different shape:

- Client `INSERT INTO distributed_table ...`
- Forwarding from the Distributed table to shard-local tables (`*_local` in the demo schema), which may be synchronous or background/asynchronous depending on settings and table state.
- Optional async insert buffering and flush (`query_kind = 'AsyncInsertFlush'`) when `async_insert = 1`.
- Optional materialized-view cascades or table-engine side effects.

Observed example in `replacing_test.product_prices`:

| Query kind | Table | Breadcrumbs |
|---|---|---|
| `Insert` | `replacing_test.product_prices` | client-visible row; `is_initial_query = 1`; `ProfileEvents['InsertedRows']` / `InsertedBytes`; target is the Distributed table |
| `Insert` | `replacing_test.product_prices_local` | background/local write; `is_initial_query = 0`; `ProfileEvents['AsyncInsertQuery']` / `AsyncInsertBytes`; target is the shard-local table |
| `AsyncInsertFlush` | `replacing_test.product_prices_local` | actual async-buffer flush; `ProfileEvents['AsyncInsertRows']`, `InsertedRows`, `InsertedBytes`; can be linked from `system.asynchronous_insert_log.flush_query_id` |

Important caveat: the background local insert and async flush are not guaranteed to share the original `initial_query_id`. In the observed case, the local worker insert had a different `initial_query_id`, and the async flush used its own query id as `initial_query_id`. So write-side topology cannot rely only on `initial_query_id`.

For async inserts, the strongest expected breadcrumb is `system.asynchronous_insert_log`: it records the client insert `query_id` and the server-side `flush_query_id`, plus table/database and row/byte counters. That gives us a direct edge:

```text
client Insert query_id
  -> asynchronous_insert_log.flush_query_id
  -> query_log row where query_kind = AsyncInsertFlush
```

The `system.text_log` message `Processing batch insert for the async inserts '<query_id>'` is still useful, but it should be treated as supporting evidence or a fallback parser rather than the primary join. It can annotate the execution flow with an "async batch picked up" event and help when `system.asynchronous_insert_log` is unavailable, expired, or not granted. If both sources are present and disagree, the UI should show the conflict as low-confidence evidence instead of silently merging unrelated rows.

This means the data model should support more than one correlation strategy:

- **Strong correlation:** same `initial_query_id`, used for read fan-out and most distributed SELECT topology.
- **Async insert log correlation:** `system.asynchronous_insert_log.query_id` → `flush_query_id`, used to connect the client-visible insert to the later `AsyncInsertFlush` query.
- **Text-log async breadcrumb:** parse `Processing batch insert for the async inserts '<query_id>'` and attach it as evidence to the flush path, ideally cross-checked against `asynchronous_insert_log`.
- **Derived cascade correlation:** nearby write-side rows linked by Distributed→local table relationship, `AsyncInsert*` events, row/byte counts, and timing, used only when stronger ids are missing.
- **Uncertain correlation:** rows that look related but should be shown with lower confidence or left in Evidence until proven.

### Execution identity and resource accounting

The topology should make the execution user clear at each level. The initial query usually runs as the user who submitted the query, for example the HTTP/native client user visible on the root `system.query_log` row. Remote child queries may run under a different user depending on how inter-server communication, `remote_servers`, secrets, credentials, or cloud routing are configured. In simple demos this may be the same logical user; in production it can be a service user such as `default`, an internal distributed-query user, or another configured remote principal.

This matters for two reasons:

- **Security and attribution:** users need to know whether a remote child row is executing as the client user, a service account, or an inherited/default account.
- **Resource policy interpretation:** quotas and workload scheduling may be applied or accounted differently from what the root query label suggests.

ClickHouse quotas are defined for users and can restrict or track usage over time. The official docs explicitly note that quotas account for resources spent on all remote servers during distributed query processing, while the accumulated amounts are stored on the requestor server [[10]](#references). So a `clusterAllReplicas()` query can consume quota-relevant `read_rows`, `read_bytes`, and execution time across all targeted nodes even when only three remote child rows are visible and the fourth local read is folded into the coordinator row.

Workload scheduling adds another dimension: queries can be assigned to workloads that control shared resources such as CPU, IO, and query slots [[11]](#references). The `workload` setting may be inherited, defaulted, changed by profiles, or enforced by server settings such as `throw_on_unknown_workload`. For topology purposes, the model should preserve the observed root and child execution identity/resource context where available:

- `initial_user`: user on the root query row.
- `remote_user`: user logged on each child query row.
- `effective_user_source`: observed, inherited, configured remote user, or unknown.
- `initial_workload` / `remote_workload`: workload setting where available from logs/settings/profile data.
- `quota_key` / `quota_context`: if observable, the key or user context used for quota accounting.

The UI should avoid implying that all work was charged to the visible root user unless the evidence supports that. A compact label such as `user: read_only -> remote: default` or `workload: analytics -> remote: default` would be more useful than showing only the root user. The Evidence view should include mismatches because they explain surprising throttling, quota exhaustion, or workload-scheduler behavior.

#### Quota accounting user vs remote execution user

Do not collapse quota accounting and remote execution identity into one field. A common distributed-query shape is:

```
client user: read_only
initiator/requestor: ch-s1r2
remote child execution user: default
```

In that case the useful mental model is:

- **Quota accounting user:** the initial user on the requestor server, e.g. `read_only` on `ch-s1r2`.
- **Execution user:** the user observed on each physical execution row, e.g. `default` on remote child nodes.

ClickHouse quota docs say distributed-query quotas account for resources spent on all remote servers, but the accumulated amounts are stored on the requestor server [[10]](#references). Therefore a remote scan executed as `default` can still contribute to the initiator-side quota bucket for the initial user. Separately, the remote child can still be governed by settings, profiles, permissions, or workload defaults attached to the remote execution user. If a remote child fails because its execution user hits a remote-side limit, the initiator sees the remote exception and the whole distributed query normally fails.

The topology model should therefore preserve both:

- `quota_accounting_user`: usually the initial user on the requestor server.
- `execution_user`: the observed user for each execution node, including remote children and local folded reads.
- `quota_accounting_host`: the requestor server where distributed-query quota counters are accumulated.
- `remote_user_mismatch`: a marker when `execution_user` differs from `quota_accounting_user`.
- `limit_failure_scope`: `initiator`, `local_reader`, `remote_child`, or `unknown` when a quota/limit/workload error is observed.

The UI copy should make this explicit. For example:

`Root user read_only; remote children executed as default; quota accounting on ch-s1r2/read_only includes remote progress.`

This distinction is important for explaining "I set a quota for user X, but the child query row says user Y." The child row's `user` explains who executed on that node; the initiator/requestor user explains where distributed-query quota accounting is normally accumulated.

## Architecture constraints

- All new SQL (the Tier-2 `text_log` coordination query) lives in `packages/core/src/queries/`.
- Parsing `text_log` lines into structured coordination data (regexes for `shard_num`, `Replica number K assigned`, the `Coordination done` stats) lives in a `packages/core` mapper/service **with unit tests**, returning a typed `ShardCoordination` shape. No parsing or classification logic in TSX.
- The component renders a typed topology shape, the same way it consumes `SubQueryInfo` today. The shape should be richer than `SubQueryInfo[]` so it can power both the Overview summary and the Distributed tab.
- The model should keep raw query execution rows distinct where needed. The current `SUB_QUERIES` query groups by `query_id`; that may be too lossy for distinguishing a shard leader row from a leaf-reader row when the same node/query id appears in multiple roles. Verify whether the new model needs raw executions keyed by `query_id + hostname + query_start_time_microseconds`, or similar.
- The model should be view-neutral. Suggested core types: `DistributedTopology`, `QueryExecutionNode`, `TopologyEdge`, `ShardGroup`, `ReplicaReader`, `ShardCoordination`, `CoordinationEvent`, and `TopologyEvidence`.
- Add separate inference paths for read-side and write-side distributed shapes. SELECT topology can start from `initial_query_id`; INSERT topology may need cascade correlation across rows that do not share the same `initial_query_id`.
- Capability detection must be explicit input to topology inference. The service should know whether `query_log`, `ProfileEvents`, `processors_profile_log`, `system.clusters`, `text_log`, SharedMergeTree metadata, and cloud/environment metadata are available. Missing sources should degrade the model with clear warnings, not fail the UI.
- Capability detection must also cover storage-aware sources: object-storage cluster metadata, Iceberg metadata, Parquet metadata, Hybrid table metadata, and any file/manifest/row-group pruning counters exposed by the running build.
- Inference should be plugin-shaped: separate detectors for query-log fan-out, ProfileEvents parallel replicas, processor phases, cluster host mapping, write cascades, SharedMergeTree/shared-storage behavior, object-storage swarm execution, Iceberg/Parquet storage, Hybrid storage segments, and future text-log coordination. These plugins should be based on capabilities and evidence, not hard-coded vendor assumptions.
- Every inference should carry a decision trace that can be rendered as a human-readable report: which detectors ran, which capabilities were missing, what role each row was assigned, and which parts of the model are inferred vs directly observed.

## Open questions

- **Shard grouping without Tier 2.** `system.query_log` does not expose `shard_num` directly, but `system.clusters` can map configured hosts to shard/replica numbers and `ProfileEvents` can classify leader vs reader roles. This appears viable for configured clusters, but needs implementation validation for aliases, ClickHouse Cloud, remote table functions, and cases where the cluster name/table cannot be recovered reliably.
- **Trace-log cost.** `system.text_log` at Trace level is verbose. Filter by `logger_name` to the parallel-replicas loggers only. Is Trace logging reliably on across the cluster, or do we degrade gracefully to the Tier 1 `query_log` summary when it is not?
- **Non-parallel-replica queries.** What do the Overview summary and Distributed tab show for a plain distributed query (no parallel replicas) or a single-node query? The views should collapse cleanly.
- **Tab visibility.** Does the Distributed tab hide for single-node queries, appear disabled with a reason, or show a local-only execution summary? The tab should not imply distribution when `initial_query_id` has no correlated child executions.
- **Inference confidence.** How should the UI represent partial evidence, such as `query_log` present but `text_log` missing, or coordination lines present for only one shard?
- **Write-side correlation.** How aggressively should we infer `INSERT` cascades when rows do not share `initial_query_id`? The UI should avoid claiming a write topology unless timing, table relationship, query kind, and row/byte counts line up.
- **Object-storage work units.** Which system tables and ProfileEvents reliably expose Iceberg manifest pruning, Parquet row-group pruning, metadata-cache hits, file counts, and object reads across upstream ClickHouse, ClickHouse Cloud, and Antalya builds?
- **Hybrid segment evidence.** Can we reliably identify skipped vs executed Hybrid segments from query logs or processor profiles, or do we need engine-specific metadata queries?

## Status / next steps

Candidate starting points:

- Define the shared topology model that can feed both the Overview summary and the Distributed tab.
- Update the Overview Gantt as a compact summary / entry point using the current Tier 1 data.
- Prototype Tier 1.5 read inference: raw child executions + `ProfileEvents` role detection + `system.clusters` host→shard mapping + optional `processors_profile_log` phase hints.
- Prototype storage-aware inference: object-storage worker rows, Iceberg/Parquet table signals, and Hybrid hot/cold segment branches.
- Plan the full Tier-2 path: new `text_log` coordination query, core mapper, parser fixtures, typed topology shape, and Distributed tab views.
- Add write-side fixtures for Distributed-table INSERT and `AsyncInsertFlush` cascades.

## References

External links are official ClickHouse documentation and engineering blog posts; all were reachable as of 2026-06-20. Internal references point at TraceHouse source.

1. Distributed table engine — sends each read to one replica per shard, selected via `load_balancing`. https://clickhouse.com/docs/engines/table-engines/special/distributed
2. Parallel replicas (deployment guide) — coordinator splits work into granules, replicas request work dynamically, hash-based locality with task stealing; `enable_parallel_replicas` values `0/1/2`. https://clickhouse.com/docs/deployment-guides/parallel-replicas
3. "How we scaled raw GROUP BY…" (ClickHouse blog) — initiator forwards the query to participating replicas, each processes a distinct subset of data ranges and returns local results for the initiator to merge. https://clickhouse.com/blog/clickhouse-parallel-replicas
4. SharedMergeTree (ClickHouse blog) — single logical shard, data in object storage, stateless compute replicas, metadata via Keeper, replica count changeable without resharding. https://clickhouse.com/blog/clickhouse-cloud-boosts-performance-with-sharedmergetree-and-lightweight-updates
5. "Scale any query to 9000+ cores" (ClickHouse blog) — single query parallelized across replicas and cores. https://clickhouse.com/blog/clickhouse-group-by-parallel-replicas-8900-cores
6. `cluster` / `clusterAllReplicas` table functions — `cluster` queries one replica per shard, `clusterAllReplicas` queries every replica. https://clickhouse.com/docs/sql-reference/table-functions/cluster
7. `system.query_log` — `initial_query_id` correlates child queries to the initial query; `is_initial_query = 0` marks child queries from distributed execution. https://clickhouse.com/docs/operations/system-tables/query_log
8. PREWHERE — moves part of the `WHERE` to an earlier stage, reading the filter columns first to skip blocks before reading remaining columns. https://clickhouse.com/docs/sql-reference/statements/select/prewhere
9. `GLOBAL IN` / distributed subqueries — the initiator runs the subquery once, stores the result in a temporary table, and sends it to each remote server. https://clickhouse.com/docs/sql-reference/operators/in
10. Quotas — quotas restrict or track resource usage over time, account for resources spent on remote servers during distributed query processing, and store distributed-query accumulated amounts on the requestor server. https://clickhouse.com/docs/operations/quotas
11. Workload scheduling — workloads and resources control shared resources such as CPU, IO, and query slots; the `workload` setting and `throw_on_unknown_workload` affect whether queries use scheduler policies. https://clickhouse.com/docs/operations/workload-scheduling

Internal:

- `packages/core/src/queries/query-queries.ts` — `SUB_QUERIES` (Tier 1 topology query).
- `packages/core/src/services/query-analyzer.ts` — `getSubQueries()` → `SubQueryInfo`.
- `packages/core/src/services/cluster-service.ts:194` — `{{cluster_aware:…}}` → `clusterAllReplicas(...)` expansion.
- `frontend/src/components/query/modal/shared/DistributedQueryTopology.tsx` — current Gantt panel.
