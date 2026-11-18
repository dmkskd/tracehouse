# Distributed queries in ClickHouse

Short reference on the mechanisms that make a ClickHouse query distributed, and where each one is available (open source or Cloud). Written 2026-10-03. Statements are tied to the sources listed at the end; items that could not be confirmed are marked **unverified**.

## What "distributed" can mean

A query is distributed when work for one statement runs on more than one server process. The mechanisms differ in who decides where work goes, what moves over the network, and what the initiator does at the end.

### 1. Distributed table engine (sharding)

- The initiator receives the query and sends a rewritten sub-query to one replica of every shard.
- Each shard runs the sub-query on its local data and returns partial results (for aggregations, intermediate states).
- The initiator merges, sorts and applies the final LIMIT.
- Replica choice inside a shard follows `load_balancing`; failover is sequential.
- Relevant knobs: `optimize_skip_unused_shards` (needs a sharding key), `prefer_localhost_replica`, `distributed_product_mode` (default `deny`), `GLOBAL IN` / `GLOBAL JOIN`.
- Joins: without `GLOBAL`, the right side is evaluated on each shard against local data. With `GLOBAL`, the initiator evaluates the right side and ships it to every shard.
- Inserts are asynchronous by default (files are spooled locally and forwarded). `distributed_foreground_insert` changes that.
- Shape: a two-level tree, one initiator and N shard queries.

### 2. `cluster()` and `clusterAllReplicas()`

- Table functions that give Distributed-style fan-out without a Distributed table.
- `cluster()` queries one replica per shard. `clusterAllReplicas()` queries every replica.
- Connections are re-established per request, so the docs recommend them for debugging, comparison and infrequent manual queries.
- In `query_log` the result looks like case 1, but with one child per replica for `clusterAllReplicas()`.

### 3. Object storage cluster functions (`s3Cluster` and relatives)

- The initiator expands the file glob and hands out one file at a time to workers on request, until all files are processed.
- Work unit is the file. Workers do not need to own any data.
- Shape: coordinator plus workers pulling tasks.

### 4. Parallel replicas

- No sharding required. The coordinator splits the table into granule-based tasks and assigns them dynamically to replicas of the same data.
- Replicas return mergeable partial results; the coordinator does the final merge.
- Settings: `enable_parallel_replicas` (0, 1, 2), `max_parallel_replicas`, `cluster_for_parallel_replicas`, `parallel_replicas_mode`, `automatic_parallel_replicas_mode`.
- Documented limits: JOINs, CTEs and subqueries can hurt; disabled with `FINAL`; not compatible with projections; high-cardinality aggregations can slow down. Every replica still builds the full right-side hash table for a join, and the final aggregation still runs on the coordinator.
- Status: the settings reference still lists `allow_experimental_parallel_reading_from_replicas` as experimental (default 0), while the parallel replicas guide uses `enable_parallel_replicas`. **Unverified:** the exact maturity label per open source version. Check the running version before relying on it.
- Shape: a one-level fan-out from the coordinator, all replicas reading the same logical table.

### 5. Shared storage and SharedMergeTree (Cloud)

- The SharedMergeTree engine family powers ClickHouse Cloud. Replicas coordinate through shared storage and Keeper instead of talking to each other, so a table can have many replicas without shards.
- This is what makes "any worker can read any data" true, and it is the base for the Cloud features below.
- Open source uses ReplicatedMergeTree, where data and metadata live on each server.

### 5b. Warehouses / compute-compute separation (Cloud)

- Multiple services share the same data through object storage, with schemas synchronized by SharedMergeTree and a shared Keeper.
- This isolates workloads (for example ingestion versus reporting). It does not distribute a single query: a query stays inside its assigned service, and the replica-group concept keeps distributed work within that service.
- One primary read-write service per warehouse; secondary services are read-only or read-write. Read-only services disable background merges.
- Launched January 2025 as a general Cloud feature.
- Relevance for topology: `initial_query_id` trees stay inside one service, so existing inference still applies, but node and replica names belong to a service, not the whole warehouse. **Unverified:** how `system.clusters` and `query_log` present warehouse services.

### 6. Multi-stage distributed execution (Cloud, private preview)

- The plan is split into stages. Exchange operators move intermediate data between stages: Shuffle (M-to-N by join or GROUP BY key), Broadcast (1-to-N), Gather (N-to-1), Scatter (random).
- Data moves over direct TCP between workers or through shared object storage.
- Removes the two parallel-replicas bottlenecks: per-node right-side hash tables, and single-node final aggregation.
- Reported early TPC-H (SF 100) results: up to 3.4x on join-heavy queries, 7.4x on 8 nodes versus 1 node for scan-heavy work.
- Status in the post (May 2026): experimental, private preview, Cloud only. No settings are named in that post.
- Shape: a graph of stages, not a tree.

### 7. On-Demand Compute (Cloud, private preview)

- Individual queries lease workers from a ClickHouse-managed shared pool outside the service's own cluster, instead of scaling the service.
- Built on the multi-stage framework and a cost-based optimizer.
- Enabled per query with `make_distributed_plan = 1`, `distributed_plan_workers_num = <n>` and `enable_parallel_replicas = 0`, as shown in the announcement.
- Workers are leased for at least 60 seconds with automatic renewal; concurrent queries share workers, so the pool grows to the largest request, not the sum.
- Announced 2026-09-10; rollout after a 2026-09-24 webinar. Cloud only, private preview, first release with limited scope.

## Availability summary

| Mechanism | Open source | Cloud | Notes |
|---|---|---|---|
| Distributed table engine | Yes | Yes | Classic sharding; initiator/shard tree |
| `cluster()` / `clusterAllReplicas()` | Yes | Yes | Per-request connections; for ad hoc use |
| `s3Cluster` and similar cluster functions | Yes | Yes | File-level task dispatch. **Unverified:** the full list of formats and lakehouse variants per version |
| Parallel replicas | Yes, maturity label varies (see section 4) | Yes | Cloud uses `cluster_for_parallel_replicas = 'default'` per the guide |
| SharedMergeTree (shared-storage replicas) | No | Yes | Cloud engine family |
| Warehouses (compute-compute separation) | No | Yes (since Jan 2025) | Workload isolation; no cross-service query distribution |
| Multi-stage distributed execution | No | Private preview | Experimental, access via account team |
| On-Demand Compute | No | Private preview | Cloud only, announced 2026-09-10 |

## What this means for TraceHouse

- Today the topology inference (`packages/core/src/services/distributed-query-topology.ts`) models mechanisms 1 to 4 as a tree rooted at the initiator, linked through `initial_query_id`.
- Mechanisms 6 and 7 would need a stage graph, and exchange-aware metrics. How they appear in `system.query_log` is not documented in the sources below, so any design waits on a real Cloud query to inspect.

## Sources

- Multi-stage distributed query execution in ClickHouse Cloud: https://clickhouse.com/blog/multi-stage-distributed-query-execution-clickhouse-cloud
- ClickHouse On-Demand Compute: https://clickhouse.com/blog/on-demand-compute
- Parallel replicas guide: https://clickhouse.com/docs/deployment-guides/parallel-replicas
- Distributed table engine: https://clickhouse.com/docs/engines/table-engines/special/distributed
- Cluster table functions: https://clickhouse.com/docs/sql-reference/table-functions/cluster
- s3Cluster: https://clickhouse.com/docs/sql-reference/table-functions/s3Cluster
- SharedMergeTree: https://clickhouse.com/docs/cloud/reference/shared-merge-tree
- Warehouses: https://clickhouse.com/blog/introducing-warehouses-compute-compute-separation-in-clickhouse-cloud
- Settings reference: https://clickhouse.com/docs/operations/settings/settings
- Third-party overview (initiator/shard pipeline, `group_by_two_level_threshold`, `load_balancing`, `NetworkSendBytes`): https://oneuptime.com/blog/post/2026-03-31-clickhouse-distributed-query-execution/view
