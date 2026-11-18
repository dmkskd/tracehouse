# Documentation

## App-Specific

| Document | What it covers |
|----------|---------------|
| [metrics/](metrics/) | Every metric formula used in the app — CPU, memory, disk I/O, query efficiency, merge throughput, resource attribution. Split by topic. |
| [metrics/time-travel-events.md](metrics/time-travel-events.md) | Time Travel operational events — inclusion policy, ClickHouse sources, capability coverage, filtering dimensions, and planned sources |
| [ordering-key-algorithm.md](ordering-key-algorithm.md) | How the Analytics tab diagnoses ORDER BY key efficiency — data sources, pruning algorithms, diagnostic categories, EXPLAIN integration |
| [parts-and-lineage.md](parts-and-lineage.md) | Parts 3D visualization rendering, merge lineage tree building, space savings calculation, batch query strategy |
| [admin-polling-reference.md](admin-polling-reference.md) | Every polling interval in the app — what each page queries, which system tables are hit, load impact, scaling considerations |

## ClickHouse Reference

| Document | What it covers |
|----------|---------------|
| [clickhouse-observability-tiers.md](clickhouse-observability-tiers.md) | The three tiers of ClickHouse metrics (server-wide → per-table → per-operation), system tables, how `query_id` joins everything, what our app uses vs what we could add |
| [clickhouse-profile-events-classification.md](clickhouse-profile-events-classification.md) | Complete classification of ProfileEvents from `ProfileEvents.cpp` — merge profiling, mutations, insert pressure, replication, query performance, CPU/OS, disk I/O, S3, ZooKeeper, caches, thread pools, lock contention |

## Development

| Document | What it covers |
|----------|---------------|
| [development/clickhouse-compatibility.md](development/clickhouse-compatibility.md) | Exact matrix checkpoints, ClickHouse version boundaries, runtime capability findings, and compatibility-related test defects |
| [Grafana plugin development](site/docs/development/grafana-plugin.md#analytics-result-types-and-metadata-lookup) | Datasource integration and Analytics metadata lookup: extra request cost, lack of caching, failure symptoms and troubleshooting |
| [issues/time-travel-timezones.md](issues/time-travel-timezones.md) | Open Time Travel timezone issue, reproduction, affected code paths, and existing UTC work |
| [proposals/time-travel-utc-round-trips.md](proposals/time-travel-utc-round-trips.md) | Proposed UTC return-path fix and timezone regression coverage |
| [proposals/application-timezone-correctness.md](proposals/application-timezone-correctness.md) | Review of the partial Time Travel fix, remaining timezone defects, proposed changes, and regression checks |
| [development/timezone-implementation-plan.md](development/timezone-implementation-plan.md) | Staged timezone implementation, producer/consumer rollout gates, and regression checks for standalone and Grafana |

## Nice to Know

| Document | What it covers |
|----------|---------------|
| [proxy-alternatives.md](proxy-alternatives.md) | How to replace the built-in Node.js proxy with Caddy or Nginx — header protocol, example configs, trade-offs |

## External

| Document | What it covers |
|----------|---------------|
| [resources.md](resources.md) | Curated links to ClickHouse blog posts, Altinity KB, performance guides, and official docs |
