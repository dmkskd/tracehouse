# Metrics Calculations

How TraceHouse calculates and displays every metric, organized by topic.

| Doc | Covers |
| --- | ------ |
| [cpu.md](cpu.md) | CPU % across all pages, cgroup/k8s awareness, metric sources, clamping |
| [memory.md](memory.md) | Memory tracking, cgroup memory limits, container awareness |
| [time-travel.md](time-travel.md) | Hybrid data model, flat-band approximation, cluster "All" mode aggregation |
| [query-analysis.md](query-analysis.md) | Pruning scores, selectivity, parallelism, IO wait, color coding policy |
| [overview.md](overview.md) | Real-time attribution breakdown, merge/mutation monitoring, alerts |
| [engine-internals.md](engine-internals.md) | CPU sampling attribution, core timeline, thread pools, memory fragmentation |
| [reference.md](reference.md) | System tables, ProfileEvents, common gotchas |

**Test coverage:** Core formulas are validated against a real ClickHouse instance via testcontainers. Run with `just test-core-integration` or `npx vitest run --config vitest.integration.config.ts` from `packages/core`.
