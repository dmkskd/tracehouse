# Testing

The project uses [Vitest](https://vitest.dev/) across the entire monorepo. Tests are split into three categories: unit tests, frontend integration tests, and backend integration tests against real ClickHouse instances via testcontainers.

## Test categories

### Unit tests

Pure logic tests with no external dependencies. These run instantly and cover things like SQL template resolution, query literal escaping, and pure computation helpers.

```
packages/core/src/__tests__/cluster-service.test.ts
packages/core/src/__tests__/query-literals.test.ts
```

Run them with:

```bash
just test-core
```

### Frontend integration tests

Verify that the main app's Zustand stores correctly call shared services through mock adapters, and that shared UI components (`@tracehouse/ui-shared`) are importable and type-compatible in the main app context.

```
frontend/src/__tests__/integration/main-app-migration.test.tsx
```

These use mock adapters that return predetermined rows for specific SQL patterns, so they don't need Docker or a running ClickHouse instance.

```bash
just test-frontend
```

### Backend integration tests (testcontainers)

This is the most important test layer. Integration tests in `packages/core/src/__tests__/integration/` run against a real ClickHouse instance - either a [testcontainer](https://testcontainers.com/) (default) or an external server you provide.

Each test file spins up a fresh ClickHouse Docker container, creates databases and tables, inserts data to trigger real merges/mutations, and validates the service layer against actual system tables. Everything is torn down automatically after the test.

Current integration test coverage:

| Test file | What it validates |
|---|---|
| `merge-tracker` | Active merges, merge history, mutations, background pool metrics |
| `merge-history-enrichment` | Merge category classification across all merge types |
| `merge-lineage-bytes` | Byte-level lineage tracking through merge chains |
| `database-explorer` | Database/table/part listing against real system tables |
| `query-analyzer` | Query monitoring and history from system.processes / query_log |
| `metrics-collector` | Metric collection from system.metric_log |
| `lineage-builder` | Part lineage graph construction from part_log |
| `cluster-dedup` | Deduplication logic on a 2-node replicated cluster |
| `overview-merge-type` | Merge type breakdown for the cluster overview |
| `http-adapter` | Low-level HTTP adapter against the container |
| `connection-manager` | Connection lifecycle management |
| `error-wrapping` | Error classification and wrapping from real ClickHouse errors |
| `real-system-tables` | Schema validation against actual system table shapes |

## Test tags

Every test suite is tagged by **domain** — what capability it validates, not how it's tested. Tags use Vitest 4.x native `{ tags: [...] }` syntax on top-level `describe()` blocks.

### Available tags

| Tag | What it covers | Example tests |
|---|---|---|
| `security` | Sandbox escapes, SQL injection, privilege escalation, credential access | `readonly-sandbox`, `builder.property` |
| `merge-engine` | Merge tracking, classification, history, lineage, ETA, samples, algorithms | `merge-tracker`, `merge-history-enrichment`, `merge-classification` |
| `query-analysis` | Query monitoring, timeline, scan efficiency, EXPLAIN parsing | `query-analyzer`, `timeline-service`, `explain-parser` |
| `storage` | Parts, TTL, compression, table efficiency, pruning, storage policies | `database-explorer`, `database-ttl-detection`, `connection-http-compression`, `pruning` |
| `cluster` | Multi-node dedup, distributed queries, replica detection, topology | `cluster-dedup`, `sampling-setup-cluster` |
| `observability` | Metrics, resource monitoring, process sampling, CPU/mem/disk/net, flamegraphs | `overview-metrics-collector`, `sampling-process-history`, `query-trace-flamegraph`, `correlation` |
| `connectivity` | Connection management, HTTP adapter, retry/backoff, error wrapping | `connection-manager`, `connection-http-adapter`, `connectionRetry` |
| `analytics` | Analytics query language, dashboards, meta-language, preset queries | `metaLanguage`, `analytics-preset-smoke` |
| `setup` | Sampling setup scripts, schema provisioning, idempotency | `sampling-setup-script` |
| `visualization` | 3D rendering, pipeline graphs, chart data, formatters | `PartsVisualization`, `MergeVisualization` |

### Filtering by tag

Run only tests matching a specific domain:

```bash
# Single tag
just test-tag security

# Or directly with vitest
cd packages/core && npx vitest run --tags-filter="security"

# Boolean expressions
npx vitest run --tags-filter="merge-engine || storage"
npx vitest run --tags-filter="observability && !connectivity"
```

### Listing tags

```bash
just test-list-tags
```

### Adding tags to new tests

Declare the tag in the relevant `vitest.config.ts` under `test.tags`, then use it on top-level `describe()`:

```typescript
describe('my new feature', { tags: ['merge-engine'] }, () => {
  it('does something', () => { ... });
});
```

## Test reports

All test runs generate an HTML report and a JSON results file automatically.

### Viewing reports

After running tests, open the interactive HTML report:

```bash
just test-report
```

This serves the report at `http://localhost:4173` with a searchable, filterable UI showing every suite and test with pass/fail status, durations, and tag information.

### Report locations

| Package | HTML report | JSON report |
|---|---|---|
| `packages/core` (unit) | `test-reports/html/index.html` | `test-reports/results.json` |
| `packages/core` (integration) | `test-reports/integration-html/index.html` | `test-reports/integration-results.json` |
| `frontend` | `test-reports/html/index.html` | `test-reports/results.json` |

Reports are gitignored and regenerated on every test run.

## Testcontainer infrastructure

### Single-node container

Most tests use a single ClickHouse container via `@testcontainers/clickhouse`. The setup lives in:

```
packages/core/src/__tests__/integration/setup/
├── clickhouse-container.ts   # Start/stop a single ClickHouse container
├── cluster-container.ts      # Start/stop a 2-node cluster with Keeper
├── shadow-adapter.ts         # Adapter for shadow system tables
├── shadow-tables.ts          # Create/seed shadow copies of system tables
├── table-helpers.ts          # Helpers to create test databases and tables
└── index.ts                  # Re-exports everything
```

The `startClickHouse()` function pulls the `clickhouse/clickhouse-server:26.1-alpine` image, starts a container, and returns a context with a ready-to-use `ClusterAwareAdapter` and raw `@clickhouse/client` instance.

### Cluster container (2-node)

The `cluster-dedup` test uses a more complex setup: a 2-node ClickHouse cluster with a ClickHouse Keeper node, all on a shared Docker network. This validates cluster-aware features like replicated table deduplication and cross-node query routing.

The topology is: 1 shard × 2 replicas + 1 Keeper node = 3 containers.

## Running integration tests

### Default (testcontainer)

```bash
# All integration tests
just test-core-integration

# Single test file
cd packages/core
npx vitest run src/__tests__/integration/merge-tracker.integration.test.ts
```

Docker must be running. The container starts automatically, runs the test, and tears down.

### Against an external ClickHouse

Set `CH_TEST_URL` to skip the container and run against your own instance:

```bash
CH_TEST_URL=http://localhost:8123 \
  npx vitest run src/__tests__/integration/merge-tracker.integration.test.ts
```

This works with Docker Compose, a K8s port-forward, or any reachable ClickHouse HTTP endpoint.

### Keeping test data for UI inspection

Add `CH_TEST_KEEP_DATA=1` to skip dropping the test database on teardown. Useful when you want to point the frontend at the same ClickHouse instance and visually verify the UI against known data:

```bash
CH_TEST_URL=http://localhost:8123 CH_TEST_KEEP_DATA=1 \
  npx vitest run src/__tests__/integration/merge-history-enrichment.integration.test.ts
```

Clean up manually when done:

```sql
DROP DATABASE IF EXISTS merge_enrich_test;
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `CH_TEST_URL` | _(unset - uses testcontainer)_ | ClickHouse HTTP URL for an external instance |
| `CH_TEST_KEEP_DATA` | `0` | Set to `1` to preserve test databases on teardown |

### Limitations

The `http-adapter` and `connection-manager` tests require a testcontainer (they call container-specific APIs for host/port). They throw a clear error if you run them with `CH_TEST_URL`.

## Writing a new integration test

1. Import the setup helpers:

```typescript
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/index.js';
```

2. Start the container in `beforeAll`, tear down in `afterAll`:

```typescript
let ctx: TestClickHouseContext;

beforeAll(async () => {
  ctx = await startClickHouse();
  // Create your test database, tables, insert data...
}, 120_000); // container startup can take up to 2 minutes

afterAll(async () => {
  await ctx.client.command({ query: 'DROP DATABASE IF EXISTS my_test_db' });
  await stopClickHouse(ctx);
}, 30_000);
```

3. Use `ctx.adapter` for service-layer calls and `ctx.client` for raw SQL setup/teardown.

4. Name the file `*.integration.test.ts` so it's picked up by the integration test runner.

## Data Utils tests (Python)

The `tools/data-utils/` package has its own test suite using pytest and testcontainers. These tests validate the table plugin contracts, create/insert operations, and query generation against a real ClickHouse instance.

```
tools/data-utils/tests/
├── conftest.py              # Testcontainers fixture (session-scoped)
└── test_table_plugins.py    # Protocol conformance + integration tests
```

### Running

```bash
just test-data-utils
```

Docker must be running — the tests spin up a ClickHouse container automatically.

### What they cover

- **Protocol conformance**: each dataset plugin satisfies the `Dataset` protocol
- **Create + insert**: tables are created and data is inserted correctly
- **QuerySet validation**: each plugin's `queries` property returns well-formed SQL
- **InsertConfig**: frozen dataclass behaviour and defaults

## Quick reference

```bash
just test                    # Run all tests (unit + frontend + integration)
just test-core               # Unit tests only (packages/core)
just test-frontend           # Frontend tests only
just test-core-integration   # Integration tests only (requires Docker)
just test-data-utils         # Data utils tests (requires Docker)
just test-tag security       # Run only tests tagged 'security'
just test-list-tags          # List all available tags
just test-report             # Open HTML report in browser
```
