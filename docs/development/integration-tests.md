# Integration Tests

## Overview

Integration tests live in `packages/core/src/__tests__/integration/` and run against a real ClickHouse instance — either a testcontainer (default) or an external server you provide.

## Running against a testcontainer (default)

```bash
cd packages/core
npx vitest run src/__tests__/integration/merge-history-enrichment.integration.test.ts
```

This spins up a ClickHouse Docker container, runs the tests, and tears everything down. No data persists.

## Running against an external ClickHouse

Set `CH_TEST_URL` to point at your running instance:

```bash
# Local Docker / docker-compose
CH_TEST_URL=http://localhost:8123 \
  npx vitest run src/__tests__/integration/merge-history-enrichment.integration.test.ts

# K8s port-forward
kubectl port-forward -n clickhouse svc/dev-cluster-clickhouse 8123:8123
CH_TEST_URL=http://localhost:8123 \
  npx vitest run src/__tests__/integration/merge-history-enrichment.integration.test.ts
```

### Keeping test data for UI inspection

Add `CH_TEST_KEEP_DATA=1` to skip dropping the test database on teardown:

```bash
CH_TEST_URL=http://localhost:8123 CH_TEST_KEEP_DATA=1 \
  npx vitest run src/__tests__/integration/merge-history-enrichment.integration.test.ts
```

This creates the `merge_enrich_test` database with tables that exercise every merge category, then leaves them in place. Point the UI at the same ClickHouse instance to visually verify badges, row diffs, and category filters.

To clean up manually afterwards:

```sql
DROP DATABASE IF EXISTS merge_enrich_test
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `CH_TEST_URL` | _(unset — uses testcontainer)_ | ClickHouse HTTP URL for external instance |
| `CH_TEST_KEEP_DATA` | `0` | Set to `1` to preserve test databases on teardown |

### Limitations

The `http-adapter` and `connection-manager` integration tests require a testcontainer (they call container-specific APIs for host/port). They'll throw a clear error if you run them with `CH_TEST_URL`.

## Merge category reference

The enrichment test creates tables that produce every merge category and documents the full reference table in its file header comment:

→ [`packages/core/src/__tests__/integration/merge-history-enrichment.integration.test.ts`](../packages/core/src/__tests__/integration/merge-history-enrichment.integration.test.ts)
