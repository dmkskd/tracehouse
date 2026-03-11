# TraceHouse Infrastructure Scripts

SQL scripts for one-off ClickHouse setup tasks.

## Files

- `setup_jaeger_export.sql` — Configure Jaeger trace export
- `setup_read_only_user.sql` — Create a read-only ClickHouse user
- `uk_house_queries.sql` — Example UK house price queries

## Data Tools

The Python data loading, query generation, and mutation tooling has moved to
[`tools/data-utils/`](../../tools/data-utils/).

```bash
# Load test data
just load-data

# Run continuous queries
just run-queries

# Run mutations
just run-mutations
```
