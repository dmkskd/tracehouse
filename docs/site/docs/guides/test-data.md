# Loading Test Data

TraceHouse ships with scripts to load realistic test datasets for development and demo purposes.

## Available Datasets

| Dataset | Table | Description |
|---------|-------|-------------|
| Synthetic | `synthetic_data.*` | Generated data with configurable row counts and partitions |
| NYC Taxi | `nyc_taxi.*` | New York City taxi trip records |
| UK House Prices | `uk_price_paid.*` | UK property transaction data |
| Web Analytics | `web_analytics.*` | Simulated web analytics events |

## Loading Data

### Load All Datasets

```bash
just load-data
```

### Load Specific Datasets

```bash
just load-data taxi        # NYC Taxi only
just load-data synthetic   # Synthetic only
just load-data uk          # UK House Prices only
just load-data web         # Web Analytics only
```

### Quick Load (Small Dataset)

For fast iteration during development:

```bash
just load-data-quick
# 1M rows, small batches → many parts for merge visualization
```

### Heavy Load (Merge Stress Test)

To generate lots of merge activity:

```bash
just load-data-heavy
# 10M rows, small batches → triggers many merges
```

## Generating Activity

### Slow Queries

Generate query activity for the query monitor:

```bash
just run-queries

# Heavy load example
just run-queries --slow-workers 10 --s3-workers 6 --slow-interval 0.3
```

### Mutations

Generate mutation activity:

```bash
just run-mutations          # All mutation types
just run-mutations-heavy    # Heavy mutations only
just run-mutations-light    # Lightweight mutations only
```

By default mutations run in **async** mode (`mutations_sync=0`) - the script fires each mutation and returns immediately, so you can watch progress in the Merge Tracker UI. Use `--sync sync` to wait for each mutation to complete before starting the next:

```bash
just run-mutations --sync async   # Fire-and-forget (default)
just run-mutations --sync sync    # Wait for each mutation to finish
```

Or set via environment variable:

```bash
CH_MUTATION_SYNC=sync just run-mutations
```

:::tip
Lightweight `DELETE FROM` is synchronous by default in ClickHouse. The `--sync` flag overrides this with `mutations_sync=0` so all mutation types behave consistently.
:::

## Resetting Data

```bash
# Drop and reload all test data
just reload-data

# Drop all test tables (with confirmation)
just drop-data

# Drop without confirmation
just drop-data -y
```

## Configuration

All CLI scripts (`just load-data`, `just run-queries`, `just run-mutations`, etc.) automatically load `.env` from the repo root if it exists. This file is **not** used by the frontend app — only by the data-utils CLI tools.

```bash
cp .env.example .env
```

To use a different env file, set `CH_ENV_FILE` or pass `--env-file`:

```bash
CH_ENV_FILE=.env.clickhouse just load-data
# or
just load-data --env-file .env.aiven
```

```bash
# .env — used by CLI scripts only, not the app UI
CH_HOST=your-cluster.example.com
CH_PORT=9440
CH_USER=default
CH_PASSWORD=your-password
CH_SECURE=true

# Data loading parameters
CH_LOAD_ROWS=1000000
CH_LOAD_PARTITIONS=1
CH_LOAD_BATCH_SIZE=10000
```

See `.env.example` for the full list of available options.

CLI tools prompt for confirmation before starting. To skip the prompt, pass `-y` / `--assume-yes` or set `CH_ASSUME_YES=true` in your `.env`:

```bash
just load-data -y
# or
CH_ASSUME_YES=true just load-data
```

:::info
By default, all CLI tools automatically look for a `.env` file in the repo root. If no `.env` is found and no `CH_ENV_FILE` is set, the tools fall back to built-in defaults (`localhost:9000`, user `default`, no password) — which works out of the box for local Docker Compose or Local Binary setups.
:::
