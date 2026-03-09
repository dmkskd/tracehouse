# TraceHouse Test Scripts

Test data and query generator for the monitoring dashboard.

## Usage

```bash
# 1. Setup test data (creates ~10M rows per table)
uv run scripts/setup_test_data.py

# 2. Run continuous queries (generates slow + fast queries)
uv run scripts/run_queries.py
```

## Options

```bash
# Smaller dataset for quick testing
uv run scripts/setup_test_data.py --rows 1000000

# Custom ClickHouse host
uv run scripts/setup_test_data.py --host myhost --port 9000
uv run scripts/run_queries.py --host myhost --port 9000
```

## What it creates

- `synthetic_data.events` - Web analytics events
- `nyc_taxi.trips` - Taxi trip data

Both are MergeTree tables partitioned by month.
