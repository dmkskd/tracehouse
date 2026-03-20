# Pipeline Profile

Visual DAG of the query execution pipeline with per-processor performance metrics.

## Data Source

- **Pipeline DAG**: `EXPLAIN PIPELINE` output, parsed into a directed graph of processors
- **Per-processor stats**: `system.processors_profile_log`, joined by `query_id`

Requires `log_processors_profiles = 1` in the server profile (set in TraceHouse's default config).

## Metrics Per Processor

| Metric | Source Column | Description |
| --- | --- | --- |
| Elapsed µs | `elapsed_us` | Wall time spent in this processor |
| Input Wait µs | `input_wait_elapsed_us` | Time waiting for input data |
| Output Wait µs | `output_wait_elapsed_us` | Time waiting for output buffer space |
| Input Rows | `input_rows` | Rows consumed |
| Input Bytes | `input_bytes` | Bytes consumed |
| Output Rows | `output_rows` | Rows produced |
| Output Bytes | `output_bytes` | Bytes produced |

## Processor Categories

Processors are grouped by stage type:
- **ReadFromMergeTree** — data source (part reads, index evaluation)
- **Expression** — column transformations, function evaluation
- **Filter** — WHERE/HAVING predicate filtering
- **Aggregating** — GROUP BY partial/final aggregation
- **Sorting** — ORDER BY
- **Limit** — LIMIT/OFFSET

## Interpretation

- High `input_wait_elapsed_us` → processor is starved (upstream is slow)
- High `output_wait_elapsed_us` → processor is blocked (downstream can't consume fast enough)
- Compare `input_rows` vs `output_rows` to see selectivity at each stage
