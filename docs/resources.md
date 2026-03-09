# External Resources & References

Useful links for ClickHouse monitoring, performance tuning, and internals.

---

## Monitoring & Troubleshooting

- [Essential Monitoring Queries — INSERT Queries](https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse)
  ClickHouse blog. Practical SQL queries for monitoring insert throughput, async insert flushing,
  part creation frequency, insert duration SLAs, memory/CPU per insert, and part error detection.

- [Essential Monitoring Queries — SELECT Queries](https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse)
  ClickHouse blog. Queries for identifying expensive SELECTs, comparing query metrics side-by-side,
  deep-diving into query execution, tracking average duration trends, and diagnosing
  `TOO_MANY_SIMULTANEOUS_QUERIES`.

## Query Performance

- [Secrets of ClickHouse Query Performance (PDF)](https://altinity.com/wp-content/uploads/2024/05/Secrets-of-ClickHouse-Query-Performance.pdf)
  Altinity webinar slides. Covers reading system logs to understand query behavior, data types
  and encodings, filtering strategies, join reordering, skip indexes, materialized views,
  and session parameters.

- [The Definitive Guide to ClickHouse Query Optimization](https://clickhouse.com/resources/engineering/clickhouse-query-optimisation-definitive-guide)
  ClickHouse official guide (2026). Full query optimization reference.

- [Guide for Query Optimization](https://clickhouse.com/docs/optimize/query-optimization)
  ClickHouse docs. Analyzer, query profiling, avoiding nullable columns, and other
  optimization techniques.

## Useful Query Collections

- [Altinity KB — Useful Queries](https://kb.altinity.com/altinity-kb-useful-queries/)
  Altinity knowledge base. Collection of practical ClickHouse queries for operations
  and troubleshooting.

## Internals & Architecture

- [Sparse Primary Indexes](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes)
  ClickHouse docs. How the primary index works, binary search vs generic exclusion search,
  and how ORDER BY key design affects query performance. Referenced by our
  [ordering key algorithm](ordering-key-algorithm.md).
