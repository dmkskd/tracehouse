# TraceHouse visual grammar

## Choose the block by the question

| Question | Block | Required bindings |
|---|---|---|
| What changed over time, and when? | `timeseries.annotated` | `x`, one or more `y` fields |
| Which entities contributed most? | `table.ranked` | `rankBy`, `label` |
| Which short facts support a mechanism? | `facts.list` | `label`, `value` |

Use the simplest block that establishes the claim. Domain-specific blocks will
be added for query plans, distributed topology, merges/parts, replication, and
flame graphs; never emulate them with an invented generic specification.

## Records versus facts

Use a table for repeated query executions, failures, or log records. A full
exception message is record detail, not a metric to display in a large tile.
Reserve `facts.list` for short values such as “Error code: 241” or
“Configured query limit: 1,000,000 bytes”.

Keep the main table focused on the comparison: for example, failure time,
query ID, error code, and measured values with units. Retain full SQL and
exceptions as supporting evidence. Do not invent extracted metrics: use
queried values or record how values were deterministically extracted.

Use cell `columns` to select fields, give them readable labels, and declare
semantic types (`query`, `sql`, `bytes`, `timestamp`, or `text`). A `query`
column opens the existing query-details route in a new tab; it requires a
connection and retained query logs. Keep full captured fields in evidence;
row details expose them even when they are omitted from the main table.

Use a meaningful description as the main label for grouped queries. A hash
such as `normalized_query_hash` is an identity for matching, not a readable
query name; never rename it “workload ID” and expect readers to understand it.
Author a concise description grounded in the captured SQL, retain that SQL
and hash in details, and record the description as author-written metadata.

`table.ranked` requires a meaningful `rankBy`; report the limitation if the
records need a table ordering the current block cannot express.

## Consistency rules

- Use absolute UTC timestamps in evidence and scope.
- Preserve supplied units. Do not infer that a ratio is a percentage.
- Keep one stable identity for the same query, host, table, part, or event.
- Use the same unit and baseline when comparing values.
- Rank by the value named in the headline and retain exact values in the table.
- Keep visual cells ordered as symptom → responsible work → mechanism → tested
  alternatives → action.
- Put one primary claim in each cell.

## Saying how sure you are

Say it in the takeaway, in ordinary words. There is no field for it.

State a measured finding directly: "Three queries returned error 241." For an
uncertain explanation, write it as one: "Likely explanation: overlapping queries
increased memory pressure," followed by what is missing: "We don't have
per-query measurements to confirm this." Use "Suggested next cell" when
introducing an action, and do not claim it is already proven to work.

Avoid "observed", "derived", "inferred", "epistemic" and "Inference boundary" as
reader-facing labels.

## Evidence links

An evidence link must reopen the relevant TraceHouse surface with source,
cluster, absolute time, filters, selection, and view state when supported. Do
not construct query strings by hand when a TraceHouse link tool is available.
Opening the link re-queries authorized data, so keep snapshot rows beside it
when the result may change or expire.
