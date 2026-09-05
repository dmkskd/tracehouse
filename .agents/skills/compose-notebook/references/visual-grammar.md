# TraceHouse visual grammar

## Choose the block by the question

| Question | Block | Required bindings |
|---|---|---|
| What changed over time, and when? | `timeseries.annotated` | `x`, one or more `y` fields |
| Which entities contributed most? | `table.ranked` | `rankBy`, `label` |
| Which exact facts support a mechanism? | `facts.list` | `label`, `value` |

Use the simplest block that establishes the claim. Domain-specific blocks will
be added for query plans, distributed topology, merges/parts, replication, and
flame graphs; never emulate them with an invented generic specification.

## Consistency rules

- Use absolute UTC timestamps in evidence and scope.
- Preserve supplied units. Do not infer that a ratio is a percentage.
- Keep one stable identity for the same query, host, table, part, or event.
- Use the same unit and baseline when comparing values.
- Rank by the value named in the headline and retain exact values in the table.
- Keep visual stages ordered as symptom → responsible work → mechanism → tested
  alternatives → action.
- Put one primary claim in each stage.

## Claim rules

- `observed`: directly present in evidence rows.
- `derived`: deterministic arithmetic from evidence; state the calculation.
- `inferred`: a mechanism or cause supported but not directly measured; state
  the missing measurement or alternative in `caveat`.
- `recommended`: an action motivated by earlier stages; point to the evidence
  that motivates it and avoid claiming the action is already proven effective.

## Evidence links

An evidence link must reopen the relevant TraceHouse surface with source,
cluster, absolute time, filters, selection, and view state when supported. Do
not construct query strings by hand when a TraceHouse link tool is available.
Opening the link re-queries authorized data, so keep snapshot rows beside it
when the result may change or expire.
