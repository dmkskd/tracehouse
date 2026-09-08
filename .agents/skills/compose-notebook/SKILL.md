---
name: compose-notebook
description: Turn ClickHouse findings, diagnostic rows, or TraceHouse evidence links into a validated TraceHouse notebook. Use when an agent needs to explain an incident, error, performance problem, query, merge, replication issue, or other ClickHouse finding; create a notebook (investigation, runbook, or report); choose consistent visual blocks; or attach claims to native TraceHouse evidence routes.
---

# Compose a TraceHouse notebook

Produce a short chain of supported claims that a human can understand and
inspect. Author data and semantic intent only; never generate React, SVG, CSS,
or arbitrary HTML.

## Required references

Read both references before composing:

- [manifest-format.md](references/manifest-format.md) for the output contract.
- [visual-grammar.md](references/visual-grammar.md) for block selection and
  consistency rules.

## Workflow

1. Set `kind` for what you are producing. `investigation` answers one question
   about something that already happened, and is the default when unset.
   `runbook` is a reusable procedure meant to be re-run against new time ranges.
   `report` is a summary meant to be read rather than acted on.
2. Inspect the evidence values, scope, units, stable IDs, provenance, and any
   existing TraceHouse links. Do not chart column names blindly.
3. Restate the question. Separate what was observed from the proposed cause.
4. Select the minimum evidence needed to answer the question. Retain material
   counter-evidence.
5. Write an ordered sequence of conclusion-shaped headlines. Prefer “Query
   q-123 accounted for 61% of the peak” over “Top queries.”
6. Say how sure you are in the takeaway itself, in plain words. State a
   measured finding directly; write an uncertain explanation as one, naming
   what would confirm it. Follow the plain-language examples in the visual
   grammar; do not add classification jargon to prose.
7. Bind each cell to one primary evidence ID and a stable highlight. Use entity
   keys and timestamps, never row positions.
8. Choose a supported block using the visual grammar. Repeated query or error
   records belong in a compact table; `facts.list` is for short label/value
   facts, not full exception messages or SQL. Preserve the underlying evidence
   without putting every captured field in the main visual.
9. Preserve two evidence forms when available:
   - snapshot rows used for the claim;
   - a TraceHouse-native `view.route` with frozen absolute time.
10. Write `notebook.json`. TraceHouse validates it on load and lists every
    problem it finds, so paste it into the Notebooks page to check it.
11. Feed the validated document to TraceHouse's notebook route.
    TraceHouse—not the agent—renders it with dashboard chart and table
    primitives, focus navigation, and native evidence actions.
12. Return a two- or three-sentence conclusion, the document or notebook link,
    and any important limitation. Do not overstate an inconclusive
    investigation.

## Guardrails

- Never invent missing rows, SQL results, links, fields, units, or identities.
- Do not present temporal overlap or correlation as causation.
- Do not compare mismatched hosts, clusters, populations, or time windows.
- Do not mix total rows with their component rows in a ranking or breakdown.
- Treat encoded SQL and identifiers as visible, not secret. Apply the supplied
  redaction policy before writing links or snapshots.
- A polished visual cannot raise confidence above the underlying evidence.

## Prototype scope

The current TraceHouse notebook supports `timeseries.annotated`,
`table.ranked`, and `facts.list`. It reuses the dashboard `ChartRenderer`,
`ResultsTable`, and Focus Cell interaction conventions. If another block is
needed, report the unsupported visual instead of inventing a block name.

The supported product experience is the TraceHouse `/notebooks` route in
standalone and Grafana. Confidence lives in the prose, not in badges or
metadata fields.
