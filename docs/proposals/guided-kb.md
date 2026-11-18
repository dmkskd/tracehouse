# Guided Knowledge Base (symptom-first runbooks)

Status: proposal, not built. Captured 2026-10-16.

## Why

The Analytics Knowledge Base today is a port of two online KBs
(ClickHouse support center, kb.altinity.com): one dashboard per source, one
collapsible section per article, the article's queries pre-wired. That removes
the copy-paste step, but it keeps the online KB's structure, and the hard part
of troubleshooting is not running the query:

1. **Finding the right article.** A user starts from a symptom
   (`Too many parts (300)`, "replica is read-only", "mutation never finishes",
   "queries got slower since yesterday"), not from an article title. Today they
   must know that "Too many parts" lives under "Number of active parts in a
   partition" and "Ingestion metrics from system.part_log".
2. **Reading the result.** Panel descriptions say what a panel shows, not when a
   value is bad. `@cell: rag` thresholds are used on few panels, and are global
   constants rather than the table's own limits.
3. **Knowing the next step.** The online articles chain queries with prose
   ("if X, check Y"). The dashboards drop the chain and drop the fix, because
   panels are read-only (the MOVE PART generator, `SYSTEM JEMALLOC PURGE`,
   read-only replica recovery are all omitted).

Example, `Too many parts` on insert:

| | Steps for the user |
|---|---|
| Online KB | search, read, copy query, paste in SQL editor, run, read on, copy next query |
| TraceHouse today | open Altinity KB, know which 2 of ~20 sections apply, interpret raw counts, go back to the article for the fix |
| Guided | pick "Inserts rejected: Too many parts", follow 3 steps, copy the fix |

## What

A list of **symptoms** as the entry point. Each symptom is a short runbook:

1. **Locate**: which host / table / partition is affected, with values coloured
   against the object's own limit (e.g. active parts vs that table's
   `parts_to_throw_insert`), not a global constant.
2. **Diagnose**: one or two queries that split the likely causes, each with a
   one-line reading rule ("parts/minute > 60 -> inserts too small, go to fix A;
   merge pool saturated -> go to fix B").
3. **Fix**: the remediation as copyable SQL / config text, never executed by the
   app (`KILL MUTATION`, `SYSTEM RESTORE REPLICA`, `ALTER TABLE ... DROP DETACHED
   PART`, async_insert settings). Placeholders are filled from step 1 (the
   table/partition the user picked), so the text is ready to paste.
4. **Verify**: the step 1 query again, to confirm the value is moving.

Each step keeps its `@source` link to the article it came from.

### First runbooks (most common, mostly built from existing panels)

| Symptom | Existing panels reused | New |
|---|---|---|
| Inserts rejected: Too many parts | Active Parts per Partition, Too Fast Inserts, Background Pool Utilization | parts vs per-table limits, async_insert fix text |
| Mutation never finishes | KB Mutations, Failed / Stuck Mutations | blocking merges, `KILL MUTATION` text |
| Replica read-only or lagging | Read-Only Replicas, Replication Queue by Type, Keeper status | metadata diff, `SYSTEM RESTORE REPLICA` text |
| Queries slower than yesterday | Compare query_log for 2 intervals | per-shape drill into executions / query comparison |

Later: disk filling up, OOM / memory limit exceeded, Kafka consumer stuck,
TTL not deleting data, distributed DDL hanging.

## How

The runbook kind in the Notebooks proposal (`docs/proposals/notebooks.md`,
kind = runbook) is the vehicle: ordered cells, live SQL cells, markdown between
them. What runbooks need on top of notebooks:

- **Symptom index**: entry page listing runbooks by symptom and by error code
  (`TOO_MANY_PARTS`, `MEMORY_LIMIT_EXCEEDED`, `TABLE_IS_READ_ONLY` ...). Error
  codes can also link in from places that already show exceptions (query
  detail, Events dashboard), so a failed query opens its runbook directly.
- **Step parameters**: a value picked in one step (row click, as `@drill` does
  today) becomes a parameter for later steps and for the fix text.
- **Reading rules**: a declarative rule per diagnostic result (threshold or
  boolean column) that highlights which branch applies. Rule evaluation is
  business logic and belongs in `packages/core` with tests, not in TSX.
- **Fix blocks**: a non-executable SQL cell type with copy button and
  parameter substitution. Never auto-run; the app stays read-only.

Dashboards stay as the reference / browse view; runbooks link to the panels
they reuse rather than copying SQL.

## Open questions

- Per-table limits: `merge_tree_settings` gives server defaults; table-level
  overrides are only in `create_table_query`. Parse it, or show defaults with a
  note?
- Runbook authoring format: the existing `@meta` directive SQL strings, or the
  notebook document format?
- Cloud vs self-managed: some fixes (config.xml, `SYSTEM` commands) do not apply
  on ClickHouse Cloud; runbooks may need a deployment-type switch.
