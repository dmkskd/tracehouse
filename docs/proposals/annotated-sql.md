# Proposal: Annotated SQL view (Query Details → SQL tab)

**Status:** prototyped end-to-end and reverted, 2026-07-06. Parked pending a
better answer on parsing trust and annotation UX. This document is the record
of the problem space, what was tried, and what was learned.

## Goal

Make the SQL tab a navigation surface, not a text dump: render a SELECT as one
line per clause (linter-style), fold each clause to a preview chip, and pin
performance findings to the clause that caused them:

```
SELECT  …23 columns ⌄                      ⚠ all columns read — nothing pruned
FROM    clusterAllReplicas(dev, system.query_log)   ℹ fan-out · 4 nodes
WHERE   event_time > '2026-07-01' AND … ⌄  ⚠ no effective index use — 1576:1
ORDER BY event_time DESC   LIMIT 100       ✓ index pruning 98.7%
```

Clicking a chip expands the original clause text in place; clicking a finding
jumps to the tab that owns the full analysis (Analytics, Distributed, Details).

### The reframe that makes it tractable

**Annotations are never derived from the SQL text.** A findings engine computes
them from metrics we already collect (ProfileEvents, query_log metadata,
EXPLAIN indexes = 1, distributed topology); the SQL is only the surface they
are pinned to. The problem decomposes into three deterministic steps:

1. **Skeletonize** — split the statement into top-level clause spans with byte
   offsets into the original text (click-to-expand = reveal the substring).
2. **Findings** — rule engine over existing metrics; each finding carries a
   `scope` (select_list / from / where / group_by / order_by / settings / global)
   plus severity, metric, and an owning tab for deep links.
3. **Anchor** — join findings onto clauses via a static scope→clause-kind map.
   Findings without a matching clause render in a footer list.

Because preview chips, tab badges, the Overview, and the annotations would all
render the same findings array from one thresholds table, they stay consistent
by construction. (Sharing that array across tabs was designed as phase 3 and
never built.)

## What was tried

A full working implementation existed and was green (269 new tests) before the
revert:

- **Clause scanner** (`packages/core/src/utils/sql-skeleton.ts`): a
  string/comment/paren-aware scanner — deliberately *not* a parser. It only
  understood quoting rules, comments, bracket depth, and top-level clause
  keywords; everything inside parentheses was opaque. Returned `null` on
  anything it could not segment, degrading to the plain formatted view.
  Also produced per-clause item counts (top-level commas; ANDs for predicates,
  BETWEEN-aware) and previews with long literals (>24 chars) folded to `$n`.
- **Findings engine** (`packages/core/src/services/query-findings.ts`):
  columns-read ratio (vs `system.columns`), pruning verdict (reusing
  `calculatePruning`), WHERE-vs-sorting-key diagnosis (reusing
  `diagnoseOrderingKeyUsage`, EXPLAIN-keys aware, only when exactly one keyed
  table), distributed fan-out, external sort/aggregation spill, settings count.
  One exported thresholds table so no two views could disagree.
- **Frontend**: a lazy hook fetching table columns, sorting keys
  (new `queryAnalyzer.getTableSortingKeys`), and `explainIndexes` when the SQL
  tab activated, each degrading independently; a purely presentational
  `AnnotatedSql` component; a three-way mode toggle
  (annotated / formatted / raw) with annotated as the SELECT default.

### What the testing process taught us

ClickHouse keywords are **non-reserved**, and for a ClickHouse monitoring tool
keyword-as-identifier is the common case, not the edge case. Real bugs found
during live use and stress testing:

1. `SELECT query_id, Settings, ProfileEvents FROM system.query_log` — the
   `Settings` **column** split the select list into a phantom SETTINGS clause.
   (Fix: SETTINGS is a clause only when followed by `name =`.)
2. `ORDER BY ts WITH FILL FROM ... TO ... STEP ...` — the `FROM` spawned a
   phantom FROM clause. (Fix: FROM only valid right after a select list.)
3. `SELECT * EXCEPT (col)` column transformer misread as an EXCEPT set
   operation. (Fix: bare `EXCEPT (` only counts when the paren opens SELECT/WITH.)
4. Folding *all* literals to `$n` hid information (`LIMIT 50` → `LIMIT $3`,
   `user != ''` → `user != $2`); only long literals should fold.

The test architecture that caught these is the reusable lesson — three layers:

- **Keyword-collision matrix + complex-SQL unit tests**: every clause keyword
  as a column name; window functions (`ORDER BY` inside `OVER()`), lambdas,
  nested CTEs, JSON braces inside string literals, a 12-clause kitchen sink.
- **Corpus invariants**: every SQL statement shipped in
  `packages/core/src/queries/` (181 statements) through the scanner, asserting
  spans are ordered/in-bounds and keywords round-trip. Final state was
  **181/181 segmenting**, pinned exactly (a decline fails the suite).
- **Property-based tests** (fast-check): arbitrary unicode never throws;
  SQL-shaped token soup never yields inconsistent spans; structurally generated
  SELECTs round-trip clause kinds and item counts exactly.

## Why it was reverted

Not correctness at the end — the scanner beat every tested library on real
query_log traffic. Two judgment calls:

- **Annotation UX wasn't landing.** First live demos required explanation
  (folded literals, redundant badge text, and the initial `Settings` mis-split
  eroded confidence in the skeleton even after it was fixed).
- **Trust question unsettled**: whether a hand-rolled scanner can be trusted on
  *arbitrary* user SQL — beyond our corpus — was judged to need either a
  differential oracle in CI or a real ClickHouse parser before shipping.

## Library survey (tested hands-on, 2026-07-06)

Hard requirements: understands ClickHouse syntax **and** returns byte offsets
into the original text **and** runs client-side with acceptable size.
No option satisfied all three:

| Option | Verdict |
|---|---|
| **sqlglot** (Python) | Best ClickHouse dialect outside CH itself. Wrong runtime for a TS/browser app. Viable CI oracle. |
| **polyglot** `@polyglot-sql/sdk` v0.5.13 (Rust/WASM, MIT) | Tested against our stress queries: parses the kitchen sink, WITH FILL, lambdas, transformers; validated upstream against ClickHouse's 7,047-case parser corpus. **But:** AST `span` fields are declared in the types yet come back empty across the WASM boundary; 18.8 MB WASM; hard-fails on the `Settings`-column query above; unparseable SQL returns `success: true` wrapped in a `command` fallback node. Not a runtime option yet — re-check spans and size on future releases. Its AST select-node keys (`from`, `prewhere`, `where_clause`, `group_by`, `having`, `order_by`, `limit`, `limit_by`, `settings`) map 1:1 onto clause kinds, which makes it a good **devDependency differential oracle**. |
| **EXPLAIN AST** (server-side) | Parse-only, so it works even for dropped tables; authoritative. But no source offsets, a round-trip before first paint, and it rejects truncated query_log text. Right tool as a testcontainers CI oracle, wrong tool for rendering. |
| **node-sql-parser / sql-parser-cst / dt-sql-parser** (JS) | No ClickHouse dialect; fail on PREWHERE/SETTINGS/lambdas — the queries a ClickHouse tool sees all day. |
| **ANTLR grammars-v4 ClickHouse grammar** | Token positions available, but the community grammar lags CH syntax and ClickHouse itself abandoned its ANTLR parser. |
| **lezer via `@codemirror/lang-sql`** (already bundled) | Error-tolerant with token positions, but no clause structure — could only replace the tokenizer half of a scanner. |
| **AfterShip `clickhouse-sql-parser`** (Go) | Real CH AST with positions; needs Go→WASM (several MB) plus bindings. The candidate if deep structure (alias resolution, join graphs) ever becomes a requirement. |

Core insight from the survey: the view needs **positions plus shallow
structure**; parsers provide deep structure minus positions. That mismatch is
why the scanner approach was chosen, and it remains true.

## If we pick this up again

1. Rebuild along the same three-step decomposition — the design held up; the
   scanner + findings split is the right architecture. Budget most of the effort
   for the test suite (collision matrix, corpus invariants, properties) and for
   annotation UX, not for the happy path.
2. Add a differential oracle from day one: polyglot as a devDependency
   (clause-key comparison) and/or EXPLAIN AST via the existing
   `@testcontainers/clickhouse` integration setup.
3. Iterate the annotation presentation with real users before widening the rule
   set: what deserves a badge, badge wording, fold thresholds.
4. Then phase 3: move `buildScanEfficiency` out of `ScanEfficiencyTab.tsx` into
   core and have Overview + tab badges consume the same findings array.

## Known scanner gaps at time of revert (all declined safely to the plain view)

- Parenthesized union heads: `(SELECT ..) UNION ALL (SELECT ..)`
- Dollar-quoted strings `$tag$...$tag$`
- Multi-statement input beyond the first `;`
- Non-SELECT statements (by design — the view was SELECT-only)
