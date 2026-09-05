# Notebooks

Status: experimental, shipped behind a gate, unfinished by design.

The single notebook document. Supersedes and replaces three earlier proposals
(`agentic-visual-investigations.md`, its design note, and
`notebook-evidence-as-queries.md`), which were folded in here and removed.

---

## 1. What it is

A **notebook** is a document TraceHouse renders: an ordered argument about a
ClickHouse system, where every claim is attached to the evidence behind it and
to the live view that shows it.

Notebooks are composed elsewhere. An agent investigating an incident, or a
person writing against the schema, produces a document; TraceHouse renders it.
TraceHouse never generates React, SVG or HTML on an agent's behalf, and the
agent never learns anything about the frontend.

`kind` says what a given notebook is for:

- `investigation` — answers one question about something that already happened.
  The default when unset.
- `runbook` — a reusable procedure, re-run against new time ranges.
- `report` — a summary meant to be read rather than acted on.

A runbook is an investigation with the snapshots stripped. They are the same
format, which is the main reason the format is shaped the way it is.

**Naming.** The feature was called "Investigations" until 2026-08-19. Renamed
because Grafana ships its own Investigations app and TraceHouse ships as a
Grafana plugin, so the two would collide in one left nav. "Notebooks" is also
the industry noun: Datadog, New Relic and ClickStack all use it, and Datadog
carries the same kind-of-notebook distinction as a type tag.

---

## 2. What exists today

Shipped, gated behind the existing `experimentalEnabled` toggle, badged `EXP`,
with a banner saying the format will change.

- **Renderer.** Three blocks (`timeseries.annotated`, `table.ranked`,
  `facts.list`) drawn with the analytics chart primitives. Focus mode for
  presenting. Per-panel toolbar: Source, Evidence, Focus.
- **Validator.** `validate.ts`, the only validator. Runs at load, returns every
  problem rather than throwing, and nothing renders unless the whole document
  is valid.
- **Input.** Paste, drag-drop, or file picker. No storage, no identity, no list.
- **Markdown view.** Any notebook can be read as Markdown, per stage or whole.
- **Compose skill.** `.agents/skills/compose-notebook/`, with the schema.
- **Eval harness.** `tracehouse-scenarios` (tools/data-utils) injects a known
  fault, emits a symptom-only question asking for a notebook, and keeps an
  answer key naming the real mechanism. Includes a `control` scenario that
  injects nothing, to catch invented causes.

Not built: storage, identity, a list page, presets, editing, and query-based
evidence.

---

## 3. Product model

### Notebook

A scoped answer to a question. It carries the question, the scope (cluster,
hosts, time range), an ordered set of stages, evidence, optional limitations,
and provenance for anything executed.

Not necessarily a completed incident report. Two stages is a legitimate
notebook.

### Stage

One step in the argument:

- a conclusion-shaped headline
- one primary visual
- a short takeaway
- evidence anchored into the visual
- an optional caveat

Headlines state findings, not diagnostics. Prefer:

> Query `abc…` accounted for 61% of the memory peak

over:

> Top queries by peak memory

The second names a panel. The first says what the panel established.

### Claim types

Every stage declares how much to trust it:

- **observed** — returned directly by ClickHouse
- **derived** — deterministically calculated from returned values
- **inferred** — an interpretation supported by evidence but not measured
- **recommended** — an action motivated by the findings

`inferred` is the one that earns its keep: it is where a plausible story gets
marked as a story. The validator **requires a caveat** on inferred stages.

`observed` versus `derived` is a subtle distinction and may not deserve its own
badge; both mean "the data says so". Worth revisiting.

### Evidence anchors

An anchor should be more precise than "this panel": a timestamp, a series and
value, a table row identified by a stable key, a node in a topology, an operator
in a plan. Row *positions* are rejected by the validator, because they are not
stable.

Selecting a claim should highlight its evidence, answering "why do you believe
this?" without another prompt.

---

## 4. Evidence: queries, not frozen rows

Agreed 2026-09-05. Not built. This is the next significant change.

### Why

Evidence is currently `columns` + `rows`, captured at compose time. That works
for investigations, where frozen rows keep a claim auditable after retention has
removed the source data, and let the document travel to someone without cluster
access.

It fails for runbooks and presets, which have no data at compose time and must
ask the user's cluster. It also makes documents mostly data, so editing a
sentence means scrolling past a thousand-row table.

### The model

Evidence carries a query. A snapshot records what that query returned.

```json
"memory-contributors": {
  "title": "Attributed memory contributors at 13:05",
  "sql": "SELECT actor_id, peak_gib FROM ... WHERE event_time BETWEEN {from} AND {to}",
  "units": { "peak_gib": "GiB" },
  "view": { "route": "/queries?qd_id=q-123", "descriptorVersion": 1 },
  "snapshot": {
    "takenAt": "2026-07-28T13:20:00Z",
    "resolvedSql": "SELECT ... BETWEEN '2026-07-28T12:55:00Z' AND '2026-07-28T13:15:00Z'",
    "rows": []
  }
}
```

No snapshot means the block runs live. A snapshot present means it shows the
capture, with the query still there to re-run.

**The `mode` enum is deleted.** `snapshot | live-link | snapshot-with-live-link`
existed to say which case applied; presence of `snapshot` now says it.

### Snapshot is an operation, not a setting

This framing is what makes the rest fall out. You investigate live, explore,
discard most of it, and when you find the thing you *snapshot* it. The snapshot
is the moment you commit to a claim. It is a button.

- Exploration stays out of the document, so dead ends do not accumulate.
- **Drift is free.** Query and capture are both present, so re-running gives
  "61% then, 48% now". For a runbook that difference is the point, and neither
  pure-live nor pure-frozen evidence can express it.
- Export stops being a special case: packing is "snapshot every stage".

### Where the SQL comes from

Exactly one of:

- **`queryId`** — resolves against `packages/core/src/queries/`. Presets use
  this, so shipped SQL stays where it belongs and improving the query improves
  every notebook referencing it.
- **`sql`** — inline, for anything composed about a specific incident. Most
  notebooks.

The promotion path matters: an agent investigating something novel writes
inline SQL; if it proves broadly useful it becomes a core query and presets
reference it by id. A normal refactor, not a format change.

A `queryId` that no longer resolves must fail loudly at load, never render an
empty block.

### Scope binding

`scope.from`/`to` bind into the SQL. Absolute range plus the same SQL returns
the same answer, so an investigation's headline stays supported by what sits
under it. A runbook uses a relative range and is live by design.

The current validator *requires* absolute timestamps, so relative scope is a
change it has to allow.

### Editing invalidates snapshots

Edit the SQL and the captured rows came from a different query; the document
would quietly lie. Store a hash of the SQL with the snapshot and mark it
**stale** when they diverge.

Mark, do not drop. Losing captured evidence to a typo is worse than showing it
with a warning, especially once retention has removed the ability to re-capture.

### Consequences

- **Validation splits.** Structure is checked at load; column references
  (`encoding.label` names a real column) move to execution, because columns are
  unknown until the query runs. "Valid notebook" stops meaning "will render".
- **Retention becomes a visible state.** A query returning nothing because
  `query_log` rotated needs an explicit "evidence no longer available".
- **Guardrails.** SELECT-only, `max_execution_time`, `max_result_rows`. Note
  this is not new capability: `customQueries.ts` already stores and runs
  user-authored SQL. What is new is SQL arriving in a pasted document, which
  the Markdown source view mitigates by showing exactly what will run.

---

## 5. Architecture and isolation

The feature is unfinished and will churn. It is therefore contained so that it
can change or be deleted cheaply.

### One public surface

`frontend/src/features/notebooks/index.ts` is the only entry point. Nothing
outside the directory may reach past it. Verified: no such import exists.

### Four hooks, all registration

1. `packages/ui-shared/src/navigation.ts` — nav entry, plus the two facts the
   shells need: `experimental: true` and `requiresDatasource: false`
2. `frontend/src/pages/Notebooks.tsx` — standalone page
3. `frontend/src/App.tsx` — standalone route
4. `grafana-app-plugin/src/App.tsx` — plugin route

Deliberately absent: an entry in `grafana-app-plugin/src/plugin.json`. Grafana
builds its left nav statically from that file and cannot honour the
experimental gate, so an entry there would be an ungated door in.

### Capabilities are declared, never branched on

The Grafana shell previously carried `routeKey !== 'notebooks'` inline to skip
its "no datasource" message. That put one feature's requirements inside the
shell: every further exception adds a clause, and removing a feature means
finding and unpicking it.

It is now `requiresDatasource` on the nav entry, read through
`routeRequiresDatasource()`, defaulting to `true` for unknown routes.

**This is the rule for everything that follows.** A capability a feature needs
is data on the feature, not a conditional in a host.

### Dependencies point inward

Notebooks uses shared code. Nothing shared learns what a notebook is: no
notebook types in `packages/core` or `packages/ui-shared`. When presets arrive,
their SQL goes into `packages/core/src/queries/` as ordinary queries with no
notebook awareness.

The feature currently reaches into the analytics chart primitives
(`components/analytics/charts`, `ResultsTable`). Correct direction, but worth
funnelling through one adapter file so there is a single place to fix when
analytics moves.

### Removal

Delete the directory, the four hooks, and `.agents/skills/compose-notebook/`.
The `experimental` gate and `visibleNavigationItems` stay: they are generic
infrastructure any experiment can use.

---

## 6. Getting a notebook into the app

Both hosts can reach exactly one thing in common: ClickHouse. The standalone app
has `packages/proxy`; the Grafana plugin has no backend at all.

Options considered:

- **Paste / drop a file.** Works in both, zero infrastructure. **Implemented.**
  No sharing, no history.
- **URL payload.** Works in both, no storage, shareable by copy-paste. Dies on
  size once evidence rows are involved.
- **ClickHouse-backed store** (`tracehouse.notebooks`). Works identically in
  both hosts because both already talk to ClickHouse. The agent `INSERT`s, the
  app `SELECT`s, so **the app stays read-only**. Gets identity, sharing and a
  list page, which presets need anyway. The likely next step.
- **Proxy endpoint or Grafana plugin storage.** Rejected: each exists in only
  one host, so the feature would need two divergent implementations.

---

## 7. Trust and provenance

The artifact must be easier to audit than a prose answer:

- SQL is inspectable before it runs, and the Markdown source view shows it
- claims declare their epistemic type, and inference requires a caveat
- evidence anchors are stable keys, never row positions
- evidence routes are validated: app-relative single-slash paths or absolute
  HTTP(S) only, so `javascript:` and protocol-relative URLs are rejected
- a snapshot states when it was taken, and says so rather than implying current
  state

---

## 8. Phases

1. **Schema and model.** `sql` required, `snapshot` optional, `mode` deleted.
   Nothing executes yet; the renderer still draws from `evidence.snapshot`, so
   the app keeps working while the format changes underneath. ~1 day.
2. **Execution.** Evidence SQL through the services layer, so it works unchanged
   in both hosts. Guardrails, per-block loading/error/empty states, runtime
   column validation. ~1-2 days. Unblocks presets.
3. **Snapshot as an action.** Per-stage button, staleness via SQL hash, re-run
   with drift shown. ~1 day.
4. **Markdown editing.** Prose and SQL editable, round-tripping into the
   document. ~1-2 days.
5. **Presets and an index.** Presets referencing core queries by id, a list
   page, and a first preset: diagnosing a stalled lightweight delete. ~2 days.

---

## 9. Known gaps

- **Styling.** The page reads as a document dropped into an instrument panel:
  prose first, data second, larger type than the rest of the app. The hierarchy
  should invert — evidence gets the space, the headline becomes a caption.
- **No identity.** No id, no URL, no list. Storage, sharing, presets and drift
  all assume one. Retrofitting means changing the format and the routes.
- **Notebook versus dashboard.** Analytics panels are already SQL + chart +
  prose + drill, specified in a directive language with their own validator.
  Notebooks are SQL + block + claim + evidence link, specified in JSON with a
  second validator, rendering through the same primitives. Either a notebook is
  an ordered list of panels carrying claims, or the difference needs stating.
  Left unresolved, the two drift.
- **Time.** Notebooks carry absolute scope; the app has a global time picker.
  What happens when they disagree is undefined.
- **The example** does not say it is a work in progress in its own text.

---

## 10. Open questions

Carried forward from the design note, minus those now decided.

1. Should the notebook format extend the analytics dashboard model, or should
   both consume a lower-level document model? (See "notebook versus dashboard".)
2. Which existing panels are stable enough to receive public diagnostic ids?
3. How expressive should evidence anchors be?
4. What result data may be exposed to a remote agent, and what redaction is
   needed?
5. Should a notebook retain the agent/tool transcript, a concise provenance log,
   or neither?
6. How should the runtime say that no diagnosis was possible from the available
   tables, permissions, or retention window?
7. What is the smallest set of visual blocks that covers the golden scenarios
   without becoming a generic chart grammar?
8. JSON or Markdown as the authored format. Markdown is currently a read-only
   view; the options are prose-only editing, lossless round-tripping, or
   Markdown replacing JSON.

Decided since: the artifact is called a Notebook (§1); storage should be
ClickHouse-backed (§6); evidence is a query and snapshot is an action (§4).

---

## 11. Notes from the earlier proposals

Kept because they are worth remembering, not because they are planned. Both
assume identity, storage and revisions, none of which exist yet.

### A review pass before publishing

The original design split the agent's job three ways — investigate, compose,
review — where review is a publish gate that independently tests whether the
notebook says more than its evidence establishes. Its checklist:

- every conclusion has an evidence anchor
- observed, derived and inferred are not blurred together
- time ranges, hosts, clusters and units match across compared stages
- relative time was frozen, so the result is reproducible
- live and snapshot content are labelled correctly
- totals are not mixed with their components or double-counted
- correlation is not presented as causation
- likely alternatives were tested or explicitly left open
- SQL, identifiers and user data follow redaction policy

This overlaps with what `tracehouse-scenarios` is already reaching for: the
answer key names the real mechanism, and these checks catch overclaiming. The
eval harness and the review pass want to meet.

The same split proposed problem-family references for the investigate skill
(memory, merges and parts, replication, distributed queries, cpu-io), each
stating which sources are authoritative **and which are misleading** for that
family. That last part is the valuable bit and is hard-won knowledge.

### One artifact, many channels

Slack, Grafana, a terminal and an incident tracker have different rendering and
interaction capabilities, and must not each invent their own answer format. One
canonical notebook; adapters translate.

Two rules worth keeping:

- **Chat is an entry and notification surface, not an evidence renderer.** Do
  not flatten a notebook into thirty messages; post a finding card and a link.
- **A ticket gets an immutable revision, not a live view** — "a record of what
  was known at that point, not a silently updating dashboard."

The second is the snapshot-versus-live distinction arrived at from the delivery
side, which is some evidence the model in §4 holds up.

---

## 12. Non-goals

- A general-purpose agent UI framework
- A replacement for the dashboards or query explorer
- Arbitrary agent-generated web applications
- Automatic remediation or configuration changes
- Hiding SQL and raw evidence behind an uninspectable summary
- Treating every answer as a long incident report
- Requiring an LLM to understand frontend implementation details

---

## 13. Removed: standalone HTML export

`tracehouse-agent-kit/` held a CLI that validated a notebook and packed it into
a single self-contained HTML file, openable with no server and no ClickHouse.
The intent was sending an investigation to someone without TraceHouse.

Removed before first commit, and gitignored rather than deleted:

- **The validator existed twice.** `validate.ts` enforces the same contract in
  TypeScript, and the app validates on load. Two implementations drift.
- **The use case was unproven.** Nobody had asked to send a notebook outside the
  app.
- **Query-based evidence makes it harder.** Packing would have to execute the
  queries and materialise results, giving the packer a ClickHouse dependency it
  did not have.

The schema and compose skill survived, at `.agents/skills/compose-notebook/`.

Revisit if someone needs a notebook outside TraceHouse. The natural shape is
"snapshot every stage, then render", which is the Snapshot action applied to all
of them.
