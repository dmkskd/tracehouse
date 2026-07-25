# Proposal: UI Redesign

Status: draft, awaiting decision
Author: design session 2026-07-21/22

---

## 1. Problem statement

The app's visual identity is indistinguishable from the default output of the
current generation of React dashboard tooling (dark slate palette, neutral sans,
rounded cards, pill tabs, badge chips). This is a product problem, not an
engineering defect: the UI works, is dense, and several pages (Time Travel above
all) are strong. What the generic surface fails to do is communicate what is
actually different about the product:

- **Attribution** ("CPU is 200% — of which 120% queries, 60% merges, 20% a stuck
  mutation") is the stated design thesis (see `docs/design-vision.md`), but no
  page renders it as a first-class visual. It exists in the data layer
  (`resourceAttribution`) and mostly stops there.
- **Time Travel** is the standout feature, but its unique value (scrub history,
  inspect a moment) is presented with the same chrome as every other page.

Secondary, concrete issues:

- Metric presentation is inconsistent across pages (big-number cards, heat
  strips, sparklines, radar chart — each used once, none forming a grammar).
- Redundant navigation: the Overview destination tiles duplicate the top nav.
- Idle/empty states are accidental — widgets render near-empty tracks when the
  server is quiet, which reads as "broken" rather than "calm".
- Control chrome on some pages (Time Travel: two rows) competes with content.

## 2. Evidence base, and its limits

Five static HTML mockups were built in `demos/` (three persona explorations, one
synthesis overview, one time-travel rework). They demonstrate that a distinct
visual language is *possible* (mono type, hairline borders, single accent color,
isometric diorama). They prove nothing about feasibility against real data — one
of them (time-travel-recorder) was shown to the author and rejected, correctly,
because its synthetic data made the redesign look like information removal.

Treat the mockups as style references, not as specifications.

## 3. Proposed direction

Three commitments, everything else follows:

1. **Mono-first typography.** A real monospace family (JetBrains Mono or IBM
   Plex Mono, self-hosted) for all data, metrics, code, and labels; a neutral
   grotesk only for prose. This is the cheapest identity lever and matches the
   audience (people who live in `clickhouse-client`).
2. **Reduced chrome.** Smaller border radius (0–4px), 1px hairline borders
   instead of card-background stacking, no shadows, one signature accent color
   plus red/amber/green reserved for status. The app already themes through CSS
   variables, so this layer is largely a tokens change.
3. **A recurring attribution grammar.** Every resource gauge, everywhere,
   rendered the same way: value + stacked composition by actor (queries /
   merges / mutations / other) + one ranked list of top consumers on demand.
   Designed explicitly for both states: busy (composition visible) and idle
   (explicit "all idle" rendering, not an empty track).

## 4. Scope

### In scope

- Design tokens: fonts, palette, radius, borders, spacing (`frontend/src/index.css`,
  `frontend/src/styles/`).
- Shared components used across pages: stat cards, tables, chips, tabs.
- Page-level grammar, one page at a time, in this order:
  1. Overview (most visited, most generic today)
  2. Query Monitor (highest information density, benefits most from mono type)
  3. Remaining pages as the grammar stabilizes
- Idle/empty-state design for every touched widget.

### Out of scope (explicit non-goals)

- No re-architecture of working layouts. Pages keep their information content
  and controls; the change is in presentation grammar and hierarchy.
- Time Travel: no chart rework. At most: chrome consolidation, and a ranked
  per-moment contributor list *if* validated against real `query_log`/`part_log`
  data. Its per-host stacked chart stays as is.
- No new charting/visualization library, no layout-grid framework change.
- The 3D arena and observability map stay; restyle only if the token pass
  leaves them visually inconsistent.

## 5. Sequencing and cost

Rough, single-engineer estimates; calendar time depends on review cycles.

| Phase | Content | Effort | Risk |
|---|---|---|---|
| 1 | Tokens: font self-hosting, CSS variables, radius/borders | 1–2 days | Low — one revertible commit |
| 2 | Shared components adopt tokens; fix visual regressions | 2–4 days | Low–medium — screenshot diffing needed |
| 3 | Overview pilot: hierarchy, remove destination-tile duplication, attribution grammar with real data | 3–5 days | Medium — first real layout judgment |
| 4 | Query Monitor | 2–3 days | Low |
| 5 | Remaining pages | 1–2 weeks | Medium — this is where redesigns stall; each page is a separate review |

Phase 1+2 alone deliver most of the identity change and are fully reversible.
Phases 3–5 are where judgment calls accumulate; they should each ship behind
review, not as one big-bang branch.

## 6. Risks

- **Mockup-driven overcommitment.** Static mocks with synthetic data have
  already produced one rejected design (vitals strip) and one rejected mockup
  (time travel). Mitigation: every page change is validated against the live
  demo cluster (4 nodes, real load) before review, including the idle state.
- **Muscle memory.** Existing users (however few) know the current layout.
  Mitigation: no control moves in phases 1–2; layout changes confined to
  per-page phases with changelog entries.
- **Migration limbo.** Two aesthetics coexisting across pages for weeks.
  Mitigation: accept it, sequence pages by traffic, don't start phase 3 until
  1–2 are merged.
- **Opportunity cost.** This is 3–5 weeks of frontend time that doesn't add
  features. Whether the identity gain is worth that is a product decision,
  not an engineering one — flagged here rather than assumed.

## 7. Decision points (owner: project lead)

1. Proceed at all? The engineering case is optional; the case is product
   differentiation.
2. If yes: approve phase 1–2 (tokens) independently — they are reversible and
   cheap. Judge the result on the real app before committing to 3+.
3. Font choice: JetBrains Mono vs IBM Plex Mono (license is OFL for both;
   self-host to avoid CDN dependency in the Grafana plugin and offline installs).
4. Accent color: ClickHouse yellow is the strongest identity candidate but has
   brand-association implications; alternatives: keep current purple with mono
   type (weaker identity, zero brand risk).

## 8. What was tried and rejected in the exploration phase

- Vitals-strip attribution cards on Overview (implemented, reverted): failed on
  idle servers, where attribution data is near-zero and the cards rendered
  empty. Any attribution grammar must design the idle state first.
- Radar-chart "Resource Radar": illegible at card size; do not reintroduce.
- Time Travel chart restacked by actor instead of host: rejected on review —
  per-host stacking answers the primary cluster question; actor colors
  (yellow/amber) were also insufficiently distinct.
