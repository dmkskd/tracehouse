# Notebook manifest format

The prototype contract is `schemaVersion: "0.1"` and is described formally by
`notebook.schema.json`.

Required top-level fields:

- `schemaVersion`
- `title`
- `question`
- `scope.from` and `scope.to` as absolute UTC timestamps
- `evidence`, keyed by stable evidence ID
- one or more ordered `stages`

Each evidence item contains:

- `title`
- `columns`: field names used by the rows
- `rows`: snapshot values used in the notebook
- optional `units`: field-to-unit mapping
- optional `provenance`: source, query/diagnostic ID, and captured time
- optional `view.route`: an app-relative TraceHouse route such as
  `/queries?qd_id=q-123`; the standalone and Grafana shells translate it
- optional `view.href`: an absolute external evidence link; prefer `route` for
  TraceHouse-owned evidence
- optional `view.descriptorVersion`

Each stage contains:

- `id`: stable within the notebook
- `headline`: a conclusion, not a panel label
- `claimType`: `observed`, `derived`, `inferred`, or `recommended`
- `block`: an ID from `visual-catalog.json`
- `evidence`: one evidence ID
- `encoding`: block-specific field bindings
- `takeaway`: at most two short sentences
- optional `highlight`: a timestamp, interval, or stable `rowKey`
- optional `caveat`: required for inferred claims
- optional `actions`: currently `open-evidence`

Use `snapshot-with-live-link` when both rows and a reproducible view exist,
`snapshot` when only captured rows exist, and `live-link` only when the result
can be re-queried but was not embedded. Prefer snapshots for claims that must
remain auditable.

The validator checks references and block bindings in addition to JSON shape.
It rejects unknown blocks, missing evidence, missing encoded columns, relative
scope timestamps, row-position highlights, and inferred claims without a
caveat.
