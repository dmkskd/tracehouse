/**
 * Notebooks — the feature's entire public surface.
 *
 * Experimental and unfinished. Evidence is currently frozen rows and is moving
 * to queries; see docs/proposals/notebooks.md.
 *
 * ## Everything outside this directory imports from here
 *
 * Nothing else in the app may reach into `features/notebooks/*`. That keeps the
 * churn inside a feature that is expected to change shape, and it makes removal
 * a directory delete plus the hooks listed below.
 *
 * ## Where this is hooked into the app
 *
 * There are exactly four, and they are all registration — no host contains a
 * branch about notebooks:
 *
 *   1. packages/ui-shared/src/navigation.ts   nav entry, and the two things the
 *                                             shells need to know about it:
 *                                             `experimental` and
 *                                             `requiresDatasource: false`
 *   2. frontend/src/pages/Notebooks.tsx       standalone page, renders NotebookLoader
 *   3. frontend/src/App.tsx                   standalone route
 *   4. grafana-app-plugin/src/App.tsx         plugin route (ROUTES map)
 *
 * Deliberately absent: an entry in grafana-app-plugin/src/plugin.json. Grafana
 * builds its left nav statically from that file and cannot honour the
 * experimental gate, so an entry there would be an ungated door into the
 * feature.
 *
 * ## Dependencies point inward
 *
 * This feature uses shared code (analytics chart primitives, core services).
 * Nothing shared may learn what a notebook is: no notebook types in
 * `packages/core` or `packages/ui-shared`. When preset notebooks arrive, their
 * SQL goes into `packages/core/src/queries/` as ordinary queries with no
 * notebook awareness.
 */

export { NotebookLoader } from './NotebookLoader';
export { NotebookView } from './NotebookView';
export { NotebookErrorBoundary } from './NotebookErrorBoundary';
export { parseNotebook, validateNotebook, type NotebookValidation } from './validate';
export { notebookToMarkdown } from './markdown';
export type {
  NotebookDocument,
  NotebookEvidence,
  NotebookKind,
  NotebookStage,
} from './model';
