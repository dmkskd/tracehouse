# Isometric object catalog

Experimental catalog renderer. The existing graph is the default. Focus, details,
collection, search, filters, URLs and access evaluation remain outside this feature.
The catalog's Diagram menu contains Graph / Isometric; there is no separate view
switch on the page. Graph-only style and routing controls are hidden in spatial mode.

## Boundary

`ObjectCatalogView.tsx` is the only production module importing this directory. It
loads `SpatialObjectView` on selection and provides an error fallback to the graph.
The Diagram menu accepts an optional content slot, and the graph/spatial renderers
accept a menu element; these generic UI slots do not depend on the experiment.
No dependency, global style, capture schema, core export or persisted preference is
introduced for the experiment.

The renderer receives `shared/ObjectViewModel.ts` and `ObjectViewActions`. Its graph
is the searched projection **before** graph-specific `essentialEdges`, pagination
and connector routing. The existing graph applies those in `CatalogGraphView.tsx`.

Shared presentation provides descriptions, canonical database membership, selection
emphasis, hover content and lazily cached per-table answers from the core evaluator.
`accessForTable` exists only for a selected user. A role/scope selection highlights
configured relationships and scope-name matches, not an authorization verdict.
The evaluator's host differences, SET ROLE requirements, column limits and evidence
gaps are preserved. Data-flow reachability is never used to compute access.

## Feature-local behaviour

- `grouping.ts`: database containers, scope groups, shared storage, and users with
  identical per-host role assignments and default-role selectors. Direct grants can
  differ; incomplete or differing users are not grouped. Group membership always
  retains original object keys. No grants or host records are merged.
- `layout.ts`: bounded, deterministic model-space rectangles. Core dependency
  ranking orders data objects; database membership controls containment. Selection
  reveals a hidden object. Privileges have a column-wide 20-card limit and Show 20
  more, rather than a separate allowance for every scope. Scope cards pack into
  grids of up to three columns. Other groups share the remaining initial 240-tile
  budget with at most 24 tiles per group; explicit Show more can exceed that budget.
  Initial group display is limited to 12 groups per category with Show more groups.
- `connectors.ts`: geometric routes and bundles. Each bundle retains its original
  graph edges, and never bundles a revoke with a positive link. With Essential lines,
  broad scope matches terminate at database headers; otherwise each table link is
  retained. Collapsed groups retain connection anchors.
- `useCamera.ts`: centre-pivot perspective CSS projection with tf-view's initial 54° tilt and
  −45° rotation. Drag orbits, Shift/middle-drag pans, wheel zooms at the pointer.
  Touch supports one-finger orbit and two-finger pan/pinch. The shared zoom control
  sits at the canvas's bottom-right with Diagram and Controls. The Controls popover
  contains Reset camera, gesture instructions and a collapsible legend. Rotation and pan
  use gestures; there are no angle sliders or drag-mode buttons. Reset uses
  the initial angles and readable entry framing (at least 80% zoom), centred on
  the diagram. Orbit keeps the ground point at the viewport centre stationary,
  including after panning or minimap navigation. A movement
  threshold preserves clicks, and completed drags suppress object activation.
  Fitting and selection framing use the same perspective distance as the CSS camera.
- `SceneOverview.tsx`: flat model-space overview with a projected viewport polygon.
  The overview sits above the bottom-right toolbar. Clicking, dragging the viewport
  outline, or using arrow keys pans without changing rotation or zoom.
- `styles.ts`: scoped Emotion selectors, including extrusion and card-content styling.

The shared `ObjectCardBody` and hover retain the same descriptions and warnings as
the graph. The spatial view uses existing focus/detail dialogs rather than a second
inspection implementation. Group/camera state is local and resets when unmounted.
Scene headings distinguish Users, Roles, Privileges by scope, Databases, and External storage. Privilege group titles use `ON <scope>` and grant cards identify holders.
The footer shows only the visible/collected object count. The shared-storage group
appears only when the graph contains an external endpoint or remote disk; local
storage-policy disks are intentionally not collected as external storage.
Show Database, card style and graph routing remain graph options. Highlight on hover
and Essential lines are shared with Isometric.

Database groups are logical catalog membership, not physical shard/replica placement.
Table counters and coverage describe collected observations, not complete visibility.

## Removing the experiment

1. Replace `ObjectCatalogView.tsx` with:

   ```tsx
   export { CatalogGraphView as ObjectCatalogView } from './CatalogGraphView';
   ```

2. Delete this directory, including its tests and this README.
3. Run the object-browser tests, frontend typecheck, style lint and build checks.

The page's import and both shared/graph implementations remain valid. The shared
presentation tests are independent of this directory. The boundary test scans all
frontend and core source to reject additional production imports of the experiment.

## Validation

Feature tests exercise assignment grouping, original evidence preservation, collapsed
anchors, finite geometry with cycles, paging, selected objects beyond the page bound,
and the single integration boundary. The integration test switches renderers without
collecting again and opens the existing focus. Shared presentation tests distinguish
core access from lineage, SET ROLE, partial revokes and host disagreement.

Validate builds in the standalone frontend, single-file HTML and Grafana plugin;
the lazy boundary must work in all three packaging modes.
