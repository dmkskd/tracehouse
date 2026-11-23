/** Public boundary: no UI, global store, import-time collection or SQL definitions. */
export * from './model/types.js';
export { canonical } from './model/canonical.js';
export { collectObjects, notGrantedSources, type CollectOptions } from './collectors/collect.js';
export { projectGraph, projectCatalogGraph, provenancePaths } from './projections/graph.js';
export { lineage, focusGraphFrom, searchGraph, essentialEdges } from './projections/focus.js';
export { withoutDefaultUser } from './projections/filters.js';
export { KINDS, type KindSpec } from './model/kinds.js';
export { REFERENCE_METHODS, type ReferenceMethod } from './model/methods.js';
export { DETAIL_GROUPS, RELATIONSHIPS, isRevokeEdge, type DetailGroup, type RelationshipFamily, type RelationshipKind, type RelationshipSpec } from './model/relationships.js';
export { engineSpec, type EngineSpec, type TableType, type ReferenceSource } from './model/engines.js';
export { edgeCaption, describeCard, describeObject, describeObjects, objectTypeOf, isAccessType, FLAG_LABELS, type ObjectCard, type ObjectHover, type ValueRange, type ObjectType, type CardFact } from './presentation/cards.js';
export { describeDetail, loadDetailStatement, loadTableActivity, loadTablePlacement, type TablePlacement, type LifecycleStep, type TableActivity, type ObjectDetail, type DetailSection, type DetailStatement, type DetailRelationItem, type DetailRelationRow, type DetailRelationGroup, type DetailDestination, type DetailLinks } from './presentation/details.js';
export { DESTINATIONS, type DestinationSpec, type DestinationInput } from './model/destinations.js';
export { layoutCatalog, layoutFocus, routeEdges, withRoutes, withSourceNotices, withEmptyAccessNotices, isColumnNotice, isWaypoint, isGap, BAND_TITLES, type RoutedLayout, type LayoutBand, type LayoutColumn } from './presentation/layout.js';
export { GUIDES, investigationFor, type Guide, type GuidePick } from './presentation/guides.js';
export { OBJECT_LINK_PARAMS, objectLinkUrl, parseObjectLink, objectLinkName, resolveObjectLink, withPrincipalContext, summarizePrincipalPaths, describeProvenance, describeMissingLink, objectLinkFor, type PrincipalContextSummary, type ProvenanceLine, type ObjectLink, type ResolvedObjectLink, type MissingObject, type MissingLinkNotice, type MissingLinkSuggestion } from './presentation/links.js';
