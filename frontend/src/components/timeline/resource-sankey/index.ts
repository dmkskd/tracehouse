/**
 * Time Travel resource Sankey views. Shared attribution is provided by core. This is the only application import boundary.
 * Set RESOURCE_SANKEY_ENABLED to false to restore the regular hover/pin behavior.
 * Removal instructions and independent shared/core fixes: README.md.
 */
export const RESOURCE_SANKEY_ENABLED = true;
export { PinnedResourceActivity } from './PinnedResourceActivity';
export { createResourceDataLoader } from './resource-data-loader';
export type { ResourceDataLoader, ResourceOperationOpen } from './resource-exploration-model';
