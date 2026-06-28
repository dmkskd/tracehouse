import type { XRayVisualizationProps } from '../../../frontend/src/components/query/modal/tabs/XRayVisualization';
import { createRuntimeComponent } from './runtimeComponent';

export type { XRayVisualizationProps } from '../../../frontend/src/components/query/modal/tabs/XRayVisualization';

export const XRayVisualization = createRuntimeComponent<XRayVisualizationProps>(
  'xrayVisualization',
  'XRayVisualization',
  'Loading X-Ray visualization...'
);

export default XRayVisualization;
