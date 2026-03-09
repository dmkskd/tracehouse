/**
 * 3D Visualization Components
 * 
 * This module exports all 3D visualization components for the TraceHouse.
 * Uses React Three Fiber (@react-three/fiber) and Drei (@react-three/drei).
 */

// Types
export type { 
  SceneConfig, 
  Scene3DProps,
  PerformanceContextValue,
} from './types';

// Configuration utilities
export { 
  defaultSceneConfig, 
  createSceneConfig,
} from './config';

// Context and hooks
export { 
  PerformanceContext,
  usePerformanceMode,
} from './PerformanceContext';

// Components
export { Scene3D } from './Scene3D';
export { 
  PartsVisualization, 
  type PartVisualizationProps, 
  type ColorScheme,
  type PartitionSummary,
  type PartsVisualizationCallbacks,
  createPartitionSummaries,
} from './PartsVisualization';
export { 
  PipelineVisualization, 
  type PipelineVisualizationProps, 
  type PipelineNode,
  parsePipelineOutput,
} from './PipelineVisualization';

export {
  MergeVisualization,
  type MergeVisualizationProps,
} from './MergeVisualization';

// Hierarchy visualization for drill-down navigation
export {
  HierarchyVisualization,
  type HierarchyItem,
  type HierarchyLevel,
  type HierarchyState,
  type HierarchyVisualizationProps,
} from './HierarchyVisualization';

// Size calculation utilities
export {
  calculatePartSizes,
  verifyProportionality,
  calculateExpectedProportion,
  defaultSizeConfig,
  type PartSizeData,
  type PartVisualSize,
  type SizeCalculationConfig,
} from './sizeCalculations';

// Error boundary for 3D rendering failures
export {
  ErrorBoundary3D,
  type ErrorBoundary3DProps,
  isWebGLSupported,
  isWebGL2Supported,
} from './ErrorBoundary3D';

// 2D Fallback components for graceful degradation
export {
  PartsFallback2D,
  type PartsFallback2DProps,
  PipelineFallback2D,
  type PipelineFallback2DProps,
  MergeFallback2D,
  type MergeFallback2DProps,
} from './Fallback2D';

// 2D Parts visualization (clean alternative to 3D)
export {
  PartsVisualization2D,
} from './PartsVisualization2D';

// 2D Hierarchy visualization (clean alternative to 3D for databases/tables/partitions)
export {
  HierarchyVisualization2D,
} from './HierarchyVisualization2D';

