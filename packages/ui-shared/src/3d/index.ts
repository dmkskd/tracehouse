/**
 * Shared 3D visualization components for TraceHouse
 *
 * React Three Fiber components for rendering database parts in 3D,
 * with merge operation overlays.
 */

// Size calculation utilities
export {
  calculatePartSizes,
  verifyProportionality,
  calculateExpectedProportion,
  defaultSizeConfig,
  type PartSizeData,
  type PartVisualSize,
  type SizeCalculationConfig,
} from './sizeCalculations.js';

// Glass box component
export { GlassBox, type GlassBoxProps } from './GlassBox.js';

// Level-based lane layout
export { MergeLanes, type MergeLanesProps } from './MergeLanes.js';

// Merge flow line
export { FlowLine, type FlowLineProps } from './FlowLine.js';

// Result part ghost
export { ResultPartGhost, type ResultPartGhostProps } from './ResultPartGhost.js';

// Unified parts scene
export { PartsScene, type PartsSceneProps } from './PartsScene.js';
