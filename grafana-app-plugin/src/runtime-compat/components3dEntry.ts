import { withRuntimeTheme } from './runtimeTheme';
import { Scene3D as Scene3DComponent } from '../../../frontend/src/components/3d/Scene3D';
import { PartsVisualization as PartsVisualizationComponent } from '../../../frontend/src/components/3d/PartsVisualization';
export { createPartitionSummaries } from '../../../frontend/src/components/3d/PartsVisualization';
import { PipelineVisualization as PipelineVisualizationComponent } from '../../../frontend/src/components/3d/PipelineVisualization';
export { parsePipelineOutput } from '../../../frontend/src/components/3d/PipelineVisualization';
import { MergeVisualization as MergeVisualizationComponent } from '../../../frontend/src/components/3d/MergeVisualization';
import { HierarchyVisualization as HierarchyVisualizationComponent } from '../../../frontend/src/components/3d/HierarchyVisualization';

export const Scene3D = withRuntimeTheme(Scene3DComponent);
export const PartsVisualization = withRuntimeTheme(PartsVisualizationComponent);
export const PipelineVisualization = withRuntimeTheme(PipelineVisualizationComponent);
export const MergeVisualization = withRuntimeTheme(MergeVisualizationComponent);
export const HierarchyVisualization = withRuntimeTheme(HierarchyVisualizationComponent);
