import { withRuntimeTheme } from './runtimeTheme';
import { ResourceSurface as ResourceSurfaceComponent } from '../../../frontend/src/components/analytics/ResourceSurface';
import { PatternSurface as PatternSurfaceComponent } from '../../../frontend/src/components/analytics/PatternSurface';
import { StressSurface as StressSurfaceComponent } from '../../../frontend/src/components/analytics/StressSurface';
import { Chart3DCanvas as Chart3DCanvasComponent } from '../../../frontend/src/components/analytics/charts3d';

export const ResourceSurface = withRuntimeTheme(ResourceSurfaceComponent);
export const PatternSurface = withRuntimeTheme(PatternSurfaceComponent);
export const StressSurface = withRuntimeTheme(StressSurfaceComponent);
export const Chart3DCanvas = withRuntimeTheme(Chart3DCanvasComponent);
