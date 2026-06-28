import type React from 'react';
import type { ResourceSurfaceProps } from '../../../frontend/src/components/analytics/ResourceSurface';
import type { PatternSurfaceProps } from '../../../frontend/src/components/analytics/PatternSurface';
import type { StressSurfaceProps } from '../../../frontend/src/components/analytics/StressSurface';
import { createRuntimeComponent } from './runtimeComponent';

type Chart3DCanvasProps = React.ComponentProps<typeof import('../../../frontend/src/components/analytics/charts3d')['Chart3DCanvas']>;

export type { ResourceSurfaceProps } from '../../../frontend/src/components/analytics/ResourceSurface';
export type { PatternSurfaceProps } from '../../../frontend/src/components/analytics/PatternSurface';
export type { StressSurfaceProps } from '../../../frontend/src/components/analytics/StressSurface';

export const ResourceSurface = createRuntimeComponent<ResourceSurfaceProps>('analytics3d', 'ResourceSurface', 'Loading resource surface...');
export const PatternSurface = createRuntimeComponent<PatternSurfaceProps>('analytics3d', 'PatternSurface', 'Loading pattern surface...');
export const StressSurface = createRuntimeComponent<StressSurfaceProps>('analytics3d', 'StressSurface', 'Loading stress surface...');
export const Chart3DCanvas = createRuntimeComponent<Chart3DCanvasProps>('analytics3d', 'Chart3DCanvas', 'Loading 3D chart...');
