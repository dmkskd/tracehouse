import type React from 'react';
import { createRuntimeComponent } from './runtimeComponent';

type TimelineChart3DProps = React.ComponentProps<typeof import('../../../frontend/src/components/timeline/TimelineChart3D')['TimelineChart3D']>;
type TimelineChart3DSurfaceProps = React.ComponentProps<typeof import('../../../frontend/src/components/timeline/TimelineChart3DSurface')['TimelineChart3DSurface']>;

export const TimelineChart3D = createRuntimeComponent<TimelineChart3DProps>('timeline3d', 'TimelineChart3D', 'Loading 3D timeline...');
export const TimelineChart3DSurface = createRuntimeComponent<TimelineChart3DSurfaceProps>('timeline3d', 'TimelineChart3DSurface', 'Loading 3D timeline surface...');
