import { withRuntimeTheme } from './runtimeTheme';
import { TimelineChart3D as TimelineChart3DComponent } from '../../../frontend/src/components/timeline/TimelineChart3D';
import { TimelineChart3DSurface as TimelineChart3DSurfaceComponent } from '../../../frontend/src/components/timeline/TimelineChart3DSurface';

export const TimelineChart3D = withRuntimeTheme(TimelineChart3DComponent);
export const TimelineChart3DSurface = withRuntimeTheme(TimelineChart3DSurfaceComponent);
