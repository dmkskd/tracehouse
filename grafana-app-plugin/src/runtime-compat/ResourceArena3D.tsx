import React, { useState } from 'react';
import type { ResourceArena3DProps as ArenaProps } from '@frontend/components/overview/ResourceArena3D';
import { useArenaEvents } from '@frontend/components/overview/useArenaEvents';
import { createRuntimeComponent } from './runtimeComponent';

export type ResourceArena3DProps = Omit<ArenaProps, 'eventState'>;

const RuntimeResourceArena3D = createRuntimeComponent<ArenaProps>(
  'resourceArena3D',
  'ResourceArena3D',
  'Loading 3D arena...',
  420
);

/** Collect in the main bundle: the isolated scene owns different contexts/stores. */
export const ResourceArena3D: React.FC<ResourceArena3DProps> = (props) => {
  const [visible, onVisibilityChange] = useState(true);
  const data = useArenaEvents(visible, props.hosts ?? []);
  return <RuntimeResourceArena3D {...props} eventState={{ data, visible, onVisibilityChange }} />;
};

export default ResourceArena3D;
