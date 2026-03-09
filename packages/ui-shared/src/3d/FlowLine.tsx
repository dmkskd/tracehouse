/**
 * FlowLine - Renders a 3D line connecting a source part to a result part
 *
 * Used to visualize active merge operations by drawing flow lines from
 * each source part to the result part ghost.
 */

import React, { useMemo } from 'react';
import * as THREE from 'three';

export interface FlowLineProps {
  /** Start position (source part) */
  from: [number, number, number];
  /** End position (result part ghost) */
  to: [number, number, number];
  /** Line color (default: cyan) */
  color?: THREE.Color | string | number;
  /** Line opacity (default 0.6) */
  opacity?: number;
  /** Line width — note: WebGL line width is limited to 1 on most platforms */
  lineWidth?: number;
}

export const FlowLine: React.FC<FlowLineProps> = ({
  from,
  to,
  color = 0x22d3ee,
  opacity = 0.6,
}) => {
  const points = useMemo(
    () => [new THREE.Vector3(...from), new THREE.Vector3(...to)],
    [from, to]
  );

  const geometry = useMemo(() => {
    const geo = new THREE.BufferGeometry().setFromPoints(points);
    return geo;
  }, [points]);

  const threeColor = useMemo(
    () =>
      typeof color === 'object' && color instanceof THREE.Color
        ? color
        : new THREE.Color(color),
    [color]
  );

  return (
    <line>
      <primitive object={geometry} attach="geometry" />
      <lineBasicMaterial
        color={threeColor}
        transparent
        opacity={opacity}
      />
    </line>
  );
};
