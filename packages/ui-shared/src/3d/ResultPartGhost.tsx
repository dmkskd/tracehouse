/**
 * ResultPartGhost - Ghost representation of a merge result part
 *
 * Renders a semi-transparent, pulsing box that represents the result part
 * being created by an active merge operation. The ghost grows as the merge
 * progresses.
 *
 * Extracted from MergeVisualization.tsx ResultPart component.
 */

import React, { useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';

export interface ResultPartGhostProps {
  /** Position in 3D space */
  position: [number, number, number];
  /** Merge progress (0 to 1) */
  progress: number;
  /** Base visual scale */
  scale: number;
  /** Ghost color */
  color: THREE.Color | string | number;
  /** Whether to animate the ghost (default true) */
  enableAnimation?: boolean;
  /** Base opacity (default 0.5) */
  opacity?: number;
}

export const ResultPartGhost: React.FC<ResultPartGhostProps> = ({
  position,
  progress,
  scale,
  color,
  enableAnimation = true,
  opacity = 0.5,
}) => {
  const meshRef = useRef<THREE.Mesh>(null);

  const threeColor =
    typeof color === 'object' && color instanceof THREE.Color
      ? color
      : new THREE.Color(color);

  useFrame((_, delta) => {
    if (!meshRef.current || !enableAnimation) return;

    // Grow based on progress
    const targetScale = (0.2 + progress * 0.8) * scale;
    const currentScale = meshRef.current.scale.x;
    const newScale = THREE.MathUtils.lerp(currentScale, targetScale, delta * 2);
    meshRef.current.scale.setScalar(newScale);

    // Pulse effect that diminishes as merge completes
    const time = Date.now() * 0.002;
    const pulse = 1 + Math.sin(time) * 0.05 * (1 - progress);
    meshRef.current.scale.multiplyScalar(pulse);
  });

  const initialScale = enableAnimation
    ? 0.2 * scale
    : (0.2 + progress * 0.8) * scale;

  return (
    <mesh ref={meshRef} position={position} scale={initialScale}>
      <boxGeometry args={[1, 1, 1]} />
      <meshStandardMaterial
        color={threeColor}
        emissive={threeColor}
        emissiveIntensity={0.2 + progress * 0.3}
        metalness={0.4}
        roughness={0.5}
        transparent
        opacity={opacity}
      />
    </mesh>
  );
};
