/**
 * GlassBox - Reusable glass-like 3D box for representing database parts
 *
 * Extracted from PartsVisualization.tsx and Scene3DWrapper.tsx.
 * Renders a box geometry with wireframe edges and transparent fill for a cyberpunk aesthetic.
 */

import React, { useRef, useMemo, useEffect } from 'react';
import { useFrame, type ThreeEvent } from '@react-three/fiber';
import * as THREE from 'three';

export interface GlassBoxProps {
  /** Position in 3D space */
  position: [number, number, number];
  /** Visual scale factor (uniform or per-axis) */
  scale: number | [number, number, number];
  /** Box color */
  color: THREE.Color | string | number;
  /** Whether the box is currently hovered */
  isHovered?: boolean;
  /** Whether the box is currently highlighted (e.g. part of active merge) */
  isHighlighted?: boolean;
  /** Emissive intensity when not hovered (default 0.1) */
  emissiveIntensity?: number;
  /** Emissive intensity when hovered (default 0.35) */
  hoveredEmissiveIntensity?: number;
  /** Opacity (default 0.15 for transparent look) */
  opacity?: number;
  /** Whether to enable hover scale animation (default true) */
  enableHoverAnimation?: boolean;
  /** Hover scale multiplier (default 1.12) */
  hoverScaleMultiplier?: number;
  /** Animation lerp speed (default 8) */
  animationSpeed?: number;
  /** Metalness (default 0.2) */
  metalness?: number;
  /** Roughness (default 0.7) */
  roughness?: number;
  /** Click handler */
  onClick?: (event: ThreeEvent<MouseEvent>) => void;
  /** Pointer over handler */
  onPointerOver?: (event: ThreeEvent<PointerEvent>) => void;
  /** Pointer out handler */
  onPointerOut?: (event: ThreeEvent<PointerEvent>) => void;
}

// Shared box geometry for edges - created once
const sharedBoxGeometry = new THREE.BoxGeometry(1, 1, 1);
const sharedEdgesGeometry = new THREE.EdgesGeometry(sharedBoxGeometry);

export const GlassBox: React.FC<GlassBoxProps> = ({
  position,
  scale,
  color,
  isHovered = false,
  isHighlighted = false,
  emissiveIntensity = 0.1,
  hoveredEmissiveIntensity = 0.35,
  opacity = 0.15,
  enableHoverAnimation = true,
  hoverScaleMultiplier = 1.12,
  animationSpeed = 8,
  metalness = 0.2,
  roughness = 0.7,
  onClick,
  onPointerOver,
  onPointerOut,
}) => {
  const groupRef = useRef<THREE.Group>(null);
  const meshRef = useRef<THREE.Mesh>(null);
  const edgesRef = useRef<THREE.LineSegments>(null);
  
  const threeColor = useMemo(() => 
    typeof color === 'object' && color instanceof THREE.Color
      ? color
      : new THREE.Color(color),
    [color]
  );

  const baseScale = typeof scale === 'number' ? scale : 1;

  // Brighter edge color for wireframe effect - use white/bright for visibility
  const edgeColor = useMemo(() => {
    const c = threeColor.clone();
    // Make edges much brighter - almost white
    c.r = Math.min(1, c.r + 0.5);
    c.g = Math.min(1, c.g + 0.5);
    c.b = Math.min(1, c.b + 0.5);
    return c;
  }, [threeColor]);

  // Set up edges material
  useEffect(() => {
    if (edgesRef.current) {
      const mat = edgesRef.current.material as THREE.LineBasicMaterial;
      mat.color = edgeColor;
      mat.opacity = isHovered || isHighlighted ? 1 : 0.9;
      mat.needsUpdate = true;
    }
  }, [edgeColor, isHovered, isHighlighted]);

  useFrame((_, delta) => {
    if (!groupRef.current || !enableHoverAnimation) return;
    const targetScale = isHovered ? baseScale * hoverScaleMultiplier : baseScale;
    const current = groupRef.current.scale.x;
    const newScale = THREE.MathUtils.lerp(current, targetScale, delta * animationSpeed);
    groupRef.current.scale.setScalar(newScale);
  });

  const currentEmissive = isHovered || isHighlighted
    ? hoveredEmissiveIntensity
    : emissiveIntensity;

  return (
    <group ref={groupRef} position={position} scale={baseScale}>
      {/* Transparent fill */}
      <mesh
        ref={meshRef}
        onClick={onClick}
        onPointerOver={onPointerOver}
        onPointerOut={onPointerOut}
      >
        <boxGeometry args={[1, 1, 1]} />
        <meshStandardMaterial
          color={threeColor}
          emissive={threeColor}
          emissiveIntensity={currentEmissive}
          metalness={metalness}
          roughness={roughness}
          transparent
          opacity={isHovered || isHighlighted ? opacity * 2 : opacity}
          depthWrite={false}
          side={THREE.DoubleSide}
        />
      </mesh>
      {/* Wireframe edges - rendered on top with no depth testing */}
      <lineSegments ref={edgesRef} geometry={sharedEdgesGeometry} renderOrder={9999}>
        <lineBasicMaterial 
          color={edgeColor}
          depthTest={false}
          depthWrite={false}
          transparent
          opacity={0.9}
          toneMapped={false}
        />
      </lineSegments>
    </group>
  );
};
