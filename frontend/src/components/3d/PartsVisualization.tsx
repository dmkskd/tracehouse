/**
 * PartsVisualization - Clean 3D visualization of database parts
 * 
 * Two-layer drill-down:
 * 1. Overview: Partitions as blocks (click to drill down)
 * 2. Detail: Parts within partition organized by level
 * 
 * All labels/info moved OUTSIDE the 3D scene - this component
 * communicates via callbacks to parent for UI updates.
 */

import React, { useMemo, useState, useCallback, useRef, useEffect } from 'react';
import { useFrame } from '@react-three/fiber';
import { Text } from '@react-three/drei';
import * as THREE from 'three';
import { usePerformanceMode } from './PerformanceContext';
import type { PartInfo } from '../../stores/databaseStore';
import { PartsScene } from '@tracehouse/ui-shared';

export type ColorScheme = 'size' | 'age' | 'level';

// Callback types for parent component
export interface PartitionSummary {
  id: string;
  parts: PartInfo[];
  totalBytes: number;
  partCount: number;
  unmergedCount: number;
  unmergedRatio: number;
  maxLevel: number;
  healthScore: number;
  healthStatus: 'good' | 'warning' | 'critical';
}

export interface PartsVisualizationCallbacks {
  onPartitionHover?: (partition: PartitionSummary | null) => void;
  onPartitionSelect?: (partition: PartitionSummary | null) => void;
  onPartHover?: (part: PartInfo | null) => void;
  onPartSelect?: (part: PartInfo | null) => void;
  onViewChange?: (view: 'overview' | 'detail', partition?: PartitionSummary) => void;
}

export interface PartVisualizationProps {
  parts: PartInfo[];
  onPartClick: (part: PartInfo) => void;
  colorScheme: ColorScheme;
  callbacks?: PartsVisualizationCallbacks;
  selectedPartitionId?: string | null;
}

// Health calculation
function calculateHealthScore(partCount: number, unmergedRatio: number): { score: number; status: 'good' | 'warning' | 'critical' } {
  let score = 100 - unmergedRatio * 60;
  if (partCount > 100) score -= 20;
  else if (partCount > 50) score -= 10;
  score = Math.max(0, Math.min(100, score));
  return { score, status: score >= 70 ? 'good' : score >= 40 ? 'warning' : 'critical' };
}

function createPartitionSummaries(parts: PartInfo[]): PartitionSummary[] {
  const groups = new Map<string, PartInfo[]>();
  for (const part of parts) {
    const id = part.partition_id || 'default';
    if (!groups.has(id)) groups.set(id, []);
    groups.get(id)!.push(part);
  }
  
  return [...groups.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([id, partitionParts]) => {
      const totalBytes = partitionParts.reduce((sum, p) => sum + p.bytes_on_disk, 0);
      const partCount = partitionParts.length;
      const unmergedCount = partitionParts.filter(p => p.level === 0).length;
      const unmergedRatio = partCount > 0 ? unmergedCount / partCount : 0;
      const maxLevel = Math.max(...partitionParts.map(p => p.level), 0);
      const { score, status } = calculateHealthScore(partCount, unmergedRatio);
      
      return { id, parts: partitionParts, totalBytes, partCount, unmergedCount, unmergedRatio, maxLevel, healthScore: score, healthStatus: status };
    });
}

function getHealthColor(status: 'good' | 'warning' | 'critical'): THREE.Color {
  return new THREE.Color(status === 'good' ? 0x22c55e : status === 'warning' ? 0xf59e0b : 0xef4444);
}

// ============================================================================
// PARTITION BLOCK (Overview)
// ============================================================================

// Shared geometry for partition block edges
const partitionBoxGeometry = new THREE.BoxGeometry(1, 1, 1);
const partitionEdgesGeometry = new THREE.EdgesGeometry(partitionBoxGeometry);

const PartitionBlock: React.FC<{
  partition: PartitionSummary;
  position: [number, number, number];
  scale: number;
  isHovered: boolean;
  onClick: () => void;
  onPointerOver: () => void;
  onPointerOut: () => void;
}> = ({ partition, position, scale, isHovered, onClick, onPointerOver, onPointerOut }) => {
  const groupRef = useRef<THREE.Group>(null);
  const color = getHealthColor(partition.healthStatus);
  const { enableAnimations } = usePerformanceMode();
  
  // Brighter edge color for wireframe effect
  const edgeColor = useMemo(() => {
    const c = color.clone();
    c.multiplyScalar(1.8);
    return c;
  }, [color]);
  
  useFrame((_, delta) => {
    if (!groupRef.current || !enableAnimations) return;
    const targetScaleX = isHovered ? 1.08 : 1;
    const targetScaleZ = isHovered ? 1.08 : 1;
    groupRef.current.scale.x = THREE.MathUtils.lerp(groupRef.current.scale.x, targetScaleX, delta * 8);
    groupRef.current.scale.z = THREE.MathUtils.lerp(groupRef.current.scale.z, targetScaleZ, delta * 8);
  });
  
  return (
    <group position={position}>
      {/* Scale the inner group for hover animation, outer group for base scale */}
      <group ref={groupRef} scale={[1, scale, 1]}>
        <mesh
          onClick={(e) => { e.stopPropagation(); onClick(); }}
          onPointerOver={(e) => { e.stopPropagation(); onPointerOver(); document.body.style.cursor = 'pointer'; }}
          onPointerOut={(e) => { e.stopPropagation(); onPointerOut(); document.body.style.cursor = 'auto'; }}
        >
          <boxGeometry args={[1, 1, 1]} />
          <meshStandardMaterial
            color={color}
            emissive={color}
            emissiveIntensity={isHovered ? 0.4 : 0.15}
            metalness={0.1}
            roughness={0.8}
            transparent
            opacity={isHovered ? 0.3 : 0.15}
            depthWrite={false}
          />
        </mesh>
        {/* Wireframe edges - using shared geometry */}
        <lineSegments geometry={partitionEdgesGeometry} renderOrder={999}>
          <lineBasicMaterial 
            color={edgeColor} 
            depthTest={false}
            transparent
            opacity={isHovered ? 1 : 0.9}
          />
        </lineSegments>
      </group>
      {/* Minimal label - just partition ID */}
      <Text
        position={[0, -scale / 2 - 0.2, 0]}
        fontSize={0.25}
        color="white"
        anchorX="center"
        anchorY="top"
        outlineWidth={0.02}
        outlineColor="black"
      >
        {partition.id}
      </Text>
    </group>
  );
};

// ============================================================================
// DETAIL VIEW (Parts within partition)
// ============================================================================

const PartitionDetailView: React.FC<{
  partition: PartitionSummary;
  onPartClick: (part: PartInfo) => void;
  onPartHover: (part: PartInfo | null) => void;
  hoveredPartName: string | null;
}> = ({ partition, onPartClick, onPartHover, hoveredPartName }) => {
  // Delegate to shared PartsScene component for level-based part rendering
  return (
    <PartsScene
      parts={partition.parts}
      hoveredPartName={hoveredPartName}
      onPartClick={onPartClick}
      onPartHover={onPartHover}
    />
  );
};

// ============================================================================
// MAIN COMPONENT
// ============================================================================

export const PartsVisualization: React.FC<PartVisualizationProps> = ({
  parts,
  onPartClick,
  colorScheme: _colorScheme,
  callbacks,
  selectedPartitionId,
}) => {
  const { maxElements, performanceMode } = usePerformanceMode();
  const [hoveredPartition, setHoveredPartition] = useState<string | null>(null);
  const [activePartition, setActivePartition] = useState<PartitionSummary | null>(null);
  const [hoveredPart, setHoveredPart] = useState<string | null>(null);
  
  // Limit parts
  const limitedParts = useMemo(() => {
    if (performanceMode && parts.length > maxElements) {
      return [...parts].sort((a, b) => b.bytes_on_disk - a.bytes_on_disk).slice(0, maxElements);
    }
    return parts;
  }, [parts, performanceMode, maxElements]);
  
  // Partition summaries
  const partitions = useMemo(() => createPartitionSummaries(limitedParts), [limitedParts]);
  
  // Handle external partition selection
  useEffect(() => {
    if (selectedPartitionId) {
      const partition = partitions.find(p => p.id === selectedPartitionId);
      if (partition) {
        setActivePartition(partition);
        callbacks?.onViewChange?.('detail', partition);
      }
    } else {
      setActivePartition(null);
      callbacks?.onViewChange?.('overview');
    }
  }, [selectedPartitionId, partitions, callbacks]);
  
  // Partition visuals
  const partitionVisuals = useMemo(() => {
    const maxBytes = Math.max(...partitions.map(p => p.totalBytes), 1);
    const cols = Math.ceil(Math.sqrt(partitions.length));
    const spacing = 2.5;
    
    return partitions.map((partition, i) => {
      const row = Math.floor(i / cols);
      const col = i % cols;
      const scale = 0.8 + (partition.totalBytes / maxBytes) * 2;
      
      return {
        partition,
        position: [col * spacing - (cols - 1) * spacing / 2, scale / 2, row * spacing - (Math.ceil(partitions.length / cols) - 1) * spacing / 2] as [number, number, number],
        scale,
      };
    });
  }, [partitions]);
  
  // Handlers
  const handlePartitionClick = useCallback((partition: PartitionSummary) => {
    setActivePartition(partition);
    callbacks?.onPartitionSelect?.(partition);
    callbacks?.onViewChange?.('detail', partition);
  }, [callbacks]);
  
  const handlePartitionHover = useCallback((partition: PartitionSummary | null) => {
    setHoveredPartition(partition?.id || null);
    callbacks?.onPartitionHover?.(partition);
  }, [callbacks]);
  
  const handlePartClick = useCallback((part: PartInfo) => {
    onPartClick(part);
    callbacks?.onPartSelect?.(part);
  }, [onPartClick, callbacks]);
  
  const handlePartHover = useCallback((part: PartInfo | null) => {
    setHoveredPart(part?.name || null);
    callbacks?.onPartHover?.(part);
  }, [callbacks]);
  
  if (parts.length === 0) {
    return (
      <Text position={[0, 0, 0]} fontSize={0.4} color="white" anchorX="center" anchorY="middle">
        No parts
      </Text>
    );
  }
  
  // DETAIL VIEW
  if (activePartition) {
    return (
      <PartitionDetailView
        partition={activePartition}
        onPartClick={handlePartClick}
        onPartHover={handlePartHover}
        hoveredPartName={hoveredPart}
      />
    );
  }
  
  // OVERVIEW
  return (
    <group>
      {partitionVisuals.map(({ partition, position, scale }) => (
        <PartitionBlock
          key={partition.id}
          partition={partition}
          position={position}
          scale={scale}
          isHovered={hoveredPartition === partition.id}
          onClick={() => handlePartitionClick(partition)}
          onPointerOver={() => handlePartitionHover(partition)}
          onPointerOut={() => handlePartitionHover(null)}
        />
      ))}
    </group>
  );
};

export default PartsVisualization;
export { createPartitionSummaries };
