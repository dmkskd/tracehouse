/**
 * MergeVisualization - 3D visualization of merge operations
 * 
 * Renders active merge operations as animated 3D elements showing source parts
 * combining into a result part. Displays merge metrics on hover and supports
 * click interaction for merge details.
 */

import React, { useMemo, useState, useCallback, useRef, useEffect } from 'react';
import { useFrame } from '@react-three/fiber';
import { Html } from '@react-three/drei';
import { SafeText as Text } from '@tracehouse/ui-shared';
import * as THREE from 'three';
import { usePerformanceMode } from './PerformanceContext';
import type { MergeInfo } from '../../stores/mergeStore';
import { formatBytes, formatDuration, formatNumber } from '../../utils/formatters';

/**
 * Props for MergeVisualization component
 */
export interface MergeVisualizationProps {
  /** Array of active merges to visualize */
  merges: MergeInfo[];
  /** Callback when a merge is clicked */
  onMergeClick: (merge: MergeInfo) => void;
}

/**
 * Internal merge data with calculated visual properties
 */
interface MergeWithVisuals extends MergeInfo {
  /** Unique identifier for the merge */
  id: string;
  /** Position of the merge group in 3D space */
  position: [number, number, number];
  /** Color based on progress */
  color: THREE.Color;
  /** Visual scale based on total size */
  visualScale: number;
}

/**
 * Color palette for merge progress
 * Progress: Red (0%) -> Yellow (50%) -> Green (100%)
 */
const PROGRESS_COLORS = {
  start: new THREE.Color(0xef4444),   // Red (0%)
  mid: new THREE.Color(0xeab308),     // Yellow (50%)
  end: new THREE.Color(0x22c55e),     // Green (100%)
};

/**
 * Source part colors
 */
const SOURCE_PART_COLOR = new THREE.Color(0x3b82f6);  // Blue
const RESULT_PART_COLOR = new THREE.Color(0x8b5cf6); // Purple

/**
 * Calculate color based on progress (0-1)
 */
function getProgressColor(progress: number): THREE.Color {
  const color = new THREE.Color();
  
  if (progress < 0.5) {
    // Interpolate between start and mid
    color.lerpColors(PROGRESS_COLORS.start, PROGRESS_COLORS.mid, progress * 2);
  } else {
    // Interpolate between mid and end
    color.lerpColors(PROGRESS_COLORS.mid, PROGRESS_COLORS.end, (progress - 0.5) * 2);
  }
  
  return color;
}

/**
 * Calculate visual scale based on total size
 */
function calculateVisualScale(
  totalBytes: number,
  allMerges: MergeInfo[]
): number {
  const maxBytes = Math.max(...allMerges.map(m => m.total_size_bytes_compressed), 1);
  const minScale = 0.5;
  const maxScale = 2.0;
  
  // Logarithmic scaling for better visual distribution
  const normalizedSize = Math.log10(totalBytes + 1) / Math.log10(maxBytes + 1);
  return minScale + normalizedSize * (maxScale - minScale);
}

/**
 * Calculate grid layout positions for merges
 */
function calculateMergePositions(
  merges: MergeInfo[]
): [number, number, number][] {
  const positions: [number, number, number][] = [];
  const gridSize = Math.ceil(Math.sqrt(merges.length));
  const spacing = 6; // Larger spacing for merge groups
  
  for (let i = 0; i < merges.length; i++) {
    const row = Math.floor(i / gridSize);
    const col = i % gridSize;
    
    // Center the grid
    const offsetX = (gridSize - 1) * spacing / 2;
    const offsetZ = (gridSize - 1) * spacing / 2;
    
    positions.push([
      col * spacing - offsetX,
      0,
      row * spacing - offsetZ,
    ]);
  }
  
  return positions;
}

/**
 * Source Part Block component - represents a source part being merged
 */
interface SourcePartProps {
  index: number;
  totalParts: number;
  progress: number;
  scale: number;
  enableAnimations: boolean;
}

const SourcePart: React.FC<SourcePartProps> = ({
  index,
  totalParts,
  progress,
  scale,
  enableAnimations,
}) => {
  const meshRef = useRef<THREE.Mesh>(null);
  
  // Calculate position in a circle around the center
  const angle = (index / totalParts) * Math.PI * 2;
  const radius = 1.5 * scale;
  const baseX = Math.cos(angle) * radius;
  const baseZ = Math.sin(angle) * radius;
  
  // Animate source parts moving toward center as progress increases
  useFrame((_, delta) => {
    if (!meshRef.current || !enableAnimations) return;
    
    // Move toward center based on progress
    const targetX = baseX * (1 - progress);
    const targetZ = baseZ * (1 - progress);
    
    meshRef.current.position.x = THREE.MathUtils.lerp(
      meshRef.current.position.x,
      targetX,
      delta * 2
    );
    meshRef.current.position.z = THREE.MathUtils.lerp(
      meshRef.current.position.z,
      targetZ,
      delta * 2
    );
    
    // Shrink as progress increases
    const targetScale = (0.3 + 0.2 * (1 - progress)) * scale;
    meshRef.current.scale.setScalar(
      THREE.MathUtils.lerp(meshRef.current.scale.x, targetScale, delta * 2)
    );
    
    // Fade opacity as progress increases
    const material = meshRef.current.material as THREE.MeshStandardMaterial;
    if (material) {
      material.opacity = THREE.MathUtils.lerp(
        material.opacity,
        0.3 + 0.7 * (1 - progress),
        delta * 2
      );
    }
  });
  
  // Initial position
  const initialX = enableAnimations ? baseX : baseX * (1 - progress);
  const initialZ = enableAnimations ? baseZ : baseZ * (1 - progress);
  const initialScale = enableAnimations ? 0.5 * scale : (0.3 + 0.2 * (1 - progress)) * scale;
  
  return (
    <mesh
      ref={meshRef}
      position={[initialX, 0.5, initialZ]}
      scale={initialScale}
    >
      <boxGeometry args={[1, 1, 1]} />
      <meshStandardMaterial
        color={SOURCE_PART_COLOR}
        transparent
        opacity={enableAnimations ? 1 : 0.3 + 0.7 * (1 - progress)}
        metalness={0.3}
        roughness={0.7}
      />
    </mesh>
  );
};

/**
 * Result Part component - represents the result part being created
 */
interface ResultPartProps {
  progress: number;
  scale: number;
  color: THREE.Color;
  enableAnimations: boolean;
}

const ResultPart: React.FC<ResultPartProps> = ({
  progress,
  scale,
  color,
  enableAnimations,
}) => {
  const meshRef = useRef<THREE.Mesh>(null);
  
  // Animate result part growing as progress increases
  useFrame((_, delta) => {
    if (!meshRef.current || !enableAnimations) return;
    
    // Grow based on progress
    const targetScale = 0.2 + progress * 0.8;
    const currentScale = meshRef.current.scale.x / scale;
    const newScale = THREE.MathUtils.lerp(currentScale, targetScale, delta * 2);
    meshRef.current.scale.setScalar(newScale * scale);
    
    // Pulse effect
    const time = Date.now() * 0.002;
    const pulse = 1 + Math.sin(time) * 0.05 * (1 - progress);
    meshRef.current.scale.multiplyScalar(pulse);
  });
  
  const initialScale = enableAnimations ? 0.2 * scale : (0.2 + progress * 0.8) * scale;
  
  return (
    <mesh
      ref={meshRef}
      position={[0, 0.5, 0]}
      scale={initialScale}
    >
      <boxGeometry args={[1, 1, 1]} />
      <meshStandardMaterial
        color={color}
        emissive={color}
        emissiveIntensity={0.2 + progress * 0.3}
        metalness={0.4}
        roughness={0.5}
      />
    </mesh>
  );
};

/**
 * Progress Ring component - shows merge progress as a ring
 */
interface ProgressRingProps {
  progress: number;
  scale: number;
  color: THREE.Color;
}

const ProgressRing: React.FC<ProgressRingProps> = ({ progress, scale, color }) => {
  const ringRef = useRef<THREE.Mesh>(null);
  
  // Create ring geometry with progress
  const ringGeometry = useMemo(() => {
    const segments = 64;
    return new THREE.RingGeometry(
      1.8 * scale,
      2.0 * scale,
      segments,
      1,
      0,
      Math.PI * 2 * progress
    );
  }, [progress, scale]);
  
  // Cleanup geometry on unmount or when it changes
  useEffect(() => {
    return () => {
      ringGeometry.dispose();
    };
  }, [ringGeometry]);
  
  return (
    <mesh
      ref={ringRef}
      rotation={[-Math.PI / 2, 0, -Math.PI / 2]}
      position={[0, 0.01, 0]}
    >
      <primitive object={ringGeometry} attach="geometry" />
      <meshBasicMaterial
        color={color}
        side={THREE.DoubleSide}
        transparent
        opacity={0.8}
      />
    </mesh>
  );
};

/**
 * Merge Group component - represents a single merge operation
 */
interface MergeGroupProps {
  merge: MergeWithVisuals;
  isHovered: boolean;
  isSelected: boolean;
  onClick: () => void;
  onPointerOver: () => void;
  onPointerOut: () => void;
  enableAnimations: boolean;
}

const MergeGroup: React.FC<MergeGroupProps> = ({
  merge,
  isHovered,
  isSelected,
  onClick,
  onPointerOver,
  onPointerOut,
  enableAnimations,
}) => {
  const groupRef = useRef<THREE.Group>(null);
  
  // Animate hover effect
  useFrame((_, delta) => {
    if (!groupRef.current || !enableAnimations) return;
    
    const targetY = isHovered || isSelected ? 0.3 : 0;
    groupRef.current.position.y = THREE.MathUtils.lerp(
      groupRef.current.position.y,
      targetY,
      delta * 5
    );
  });
  
  // Limit source parts displayed for performance
  const displayedParts = Math.min(merge.num_parts, 8);
  
  return (
    <group
      ref={groupRef}
      position={merge.position}
      onClick={(e) => {
        e.stopPropagation();
        onClick();
      }}
      onPointerOver={(e) => {
        e.stopPropagation();
        onPointerOver();
        document.body.style.cursor = 'pointer';
      }}
      onPointerOut={(e) => {
        e.stopPropagation();
        onPointerOut();
        document.body.style.cursor = 'auto';
      }}
    >
      {/* Source parts */}
      {Array.from({ length: displayedParts }).map((_, index) => (
        <SourcePart
          key={`source-${index}`}
          index={index}
          totalParts={displayedParts}
          progress={merge.progress}
          scale={merge.visualScale}
          enableAnimations={enableAnimations}
        />
      ))}
      
      {/* Result part */}
      <ResultPart
        progress={merge.progress}
        scale={merge.visualScale}
        color={merge.color}
        enableAnimations={enableAnimations}
      />
      
      {/* Progress ring */}
      <ProgressRing
        progress={merge.progress}
        scale={merge.visualScale}
        color={merge.color}
      />
      
      {/* Selection/hover indicator */}
      {(isHovered || isSelected) && (
        <mesh position={[0, 0.02, 0]} rotation={[-Math.PI / 2, 0, 0]}>
          <ringGeometry args={[2.2 * merge.visualScale, 2.4 * merge.visualScale, 32]} />
          <meshBasicMaterial
            color={isSelected ? 0x22c55e : 0xffffff}
            transparent
            opacity={0.5}
          />
        </mesh>
      )}
    </group>
  );
};

/**
 * Merge Label component showing table name
 */
interface MergeLabelProps {
  merge: MergeWithVisuals;
  visible: boolean;
}

const MergeLabel: React.FC<MergeLabelProps> = ({ merge, visible }) => {
  if (!visible) return null;
  
  const labelPosition: [number, number, number] = [
    merge.position[0],
    merge.position[1] + 2.5 * merge.visualScale,
    merge.position[2],
  ];
  
  return (
    <Text
      position={labelPosition}
      fontSize={0.3}
      color="white"
      anchorX="center"
      anchorY="bottom"
      outlineWidth={0.02}
      outlineColor="black"
    >
      {`${merge.database}.${merge.table}`}
    </Text>
  );
};

/**
 * Merge Tooltip component showing detailed metrics on hover
 */
interface MergeTooltipProps {
  merge: MergeWithVisuals;
  visible: boolean;
}

const MergeTooltip: React.FC<MergeTooltipProps> = ({ merge, visible }) => {
  if (!visible) return null;
  
  const tooltipPosition: [number, number, number] = [
    merge.position[0],
    merge.position[1] + 3 * merge.visualScale,
    merge.position[2],
  ];
  
  const progressPercent = (merge.progress * 100).toFixed(1);
  
  return (
    <Html position={tooltipPosition} center style={{ pointerEvents: 'none' }}>
      <div className="bg-gray-900 text-white text-xs p-3 rounded-lg shadow-lg min-w-[200px]">
        <div className="font-bold mb-2 text-purple-300">
          {merge.database}.{merge.table}
        </div>
        
        {/* Progress bar */}
        <div className="mb-2">
          <div className="flex justify-between mb-1">
            <span>Progress</span>
            <span className="text-green-400">{progressPercent}%</span>
          </div>
          <div className="w-full bg-gray-700 rounded-full h-2">
            <div
              className="h-2 rounded-full transition-all duration-300"
              style={{
                width: `${merge.progress * 100}%`,
                backgroundColor: `#${merge.color.getHexString()}`,
              }}
            />
          </div>
        </div>
        
        {/* Metrics */}
        <div className="space-y-1 border-t border-gray-700 pt-2">
          <div className="flex justify-between">
            <span className="text-gray-400">Elapsed:</span>
            <span>{formatDuration(merge.elapsed)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-400">Parts:</span>
            <span>{merge.num_parts}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-400">Size:</span>
            <span>{formatBytes(merge.total_size_bytes_compressed)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-400">Rows Read:</span>
            <span>{formatNumber(merge.rows_read)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-400">Rows Written:</span>
            <span>{formatNumber(merge.rows_written)}</span>
          </div>
        </div>
        
        {/* Result part name */}
        <div className="border-t border-gray-700 pt-2 mt-2">
          <div className="text-gray-400 text-[10px]">Result Part:</div>
          <div className="text-blue-300 text-[10px] break-all">
            {merge.result_part_name}
          </div>
        </div>
      </div>
    </Html>
  );
};

/**
 * Legend component showing color meanings
 */
const Legend: React.FC = () => {
  return (
    <Html position={[-8, 3, 0]}>
      <div className="bg-gray-800/90 text-white text-xs p-2 rounded pointer-events-none">
        <div className="font-bold mb-2">Merge Progress</div>
        <div className="flex items-center gap-2 mb-1">
          <div
            className="w-3 h-3 rounded"
            style={{ backgroundColor: `#${PROGRESS_COLORS.start.getHexString()}` }}
          />
          <span>0% - Starting</span>
        </div>
        <div className="flex items-center gap-2 mb-1">
          <div
            className="w-3 h-3 rounded"
            style={{ backgroundColor: `#${PROGRESS_COLORS.mid.getHexString()}` }}
          />
          <span>50% - In Progress</span>
        </div>
        <div className="flex items-center gap-2 mb-2">
          <div
            className="w-3 h-3 rounded"
            style={{ backgroundColor: `#${PROGRESS_COLORS.end.getHexString()}` }}
          />
          <span>100% - Completing</span>
        </div>
        <div className="border-t border-gray-600 pt-2">
          <div className="flex items-center gap-2 mb-1">
            <div
              className="w-3 h-3 rounded"
              style={{ backgroundColor: `#${SOURCE_PART_COLOR.getHexString()}` }}
            />
            <span>Source Parts</span>
          </div>
          <div className="flex items-center gap-2">
            <div
              className="w-3 h-3 rounded"
              style={{ backgroundColor: `#${RESULT_PART_COLOR.getHexString()}` }}
            />
            <span>Result Part</span>
          </div>
        </div>
      </div>
    </Html>
  );
};

/**
 * MergeVisualization - Main component for 3D merge visualization
 * 
 * Renders active merge operations as animated 3D elements with:
 * - Source parts moving toward center as merge progresses
 * - Result part growing as merge completes
 * - Progress ring showing completion percentage
 * - Color coding based on progress
 * - Hover tooltips with merge metrics
 * - Click interaction for merge details
 * 
 * @example
 * ```tsx
 * <Scene3D config={config}>
 *   <MergeVisualization
 *     merges={activeMerges}
 *     onMergeClick={(merge) => console.log('Clicked:', merge)}
 *   />
 * </Scene3D>
 * ```
 */
export const MergeVisualization: React.FC<MergeVisualizationProps> = ({
  merges,
  onMergeClick,
}) => {
  const { performanceMode, enableAnimations, maxElements } = usePerformanceMode();
  const [hoveredMerge, setHoveredMerge] = useState<string | null>(null);
  const [selectedMerge, setSelectedMerge] = useState<string | null>(null);
  
  // Limit merges in performance mode
  const limitedMerges = useMemo(() => {
    if (performanceMode && merges.length > maxElements) {
      // Sort by progress and take the most active merges
      return [...merges]
        .sort((a, b) => b.progress - a.progress)
        .slice(0, maxElements);
    }
    return merges;
  }, [merges, performanceMode, maxElements]);
  
  // Calculate positions for grid layout
  const positions = useMemo(() => {
    return calculateMergePositions(limitedMerges);
  }, [limitedMerges]);
  
  // Combine merges with visual properties
  const mergesWithVisuals: MergeWithVisuals[] = useMemo(() => {
    return limitedMerges.map((merge, index) => ({
      ...merge,
      id: `merge-${merge.database}-${merge.table}-${index}`,
      position: positions[index],
      color: getProgressColor(merge.progress),
      visualScale: calculateVisualScale(merge.total_size_bytes_compressed, limitedMerges),
    }));
  }, [limitedMerges, positions]);
  
  // Handle merge click
  const handleMergeClick = useCallback((merge: MergeInfo, id: string) => {
    setSelectedMerge(prev => prev === id ? null : id);
    onMergeClick(merge);
  }, [onMergeClick]);
  
  // Handle hover
  const handlePointerOver = useCallback((mergeId: string) => {
    setHoveredMerge(mergeId);
  }, []);
  
  const handlePointerOut = useCallback(() => {
    setHoveredMerge(null);
  }, []);
  
  // Show message if no merges
  if (merges.length === 0) {
    return (
      <group>
        <Text
          position={[0, 1, 0]}
          fontSize={0.5}
          color="white"
          anchorX="center"
          anchorY="middle"
        >
          No active merges
        </Text>
        <Text
          position={[0, 0.3, 0]}
          fontSize={0.25}
          color="#9ca3af"
          anchorX="center"
          anchorY="middle"
        >
          Merge operations will appear here when active
        </Text>
      </group>
    );
  }
  
  return (
    <group>
      {/* Legend */}
      <Legend />
      
      {/* Merge groups */}
      {mergesWithVisuals.map((merge) => (
        <group key={merge.id}>
          <MergeGroup
            merge={merge}
            isHovered={hoveredMerge === merge.id}
            isSelected={selectedMerge === merge.id}
            onClick={() => handleMergeClick(merge, merge.id)}
            onPointerOver={() => handlePointerOver(merge.id)}
            onPointerOut={handlePointerOut}
            enableAnimations={enableAnimations}
          />
          
          {/* Show label on hover */}
          <MergeLabel
            merge={merge}
            visible={hoveredMerge === merge.id || selectedMerge === merge.id}
          />
          
          {/* Show tooltip on hover/selection */}
          <MergeTooltip
            merge={merge}
            visible={hoveredMerge === merge.id || selectedMerge === merge.id}
          />
        </group>
      ))}
      
      {/* Performance mode indicator */}
      {performanceMode && merges.length > maxElements && (
        <Html position={[0, -2, 0]} center>
          <div className="bg-yellow-500/80 text-yellow-900 text-xs px-2 py-1 rounded">
            Showing {maxElements} of {merges.length} merges (performance mode)
          </div>
        </Html>
      )}
      
      {/* Stats display */}
      <Html position={[8, 3, 0]}>
        <div className="bg-gray-800/90 text-white text-xs p-2 rounded pointer-events-none">
          <div className="font-bold mb-1">Merge Stats</div>
          <div>Active: {mergesWithVisuals.length}</div>
          <div>
            Avg Progress:{' '}
            {mergesWithVisuals.length > 0
              ? (
                  (mergesWithVisuals.reduce((sum, m) => sum + m.progress, 0) /
                    mergesWithVisuals.length) *
                  100
                ).toFixed(1)
              : 0}
            %
          </div>
          <div>
            Total Size:{' '}
            {formatBytes(
              mergesWithVisuals.reduce((sum, m) => sum + m.total_size_bytes_compressed, 0)
            )}
          </div>
        </div>
      </Html>
    </group>
  );
};

export default MergeVisualization;
