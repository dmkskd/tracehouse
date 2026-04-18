/**
 * HierarchyVisualization v3 - Glass Boxes
 * Beautiful transparent boxes with glowing edges
 */

import React, { useMemo, useState, useCallback, useRef, useEffect } from 'react';
import { useFrame } from '@react-three/fiber';
import { Html } from '@react-three/drei';
import * as THREE from 'three';
import { usePerformanceMode } from './PerformanceContext';
import { useThemeDetection } from '../../hooks/useThemeDetection';

// Theme-aware colors
const THEME_COLORS = {
  dark: {
    ground: '#0c0c1a',
    groundGrid1: '#1a1a3a',
    groundGrid2: '#0f0f2a',
    lanesGround: '#0a0a15',
    labelBg: 'rgba(0,0,0,0.85)',
    labelBgParts: 'rgba(0,0,0,0.7)',
    hoverCardBg: 'rgba(10, 10, 30, 0.92)',
  },
  light: {
    ground: '#303055',
    groundGrid1: '#40406a',
    groundGrid2: '#353560',
    lanesGround: '#2a2a4a',
    labelBg: 'rgba(255,255,255,0.95)',
    labelBgParts: 'rgba(255,255,255,0.9)',
    hoverCardBg: 'rgba(255, 255, 255, 0.97)',
  },
};

// Helper to dispose Three.js resources to prevent memory leaks
function disposeObject(obj: THREE.Object3D | null) {
  if (!obj) return;
  
  if (obj instanceof THREE.Mesh) {
    obj.geometry?.dispose();
    if (obj.material) {
      if (Array.isArray(obj.material)) {
        obj.material.forEach(m => m.dispose());
      } else {
        obj.material.dispose();
      }
    }
  } else if (obj instanceof THREE.LineSegments || obj instanceof THREE.Line) {
    obj.geometry?.dispose();
    if (obj.material) {
      if (Array.isArray(obj.material)) {
        obj.material.forEach(m => m.dispose());
      } else {
        (obj.material as THREE.Material).dispose();
      }
    }
  }
}

export type HierarchyLevel = 'databases' | 'tables' | 'partitions' | 'parts';

export interface HierarchyItem {
  id: string;
  name: string;
  size: number;
  health: 'good' | 'warning' | 'critical' | 'merging';
  healthScore: number;
  metrics: Record<string, string | number>;
  childCount?: number;
  issues?: string[];
  // Merge-related properties
  merging?: boolean;
  mergeProgress?: number;
  mergeTarget?: string;
  mergeId?: string;
}

export interface HierarchyState {
  level: HierarchyLevel;
  path: { level: HierarchyLevel; id: string; name: string }[];
}

export interface HierarchyVisualizationProps {
  items: HierarchyItem[];
  level: HierarchyLevel;
  path?: string[];  // Breadcrumb path for cyberpunk display
  onItemClick: (item: HierarchyItem) => void;
  onItemHover: (item: HierarchyItem | null) => void;
  onPathClick?: (index: number) => void;  // Navigate breadcrumb
  highlightedMergeId?: string | null;  // Merge ID to highlight (dims other parts)
  mergeColorMap?: Map<string, number>;  // Shared color assignments from parent
}

const LEVEL_COLORS: Record<HierarchyLevel, { main: string; edge: string; glow: string }> = {
  databases: { main: '#7c3aed', edge: '#a78bfa', glow: '#c4b5fd' },  // Purple
  tables: { main: '#2563eb', edge: '#60a5fa', glow: '#93c5fd' },     // Blue
  partitions: { main: '#059669', edge: '#34d399', glow: '#6ee7b7' }, // Emerald
  parts: { main: '#ec4899', edge: '#f472b6', glow: '#fbcfe8' },      // Pink/Magenta
};

const HEALTH_COLORS = { 
  good: '#22c55e', 
  warning: '#eab308', 
  critical: '#ef4444',
  merging: '#f97316', // Orange for merging parts
};

// Colors for merge levels (L0, L1, L2, etc.)
const MERGE_LEVEL_COLORS = [
  '#ef4444', // L0 - Red (unmerged, needs attention)
  '#f97316', // L1 - Orange
  '#eab308', // L2 - Yellow
  '#22c55e', // L3 - Green
  '#14b8a6', // L4 - Teal
  '#3b82f6', // L5+ - Blue (well merged)
];

function getMergeLevelFromMetrics(item: HierarchyItem): number {
  const levelStr = item.metrics?.level;
  if (typeof levelStr === 'string' && levelStr.startsWith('L')) {
    return parseInt(levelStr.slice(1), 10) || 0;
  }
  return 0;
}

// Parse part name to extract block range: partition_minBlock_maxBlock_level
function parsePartName(name: string): { minBlock: number; maxBlock: number } | null {
  // Handle truncated names (e.g., "202601_1...36_2" or full "202601_131_134_1")
  // Full name pattern: partition_min_max_level
  const parts = name.split('_');
  if (parts.length >= 4) {
    const minBlock = parseInt(parts[1], 10);
    const maxBlock = parseInt(parts[2], 10);
    if (!isNaN(minBlock) && !isNaN(maxBlock)) {
      return { minBlock, maxBlock };
    }
  }
  return null;
}

// Special layout for parts - grouped by merge level, positioned by block range
function calculatePartsLayout(items: HierarchyItem[]) {
  if (items.length === 0) return { layout: [], maxLevel: 0, levels: [] as number[] };
  
  // Parse block ranges for all items
  const itemsWithBlocks = items.map(item => {
    // Try to get full part name from id (which has the full name)
    const blockInfo = parsePartName(item.id);
    return {
      item,
      minBlock: blockInfo?.minBlock ?? 0,
      maxBlock: blockInfo?.maxBlock ?? 0,
    };
  });
  
  // Group by merge level
  const byLevel = new Map<number, typeof itemsWithBlocks>();
  itemsWithBlocks.forEach(entry => {
    const lvl = getMergeLevelFromMetrics(entry.item);
    if (!byLevel.has(lvl)) byLevel.set(lvl, []);
    byLevel.get(lvl)!.push(entry);
  });
  
  // Sort each level by minBlock (so consecutive parts are adjacent)
  byLevel.forEach((entries) => {
    entries.sort((a, b) => a.minBlock - b.minBlock);
  });
  
  const maxLevel = Math.max(...byLevel.keys(), 0);
  const laneWidth = 5; // Width of each lane

  // Compact: map populated levels to sequential lane indices so empty levels
  // don't waste space (e.g. L1 + L210 → lane 0 + lane 1 instead of 209 gap)
  // Always include L0 — it's the landing zone for new inserts
  const sortedLevels = [0, ...([...byLevel.keys()].filter(l => l !== 0))].sort((a, b) => a - b);
  const laneIndex = new Map(sortedLevels.map((lvl, i) => [lvl, i]));
  const maxLaneIdx = Math.max(sortedLevels.length - 1, 0);

  const sizes = items.map(i => i.size);
  const maxSize = Math.max(...sizes, 1);
  const minSize = Math.min(...sizes, 1);
  const sizeRange = maxSize - minSize || 1;

  const layout: { position: [number, number, number]; size: [number, number, number]; mergeLevel: number }[] = [];

  // Create a map from item id to layout for consistent ordering
  const layoutMap = new Map<string, { position: [number, number, number]; size: [number, number, number]; mergeLevel: number }>();

  // Minimum spacing between parts
  const minSpacing = 2.0;

  byLevel.forEach((entries, lvl) => {
    // For each level, position parts sequentially with proper spacing
    // Parts are sorted by minBlock ascending
    // Higher block numbers (newer) should be at higher Z (closer to camera at +Z)
    const levelStartZ = -(entries.length - 1) * minSpacing / 2; // Center the level
    const lane = laneIndex.get(lvl)!;

    entries.forEach((entry, index) => {
      const { item } = entry;

      const normalizedSize = (item.size - minSize) / sizeRange;
      const baseSize = 0.8 + normalizedSize * 0.6;
      const height = 0.6 + normalizedSize * 1.5;

      // X = compact lane position (skip empty levels)
      const x = lane * laneWidth - (maxLaneIdx * laneWidth) / 2;

      // Z = sequential position with minimum spacing
      // Higher index (higher block number = newer) gets higher Z (closer to camera)
      const z = levelStartZ + index * minSpacing;

      layoutMap.set(item.id, {
        position: [x, height / 2 + 0.1, z] as [number, number, number],
        size: [baseSize, height, baseSize] as [number, number, number],
        mergeLevel: lvl,
      });
    });
  });

  // Return layout in original item order
  items.forEach(item => {
    const entry = layoutMap.get(item.id);
    if (entry) {
      layout.push(entry);
    } else {
      // Fallback for items without block info
      const lvl = getMergeLevelFromMetrics(item);
      const normalizedSize = (item.size - minSize) / sizeRange;
      const baseSize = 0.8 + normalizedSize * 0.6;
      const height = 0.6 + normalizedSize * 1.5;
      const lane = laneIndex.get(lvl) ?? 0;
      layout.push({
        position: [lane * laneWidth - (maxLaneIdx * laneWidth) / 2, height / 2 + 0.1, 0] as [number, number, number],
        size: [baseSize, height, baseSize] as [number, number, number],
        mergeLevel: lvl,
      });
    }
  });
  
  return { layout, maxLevel, levels: sortedLevels };
}

function calculateLayout(items: HierarchyItem[]) {
  if (items.length === 0) return [];
  const sizes = items.map(i => i.size);
  const maxSize = Math.max(...sizes, 1);
  const minSize = Math.min(...sizes, 1);
  const sizeRange = maxSize - minSize || 1;
  const cols = Math.ceil(Math.sqrt(items.length));
  const spacing = 4.5;

  return items.map((item, i) => {
    const row = Math.floor(i / cols);
    const col = i % cols;
    const normalizedSize = (item.size - minSize) / sizeRange;
    const baseSize = 1.2;
    const height = 0.8 + normalizedSize * 2;
    return {
      position: [
        col * spacing - (cols - 1) * spacing / 2,
        height / 2 + 0.1,
        row * spacing - (Math.ceil(items.length / cols) - 1) * spacing / 2,
      ] as [number, number, number],
      size: [baseSize, height, baseSize] as [number, number, number],
    };
  });
}

interface GlassBoxProps {
  item: HierarchyItem;
  position: [number, number, number];
  size: [number, number, number];
  level: HierarchyLevel;
  mergeLevel?: number;  // For parts: L0, L1, L2, etc. - determines color
  isHovered: boolean;
  isDimmed?: boolean;  // Dim this part (when another merge is highlighted)
  mergeColorIndex?: number;  // Color index for merge group
  theme: 'dark' | 'light';  // Theme for label colors
  onClick: () => void;
  onPointerOver: () => void;
  onPointerOut: () => void;
}

const GlassBox: React.FC<GlassBoxProps> = ({ item, position, size, level, mergeLevel, isHovered, isDimmed, mergeColorIndex, theme, onClick, onPointerOver, onPointerOut }) => {
  const meshRef = useRef<THREE.Mesh>(null);
  const edgesRef = useRef<THREE.LineSegments>(null);
  const healthBarRef = useRef<THREE.Mesh>(null);
  const glowPlaneRef = useRef<THREE.Mesh>(null);
  const mergeRingRef = useRef<THREE.Mesh>(null);
  const { enableAnimations } = usePerformanceMode();
  
  // Theme-aware colors for labels
  const themeColors = THEME_COLORS[theme];
  
  // For parts level, use merge level colors (L0=red, L1=orange, etc.)
  // For other levels, use the standard level colors
  const isPartsLevel = level === 'parts';
  const mergeLevelColor = isPartsLevel && mergeLevel !== undefined
    ? MERGE_LEVEL_COLORS[Math.min(mergeLevel, MERGE_LEVEL_COLORS.length - 1)]
    : null;
  
  const colors = mergeLevelColor 
    ? { main: mergeLevelColor, edge: mergeLevelColor, glow: mergeLevelColor }
    : LEVEL_COLORS[level];
  
  const healthColor = HEALTH_COLORS[item.health] || HEALTH_COLORS.good;
  const isMerging = item.merging === true;
  
  // Create edges geometry with threshold to only show hard edges (90 degree angles)
  // This prevents showing the diagonal triangulation lines inside box faces
  const edgesGeometry = useMemo(() => {
    const boxGeo = new THREE.BoxGeometry(size[0] * 1.002, size[1] * 1.002, size[2] * 1.002);
    return new THREE.EdgesGeometry(boxGeo, 1); // threshold of 1 degree - only shows edges > 1 degree
  }, [size]);
  
  // Cleanup Three.js resources on unmount to prevent memory leaks
  useEffect(() => {
    return () => {
      disposeObject(meshRef.current);
      disposeObject(healthBarRef.current);
      disposeObject(glowPlaneRef.current);
      disposeObject(mergeRingRef.current);
      edgesGeometry.dispose();
    };
  }, [edgesGeometry]);
  
  // Get merge group color if merging
  const mergeGroupColor = isMerging && mergeColorIndex !== undefined 
    ? MERGE_GROUP_COLORS[mergeColorIndex % MERGE_GROUP_COLORS.length]
    : null;
  
  // Dimming factor for non-highlighted parts
  const dimFactor = isDimmed ? 0.25 : 1;

  useFrame((state) => {
    if (!meshRef.current || !enableAnimations) return;
    const time = state.clock.elapsedTime;
    
    // Base floating animation
    const floatY = position[1] + Math.sin(time * 0.8 + position[0]) * 0.05;
    meshRef.current.position.y = floatY;
    
    // Keep edges in sync with mesh
    if (edgesRef.current) {
      edgesRef.current.position.y = floatY;
    }
    
    // Merging parts: pulsing glow effect
    if (isMerging && meshRef.current.material && !isDimmed) {
      const mat = meshRef.current.material as THREE.MeshPhysicalMaterial;
      const pulse = Math.sin(time * 4) * 0.5 + 0.5; // Fast pulse 0-1
      mat.emissiveIntensity = 0.3 + pulse * 0.5;
      mat.opacity = 0.5 + pulse * 0.3;
    } else if (isHovered && meshRef.current.material) {
      const mat = meshRef.current.material as THREE.MeshPhysicalMaterial;
      mat.emissiveIntensity = 0.3 + Math.sin(time * 3) * 0.1;
    }
  });

  // Merging parts use their merge group color
  const boxColor = mergeGroupColor ? mergeGroupColor.line : (isMerging ? '#f97316' : colors.main);
  const edgeColor = mergeGroupColor ? mergeGroupColor.line : (isMerging ? '#fb923c' : (isHovered ? colors.glow : colors.edge));
  const glowColor = mergeGroupColor ? mergeGroupColor.particle : (isMerging ? '#fdba74' : colors.glow);
  const mergeLabelBg = theme === 'light' ? 'rgba(255, 237, 213, 0.95)' : 'rgba(249, 115, 22, 0.3)';
  const mergeLabelBorder = theme === 'light' ? 'rgba(234, 88, 12, 0.4)' : '#f9731680';
  const mergeLabelText = theme === 'light' ? '#9a3412' : '#fff';
  const labelBgColor = mergeGroupColor ? (theme === 'light' ? `rgba(255,255,255,0.95)` : `${mergeGroupColor.line}40`) : (isMerging ? mergeLabelBg : themeColors.labelBg);
  const labelBorderColor = mergeGroupColor ? (theme === 'light' ? `${mergeGroupColor.line}60` : `${mergeGroupColor.line}80`) : (isMerging ? mergeLabelBorder : colors.edge + '40');
  const labelTextColor = mergeGroupColor ? (theme === 'light' ? mergeGroupColor.line : mergeGroupColor.particle) : (isMerging ? mergeLabelText : (theme === 'light' ? 'rgba(0,0,0,0.85)' : 'rgba(255,255,255,0.85)'));

  return (
    <group position={[position[0], 0, position[2]]} scale={isDimmed ? 0.95 : 1}>
      <mesh
        ref={meshRef}
        position={[0, position[1], 0]}
        onClick={(e) => { e.stopPropagation(); onClick(); }}
        onPointerOver={(e) => { e.stopPropagation(); onPointerOver(); document.body.style.cursor = 'pointer'; }}
        onPointerOut={(e) => { e.stopPropagation(); onPointerOut(); document.body.style.cursor = 'auto'; }}
        castShadow receiveShadow
      >
        <boxGeometry args={size} />
        <meshPhysicalMaterial
          color={boxColor} metalness={0.1} roughness={0.05}
          transmission={isMerging ? 0.4 : 0.6} thickness={1.5} transparent
          opacity={(isHovered ? 0.85 : (isMerging ? 0.7 : 0.6)) * dimFactor}
          emissive={boxColor} emissiveIntensity={(isHovered ? 0.4 : (isMerging ? 0.5 : 0.15)) * dimFactor}
          clearcoat={1} clearcoatRoughness={0.1} ior={1.5}
        />
      </mesh>
      
      {/* Clean edges - only the 12 outer edges of the box */}
      <lineSegments ref={edgesRef} position={[0, position[1], 0]}>
        <primitive object={edgesGeometry} attach="geometry" />
        <lineBasicMaterial color={edgeColor} transparent opacity={(isHovered ? 1 : (isMerging ? 0.9 : 0.7)) * dimFactor} />
      </lineSegments>
      
      {/* Health indicator bar - orange for merging */}
      <mesh ref={healthBarRef} position={[0, 0.05, 0]}>
        <boxGeometry args={[size[0] * 1.1, 0.08, size[2] * 1.1]} />
        <meshBasicMaterial color={healthColor} transparent opacity={0.9 * dimFactor} />
      </mesh>
      
      {/* Hover glow */}
      {isHovered && !isDimmed && (
        <mesh ref={glowPlaneRef} position={[0, 0.02, 0]} rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[size[0] * 2, size[2] * 2]} />
          <meshBasicMaterial color={glowColor} transparent opacity={0.15} />
        </mesh>
      )}
      
      {/* Merging indicator - pulsing ring around the part */}
      {isMerging && !isDimmed && (
        <mesh ref={mergeRingRef} position={[0, 0.1, 0]} rotation={[-Math.PI / 2, 0, 0]}>
          <ringGeometry args={[size[0] * 0.7, size[0] * 0.9, 32]} />
          <meshBasicMaterial color="#f97316" transparent opacity={0.6} side={THREE.DoubleSide} />
        </mesh>
      )}
      
      {/* Simple label always visible */}
      <Html 
        position={[0, position[1] + size[1] / 2 + 0.3, 0]} 
        center 
        style={{ pointerEvents: 'none', opacity: level === 'parts' ? dimFactor * 0.7 : dimFactor }}
      >
        <div style={{
          background: isMerging ? labelBgColor : (level === 'parts' ? themeColors.labelBgParts : themeColors.labelBg),
          padding: level === 'parts' ? '2px 6px' : '4px 10px',
          borderRadius: '2px',
          border: `1px solid ${isMerging ? labelBorderColor : (level === 'parts' ? (theme === 'light' ? 'rgba(0,0,0,0.15)' : 'rgba(255,255,255,0.15)') : colors.edge + '40')}`,
          backdropFilter: 'blur(4px)',
          animation: isMerging ? 'pulse 1s ease-in-out infinite' : 'none',
        }}>
          <div style={{ 
            color: isMerging ? labelTextColor : (level === 'parts' ? (theme === 'light' ? 'rgba(0,0,0,0.5)' : 'rgba(255,255,255,0.5)') : labelTextColor), 
            fontSize: level === 'parts' ? '9px' : '12px', 
            fontWeight: isMerging ? 600 : (level === 'parts' ? 400 : 500),
            fontFamily: 'ui-monospace, monospace',
            whiteSpace: 'nowrap',
          }}>
            {isMerging && '⟳ '}{item.name}
          </div>
          {isMerging && item.mergeProgress !== undefined && (
            <div style={{
              marginTop: '2px',
              height: '2px',
              background: 'rgba(0,0,0,0.3)',
              borderRadius: '2px',
              overflow: 'hidden',
            }}>
              <div style={{
                width: `${item.mergeProgress * 100}%`,
                height: '100%',
                background: mergeGroupColor ? mergeGroupColor.particle : '#22c55e',
                transition: 'width 0.3s ease',
              }} />
            </div>
          )}
        </div>
      </Html>
      
      {/* Expanded hover card - only on hover */}
      {isHovered && (
        <Html 
          position={[0, position[1] + size[1] / 2 + 1.2, 0]} 
          center 
          style={{ pointerEvents: 'none', zIndex: 1000 }}
        >
          {/* Expanded hover card - edgy cyberpunk style */}
          <div style={{
            background: isMerging ? (theme === 'light' ? 'rgba(255, 237, 213, 0.97)' : 'rgba(30, 15, 5, 0.92)') : themeColors.hoverCardBg,
            padding: '12px 16px',
            borderRadius: '4px',
            border: `1px solid ${isMerging ? '#f9731680' : colors.edge + '60'}`,
            boxShadow: `0 0 20px ${isMerging ? '#f9731640' : colors.main + '20'}, inset 0 0 40px ${isMerging ? '#f9731610' : colors.main + '08'}`,
            minWidth: '180px',
            backdropFilter: 'blur(12px)',
            transform: 'scale(0.85)',
          }}>
            {/* Header */}
            <div style={{ 
              display: 'flex', 
              justifyContent: 'space-between', 
              alignItems: 'flex-start',
              marginBottom: '8px',
              paddingBottom: '8px',
              borderBottom: `1px solid ${isMerging ? '#f9731660' : colors.edge + '40'}`,
            }}>
              <div>
                <div style={{ 
                  color: theme === 'light' ? colors.main : colors.glow, 
                  fontSize: '14px', 
                  fontWeight: 600,
                  letterSpacing: '0.5px',
                  fontFamily: 'ui-monospace, monospace',
                }}>
                  {item.name}
                </div>
                <div style={{ 
                  color: theme === 'light' ? 'rgba(0,0,0,0.4)' : 'rgba(255,255,255,0.4)', 
                  fontSize: '9px', 
                  textTransform: 'uppercase',
                  letterSpacing: '2px',
                  marginTop: '2px',
                }}>
                  {level.slice(0, -1)}
                </div>
              </div>
            </div>
            
            {/* Metrics */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
              {Object.entries(item.metrics).slice(0, 6).map(([key, value]) => (
                <div key={key} style={{ 
                  display: 'flex', 
                  justifyContent: 'space-between',
                  alignItems: 'center',
                }}>
                  <span style={{ 
                    color: theme === 'light' ? 'rgba(0,0,0,0.5)' : 'rgba(255,255,255,0.5)', 
                    fontSize: '10px',
                    textTransform: 'capitalize',
                  }}>
                    {key}
                  </span>
                  <span style={{ 
                    color: theme === 'light' ? 'rgba(0,0,0,0.9)' : 'rgba(255,255,255,0.9)', 
                    fontSize: '11px',
                    fontWeight: 500,
                    fontFamily: 'ui-monospace, monospace',
                  }}>
                    {value}
                  </span>
                </div>
              ))}
            </div>
            
            {/* Footer */}
            <div style={{
              marginTop: '8px',
              paddingTop: '8px',
              borderTop: `1px solid ${colors.edge}40`,
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}>
              <span style={{ color: theme === 'light' ? 'rgba(0,0,0,0.3)' : 'rgba(255,255,255,0.3)', fontSize: '9px', letterSpacing: '1px' }}>
                CLICK TO EXPLORE
              </span>
              <span style={{ color: theme === 'light' ? colors.main : colors.glow, fontSize: '12px' }}>→</span>
            </div>
          </div>
        </Html>
      )}
    </group>
  );
};


const Ground: React.FC<{ size: number; theme: 'dark' | 'light' }> = ({ size, theme }) => {
  const meshRef = useRef<THREE.Mesh>(null);
  const gridRef = useRef<THREE.GridHelper>(null);
  const colors = THEME_COLORS[theme];
  
  // Cleanup on unmount
  useEffect(() => {
    return () => {
      disposeObject(meshRef.current);
      if (gridRef.current) {
        gridRef.current.geometry?.dispose();
        if (gridRef.current.material) {
          if (Array.isArray(gridRef.current.material)) {
            gridRef.current.material.forEach(m => m.dispose());
          } else {
            (gridRef.current.material as THREE.Material).dispose();
          }
        }
      }
    };
  }, []);
  
  // Both themes: metallic sheen for reflective floor look
  // Light mode: less metallic to avoid harsh specular hotspots
  const materialProps = theme === 'light' 
    ? { metalness: 0.4, roughness: 0.6 }
    : { metalness: 0.8, roughness: 0.4 };
  
  return (
    <group>
      <mesh ref={meshRef} rotation={[-Math.PI / 2, 0, 0]} position={[0, 0, 0]} receiveShadow>
        <planeGeometry args={[size * 2.5, size * 2.5]} />
        <meshStandardMaterial color={colors.ground} {...materialProps} />
      </mesh>
      <gridHelper ref={gridRef} args={[size * 2, Math.floor(size * 2), colors.groundGrid1, colors.groundGrid2]} position={[0, 0.01, 0]} />
    </group>
  );
};

// Highway lanes for parts view - shows merge levels
interface MergeLanesProps {
  maxLevel: number;
  levels?: number[];
  laneLength: number;
  theme: 'dark' | 'light';
}

const MergeLanes: React.FC<MergeLanesProps> = ({ maxLevel, levels, laneLength, theme }) => {
  const laneWidth = 5;
  const groundRef = useRef<THREE.Mesh>(null);
  const laneRefs = useRef<Map<number, { glow: THREE.Mesh | null; outer: THREE.Mesh | null }>>(new Map());
  const colors = THEME_COLORS[theme];

  // Use populated levels if provided, otherwise fall back to 0..maxLevel
  const populatedLevels = levels ?? Array.from({ length: maxLevel + 1 }, (_, i) => i);
  const numLanes = populatedLevels.length;
  const maxLaneIdx = Math.max(numLanes - 1, 0);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      disposeObject(groundRef.current);
      laneRefs.current.forEach(refs => {
        disposeObject(refs.glow);
        disposeObject(refs.outer);
      });
    };
  }, []);

  return (
    <group>
      {/* Ground with lanes */}
      <mesh ref={groundRef} rotation={[-Math.PI / 2, 0, 0]} position={[0, 0, 0]} receiveShadow>
        <planeGeometry args={[(numLanes + 1) * laneWidth, laneLength + 10]} />
        <meshStandardMaterial color={colors.lanesGround} metalness={theme === 'dark' ? 0.9 : 0.4} roughness={theme === 'dark' ? 0.3 : 0.6} />
      </mesh>

      {/* Lane dividers and labels */}
      {populatedLevels.map((lvl, laneIdx) => {
        const x = laneIdx * laneWidth - (maxLaneIdx * laneWidth) / 2;
        const color = MERGE_LEVEL_COLORS[Math.min(lvl, MERGE_LEVEL_COLORS.length - 1)];

        return (
          <group key={lvl} position={[x, 0, 0]}>
            {/* Lane glow line */}
            <mesh
              ref={(el) => {
                if (!laneRefs.current.has(lvl)) laneRefs.current.set(lvl, { glow: null, outer: null });
                laneRefs.current.get(lvl)!.glow = el;
              }}
              position={[0, 0.02, 0]}
              rotation={[-Math.PI / 2, 0, 0]}
            >
              <planeGeometry args={[0.15, laneLength + 6]} />
              <meshBasicMaterial color={color} transparent opacity={0.6} />
            </mesh>

            {/* Lane outer glow */}
            <mesh
              ref={(el) => {
                if (!laneRefs.current.has(lvl)) laneRefs.current.set(lvl, { glow: null, outer: null });
                laneRefs.current.get(lvl)!.outer = el;
              }}
              position={[0, 0.01, 0]}
              rotation={[-Math.PI / 2, 0, 0]}
            >
              <planeGeometry args={[0.8, laneLength + 6]} />
              <meshBasicMaterial color={color} transparent opacity={0.1} />
            </mesh>

            {/* Level label at the back */}
            <Html position={[0, 0.5, -(laneLength / 2 + 2)]} center style={{ pointerEvents: 'none' }}>
              <div style={{
                color: color,
                fontSize: '24px',
                fontWeight: 900,
                fontFamily: 'ui-monospace, monospace',
                textShadow: `0 0 20px ${color}, 0 0 40px ${color}50`,
                letterSpacing: '2px',
              }}>
                L{lvl}
              </div>
              <div style={{
                color: 'rgba(255,255,255,0.4)',
                fontSize: '10px',
                textTransform: 'uppercase',
                letterSpacing: '1px',
                marginTop: '4px',
              }}>
                {lvl === 0 ? 'UNMERGED' : 'MERGED'}
              </div>
            </Html>
          </group>
        );
      })}

      {/* Arrow indicators and skip markers between lanes */}
      {populatedLevels.slice(0, -1).map((lvl, laneIdx) => {
        const nextLvl = populatedLevels[laneIdx + 1];
        const skipped = nextLvl - lvl - 1;
        const x1 = laneIdx * laneWidth - (maxLaneIdx * laneWidth) / 2;
        const x2 = (laneIdx + 1) * laneWidth - (maxLaneIdx * laneWidth) / 2;
        const midX = (x1 + x2) / 2;

        return (
          <group key={`between-${laneIdx}`}>
            <Html position={[midX, 0.3, -(laneLength / 2 + 1)]} center style={{ pointerEvents: 'none' }}>
              <div style={{
                color: 'rgba(255,255,255,0.3)',
                fontSize: '20px',
              }}>
                →
              </div>
            </Html>
            {skipped > 0 && (
              <>
                {/* Dashed skip line on the ground */}
                {Array.from({ length: Math.floor((laneLength + 6) / 1.2) }, (_, di) => {
                  const dashZ = -(laneLength + 6) / 2 + di * 1.2 + 0.3;
                  return (
                    <mesh key={di} position={[midX, 0.02, dashZ]} rotation={[-Math.PI / 2, 0, 0]}>
                      <planeGeometry args={[0.1, 0.5]} />
                      <meshBasicMaterial color="#6366f1" transparent opacity={0.35} />
                    </mesh>
                  );
                })}
                {/* Skip label */}
                <Html position={[midX, 0.5, -(laneLength / 2 + 2)]} center style={{ pointerEvents: 'none' }}>
                  <div style={{
                    color: 'rgba(139, 142, 255, 0.5)',
                    fontSize: '11px',
                    fontFamily: 'ui-monospace, monospace',
                    letterSpacing: '1px',
                    whiteSpace: 'nowrap',
                    marginTop: '28px',
                  }}>
                    ⋯ {skipped} level{skipped > 1 ? 's' : ''} ⋯
                  </div>
                </Html>
              </>
            )}
          </group>
        );
      })}
    </group>
  );
};

// ============================================================================
// MERGE FLOW VISUALIZATION - Swim lanes connecting source parts to result
// ============================================================================

interface MergeGroup {
  mergeId: string;
  targetName: string;
  sourcePositions: [number, number, number][];
  progress: number;
}

interface MergeFlowLinesProps {
  items: HierarchyItem[];
  layout: { position: [number, number, number]; size: [number, number, number]; mergeLevel: number }[];
  maxLevel: number;
  highlightedMergeId?: string | null;
  mergeColorMap?: Map<string, number>;
  theme?: 'dark' | 'light';
}

// Colors for different merge groups - AVOID RED/GREEN (status colors)
// Extended palette to avoid color reuse when multiple merges complete/start
const MERGE_GROUP_COLORS = [
  { line: '#06b6d4', particle: '#67e8f9', ghost: '#06b6d4' }, // Cyan
  { line: '#f59e0b', particle: '#fcd34d', ghost: '#f59e0b' }, // Amber
  { line: '#ec4899', particle: '#f9a8d4', ghost: '#ec4899' }, // Pink/Magenta
  { line: '#3b82f6', particle: '#93c5fd', ghost: '#3b82f6' }, // Blue
  { line: '#8b5cf6', particle: '#c4b5fd', ghost: '#8b5cf6' }, // Purple
  { line: '#14b8a6', particle: '#5eead4', ghost: '#14b8a6' }, // Teal
  { line: '#f472b6', particle: '#fbcfe8', ghost: '#f472b6' }, // Light Pink
  { line: '#a78bfa', particle: '#ddd6fe', ghost: '#a78bfa' }, // Violet
  // Extended palette
  { line: '#0891b2', particle: '#22d3ee', ghost: '#0891b2' }, // Dark Cyan
  { line: '#d97706', particle: '#fbbf24', ghost: '#d97706' }, // Dark Amber
  { line: '#db2777', particle: '#f472b6', ghost: '#db2777' }, // Dark Pink
  { line: '#2563eb', particle: '#60a5fa', ghost: '#2563eb' }, // Dark Blue
  { line: '#7c3aed', particle: '#a78bfa', ghost: '#7c3aed' }, // Dark Purple
  { line: '#0d9488', particle: '#2dd4bf', ghost: '#0d9488' }, // Dark Teal
  { line: '#c026d3', particle: '#e879f9', ghost: '#c026d3' }, // Fuchsia
  { line: '#4f46e5', particle: '#818cf8', ghost: '#4f46e5' }, // Indigo
  { line: '#0284c7', particle: '#38bdf8', ghost: '#0284c7' }, // Sky Blue
  { line: '#ea580c', particle: '#fb923c', ghost: '#ea580c' }, // Orange
  { line: '#9333ea', particle: '#c084fc', ghost: '#9333ea' }, // Purple 2
  { line: '#0e7490', particle: '#06b6d4', ghost: '#0e7490' }, // Cyan 2
];

// Animated flow line from source to target
const FlowLine: React.FC<{
  start: [number, number, number];
  end: [number, number, number];
  progress: number;
  index: number;
  colorIndex?: number;
  isDimmed?: boolean;
}> = ({ start, end, index, colorIndex = 0, isDimmed }) => {
  const particleRef = useRef<THREE.Mesh>(null);
  const lineRef = useRef<THREE.Line>(null);
  const colors = MERGE_GROUP_COLORS[colorIndex % MERGE_GROUP_COLORS.length];
  const dimFactor = isDimmed ? 0.2 : 1;
  const { enableAnimations } = usePerformanceMode();
  
  // Curved line from source to target
  const curve = useMemo(() => {
    const midY = Math.max(start[1], end[1]) + 0.5;
    return new THREE.CatmullRomCurve3([
      new THREE.Vector3(start[0], start[1], start[2]),
      new THREE.Vector3((start[0] + end[0]) / 2, midY, (start[2] + end[2]) / 2),
      new THREE.Vector3(end[0], end[1], end[2]),
    ]);
  }, [start, end]);
  
  const points = useMemo(() => curve.getPoints(24), [curve]);
  const geometry = useMemo(() => new THREE.BufferGeometry().setFromPoints(points), [points]);
  const material = useMemo(() => new THREE.LineBasicMaterial({ 
    color: colors.line, 
    transparent: true, 
    opacity: 0.5 * dimFactor,
  }), [colors.line, dimFactor]);
  
  // Cleanup Three.js resources on unmount
  useEffect(() => {
    return () => {
      geometry.dispose();
      material.dispose();
      disposeObject(particleRef.current);
    };
  }, [geometry, material]);
  
  // Animate particle along the curve
  useFrame((state) => {
    if (!particleRef.current || !enableAnimations) return;
    const time = state.clock.elapsedTime;
    const t = ((time * 0.4 + index * 0.15) % 1);
    const point = curve.getPoint(t);
    particleRef.current.position.copy(point);
  });
  
  return (
    <group>
      {/* Thin colored line */}
      <primitive object={new THREE.Line(geometry, material)} ref={lineRef} />
      
      {/* Small animated particle */}
      {!isDimmed && (
        <mesh ref={particleRef}>
          <sphereGeometry args={[0.06, 6, 6]} />
          <meshBasicMaterial color={colors.particle} transparent opacity={0.9} />
        </mesh>
      )}
    </group>
  );
};

// Result part placeholder - transparent ghost box with progress fill
const ResultPartGhost: React.FC<{
  position: [number, number, number];
  targetName: string;
  progress: number;
  sourceCount: number;
  colorIndex?: number;
  isDimmed?: boolean;
  theme?: 'dark' | 'light';
}> = ({ position, targetName, progress, sourceCount, colorIndex = 0, isDimmed, theme = 'dark' }) => {
  const meshRef = useRef<THREE.Mesh>(null);
  const edgesRef = useRef<THREE.LineSegments>(null);
  const fillRef = useRef<THREE.Mesh>(null);
  const [isHovered, setIsHovered] = useState(false);
  const progressPct = Math.round(progress * 100);
  const colors = MERGE_GROUP_COLORS[colorIndex % MERGE_GROUP_COLORS.length];
  const dimFactor = isDimmed ? 0.2 : 1;
  const { enableAnimations } = usePerformanceMode();
  
  const boxHeight = 1.6;
  const boxWidth = 1.2;
  // Fill height based on progress (minimum 0.05 to always show something)
  const fillHeight = Math.max(0.05, boxHeight * progress);
  // Position fill at bottom of box, growing upward
  const fillY = fillHeight / 2;
  
  // Memoize the edges geometry to avoid recreating it
  const edgesGeometry = useMemo(() => new THREE.EdgesGeometry(new THREE.BoxGeometry(1.2, 1.6, 1.2)), []);
  
  // Cleanup Three.js resources on unmount
  useEffect(() => {
    return () => {
      disposeObject(meshRef.current);
      disposeObject(fillRef.current);
      edgesGeometry.dispose();
      if (edgesRef.current) {
        (edgesRef.current.material as THREE.Material)?.dispose();
      }
    };
  }, [edgesGeometry]);
  
  useFrame((state) => {
    if (!meshRef.current || isDimmed || !enableAnimations) return;
    const time = state.clock.elapsedTime;
    // Subtle pulsing effect
    const pulse = Math.sin(time * 2) * 0.5 + 0.5;
    const mat = meshRef.current.material as THREE.MeshBasicMaterial;
    mat.opacity = 0.03 + pulse * 0.05;
    
    // Pulse edges - brighter when hovered
    if (edgesRef.current) {
      const edgeMat = edgesRef.current.material as THREE.LineBasicMaterial;
      edgeMat.opacity = isHovered ? 0.9 : (0.4 + pulse * 0.4);
    }
    
    // Pulse fill glow
    if (fillRef.current) {
      const fillMat = fillRef.current.material as THREE.MeshBasicMaterial;
      fillMat.opacity = (isHovered ? 0.5 : (0.3 + pulse * 0.2)) * dimFactor;
    }
  });
  
  const handlePointerOver = (e: { stopPropagation: () => void }) => {
    e.stopPropagation();
    setIsHovered(true);
    document.body.style.cursor = 'pointer';
  };
  
  const handlePointerOut = (e: { stopPropagation: () => void }) => {
    e.stopPropagation();
    setIsHovered(false);
    document.body.style.cursor = 'auto';
  };
  
  return (
    <group position={[position[0], 0, position[2]]}>
      {/* Very transparent box - can see through (full height ghost) */}
      <mesh 
        ref={meshRef} 
        position={[0, position[1], 0]}
        onPointerOver={handlePointerOver}
        onPointerOut={handlePointerOut}
      >
        <boxGeometry args={[boxWidth, boxHeight, boxWidth]} />
        <meshBasicMaterial color={colors.ghost} transparent opacity={0.05 * dimFactor} />
      </mesh>
      
      {/* Progress fill - grows from bottom */}
      <mesh ref={fillRef} position={[0, position[1] - boxHeight / 2 + fillY, 0]}>
        <boxGeometry args={[boxWidth * 0.9, fillHeight, boxWidth * 0.9]} />
        <meshBasicMaterial color={colors.ghost} transparent opacity={0.35 * dimFactor} />
      </mesh>
      
      {/* Glowing edges - main visibility */}
      <lineSegments ref={edgesRef} position={[0, position[1], 0]}>
        <primitive object={edgesGeometry} attach="geometry" />
        <lineBasicMaterial color={colors.ghost} transparent opacity={0.6 * dimFactor} linewidth={2} />
      </lineSegments>
      
      {/* Compact label - always visible */}
      <Html position={[0, position[1] + 0.9, 0]} center style={{ pointerEvents: 'none', opacity: dimFactor * 0.85 }}>
        <div style={{
          background: theme === 'light' ? 'rgba(255,255,255,0.85)' : 'rgba(0,0,0,0.5)',
          padding: '4px 6px',
          borderRadius: '2px',
          border: `1px solid ${colors.ghost}${isHovered ? '' : '60'}`,
          boxShadow: `0 0 ${isHovered ? '12px' : '8px'} ${colors.ghost}${isHovered ? '40' : '20'}`,
          backdropFilter: 'blur(4px)',
          transition: 'all 0.2s ease',
        }}>
          <div style={{
            color: colors.ghost,
            fontSize: '8px',
            fontFamily: 'ui-monospace, monospace',
            fontWeight: 500,
            textTransform: 'uppercase',
            letterSpacing: '1px',
            marginBottom: '2px',
            opacity: 0.9,
          }}>
            ⟳ Merging
          </div>
          <div style={{
            color: theme === 'light' ? 'rgba(0,0,0,0.7)' : 'rgba(255,255,255,0.7)',
            fontSize: '9px',
            fontFamily: 'ui-monospace, monospace',
            fontWeight: 400,
          }}>
            {targetName.length > 14 ? targetName.slice(0, 6) + '…' + targetName.slice(-6) : targetName}
          </div>
          <div style={{
            color: theme === 'light' ? 'rgba(0,0,0,0.4)' : 'rgba(255,255,255,0.4)',
            fontSize: '8px',
            fontFamily: 'ui-monospace, monospace',
            marginTop: '2px',
          }}>
            {sourceCount} → <span style={{ color: colors.ghost, fontWeight: 500, opacity: 0.9 }}>{progressPct}%</span>
          </div>
        </div>
      </Html>
      
      {/* Expanded hover card - only on hover */}
      {isHovered && (
        <Html 
          position={[0, position[1] + 1.8, 0]} 
          center 
          style={{ pointerEvents: 'none', zIndex: 1000 }}
        >
          <div style={{
            background: theme === 'light' ? 'rgba(255,255,255,0.95)' : 'rgba(0,0,0,0.7)',
            padding: '10px 14px',
            borderRadius: '4px',
            border: `1.5px solid ${colors.ghost}`,
            boxShadow: `0 0 20px ${colors.ghost}40`,
            minWidth: '160px',
            backdropFilter: 'blur(8px)',
          }}>
            <div style={{
              color: colors.ghost,
              fontSize: '11px',
              fontWeight: 600,
              fontFamily: 'ui-monospace, monospace',
              marginBottom: '6px',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
            }}>
              <span style={{ fontSize: '14px' }}>⟳</span> Merge in Progress
            </div>
            
            <div style={{
              color: theme === 'light' ? 'rgba(0,0,0,0.9)' : 'rgba(255,255,255,0.9)',
              fontSize: '10px',
              fontFamily: 'ui-monospace, monospace',
              marginBottom: '8px',
              wordBreak: 'break-all',
            }}>
              {targetName}
            </div>
            
            {/* Progress bar */}
            <div style={{
              background: theme === 'light' ? 'rgba(0,0,0,0.1)' : 'rgba(255,255,255,0.1)',
              borderRadius: '2px',
              height: '6px',
              overflow: 'hidden',
              marginBottom: '6px',
            }}>
              <div style={{
                width: `${progressPct}%`,
                height: '100%',
                background: colors.ghost,
                transition: 'width 0.3s ease',
              }} />
            </div>
            
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}>
              <span style={{ 
                color: theme === 'light' ? 'rgba(0,0,0,0.5)' : 'rgba(255,255,255,0.5)', 
                fontSize: '9px',
                fontFamily: 'ui-monospace, monospace',
              }}>
                {sourceCount} parts merging
              </span>
              <span style={{ 
                color: colors.ghost, 
                fontSize: '12px',
                fontWeight: 600,
                fontFamily: 'ui-monospace, monospace',
              }}>
                {progressPct}%
              </span>
            </div>
          </div>
        </Html>
      )}
    </group>
  );
};

// Main merge flow component - draws lines from EACH source part to result
const MergeFlowLines: React.FC<MergeFlowLinesProps> = ({ items, layout, highlightedMergeId, mergeColorMap, theme = 'dark' }) => {
  // Group merging items by their merge target
  const mergeGroups = useMemo(() => {
    const groups = new Map<string, MergeGroup>();
    
    items.forEach((item, i) => {
      if (item.merging && item.mergeTarget && item.mergeId) {
        if (!groups.has(item.mergeId)) {
          groups.set(item.mergeId, {
            mergeId: item.mergeId,
            targetName: item.mergeTarget,
            sourcePositions: [],
            progress: item.mergeProgress || 0,
          });
        }
        groups.get(item.mergeId)!.sourcePositions.push(layout[i].position);
        // Update progress to latest
        if (item.mergeProgress !== undefined) {
          groups.get(item.mergeId)!.progress = item.mergeProgress;
        }
      }
    });
    
    return Array.from(groups.values());
  }, [items, layout]);
  
  if (mergeGroups.length === 0) return null;
  
  const laneWidth = 5;
  
  // Group all parts by their X position (lane) to find frontmost part per lane
  const partsByLane = new Map<number, number[]>(); // X -> array of Z positions
  layout.forEach(l => {
    const laneX = Math.round(l.position[0]); // Round to handle floating point
    if (!partsByLane.has(laneX)) partsByLane.set(laneX, []);
    partsByLane.get(laneX)!.push(l.position[2]);
  });
  
  // Sort merge groups by their average Z position to assign distinct slots
  const sortedGroups = [...mergeGroups].sort((a, b) => {
    const avgZa = a.sourcePositions.reduce((sum, p) => sum + p[2], 0) / a.sourcePositions.length;
    const avgZb = b.sourcePositions.reduce((sum, p) => sum + p[2], 0) / b.sourcePositions.length;
    return avgZa - avgZb;
  });
  
  // Group merges by their target lane (X position) to only add spacing between merges on same lane
  const mergesByTargetLane = new Map<number, number>(); // targetLaneX -> count of merges already placed
  
  return (
    <group>
      {sortedGroups.map((group) => {
        const maxSourceX = Math.max(...group.sourcePositions.map(p => p[0]));
        const targetX = maxSourceX + laneWidth; // Next lane to the right
        const targetLaneX = Math.round(targetX);
        
        // Get how many merges are already on this lane
        const laneIndex = mergesByTargetLane.get(targetLaneX) || 0;
        mergesByTargetLane.set(targetLaneX, laneIndex + 1);
        
        // Find the frontmost part in the target lane
        const partsInTargetLane = partsByLane.get(targetLaneX) || [];
        const maxZInTargetLane = partsInTargetLane.length > 0 
          ? Math.max(...partsInTargetLane) 
          : Math.max(...group.sourcePositions.map(p => p[2])); // Fallback to source parts
        
        // Target position: in front of the frontmost part
        // Only add spacing if there are multiple merges on the SAME lane
        const targetZ = maxZInTargetLane + 1.8 + (laneIndex * 1.8);
        const targetY = 1.0;
        const targetPos: [number, number, number] = [targetX, targetY, targetZ];
        
        // Dim this merge if another merge is highlighted
        const isDimmed = highlightedMergeId ? group.mergeId !== highlightedMergeId : false;
        
        // Get color index from shared map, fallback to 0 if not found
        const colorIndex = mergeColorMap?.get(group.mergeId) ?? 0;
        
        return (
          <group key={group.mergeId}>
            {/* Individual flow lines from EACH source part to target */}
            {group.sourcePositions.map((sourcePos, i) => (
              <FlowLine
                key={i}
                start={sourcePos}
                end={targetPos}
                progress={group.progress}
                index={laneIndex * 10 + i}
                colorIndex={colorIndex}
                isDimmed={isDimmed}
              />
            ))}
            
            {/* Result part ghost */}
            <ResultPartGhost
              position={targetPos}
              targetName={group.targetName}
              progress={group.progress}
              sourceCount={group.sourcePositions.length}
              colorIndex={colorIndex}
              isDimmed={isDimmed}
              theme={theme}
            />
          </group>
        );
      })}
    </group>
  );
};

export const HierarchyVisualization: React.FC<HierarchyVisualizationProps> = ({ items, level, onItemClick, onItemHover, onPathClick: _onPathClick, highlightedMergeId, mergeColorMap: externalColorMap }) => {
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const { maxElements, performanceMode } = usePerformanceMode();
  const theme = useThemeDetection();
  
  // Sticky board size - prevents constant resizing
  // Uses refs to avoid re-render loops, only shrinks after delay
  const stickyGridSizeRef = useRef<number>(10);
  const stickyLaneLengthRef = useRef<number>(20);
  const shrinkCounterRef = useRef<number>(0);
  const SHRINK_DELAY = 10; // Number of renders before allowing shrink
  
  // NOTE: Fading parts animation disabled for now
  // Keep this state structure for when we re-enable animations
  /* ANIMATION STATE - uncomment to re-enable
  const [fadingParts, setFadingParts] = useState<Map<string, { 
    item: HierarchyItem; 
    layout: { position: [number, number, number]; size: [number, number, number] }; 
    colorIndex: number;
    startTime: number;
  }>>(new Map());
  const prevItemsRef = useRef<Map<string, HierarchyItem>>(new Map());
  const prevLayoutRef = useRef<{ position: [number, number, number]; size: [number, number, number] }[]>([]);
  */

  const limitedItems = useMemo(() => {
    if (performanceMode && items.length > maxElements) return [...items].sort((a, b) => b.size - a.size).slice(0, maxElements);
    return items;
  }, [items, performanceMode, maxElements]);

  // Use special layout for parts level
  const isPartsLevel = level === 'parts';
  
  const partsLayoutData = useMemo(() => 
    isPartsLevel ? calculatePartsLayout(limitedItems) : { layout: [], maxLevel: 0, levels: [] as number[] },
    [limitedItems, isPartsLevel]
  );
  
  /* ANIMATION EFFECT - uncomment to re-enable position holding and fading
  // Include fading parts in layout calculation so they "hold" their position
  const itemsWithFading = useMemo(() => {
    if (!isPartsLevel) return limitedItems;
    const fadingItems = Array.from(fadingParts.values()).map(f => f.item);
    const currentIds = new Set(limitedItems.map(i => i.id));
    const uniqueFading = fadingItems.filter(f => !currentIds.has(f.id));
    return [...limitedItems, ...uniqueFading];
  }, [limitedItems, fadingParts, isPartsLevel]);
  
  useEffect(() => {
    if (!isPartsLevel) {
      prevItemsRef.current = new Map(limitedItems.map(item => [item.id, item]));
      prevLayoutRef.current = partsLayoutData.layout;
      return;
    }
    
    const currentIds = new Set(limitedItems.map(item => item.id));
    const now = Date.now();
    const newFading = new Map(fadingParts);
    const prevItems = Array.from(prevItemsRef.current.values());
    const mergeTargetColors = new Map<string, number>();
    let nextColor = 0;
    
    prevItemsRef.current.forEach((item, id) => {
      if (!currentIds.has(id) && !newFading.has(id)) {
        const itemIndex = prevItems.findIndex(p => p.id === id);
        if (itemIndex >= 0 && prevLayoutRef.current[itemIndex]) {
          const target = item.mergeTarget || 'unknown';
          if (!mergeTargetColors.has(target)) {
            mergeTargetColors.set(target, nextColor++);
          }
          newFading.set(id, {
            item,
            layout: prevLayoutRef.current[itemIndex],
            colorIndex: mergeTargetColors.get(target)!,
            startTime: now,
          });
        }
      }
    });
    
    newFading.forEach((data, id) => {
      if (now - data.startTime > 1500) newFading.delete(id);
    });
    
    if (newFading.size !== fadingParts.size || 
        Array.from(newFading.keys()).some(k => !fadingParts.has(k))) {
      setFadingParts(newFading);
    }
    
    prevItemsRef.current = new Map(limitedItems.map(item => [item.id, item]));
    prevLayoutRef.current = partsLayoutData.layout;
  }, [limitedItems, isPartsLevel, fadingParts, partsLayoutData.layout]);
  */
  
  const standardLayout = useMemo(() => 
    !isPartsLevel ? calculateLayout(limitedItems) : [],
    [limitedItems, isPartsLevel]
  );

  // Calculate desired sizes and apply sticky logic (using refs to avoid re-render loops)
  const { gridSize, laneLength } = useMemo(() => {
    let calculatedGridSize: number;
    let calculatedLaneLength: number;
    
    if (isPartsLevel) {
      const { layout, levels } = partsLayoutData;
      if (layout.length === 0) {
        calculatedGridSize = 10;
        calculatedLaneLength = 20;
      } else {
        const numLanes = levels.length || 1;
        const maxZ = Math.max(...layout.map(l => Math.abs(l.position[2]))) + 4;
        calculatedGridSize = Math.max((numLanes + 1) * 5, maxZ, 10);
        calculatedLaneLength = Math.max(...layout.map(l => Math.abs(l.position[2]))) * 2 + 8;
      }
    } else {
      if (standardLayout.length === 0) {
        calculatedGridSize = 10;
      } else {
        const maxX = Math.max(...standardLayout.map(l => Math.abs(l.position[0]))) + 4;
        const maxZ = Math.max(...standardLayout.map(l => Math.abs(l.position[2]))) + 4;
        calculatedGridSize = Math.max(maxX, maxZ, 10);
      }
      calculatedLaneLength = 20;
    }
    
    const currentGridSize = stickyGridSizeRef.current;
    const currentLaneLength = stickyLaneLengthRef.current;
    
    // Check if we need to expand (immediate)
    if (calculatedGridSize > currentGridSize || calculatedLaneLength > currentLaneLength) {
      stickyGridSizeRef.current = Math.max(calculatedGridSize, currentGridSize);
      stickyLaneLengthRef.current = Math.max(calculatedLaneLength, currentLaneLength);
      shrinkCounterRef.current = 0;
      return { 
        gridSize: stickyGridSizeRef.current, 
        laneLength: stickyLaneLengthRef.current 
      };
    }
    
    // Check if we want to shrink (only if significantly smaller)
    const wantsShrink = calculatedGridSize < currentGridSize * 0.6 || calculatedLaneLength < currentLaneLength * 0.6;
    
    if (wantsShrink) {
      shrinkCounterRef.current++;
      if (shrinkCounterRef.current >= SHRINK_DELAY) {
        // Finally allowed to shrink
        stickyGridSizeRef.current = calculatedGridSize;
        stickyLaneLengthRef.current = calculatedLaneLength;
        shrinkCounterRef.current = 0;
        return { gridSize: calculatedGridSize, laneLength: calculatedLaneLength };
      }
    } else {
      // Not shrinking significantly, reset counter
      shrinkCounterRef.current = 0;
    }
    
    // Keep current sticky size
    return { gridSize: currentGridSize, laneLength: currentLaneLength };
  }, [isPartsLevel, partsLayoutData, standardLayout]);

  const handleHover = useCallback((item: HierarchyItem | null) => {
    setHoveredId(item?.id || null);
    onItemHover(item);
  }, [onItemHover]);
  
  // Use external color map if provided, otherwise use empty map
  // The parent component (DatabaseExplorer) manages the shared color assignments
  const mergeColorMap = externalColorMap || new Map<string, number>();
  
  if (items.length === 0) {
    return (
      <group>
        <Ground size={10} theme={theme} />
        <Html position={[0, 2, 0]} center style={{ pointerEvents: 'none' }}><div style={{ color: '#64748b', fontSize: '20px' }}>No {level} found</div></Html>
      </group>
    );
  }

  // Parts level - highway lanes layout
  if (isPartsLevel) {
    const { layout, maxLevel, levels } = partsLayoutData;
    // Use sticky laneLength from the memoized calculation above

    return (
      <group>
        <MergeLanes maxLevel={maxLevel} levels={levels} laneLength={laneLength} theme={theme} />
        
        {/* Merge flow visualization - swim lanes connecting source parts to result */}
        <MergeFlowLines items={limitedItems} layout={layout} maxLevel={maxLevel} highlightedMergeId={highlightedMergeId} mergeColorMap={mergeColorMap} theme={theme} />
        
        {limitedItems.map((item, i) => {
          // Determine if this part should be dimmed
          // Dim if there's a highlighted merge and this part is NOT part of it
          const isDimmed = highlightedMergeId 
            ? item.mergeId !== highlightedMergeId 
            : false;
          
          // Get merge color index for this part
          const mergeColorIndex = item.mergeId ? mergeColorMap.get(item.mergeId) : undefined;
          
          return (
            <GlassBox 
              key={item.id} 
              item={item} 
              position={layout[i].position} 
              size={layout[i].size} 
              level={level}
              mergeLevel={layout[i].mergeLevel}
              isHovered={hoveredId === item.id}
              isDimmed={isDimmed}
              mergeColorIndex={mergeColorIndex}
              theme={theme}
              onClick={() => onItemClick(item)}
              onPointerOver={() => handleHover(item)} 
              onPointerOut={() => handleHover(null)} 
            />
          );
        })}
        
        {/* NOTE: Fading parts animation disabled - uncomment to re-enable
        {Array.from(fadingParts.values()).map(({ item, layout: fadeLayout, colorIndex, startTime }) => (
          <FadingGlassBox
            key={`fading-${item.id}`}
            position={fadeLayout.position}
            size={fadeLayout.size}
            colorIndex={colorIndex}
            startTime={startTime}
          />
        ))}
        */}
      </group>
    );
  }

  // Standard grid layout for other levels
  return (
    <group>
      <Ground size={gridSize} theme={theme} />
      {limitedItems.map((item, i) => (
        <GlassBox key={item.id} item={item} position={standardLayout[i].position} size={standardLayout[i].size} level={level}
          isHovered={hoveredId === item.id} theme={theme} onClick={() => onItemClick(item)}
          onPointerOver={() => handleHover(item)} onPointerOut={() => handleHover(null)} />
      ))}
    </group>
  );
};

export default HierarchyVisualization;
