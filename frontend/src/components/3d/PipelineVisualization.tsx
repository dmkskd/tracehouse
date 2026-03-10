/**
 * PipelineVisualization - 3D visualization of query execution pipeline
 * 
 * Parses EXPLAIN PIPELINE output into a node graph structure and renders
 * pipeline stages as connected 3D nodes. Supports highlighting nodes with
 * correlated log entries and click interaction for node details.
 */

import React, { useMemo, useState, useCallback, useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import { Html, Line } from '@react-three/drei';
import { SafeText as Text } from '@tracehouse/ui-shared';
import * as THREE from 'three';
import { usePerformanceMode } from './PerformanceContext';
import type { ExplainResult, TraceLog } from '../../stores/traceStore';

/**
 * Pipeline node representing a stage in the execution plan
 */
export interface PipelineNode {
  /** Unique identifier for the node */
  id: string;
  /** Display name of the pipeline stage */
  name: string;
  /** Full text of the pipeline stage */
  fullText: string;
  /** Depth level in the pipeline tree */
  depth: number;
  /** Parent node ID (null for root nodes) */
  parentId: string | null;
  /** Child node IDs */
  childIds: string[];
  /** Whether this node has correlated log entries */
  hasCorrelatedLogs: boolean;
  /** Number of correlated log entries */
  correlatedLogCount: number;
  /** Additional metadata extracted from the pipeline */
  metadata: Record<string, string>;
}

/**
 * Props for PipelineVisualization component
 */
export interface PipelineVisualizationProps {
  /** EXPLAIN result containing pipeline output */
  pipeline: ExplainResult;
  /** Optional trace logs for correlation highlighting */
  traceLogs?: TraceLog[];
  /** Callback when a node is clicked */
  onNodeClick: (node: PipelineNode) => void;
}

/**
 * Internal node data with calculated visual properties
 */
interface NodeWithVisuals extends PipelineNode {
  position: [number, number, number];
  color: THREE.Color;
}

/**
 * Color palette for pipeline nodes
 */
const NODE_COLORS = {
  default: new THREE.Color(0x3b82f6),      // Blue
  highlighted: new THREE.Color(0xf59e0b),   // Amber (has logs)
  selected: new THREE.Color(0x22c55e),      // Green
  root: new THREE.Color(0x8b5cf6),          // Purple
  leaf: new THREE.Color(0x06b6d4),          // Cyan
};

/**
 * Parse EXPLAIN PIPELINE output into a graph structure
 * 
 * The EXPLAIN PIPELINE output typically looks like:
 * (Expression)
 * ExpressionTransform
 *   (ReadFromMergeTree)
 *   MergeTreeSelect(pool: ReadPool, algorithm: Thread)
 *     ...
 */
export function parsePipelineOutput(output: string): PipelineNode[] {
  const lines = output.split('\n').filter(line => line.trim());
  const nodes: PipelineNode[] = [];
  const nodeStack: { id: string; depth: number }[] = [];
  let nodeId = 0;

  for (const line of lines) {
    // Calculate depth based on leading whitespace
    const trimmedLine = line.trimStart();
    const leadingSpaces = line.length - trimmedLine.length;
    const depth = Math.floor(leadingSpaces / 2); // Assume 2-space indentation

    // Extract node name and metadata
    const { name, metadata } = parseNodeLine(trimmedLine);
    
    if (!name) continue;

    // Create node
    const id = `node-${nodeId++}`;
    
    // Find parent by looking at the stack
    let parentId: string | null = null;
    while (nodeStack.length > 0 && nodeStack[nodeStack.length - 1].depth >= depth) {
      nodeStack.pop();
    }
    if (nodeStack.length > 0) {
      parentId = nodeStack[nodeStack.length - 1].id;
      // Add this node as a child of the parent
      const parentNode = nodes.find(n => n.id === parentId);
      if (parentNode) {
        parentNode.childIds.push(id);
      }
    }

    const node: PipelineNode = {
      id,
      name,
      fullText: trimmedLine,
      depth,
      parentId,
      childIds: [],
      hasCorrelatedLogs: false,
      correlatedLogCount: 0,
      metadata,
    };

    nodes.push(node);
    nodeStack.push({ id, depth });
  }

  return nodes;
}

/**
 * Parse a single line from the pipeline output
 */
function parseNodeLine(line: string): { name: string; metadata: Record<string, string> } {
  const metadata: Record<string, string> = {};
  
  // Handle lines wrapped in parentheses (stage names)
  if (line.startsWith('(') && line.includes(')')) {
    const match = line.match(/^\(([^)]+)\)/);
    if (match) {
      return { name: match[1], metadata };
    }
  }

  // Handle lines with parameters like "MergeTreeSelect(pool: ReadPool, algorithm: Thread)"
  const paramMatch = line.match(/^(\w+)\(([^)]*)\)/);
  if (paramMatch) {
    const name = paramMatch[1];
    const params = paramMatch[2];
    
    // Parse parameters
    if (params) {
      const paramPairs = params.split(',').map(p => p.trim());
      for (const pair of paramPairs) {
        const [key, value] = pair.split(':').map(s => s.trim());
        if (key && value) {
          metadata[key] = value;
        }
      }
    }
    
    return { name, metadata };
  }

  // Handle simple names (e.g., "ExpressionTransform")
  const simpleMatch = line.match(/^(\w+)/);
  if (simpleMatch) {
    return { name: simpleMatch[1], metadata };
  }

  return { name: line.trim(), metadata };
}

/**
 * Correlate nodes with trace logs
 */
function correlateNodesWithLogs(
  nodes: PipelineNode[],
  traceLogs: TraceLog[]
): PipelineNode[] {
  if (!traceLogs || traceLogs.length === 0) {
    return nodes;
  }

  return nodes.map(node => {
    // Look for log entries that mention this node's name
    const correlatedLogs = traceLogs.filter(log => {
      const message = log.message.toLowerCase();
      const nodeName = node.name.toLowerCase();
      return message.includes(nodeName) || 
             message.includes(nodeName.replace(/transform$/i, '')) ||
             message.includes(nodeName.replace(/step$/i, ''));
    });

    return {
      ...node,
      hasCorrelatedLogs: correlatedLogs.length > 0,
      correlatedLogCount: correlatedLogs.length,
    };
  });
}

/**
 * Calculate 3D positions for nodes in a tree layout
 */
function calculateNodePositions(nodes: PipelineNode[]): Map<string, [number, number, number]> {
  const positions = new Map<string, [number, number, number]>();
  
  if (nodes.length === 0) return positions;

  // Group nodes by depth
  const nodesByDepth = new Map<number, PipelineNode[]>();
  for (const node of nodes) {
    const depthNodes = nodesByDepth.get(node.depth) || [];
    depthNodes.push(node);
    nodesByDepth.set(node.depth, depthNodes);
  }

  const maxDepth = Math.max(...nodes.map(n => n.depth));
  const horizontalSpacing = 3;
  const verticalSpacing = 2.5;

  // Position nodes at each depth level
  for (const [depth, depthNodes] of nodesByDepth) {
    const count = depthNodes.length;
    const totalWidth = (count - 1) * horizontalSpacing;
    const startX = -totalWidth / 2;

    depthNodes.forEach((node, index) => {
      const x = startX + index * horizontalSpacing;
      const y = (maxDepth - depth) * verticalSpacing; // Root at top
      const z = 0;
      positions.set(node.id, [x, y, z]);
    });
  }

  return positions;
}

/**
 * Get color for a node based on its properties
 */
function getNodeColor(node: PipelineNode, isSelected: boolean, isHovered: boolean): THREE.Color {
  if (isSelected) return NODE_COLORS.selected;
  if (node.hasCorrelatedLogs) return NODE_COLORS.highlighted;
  if (isHovered) return new THREE.Color(0x60a5fa); // Lighter blue
  if (node.parentId === null) return NODE_COLORS.root;
  if (node.childIds.length === 0) return NODE_COLORS.leaf;
  return NODE_COLORS.default;
}

/**
 * Individual Pipeline Node component
 */
interface PipelineNodeMeshProps {
  node: NodeWithVisuals;
  isHovered: boolean;
  isSelected: boolean;
  onClick: () => void;
  onPointerOver: () => void;
  onPointerOut: () => void;
  enableAnimations: boolean;
}

const PipelineNodeMesh: React.FC<PipelineNodeMeshProps> = ({
  node,
  isHovered,
  isSelected,
  onClick,
  onPointerOver,
  onPointerOut,
  enableAnimations,
}) => {
  const meshRef = useRef<THREE.Mesh>(null);
  const baseScale = 0.4;
  
  // Animate hover/selection effect
  useFrame((_, delta) => {
    if (!meshRef.current || !enableAnimations) return;
    
    const targetScale = isHovered || isSelected ? baseScale * 1.2 : baseScale;
    const currentScale = meshRef.current.scale.x;
    const newScale = THREE.MathUtils.lerp(currentScale, targetScale, delta * 8);
    
    meshRef.current.scale.setScalar(newScale);
  });
  
  // Calculate emissive intensity for hover/selection
  const emissiveIntensity = isSelected ? 0.5 : isHovered ? 0.3 : node.hasCorrelatedLogs ? 0.2 : 0;
  
  return (
    <mesh
      ref={meshRef}
      position={node.position}
      scale={enableAnimations ? undefined : baseScale}
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
      <sphereGeometry args={[1, 16, 16]} />
      <meshStandardMaterial
        color={node.color}
        emissive={node.color}
        emissiveIntensity={emissiveIntensity}
        metalness={0.3}
        roughness={0.6}
      />
    </mesh>
  );
};

/**
 * Node label component
 */
interface NodeLabelProps {
  node: NodeWithVisuals;
  visible: boolean;
}

const NodeLabel: React.FC<NodeLabelProps> = ({ node, visible }) => {
  if (!visible) return null;
  
  const labelPosition: [number, number, number] = [
    node.position[0],
    node.position[1] + 0.7,
    node.position[2],
  ];
  
  // Truncate long names
  const displayName = node.name.length > 20 
    ? node.name.substring(0, 17) + '...' 
    : node.name;
  
  return (
    <Text
      position={labelPosition}
      fontSize={0.2}
      color="white"
      anchorX="center"
      anchorY="bottom"
      outlineWidth={0.015}
      outlineColor="black"
    >
      {displayName}
    </Text>
  );
};

/**
 * Node tooltip component showing detailed info
 */
interface NodeTooltipProps {
  node: NodeWithVisuals;
  visible: boolean;
}

const NodeTooltip: React.FC<NodeTooltipProps> = ({ node, visible }) => {
  if (!visible) return null;
  
  const tooltipPosition: [number, number, number] = [
    node.position[0],
    node.position[1] + 1.2,
    node.position[2],
  ];
  
  return (
    <Html position={tooltipPosition} center>
      <div className="bg-gray-900 text-white text-xs p-3 rounded-lg shadow-lg max-w-xs pointer-events-none">
        <div className="font-bold mb-1 text-blue-300">{node.name}</div>
        <div className="text-gray-300 text-[10px] mb-2 break-all">{node.fullText}</div>
        
        {Object.keys(node.metadata).length > 0 && (
          <div className="border-t border-gray-700 pt-2 mt-2">
            {Object.entries(node.metadata).map(([key, value]) => (
              <div key={key} className="flex justify-between gap-2">
                <span className="text-gray-400">{key}:</span>
                <span className="text-green-300">{value}</span>
              </div>
            ))}
          </div>
        )}
        
        {node.hasCorrelatedLogs && (
          <div className="border-t border-gray-700 pt-2 mt-2">
            <span className="text-amber-400">
              * {node.correlatedLogCount} correlated log{node.correlatedLogCount !== 1 ? 's' : ''}
            </span>
          </div>
        )}
        
        <div className="text-gray-500 text-[9px] mt-2">
          Depth: {node.depth} | Children: {node.childIds.length}
        </div>
      </div>
    </Html>
  );
};

/**
 * Connection line between parent and child nodes
 */
interface ConnectionLineProps {
  start: [number, number, number];
  end: [number, number, number];
  highlighted: boolean;
}

const ConnectionLine: React.FC<ConnectionLineProps> = ({ start, end, highlighted }) => {
  const color = highlighted ? '#f59e0b' : '#4b5563';
  
  return (
    <Line
      points={[start, end]}
      color={color}
      lineWidth={highlighted ? 2 : 1}
      opacity={highlighted ? 1 : 0.6}
      transparent
    />
  );
};

/**
 * Legend component showing node color meanings
 */
const Legend: React.FC = () => {
  const items = [
    { color: NODE_COLORS.root, label: 'Root Stage' },
    { color: NODE_COLORS.default, label: 'Pipeline Stage' },
    { color: NODE_COLORS.leaf, label: 'Leaf Stage' },
    { color: NODE_COLORS.highlighted, label: 'Has Logs' },
  ];
  
  return (
    <Html position={[-6, 4, 0]}>
      <div className="bg-gray-800/90 text-white text-xs p-2 rounded pointer-events-none">
        <div className="font-bold mb-1">Pipeline Stages</div>
        {items.map(({ color, label }) => (
          <div key={label} className="flex items-center gap-2">
            <div
              className="w-3 h-3 rounded-full"
              style={{ backgroundColor: `#${color.getHexString()}` }}
            />
            <span>{label}</span>
          </div>
        ))}
      </div>
    </Html>
  );
};

/**
 * PipelineVisualization - Main component for 3D pipeline visualization
 * 
 * Renders query execution pipeline as a 3D graph with:
 * - Pipeline stages as connected spheres
 * - Color coding based on node type and log correlation
 * - Click interaction for node details
 * - Highlighting for nodes with correlated logs
 * 
 * @example
 * ```tsx
 * <Scene3D config={config}>
 *   <PipelineVisualization
 *     pipeline={explainResult}
 *     traceLogs={traceLogs}
 *     onNodeClick={(node) => console.log('Clicked:', node)}
 *   />
 * </Scene3D>
 * ```
 */
export const PipelineVisualization: React.FC<PipelineVisualizationProps> = ({
  pipeline,
  traceLogs,
  onNodeClick,
}) => {
  const { performanceMode, enableAnimations, maxElements } = usePerformanceMode();
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  
  // Parse pipeline output into nodes
  const parsedNodes = useMemo(() => {
    if (!pipeline?.output) return [];
    return parsePipelineOutput(pipeline.output);
  }, [pipeline?.output]);
  
  // Correlate nodes with trace logs
  const correlatedNodes = useMemo(() => {
    return correlateNodesWithLogs(parsedNodes, traceLogs || []);
  }, [parsedNodes, traceLogs]);
  
  // Limit nodes in performance mode
  const limitedNodes = useMemo(() => {
    if (performanceMode && correlatedNodes.length > maxElements) {
      return correlatedNodes.slice(0, maxElements);
    }
    return correlatedNodes;
  }, [correlatedNodes, performanceMode, maxElements]);
  
  // Calculate positions for nodes
  const positions = useMemo(() => {
    return calculateNodePositions(limitedNodes);
  }, [limitedNodes]);
  
  // Combine nodes with visual properties
  const nodesWithVisuals: NodeWithVisuals[] = useMemo(() => {
    return limitedNodes.map(node => ({
      ...node,
      position: positions.get(node.id) || [0, 0, 0],
      color: getNodeColor(
        node,
        selectedNode === node.id,
        hoveredNode === node.id
      ),
    }));
  }, [limitedNodes, positions, selectedNode, hoveredNode]);
  
  // Create a map for quick node lookup
  const nodeMap = useMemo(() => {
    return new Map(nodesWithVisuals.map(n => [n.id, n]));
  }, [nodesWithVisuals]);
  
  // Generate connection lines between parent-child nodes
  const connections = useMemo(() => {
    const lines: { start: [number, number, number]; end: [number, number, number]; highlighted: boolean }[] = [];
    
    for (const node of nodesWithVisuals) {
      if (node.parentId) {
        const parentNode = nodeMap.get(node.parentId);
        if (parentNode) {
          const isHighlighted = 
            node.hasCorrelatedLogs || 
            parentNode.hasCorrelatedLogs ||
            selectedNode === node.id ||
            selectedNode === parentNode.id;
          
          lines.push({
            start: parentNode.position,
            end: node.position,
            highlighted: isHighlighted,
          });
        }
      }
    }
    
    return lines;
  }, [nodesWithVisuals, nodeMap, selectedNode]);
  
  // Handle node click
  const handleNodeClick = useCallback((node: PipelineNode) => {
    setSelectedNode(prev => prev === node.id ? null : node.id);
    onNodeClick(node);
  }, [onNodeClick]);
  
  // Handle hover
  const handlePointerOver = useCallback((nodeId: string) => {
    setHoveredNode(nodeId);
  }, []);
  
  const handlePointerOut = useCallback(() => {
    setHoveredNode(null);
  }, []);
  
  // Show message if no pipeline data
  if (!pipeline?.output || parsedNodes.length === 0) {
    return (
      <Text
        position={[0, 1, 0]}
        fontSize={0.4}
        color="white"
        anchorX="center"
        anchorY="middle"
      >
        No pipeline data to display
      </Text>
    );
  }
  
  return (
    <group>
      {/* Legend */}
      <Legend />
      
      {/* Connection lines */}
      {connections.map((conn, index) => (
        <ConnectionLine
          key={`conn-${index}`}
          start={conn.start}
          end={conn.end}
          highlighted={conn.highlighted}
        />
      ))}
      
      {/* Pipeline nodes */}
      {nodesWithVisuals.map((node) => (
        <group key={node.id}>
          <PipelineNodeMesh
            node={node}
            isHovered={hoveredNode === node.id}
            isSelected={selectedNode === node.id}
            onClick={() => handleNodeClick(node)}
            onPointerOver={() => handlePointerOver(node.id)}
            onPointerOut={handlePointerOut}
            enableAnimations={enableAnimations}
          />
          
          {/* Show label on hover or selection */}
          <NodeLabel
            node={node}
            visible={hoveredNode === node.id || selectedNode === node.id}
          />
          
          {/* Show tooltip on selection */}
          <NodeTooltip
            node={node}
            visible={selectedNode === node.id}
          />
        </group>
      ))}
      
      {/* Performance mode indicator */}
      {performanceMode && correlatedNodes.length > maxElements && (
        <Html position={[0, -2, 0]} center>
          <div className="bg-yellow-500/80 text-yellow-900 text-xs px-2 py-1 rounded">
            Showing {maxElements} of {correlatedNodes.length} nodes (performance mode)
          </div>
        </Html>
      )}
      
      {/* Stats display */}
      <Html position={[6, 4, 0]}>
        <div className="bg-gray-800/90 text-white text-xs p-2 rounded pointer-events-none">
          <div className="font-bold mb-1">Pipeline Stats</div>
          <div>Stages: {nodesWithVisuals.length}</div>
          <div>Max Depth: {Math.max(...nodesWithVisuals.map(n => n.depth), 0)}</div>
          {traceLogs && traceLogs.length > 0 && (
            <div>Logs: {traceLogs.length}</div>
          )}
          <div className="text-amber-400">
            Correlated: {nodesWithVisuals.filter(n => n.hasCorrelatedLogs).length}
          </div>
        </div>
      </Html>
    </group>
  );
};

export default PipelineVisualization;
