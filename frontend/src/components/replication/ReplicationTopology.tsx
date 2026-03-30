/**
 * ReplicationTopology — per-table 3D + 2D visualization showing shard/replica layout,
 * ZooKeeper connections, data distribution, and queue health.
 *
 * The 3D scene reuses the glass-box visual language from ClusterTopology.
 * Below it, compact 2D panels show data distribution, keeper sync state, and recovery guides.
 */

import React, { useEffect, useState, useMemo, useRef, useCallback } from 'react';
import { Canvas, useFrame } from '@react-three/fiber';
import { Html, OrbitControls } from '@react-three/drei';
import * as THREE from 'three';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useConnectionStore } from '../../stores/connectionStore';
import { useClusterStore } from '../../stores/clusterStore';
import { ReplicationService, classifyReplicaHealth, classifyDelaySeverity, tagQuery, buildQuery, sourceTag, GET_ZK_CHILDREN, mapZkChildNode } from '@tracehouse/core';
import type {
  ShardPartitionDist,
  TopologyShard,
  TopologyReplica,
  ReplicationTopologyData,
  TopologyQueueEntry,
  KeeperTableInfo,
  KeeperConnection,
  DistributionQueueEntry,
  ZkChildNode,
} from '@tracehouse/core';
import { ErrorBoundary3D } from '../3d/ErrorBoundary3D';
import { useThemeDetection } from '../../hooks/useThemeDetection';
import { formatBytes } from '../../utils/formatters';

// ── Helpers ──

function formatDelay(seconds: number): string {
  if (seconds === 0) return '0s';
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

// ── Color constants ──

const COL = {
  green: '#22c55e', greenBg: 'rgba(34,197,94,0.12)',
  amber: '#f59e0b', amberBg: 'rgba(245,158,11,0.12)',
  red: '#ef4444', redBg: 'rgba(239,68,68,0.12)',
  blue: '#60a5fa', blueBg: 'rgba(96,165,250,0.12)',
  purple: '#a78bfa', purpleBg: 'rgba(167,139,250,0.12)',
  muted: 'var(--text-muted)', primary: 'var(--text-primary)',
  border: 'var(--border-secondary)', cardBg: 'var(--bg-card)', bgSecondary: 'var(--bg-secondary)',
};

const PARTITION_COLORS = [
  '#60a5fa', '#34d399', '#fbbf24', '#f87171', '#a78bfa',
  '#fb923c', '#2dd4bf', '#e879f9', '#38bdf8', '#4ade80',
];

// ── 3D Colors ──

const COLORS_3D = {
  healthy:  { main: '#22c55e', edge: '#4ade80', glow: '#22c55e' },
  lag:      { main: '#f59e0b', edge: '#fbbf24', glow: '#f59e0b' },
  error:    { main: '#ef4444', edge: '#f87171', glow: '#ef4444' },
  leader:   { main: '#58a6ff', edge: '#79b8ff', glow: '#58a6ff' },
  keeper:   { main: '#a78bfa', edge: '#c4b5fd', glow: '#a78bfa' },
};

// ── 3D: Hover Row ──

const HoverRow: React.FC<{ label: string; value: string; theme: 'dark' | 'light'; color?: string }> = ({ label, value, theme, color }) => (
  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}>
    <span style={{ color: theme === 'light' ? 'rgba(0,0,0,0.45)' : 'rgba(255,255,255,0.45)', fontSize: 10 }}>{label}</span>
    <span style={{ color: color ?? (theme === 'light' ? 'rgba(0,0,0,0.85)' : 'rgba(255,255,255,0.85)'), fontSize: 11, fontWeight: 500, fontFamily: 'ui-monospace, monospace' }}>{value}</span>
  </div>
);

// ── 3D: Replica Queue Block (hoverable, shows operation type) ──

const ReplicaQueueBlock: React.FC<{
  position: [number, number, number];
  size: [number, number, number];
  color: { main: string; edge: string; glow: string };
  entry: TopologyQueueEntry;
  isExec: boolean;
  hasErr: boolean;
  theme: 'dark' | 'light';
}> = ({ position, size, color, entry, isExec, hasErr, theme }) => {
  const [hovered, setHovered] = useState(false);
  return (
    <group>
      <mesh
        position={position}
        onPointerOver={(e) => { e.stopPropagation(); setHovered(true); document.body.style.cursor = 'pointer'; }}
        onPointerOut={(e) => { e.stopPropagation(); setHovered(false); document.body.style.cursor = 'auto'; }}
      >
        <boxGeometry args={size} />
        <meshStandardMaterial
          color={color.main}
          emissive={color.main}
          emissiveIntensity={hovered ? 0.8 : isExec ? 0.5 : 0.3}
          transparent opacity={0.9}
          metalness={0.2} roughness={0.3}
        />
      </mesh>
      {hovered && (
        <Html position={[position[0] + size[0] / 2 + 0.1, position[1], position[2]]} center style={{ pointerEvents: 'none', zIndex: 1000 }}>
          <div style={{
            background: theme === 'light' ? 'rgba(255,255,255,0.95)' : 'rgba(15,23,42,0.95)',
            padding: '6px 10px', borderRadius: 4,
            border: `1px solid ${color.edge}60`,
            backdropFilter: 'blur(12px)', transform: 'scale(0.8)',
            whiteSpace: 'nowrap', minWidth: 120,
          }}>
            <div style={{ fontSize: 10, fontWeight: 600, fontFamily: 'monospace', color: color.glow, marginBottom: 3 }}>
              {entry.type}
              <span style={{ fontSize: 8, marginLeft: 4, padding: '0 3px', borderRadius: 2,
                backgroundColor: hasErr ? `${COLORS_3D.error.main}30` : isExec ? `${COLORS_3D.healthy.main}30` : `${COLORS_3D.lag.main}30`,
                color: hasErr ? COLORS_3D.error.main : isExec ? COLORS_3D.healthy.main : COLORS_3D.lag.main,
              }}>
                {hasErr ? 'ERROR' : isExec ? 'RUNNING' : 'PENDING'}
              </span>
            </div>
            <div style={{ fontSize: 9, fontFamily: 'monospace', color: theme === 'light' ? 'rgba(0,0,0,0.6)' : 'rgba(255,255,255,0.6)', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 200 }}>
              {entry.new_part_name}
            </div>
            {entry.num_tries > 1 && (
              <div style={{ fontSize: 8, color: COLORS_3D.lag.main, marginTop: 2 }}>
                {entry.num_tries} tries
              </div>
            )}
            {entry.last_exception && (
              <div style={{ fontSize: 8, color: COLORS_3D.error.main, marginTop: 2, maxWidth: 220, wordBreak: 'break-all' }}>
                {entry.last_exception.slice(0, 120)}
              </div>
            )}
          </div>
        </Html>
      )}
    </group>
  );
};

// ── 3D: Replica Node Box ──

const ReplicaNodeBox: React.FC<{
  position: [number, number, number];
  size: [number, number, number];
  replica: TopologyReplica;
  logHead: number;
  queueEntries: TopologyQueueEntry[];
  allLeaders: boolean;
  theme: 'dark' | 'light';
}> = ({ position, size, replica, logHead, queueEntries, allLeaders, theme }) => {
  const meshRef = useRef<THREE.Mesh>(null);
  const [hovered, setHovered] = useState(false);
  const { info, parts } = replica;

  const isLeader = Number(info.is_leader) === 1;
  const delay = Number(info.absolute_delay);
  const queue = Number(info.queue_size);
  const logBehind = logHead - Number(info.log_pointer);
  const myQueue = queueEntries.filter(e => e.replica_name === info.replica_name);

  const hasData = parts != null && parts.bytes_on_disk > 0;
  const health = classifyReplicaHealth(info, queueEntries);

  const color = health.status === 'error' ? COLORS_3D.error
    : health.status === 'warning' ? COLORS_3D.lag
    : !hasData ? { main: '#475569', edge: '#64748b', glow: '#64748b' }
    : COLORS_3D.healthy;

  const edgesGeometry = useMemo(() => {
    const geo = new THREE.BoxGeometry(size[0] * 1.002, size[1] * 1.002, size[2] * 1.002);
    return new THREE.EdgesGeometry(geo, 1);
  }, [size]);

  useEffect(() => () => { edgesGeometry.dispose(); }, [edgesGeometry]);

  useFrame((state) => {
    if (!meshRef.current) return;
    if (!hasData) return; // no bobbing for empty replicas
    if (myQueue.length > 0) return; // no bobbing when work is pending — blocks are grounded
    const t = state.clock.elapsedTime;
    meshRef.current.position.y = position[1] + Math.sin(t * 0.6 + position[0] * 2) * 0.04;
  });

  return (
    <group position={[position[0], 0, position[2]]}>
      {hasData ? (
        <mesh
          ref={meshRef}
          position={[0, position[1], 0]}
          onPointerOver={(e) => { e.stopPropagation(); setHovered(true); document.body.style.cursor = 'pointer'; }}
          onPointerOut={(e) => { e.stopPropagation(); setHovered(false); document.body.style.cursor = 'auto'; }}
          castShadow receiveShadow
        >
          <boxGeometry args={size} />
          <meshPhysicalMaterial
            color={color.main} metalness={0.1} roughness={0.05}
            transmission={0.5} thickness={1.5} transparent
            opacity={hovered ? 0.85 : 0.65}
            emissive={color.main}
            emissiveIntensity={hovered ? 0.4 : (isLeader ? 0.25 : 0.15)}
            clearcoat={1} clearcoatRoughness={0.1} ior={1.5}
          />
        </mesh>
      ) : (
        /* Empty replica — wireframe ghost only */
        <mesh
          ref={meshRef}
          position={[0, position[1], 0]}
          onPointerOver={(e) => { e.stopPropagation(); setHovered(true); document.body.style.cursor = 'pointer'; }}
          onPointerOut={(e) => { e.stopPropagation(); setHovered(false); document.body.style.cursor = 'auto'; }}
        >
          <boxGeometry args={size} />
          <meshBasicMaterial color={color.main} transparent opacity={0.08} wireframe={false} />
        </mesh>
      )}

      <lineSegments position={[0, position[1], 0]}>
        <primitive object={edgesGeometry} attach="geometry" />
        <lineBasicMaterial color={color.edge} transparent opacity={hasData ? (hovered ? 1 : 0.7) : 0.2} />
      </lineSegments>

      {/* Health bar at base */}
      <mesh position={[0, 0.04, 0]}>
        <boxGeometry args={[size[0] * 1.08, 0.06, size[2] * 1.08]} />
        <meshBasicMaterial color={color.main} transparent opacity={hasData ? 0.8 : 0.15} />
      </mesh>

      {/* Replication queue blocks — stacked up from ground directly under replica */}
      {myQueue.length > 0 && (
        <group position={[0, 0, 0]}>
          {myQueue.slice(0, 5).map((entry, i) => {
            const isExec = Number(entry.is_currently_executing) === 1;
            const hasErr = !!entry.last_exception;
            const qColor = hasErr ? COLORS_3D.error : isExec ? COLORS_3D.healthy : COLORS_3D.lag;
            const bSize: [number, number, number] = [size[0] * 0.55, 0.05, size[2] * 0.55];
            // Stack upward from ground
            const y = 0.07 + i * (bSize[1] + 0.02) + bSize[1] / 2;
            return (
              <ReplicaQueueBlock key={i} position={[0, y, 0]} size={bSize} color={qColor} entry={entry} isExec={isExec} hasErr={hasErr} theme={theme} />
            );
          })}
          {myQueue.length > 5 && (
            <Html position={[0, 0.07 + 5 * 0.07 + 0.08, 0]} center style={{ pointerEvents: 'none' }}>
              <span style={{ fontSize: 7, color: 'rgba(255,255,255,0.5)', fontFamily: 'monospace' }}>+{myQueue.length - 5}</span>
            </Html>
          )}
        </group>
      )}

      {/* Always-visible info card above node */}
      <Html
        position={[0, position[1] + size[1] / 2 + 0.2, 0]}
        center style={{ pointerEvents: 'none', whiteSpace: 'nowrap' }}
        zIndexRange={[10, 0]}
      >
        <div style={{
          textAlign: 'center',
          background: 'rgba(10,15,30,0.85)',
          border: `1px solid ${color.edge}40`,
          borderRadius: 4,
          padding: '4px 8px',
          backdropFilter: 'blur(6px)',
          minWidth: 100,
        }}>
          <div style={{
            fontSize: 10, fontFamily: 'ui-monospace, monospace', fontWeight: 600,
            color: color.glow, marginBottom: 2,
            display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 4,
          }}>
            {info.hostname}
            {isLeader && !allLeaders && <span style={{ fontSize: 8, padding: '0 3px', borderRadius: 2, backgroundColor: `${COLORS_3D.leader.main}30`, color: COLORS_3D.leader.main }}>LEADER</span>}
            {health.status === 'error' && <span style={{ fontSize: 8, padding: '0 3px', borderRadius: 2, backgroundColor: `${COLORS_3D.error.main}30`, color: COLORS_3D.error.main }}>
              {health.reasons[0]?.includes('read-only') ? 'READ-ONLY' : 'ERROR'}
            </span>}
          </div>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'center', fontSize: 9, fontFamily: 'ui-monospace, monospace' }}>
            {parts && Number(parts.bytes_on_disk) > 0 ? (
              <>
                <span style={{ color: 'rgba(255,255,255,0.5)' }}>{formatBytes(Number(parts.bytes_on_disk))}</span>
                <span style={{ color: 'rgba(255,255,255,0.5)' }}>{Number(parts.part_count)}p</span>
                <span style={{ color: 'rgba(255,255,255,0.4)' }}>{Number(parts.total_rows).toLocaleString()} rows</span>
              </>
            ) : (
              <span style={{ color: 'rgba(255,255,255,0.3)', fontStyle: 'italic' }}>no data</span>
            )}
            {delay > 0 && <span style={{ color: delay > 300 ? COLORS_3D.error.main : COLORS_3D.lag.main }}>{formatDelay(delay)} behind</span>}
            {queue > 0 && <span style={{ color: COLORS_3D.lag.main }}>{queue} queued</span>}
          </div>
          {logBehind > 0 && delay > 0 && (
            <div style={{ fontSize: 8, color: COLORS_3D.lag.main, marginTop: 1 }}>
              log: -{logBehind} behind
            </div>
          )}
        </div>
      </Html>

      {/* Glow on hover */}
      {hovered && (
        <mesh position={[0, 0.02, 0]} rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[size[0] * 2, size[2] * 2]} />
          <meshBasicMaterial color={color.glow} transparent opacity={0.12} />
        </mesh>
      )}

      {/* Hover card */}
      {hovered && (
        <Html
          position={[0, position[1] + size[1] / 2 + 0.25, size[2] / 2 + 0.3]}
          center style={{ pointerEvents: 'none', zIndex: 1000 }}
        >
          <div style={{
            background: theme === 'light' ? 'rgba(255,255,255,0.95)' : 'rgba(15,23,42,0.92)',
            padding: '10px 14px', borderRadius: 4,
            border: `1px solid ${color.edge}50`,
            boxShadow: `0 0 16px ${color.main}20`,
            minWidth: 200, backdropFilter: 'blur(12px)', transform: 'scale(0.85)',
          }}>
            <div style={{
              fontSize: 12, fontWeight: 600, fontFamily: 'ui-monospace, monospace',
              color: color.glow, marginBottom: 6, paddingBottom: 6,
              borderBottom: `1px solid ${color.edge}30`,
              display: 'flex', alignItems: 'center', gap: 6,
            }}>
              {info.replica_name}
              {isLeader && !allLeaders && <span style={{ fontSize: 9, padding: '0 4px', borderRadius: 3, backgroundColor: `${COLORS_3D.leader.main}30`, color: COLORS_3D.leader.main }}>LEADER</span>}
              {health.status !== 'healthy' && <span style={{ fontSize: 9, padding: '0 4px', borderRadius: 3, backgroundColor: `${health.status === 'error' ? COLORS_3D.error.main : COLORS_3D.lag.main}30`, color: health.status === 'error' ? COLORS_3D.error.main : COLORS_3D.lag.main }}>{health.status.toUpperCase()}</span>}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
              <HoverRow label="Parts" value={parts ? Number(parts.part_count).toLocaleString() : '—'} theme={theme} />
              <HoverRow label="Size" value={parts ? formatBytes(Number(parts.bytes_on_disk)) : '—'} theme={theme} />
              <HoverRow label="Rows" value={parts ? Number(parts.total_rows).toLocaleString() : '—'} theme={theme} />
              <HoverRow label="Delay" value={formatDelay(delay)} theme={theme}
                color={delay > 300 ? COLORS_3D.error.main : delay > 60 ? COLORS_3D.lag.main : undefined} />
              <HoverRow label="Log ptr" value={`${Number(info.log_pointer).toLocaleString()}${logBehind > 0 ? ` (-${logBehind})` : ''}`} theme={theme}
                color={logBehind > 0 ? COLORS_3D.lag.main : undefined} />
              {queue > 0 && <HoverRow label="Queue" value={String(queue)} theme={theme} color={COLORS_3D.lag.main} />}
            </div>
            {myQueue.length > 0 && (
              <div style={{ marginTop: 6, paddingTop: 6, borderTop: `1px solid ${color.edge}30` }}>
                {myQueue.slice(0, 3).map((entry, i) => (
                  <div key={i} style={{ fontSize: 9, display: 'flex', gap: 4, alignItems: 'center', marginBottom: 2 }}>
                    <span style={{ color: entry.type === 'GET_PART' ? COLORS_3D.healthy.main : COLORS_3D.lag.main, fontWeight: 600 }}>{entry.type}</span>
                    <span style={{ color: theme === 'light' ? 'rgba(0,0,0,0.6)' : 'rgba(255,255,255,0.6)', fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 120 }}>
                      {entry.new_part_name}
                    </span>
                    {entry.last_exception && <span style={{ color: COLORS_3D.error.main }}>ERR</span>}
                  </div>
                ))}
                {myQueue.length > 3 && (
                  <div style={{ fontSize: 9, color: theme === 'light' ? 'rgba(0,0,0,0.35)' : 'rgba(255,255,255,0.35)' }}>
                    +{myQueue.length - 3} more
                  </div>
                )}
              </div>
            )}
          </div>
        </Html>
      )}
    </group>
  );
};

// ── 3D: Shard Enclosure ──

const ShardEnclosure3D: React.FC<{
  center: [number, number, number];
  size: [number, number, number];
  label: string;
  isEmpty: boolean;
  theme: 'dark' | 'light';
}> = ({ center, size, label, isEmpty, theme }) => {
  const fillColor = isEmpty
    ? (theme === 'light' ? '#3a3a50' : '#0e0e20')
    : (theme === 'light' ? '#4a6080' : '#1a3050');
  const borderColor = isEmpty
    ? (theme === 'light' ? '#555570' : '#2a2a50')
    : (theme === 'light' ? '#6b9fd4' : '#4a90d9');
  const y = 0.04;
  const hw = size[0] / 2;
  const hd = size[2] / 2;

  const borderPoints = useMemo(() => new Float32Array([
    -hw, 0, -hd, hw, 0, -hd,
    hw, 0, -hd, hw, 0, hd,
    hw, 0, hd, -hw, 0, hd,
    -hw, 0, hd, -hw, 0, -hd,
  ]), [hw, hd]);

  const labelTexture = useMemo(() => {
    const canvas = document.createElement('canvas');
    canvas.width = 1024; canvas.height = 256;
    const ctx = canvas.getContext('2d')!;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const textFill = isEmpty
      ? (theme === 'light' ? '#606080' : '#404060')
      : (theme === 'light' ? '#90b8e0' : '#80c0ff');
    ctx.shadowColor = isEmpty ? 'rgba(0,0,0,0)' : (theme === 'dark' ? 'rgba(100,180,255,0.7)' : 'rgba(0,0,0,0)');
    ctx.shadowBlur = isEmpty ? 0 : 30;
    ctx.font = 'bold 140px ui-monospace, "SF Mono", "Cascadia Code", monospace';
    ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    ctx.fillStyle = textFill;
    const displayLabel = isEmpty ? `${label.toUpperCase()} · EMPTY` : label.toUpperCase();
    ctx.fillText(displayLabel, canvas.width / 2, canvas.height / 2);
    ctx.shadowBlur = 0;
    ctx.fillText(displayLabel, canvas.width / 2, canvas.height / 2);
    const tex = new THREE.CanvasTexture(canvas);
    tex.anisotropy = 16;
    return tex;
  }, [label, isEmpty, theme]);

  useEffect(() => () => { labelTexture.dispose(); }, [labelTexture]);

  const labelW = size[0] * 0.85;
  const labelPlaneH = 0.45;
  const labelZ = hd - labelPlaneH / 2 - 0.1;

  return (
    <group position={[center[0], y, center[2]]}>
      <mesh rotation={[-Math.PI / 2, 0, 0]} receiveShadow>
        <planeGeometry args={[size[0], size[2]]} />
        <meshBasicMaterial color={fillColor} transparent opacity={isEmpty ? 0.2 : 0.5} depthWrite={false} />
      </mesh>
      <lineSegments>
        <bufferGeometry>
          <bufferAttribute attach="attributes-position" count={8} array={borderPoints} itemSize={3} />
        </bufferGeometry>
        <lineBasicMaterial color={borderColor} transparent opacity={0.9} />
      </lineSegments>
      <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, 0.008, labelZ]}>
        <planeGeometry args={[labelW, labelPlaneH]} />
        <meshBasicMaterial map={labelTexture} transparent opacity={1} depthWrite={false} />
      </mesh>
    </group>
  );
};

// ── 3D: Keeper Node ──

const KeeperNode3D: React.FC<{
  position: [number, number, number];
  logHead: number;
  keeperInfo: KeeperTableInfo | null;
  keeperConnections: KeeperConnection[];
  theme: 'dark' | 'light';
}> = ({ position, logHead, keeperInfo, keeperConnections, theme }) => {
  const meshRef = useRef<THREE.Mesh>(null);
  const [hovered, setHovered] = useState(false);
  const size: [number, number, number] = [0.6, 0.3, 0.3];
  const color = COLORS_3D.keeper;

  const edgesGeometry = useMemo(() => {
    const geo = new THREE.BoxGeometry(size[0] * 1.002, size[1] * 1.002, size[2] * 1.002);
    return new THREE.EdgesGeometry(geo, 1);
  }, [size]);

  useEffect(() => () => { edgesGeometry.dispose(); }, [edgesGeometry]);

  useFrame((state) => {
    if (!meshRef.current) return;
    const t = state.clock.elapsedTime;
    meshRef.current.position.y = position[1] + Math.sin(t * 0.5 + 1) * 0.03;
  });

  const hasIssues = keeperConnections.some(c => c.isExpired === 1);

  return (
    <group position={[position[0], 0, position[2]]}>
      <mesh
        ref={meshRef} position={[0, position[1], 0]}
        onPointerOver={(e) => { e.stopPropagation(); setHovered(true); document.body.style.cursor = 'pointer'; }}
        onPointerOut={(e) => { e.stopPropagation(); setHovered(false); document.body.style.cursor = 'auto'; }}
      >
        <boxGeometry args={size} />
        <meshPhysicalMaterial
          color={hasIssues ? COLORS_3D.error.main : color.main} metalness={0.1} roughness={0.05}
          transmission={0.5} thickness={1.5} transparent
          opacity={hovered ? 0.85 : 0.65}
          emissive={hasIssues ? COLORS_3D.error.main : color.main} emissiveIntensity={hovered ? 0.4 : 0.15}
          clearcoat={1} clearcoatRoughness={0.1} ior={1.5}
        />
      </mesh>
      <lineSegments position={[0, position[1], 0]}>
        <primitive object={edgesGeometry} attach="geometry" />
        <lineBasicMaterial color={hasIssues ? COLORS_3D.error.edge : color.edge} transparent opacity={hovered ? 1 : 0.7} />
      </lineSegments>
      <mesh position={[0, 0.04, 0]}>
        <boxGeometry args={[size[0] * 1.08, 0.06, size[2] * 1.08]} />
        <meshBasicMaterial color={hasIssues ? COLORS_3D.error.main : color.main} transparent opacity={0.8} />
      </mesh>

      {/* Always-visible keeper label */}
      <Html
        position={[0, position[1] + size[1] / 2 + 0.15, 0]}
        center style={{ pointerEvents: 'none', whiteSpace: 'nowrap' }}
      >
        <div style={{ textAlign: 'center', transform: 'scale(0.8)' }}>
          <div style={{ fontSize: 9, fontFamily: 'ui-monospace, monospace', fontWeight: 600, color: hasIssues ? COLORS_3D.error.glow : color.glow, textShadow: `0 0 6px ${hasIssues ? COLORS_3D.error.main : color.main}` }}>
            Keeper
            {hasIssues && <span style={{ fontSize: 7, padding: '0 3px', marginLeft: 3, borderRadius: 2, backgroundColor: `${COLORS_3D.error.main}30`, color: COLORS_3D.error.main }}>EXPIRED</span>}
          </div>
          <div style={{ display: 'flex', gap: 6, justifyContent: 'center', fontSize: 8, color: 'rgba(167,139,250,0.7)' }}>
            <span>log: {logHead.toLocaleString()}</span>
            {keeperInfo && keeperInfo.mutations > 0 && <span style={{ color: COLORS_3D.lag.main }}>{keeperInfo.mutations} mut</span>}
            {keeperInfo && <span>{keeperInfo.blocks.toLocaleString()} blk</span>}
          </div>
        </div>
      </Html>

      {hovered && (
        <Html position={[0, position[1] + 0.5, 0.3]} center style={{ pointerEvents: 'none', zIndex: 1000 }}>
          <div style={{
            background: theme === 'light' ? 'rgba(255,255,255,0.95)' : 'rgba(15,23,42,0.92)',
            padding: '10px 14px', borderRadius: 4,
            border: `1px solid ${color.edge}50`, minWidth: 200,
            backdropFilter: 'blur(12px)', transform: 'scale(0.85)',
          }}>
            <div style={{ fontSize: 12, fontWeight: 600, fontFamily: 'monospace', color: color.glow, marginBottom: 6, paddingBottom: 6, borderBottom: `1px solid ${color.edge}30` }}>
              Keeper
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
              <HoverRow label="Log head" value={logHead.toLocaleString()} theme={theme} />
              {keeperInfo && (
                <>
                  <HoverRow label="Log entries" value={keeperInfo.logEntries.toLocaleString()} theme={theme} />
                  <HoverRow label="Mutations" value={keeperInfo.mutations.toLocaleString()} theme={theme}
                    color={keeperInfo.mutations > 0 ? COLORS_3D.lag.main : undefined} />
                  <HoverRow label="Dedup blocks" value={keeperInfo.blocks.toLocaleString()} theme={theme} />
                  <HoverRow label="Replicas (ZK)" value={keeperInfo.registeredReplicas.toLocaleString()} theme={theme} />
                  {keeperInfo.hasQuorum && (
                    <HoverRow label="Insert quorum" value="active" theme={theme} color={COL.blue} />
                  )}
                </>
              )}
            </div>
            {keeperConnections.length > 0 && (
              <div style={{ marginTop: 6, paddingTop: 6, borderTop: `1px solid ${color.edge}30` }}>
                <div style={{ fontSize: 9, color: theme === 'light' ? 'rgba(0,0,0,0.45)' : 'rgba(255,255,255,0.45)', marginBottom: 3 }}>Connections</div>
                {keeperConnections.map((c, i) => (
                  <div key={i} style={{ fontSize: 9, display: 'flex', gap: 4, alignItems: 'center', marginBottom: 2 }}>
                    <span style={{ width: 5, height: 5, borderRadius: '50%', backgroundColor: c.isExpired ? COLORS_3D.error.main : COLORS_3D.healthy.main, flexShrink: 0 }} />
                    <span style={{ color: theme === 'light' ? 'rgba(0,0,0,0.7)' : 'rgba(255,255,255,0.7)', fontFamily: 'monospace' }}>
                      {c.host}:{c.port}
                    </span>
                    {c.isExpired === 1 && <span style={{ color: COLORS_3D.error.main, fontWeight: 600 }}>EXPIRED</span>}
                  </div>
                ))}
              </div>
            )}
            {keeperInfo && (
              <div style={{ marginTop: 6, paddingTop: 6, borderTop: `1px solid ${color.edge}30` }}>
                <div style={{ fontSize: 8, fontFamily: 'monospace', color: theme === 'light' ? 'rgba(0,0,0,0.35)' : 'rgba(255,255,255,0.35)', wordBreak: 'break-all' }}>
                  {keeperInfo.zkPath}
                </div>
              </div>
            )}
          </div>
        </Html>
      )}
    </group>
  );
};

// ── 3D: Distribution Queue (between shards) ──

const DistributionQueue3D: React.FC<{
  position: [number, number, number];
  entries: DistributionQueueEntry[];
  shardCenters: [number, number, number][];
  theme: 'dark' | 'light';
}> = ({ position, entries, shardCenters, theme }) => {
  const [hovered, setHovered] = useState(false);

  // Aggregate across entries
  const agg = useMemo(() => {
    let files = 0, bytes = 0, errors = 0, broken = 0, blocked = false, exception = '';
    for (const e of entries) {
      files += e.dataFiles;
      bytes += e.dataCompressedBytes;
      errors += e.errorCount;
      broken += e.brokenDataFiles;
      if (e.isBlocked) blocked = true;
      if (!exception && e.lastException) exception = e.lastException;
    }
    return { files, bytes, errors, broken, blocked, exception };
  }, [entries]);

  const hasActivity = agg.files > 0 || agg.errors > 0 || agg.broken > 0;
  const hasErrors = agg.blocked || agg.broken > 0;
  const color = hasErrors ? COLORS_3D.error : hasActivity ? COLORS_3D.lag : COLORS_3D.keeper;

  // Single box — height scales with file count (log scale so it doesn't explode)
  const baseW = 0.35;
  const baseD = 0.35;
  const minH = 0.12;
  const maxH = 0.6;
  const h = agg.files > 0 ? Math.min(minH + Math.log2(1 + agg.files) * 0.06, maxH) : minH;
  const boxSize: [number, number, number] = [baseW, h, baseD];

  const lineColor = theme === 'light' ? '#94a3b8' : '#475569';

  if (!hasActivity && entries.length === 0) return null;

  const edgesGeo = useMemo(() => {
    const geo = new THREE.BoxGeometry(boxSize[0] * 1.002, boxSize[1] * 1.002, boxSize[2] * 1.002);
    return new THREE.EdgesGeometry(geo, 1);
  }, [boxSize]);

  useEffect(() => () => { edgesGeo.dispose(); }, [edgesGeo]);

  return (
    <group position={[position[0], 0, position[2]]}>
      {/* Single aggregate box */}
      <mesh
        position={[0, position[1] + h / 2, 0]}
        onPointerOver={(e) => { e.stopPropagation(); setHovered(true); document.body.style.cursor = 'pointer'; }}
        onPointerOut={(e) => { e.stopPropagation(); setHovered(false); document.body.style.cursor = 'auto'; }}
      >
        <boxGeometry args={boxSize} />
        <meshPhysicalMaterial
          color={color.main} metalness={0.1} roughness={0.05}
          transmission={0.4} thickness={1.5} transparent
          opacity={hovered ? 0.85 : 0.65}
          emissive={color.main} emissiveIntensity={hovered ? 0.4 : 0.2}
          clearcoat={1} clearcoatRoughness={0.1} ior={1.5}
        />
      </mesh>
      <lineSegments position={[0, position[1] + h / 2, 0]}>
        <primitive object={edgesGeo} attach="geometry" />
        <lineBasicMaterial color={color.edge} transparent opacity={hovered ? 1 : 0.6} />
      </lineSegments>

      {/* Base pad */}
      <mesh position={[0, 0.02, 0]}>
        <boxGeometry args={[baseW * 1.15, 0.03, baseD * 1.15]} />
        <meshBasicMaterial color={color.main} transparent opacity={0.3} />
      </mesh>

      {/* Label */}
      <Html
        position={[0, position[1] + h + 0.15, 0]}
        center style={{ pointerEvents: 'none', whiteSpace: 'nowrap' }}
      >
        <div style={{ textAlign: 'center', transform: 'scale(0.8)' }}>
          <div style={{ fontSize: 9, fontFamily: 'ui-monospace, monospace', fontWeight: 600, color: color.glow, textShadow: `0 0 6px ${color.main}` }}>
            Dist Queue
          </div>
          <div style={{ fontSize: 8, color: `${color.glow}99` }}>
            {agg.files > 0 ? `${agg.files} files` : agg.errors > 0 ? `${agg.errors} err` : 'empty'}
            {agg.broken > 0 && <span style={{ color: COLORS_3D.error.main, marginLeft: 4 }}>{agg.broken} broken</span>}
          </div>
        </div>
      </Html>

      {/* Dashed lines to shards */}
      {shardCenters.map((sp, i) => {
        const geometry = new THREE.BufferGeometry().setFromPoints([
          new THREE.Vector3(position[0], 0.08, position[2]),
          new THREE.Vector3(sp[0], 0.08, sp[2]),
        ]);
        return (
          <lineSegments key={`dist-line-${i}`} position={[-position[0], 0, -position[2]]}>
            <primitive object={geometry} attach="geometry" />
            <lineDashedMaterial color={hasActivity ? color.main : lineColor} transparent opacity={hasActivity ? 0.4 : 0.15} dashSize={0.1} gapSize={0.08} />
          </lineSegments>
        );
      })}

      {/* Hover card */}
      {hovered && (
        <Html position={[0, position[1] + 0.6, 0.3]} center style={{ pointerEvents: 'none', zIndex: 1000 }}>
          <div style={{
            background: theme === 'light' ? 'rgba(255,255,255,0.95)' : 'rgba(15,23,42,0.92)',
            padding: '10px 14px', borderRadius: 4,
            border: `1px solid ${color.edge}50`, minWidth: 180,
            backdropFilter: 'blur(12px)', transform: 'scale(0.85)',
          }}>
            <div style={{ fontSize: 12, fontWeight: 600, fontFamily: 'monospace', color: color.glow, marginBottom: 6, paddingBottom: 6, borderBottom: `1px solid ${color.edge}30` }}>
              Distribution Queue
              {agg.blocked && <span style={{ fontSize: 9, padding: '0 4px', marginLeft: 6, borderRadius: 3, backgroundColor: `${COLORS_3D.error.main}30`, color: COLORS_3D.error.main }}>BLOCKED</span>}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
              <HoverRow label="Pending files" value={String(agg.files)} theme={theme} color={agg.files > 0 ? COLORS_3D.lag.main : undefined} />
              <HoverRow label="Compressed" value={formatBytes(agg.bytes)} theme={theme} />
              <HoverRow label="Errors" value={String(agg.errors)} theme={theme} color={agg.errors > 0 ? COLORS_3D.error.main : undefined} />
              <HoverRow label="Broken files" value={String(agg.broken)} theme={theme} color={agg.broken > 0 ? COLORS_3D.error.main : undefined} />
            </div>
            {agg.exception && (
              <div style={{ marginTop: 6, paddingTop: 6, borderTop: `1px solid ${color.edge}30`, fontSize: 9, color: COLORS_3D.error.main, fontFamily: 'monospace', wordBreak: 'break-all', maxWidth: 250 }}>
                {agg.exception.slice(0, 200)}
              </div>
            )}
          </div>
        </Html>
      )}
    </group>
  );
};

// ── 3D: Table Name Label ──

const TableNameLabel3D: React.FC<{
  position: [number, number, number];
  label: string;
  width: number;
  theme: 'dark' | 'light';
}> = ({ position, label, width, theme }) => {
  const labelTexture = useMemo(() => {
    const canvas = document.createElement('canvas');
    canvas.width = 2048; canvas.height = 256;
    const ctx = canvas.getContext('2d')!;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const textFill = theme === 'light' ? '#90b8e0' : '#80c0ff';
    ctx.shadowColor = theme === 'dark' ? 'rgba(100,180,255,0.5)' : 'rgba(0,0,0,0)';
    ctx.shadowBlur = 20;
    ctx.font = 'bold 100px ui-monospace, "SF Mono", "Cascadia Code", monospace';
    ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    ctx.fillStyle = textFill;
    ctx.fillText(label, canvas.width / 2, canvas.height / 2);
    ctx.shadowBlur = 0;
    ctx.fillText(label, canvas.width / 2, canvas.height / 2);
    const tex = new THREE.CanvasTexture(canvas);
    tex.anisotropy = 16;
    return tex;
  }, [label, theme]);

  useEffect(() => () => { labelTexture.dispose(); }, [labelTexture]);

  const planeW = Math.max(width, 3);
  const planeH = 0.4;

  return (
    <mesh rotation={[-Math.PI / 2, 0, 0]} position={position}>
      <planeGeometry args={[planeW, planeH]} />
      <meshBasicMaterial map={labelTexture} transparent opacity={1} depthWrite={false} />
    </mesh>
  );
};

// ── 3D: Connection Lines ──

const ConnectionLines3D: React.FC<{
  shardCenters: [number, number, number][];
  keeperPos: [number, number, number];
  theme: 'dark' | 'light';
}> = ({ shardCenters, keeperPos, theme }) => {
  const lineColor = theme === 'light' ? '#94a3b8' : '#475569';
  return (
    <>
      {shardCenters.map((sp, i) => {
        const geometry = new THREE.BufferGeometry().setFromPoints([
          new THREE.Vector3(sp[0], 0.1, sp[2]),
          new THREE.Vector3(keeperPos[0], 0.1, keeperPos[2]),
        ]);
        return (
          <lineSegments key={i}>
            <primitive object={geometry} attach="geometry" />
            <lineDashedMaterial color={lineColor} transparent opacity={0.25} dashSize={0.15} gapSize={0.1} />
          </lineSegments>
        );
      })}
    </>
  );
};

// ── 3D: Ground ──

const Ground: React.FC<{ size: number; theme: 'dark' | 'light' }> = ({ size, theme }) => {
  const colors = theme === 'dark'
    ? { ground: '#0c0c1a', grid1: '#1a1a3a', grid2: '#0f0f2a' }
    : { ground: '#303055', grid1: '#40406a', grid2: '#353560' };
  const mat = theme === 'light' ? { metalness: 0.4, roughness: 0.6 } : { metalness: 0.8, roughness: 0.4 };
  return (
    <group>
      <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, 0, 0]} receiveShadow>
        <planeGeometry args={[size * 2.5, size * 2.5]} />
        <meshStandardMaterial color={colors.ground} {...mat} />
      </mesh>
      <gridHelper args={[size * 2, Math.floor(size * 2), colors.grid1, colors.grid2]} position={[0, 0.01, 0]} />
    </group>
  );
};

// ── 3D: Scene Layout & Content ──

function computeReplicaLayout(shards: TopologyShard[]) {
  const nodeSize: [number, number, number] = [0.7, 0.4, 0.4];
  const replicaSpacing = 0.6;
  const shardSpacing = 1.0;
  const shardPadding = 1.0;

  let xCursor = 0;
  const shardLayouts: {
    shard: TopologyShard;
    center: [number, number, number];
    enclosureSize: [number, number, number];
    replicas: { replica: TopologyReplica; position: [number, number, number] }[];
  }[] = [];

  for (const shard of shards) {
    const replicaSpan = shard.replicas.length * replicaSpacing;
    const stripeH = 0.6;
    const encW = nodeSize[0] + shardPadding * 2;
    const encH = replicaSpan + shardPadding;
    const encD = Math.max(nodeSize[2], replicaSpan) + shardPadding * 2 + stripeH;

    const centerX = xCursor + encW / 2;
    const nodeZOffset = -stripeH / 2;
    const replicas = shard.replicas.map((r, i) => ({
      replica: r,
      position: [
        centerX,
        nodeSize[1] / 2 + 0.1,
        (i - (shard.replicas.length - 1) / 2) * replicaSpacing + nodeZOffset,
      ] as [number, number, number],
    }));

    shardLayouts.push({
      shard,
      center: [centerX, encH / 2, 0],
      enclosureSize: [encW, encH, encD],
      replicas,
    });

    xCursor += encW + shardSpacing;
  }

  const totalWidth = xCursor - shardSpacing;
  const offsetX = -totalWidth / 2;

  for (const sl of shardLayouts) {
    sl.center[0] += offsetX;
    for (const r of sl.replicas) r.position[0] += offsetX;
  }

  // Keeper position below shards
  const maxEncD = Math.max(...shardLayouts.map(s => s.enclosureSize[2]));
  const keeperZ = maxEncD / 2 + 1.5;
  const keeperPos: [number, number, number] = [0, 0.25, keeperZ];

  // Distribution queue position — centered between shards
  const distQueuePos: [number, number, number] = [0, 0.15, 0];

  // Table name label between shards and keeper (positive Z, in front of shards)
  const tableLabelZ = maxEncD / 2 + 0.7;
  const tableLabelPos: [number, number, number] = [0, 0.012, tableLabelZ];

  return { shardLayouts, keeperPos, distQueuePos, tableLabelPos, totalWidth, nodeSize };
}

const ReplicaScene: React.FC<{
  data: ReplicationTopologyData;
  theme: 'dark' | 'light';
}> = ({ data, theme }) => {
  const layout = useMemo(() => computeReplicaLayout(data.shards), [data.shards]);

  return (
    <>
      <Ground size={layout.totalWidth + 6} theme={theme} />

      {layout.shardLayouts.map((sl) => (
        <React.Fragment key={sl.shard.shardNum}>
          <ShardEnclosure3D
            center={sl.center}
            size={sl.enclosureSize}
            label={`Shard ${sl.shard.shardNum}`}
            isEmpty={sl.shard.totalBytes === 0}
            theme={theme}
          />
          {sl.replicas.map((r) => (
            <ReplicaNodeBox
              key={r.replica.info.replica_name}
              position={r.position}
              size={layout.nodeSize}
              replica={r.replica}
              logHead={data.logHead}
              queueEntries={data.queueEntries}
              allLeaders={sl.shard.allLeaders}
              theme={theme}
            />
          ))}
        </React.Fragment>
      ))}

      <KeeperNode3D position={layout.keeperPos} logHead={data.logHead} keeperInfo={data.keeperInfo} keeperConnections={data.keeperConnections} theme={theme} />

      {data.distributionQueue.length > 0 && (
        <DistributionQueue3D
          position={layout.distQueuePos}
          entries={data.distributionQueue}
          shardCenters={layout.shardLayouts.map(s => s.center)}
          theme={theme}
        />
      )}

      <TableNameLabel3D
        position={layout.tableLabelPos}
        label={`${data.database}.${data.table}`}
        width={layout.totalWidth}
        theme={theme}
      />

      <ConnectionLines3D
        shardCenters={layout.shardLayouts.map(s => s.center)}
        keeperPos={layout.keeperPos}
        theme={theme}
      />
    </>
  );
};

// ── 2D: Data Distribution Bar ──

const DataDistributionBar: React.FC<{
  shards: TopologyShard[];
  totalBytes: number;
  partitionDist: ShardPartitionDist[];
}> = ({ shards, totalBytes, partitionDist }) => {
  if (totalBytes === 0) return null;

  const allPartitions = [...new Set(partitionDist.map(d => d.partition_id))].sort();
  const partColorMap = new Map(allPartitions.map((p, i) => [p, PARTITION_COLORS[i % PARTITION_COLORS.length]]));

  const hostnameToShard = new Map<string, number>();
  for (const s of shards) {
    for (const r of s.replicas) {
      if (!hostnameToShard.has(r.info.hostname)) hostnameToShard.set(r.info.hostname, s.shardNum);
    }
  }

  const shardPartitions = new Map<number, Map<string, number>>();
  for (const d of partitionDist) {
    const sNum = hostnameToShard.get(d.hostname);
    if (sNum == null) continue;
    if (!shardPartitions.has(sNum)) shardPartitions.set(sNum, new Map());
    const partMap = shardPartitions.get(sNum)!;
    const existing = partMap.get(d.partition_id) || 0;
    partMap.set(d.partition_id, Math.max(existing, Number(d.bytes_on_disk)));
  }

  return (
    <div>
      <div style={{ fontSize: 10, fontWeight: 500, color: COL.muted, marginBottom: 8 }}>
        Data Distribution by Shard
      </div>
      {shards.map(s => {
        const pct = totalBytes > 0 ? (s.totalBytes / totalBytes * 100) : 0;
        const parts = shardPartitions.get(s.shardNum) || new Map();
        return (
          <div key={s.shardNum} style={{ marginBottom: 8 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ fontSize: 10, fontWeight: 500, color: COL.primary, width: 60 }}>Shard {s.shardNum}</span>
              <div style={{ flex: 1, height: 16, borderRadius: 4, overflow: 'hidden', display: 'flex', backgroundColor: 'rgba(255,255,255,0.05)' }}>
                {allPartitions.map(pid => {
                  const bytes = parts.get(pid) || 0;
                  const segPct = totalBytes > 0 ? (bytes / totalBytes * 100) : 0;
                  if (segPct < 0.5) return null;
                  return <div key={pid} title={`${pid}: ${formatBytes(bytes)}`} style={{ width: `${segPct}%`, backgroundColor: partColorMap.get(pid), opacity: 0.7 }} />;
                })}
              </div>
              <span style={{ fontSize: 10, fontFamily: 'monospace', color: COL.muted, width: 100, textAlign: 'right' }}>{pct.toFixed(1)}% · {formatBytes(s.totalBytes)}</span>
            </div>
          </div>
        );
      })}
      {allPartitions.length > 0 && (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px 12px', marginTop: 4 }}>
          {allPartitions.slice(0, 12).map(pid => (
            <div key={pid} style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 9, color: COL.muted }}>
              <span style={{ width: 8, height: 8, borderRadius: 2, backgroundColor: partColorMap.get(pid), opacity: 0.7 }} />
              {pid}
            </div>
          ))}
          {allPartitions.length > 12 && <span style={{ fontSize: 9, color: COL.muted }}>+{allPartitions.length - 12} more</span>}
        </div>
      )}
    </div>
  );
};

// ── 2D: Sync Status ──

const SyncStatus: React.FC<{ shards: TopologyShard[]; logHead: number }> = ({ shards, logHead }) => {
  const allReplicas = shards.flatMap(s => s.replicas);

  return (
    <div>
      <div style={{ fontSize: 10, fontWeight: 500, color: COL.muted, marginBottom: 8 }}>
        Replica Sync Status
        {logHead > 0 && (
          <span style={{ fontFamily: 'monospace', fontWeight: 400, marginLeft: 8 }}>log head: {logHead.toLocaleString()}</span>
        )}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 5 }}>
        {allReplicas.map(r => {
          const delay = r.info.absolute_delay;
          const behind = logHead - r.info.log_pointer;
          const severity = classifyDelaySeverity(delay);
          const color = severity === 'critical' ? COL.red : severity === 'lagging' ? COL.amber : COL.green;
          const bgColor = severity === 'critical' ? COL.redBg : severity === 'lagging' ? COL.amberBg : COL.greenBg;
          const shortName = r.info.hostname.replace(/^chi-[a-z]+-[a-z]+-/, '').replace(/-0$/, '');
          const tooltip = r.info.hostname + (delay > 0 ? ` · ${formatDelay(delay)}${behind > 0 ? ` (log -${behind})` : ''}` : ' · in sync');
          return (
            <span key={r.info.replica_name} title={tooltip} style={{
              display: 'inline-flex', alignItems: 'center', gap: 5,
              padding: '3px 8px', borderRadius: 4, fontSize: 10,
              backgroundColor: bgColor, border: `1px solid ${color}30`,
              cursor: 'help', overflow: 'hidden',
            }}>
              <span style={{ width: 6, height: 6, borderRadius: '50%', backgroundColor: color, flexShrink: 0 }} />
              <span style={{ fontFamily: 'monospace', color: COL.primary, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{shortName}</span>
              {delay > 0 && (
                <span style={{ color, fontFamily: 'monospace', fontSize: 9, flexShrink: 0 }}>{formatDelay(delay)}</span>
              )}
            </span>
          );
        })}
      </div>
    </div>
  );
};


// ── 2D: Replication Queue Panel ──

const ReplicationQueuePanel: React.FC<{
  queueEntries: TopologyQueueEntry[];
}> = ({ queueEntries }) => {
  if (queueEntries.length === 0) {
    return (
      <div>
        <div style={{ fontSize: 10, fontWeight: 500, color: COL.muted, marginBottom: 6 }}>Replication Queue</div>
        <div style={{ fontSize: 10, color: COL.muted }}>Empty — all replicas in sync</div>
      </div>
    );
  }

  // Group by type, count executing and errors
  const byType = new Map<string, { total: number; executing: number; errors: number }>();
  let totalErrors = 0;
  for (const e of queueEntries) {
    const existing = byType.get(e.type) || { total: 0, executing: 0, errors: 0 };
    existing.total++;
    if (Number(e.is_currently_executing) === 1) existing.executing++;
    if (e.last_exception) { existing.errors++; totalErrors++; }
    byType.set(e.type, existing);
  }

  const sorted = [...byType.entries()].sort((a, b) => b[1].total - a[1].total);

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, fontWeight: 500, color: COL.muted, marginBottom: 6 }}>
        Replication Queue
        <span style={{ fontFamily: 'monospace', fontWeight: 600, color: totalErrors > 0 ? COL.red : COL.primary }}>
          {queueEntries.length}
        </span>
        {totalErrors > 0 && (
          <span style={{ fontSize: 8, padding: '0 4px', borderRadius: 2, backgroundColor: `${COL.red}20`, color: COL.red }}>
            {totalErrors} err
          </span>
        )}
      </div>
      {sorted.map(([type, stats]) => (
        <div key={type} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, padding: '2px 0' }}>
          <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)', minWidth: 100 }}>{type}</span>
          <span style={{ fontFamily: 'monospace', color: COL.muted }}>{stats.total}</span>
          {stats.executing > 0 && (
            <span style={{ fontSize: 9, color: COL.green }}>{stats.executing} running</span>
          )}
          {stats.errors > 0 && (
            <span style={{ fontSize: 9, color: COL.red }}>{stats.errors} err</span>
          )}
        </div>
      ))}
    </div>
  );
};

// ── 2D: Keeper & Distribution Panel ──

const KeeperPanel: React.FC<{
  keeperInfo: KeeperTableInfo | null;
  keeperConnections: KeeperConnection[];
  distributionQueue: DistributionQueueEntry[];
  onFetchZkChildren?: (subPath: string) => Promise<ZkChildNode[]>;
}> = ({ keeperInfo, keeperConnections, distributionQueue, onFetchZkChildren }) => {
  const [keeperExpanded, setKeeperExpanded] = useState(false);
  const [expandedPath, setExpandedPath] = useState<string | null>(null);
  const [zkChildren, setZkChildren] = useState<ZkChildNode[]>([]);
  const [zkLoading, setZkLoading] = useState(false);

  const toggleExpand = (subPath: string) => {
    if (expandedPath === subPath) { setExpandedPath(null); return; }
    if (!onFetchZkChildren) return;
    setExpandedPath(subPath);
    setZkLoading(true);
    onFetchZkChildren(subPath).then(setZkChildren).catch(() => setZkChildren([])).finally(() => setZkLoading(false));
  };

  // Aggregate distribution queue per table and hide empty entries
  const distQueueAgg = useMemo(() => {
    const map = new Map<string, { key: string; totalFiles: number; totalBytes: number; totalBroken: number; totalErrors: number; blocked: boolean; lastException: string }>();
    for (const e of distributionQueue) {
      const key = `${e.database}.${e.table}`;
      const existing = map.get(key);
      if (existing) {
        existing.totalFiles += e.dataFiles;
        existing.totalBytes += e.dataCompressedBytes;
        existing.totalBroken += e.brokenDataFiles;
        existing.totalErrors += e.errorCount;
        if (e.isBlocked === 1) existing.blocked = true;
        if (e.lastException && !existing.lastException) existing.lastException = e.lastException;
      } else {
        map.set(key, {
          key,
          totalFiles: e.dataFiles,
          totalBytes: e.dataCompressedBytes,
          totalBroken: e.brokenDataFiles,
          totalErrors: e.errorCount,
          blocked: e.isBlocked === 1,
          lastException: e.lastException,
        });
      }
    }
    return [...map.values()].filter(e => e.totalFiles > 0 || e.totalErrors > 0 || e.totalBroken > 0 || e.blocked);
  }, [distributionQueue]);

  const hasDistQueue = distQueueAgg.length > 0;
  const hasKeeperData = keeperInfo || keeperConnections.length > 0;
  if (!hasKeeperData && !hasDistQueue) return null;

  const hasKeeperIssues = keeperConnections.some(c => c.isExpired === 1) || (keeperInfo?.mutations ?? 0) > 0;

  return (
    <div>
      {/* Keeper State — stats always visible, connection/path expandable */}
      {hasKeeperData && (
        <div style={{ marginBottom: hasDistQueue ? 10 : 0 }}>
          <div style={{ fontSize: 10, fontWeight: 500, color: COL.muted, marginBottom: 6 }}>
            Keeper State
          </div>

          {/* Stats grid — always visible, clickable to browse ZK */}
          {keeperInfo && (
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '2px 12px', marginBottom: expandedPath ? 0 : 6, fontSize: 10 }}>
              {([
                { label: 'Log entries', value: keeperInfo.logEntries.toLocaleString(), color: keeperInfo.logEntries > 10000 ? COL.amber : undefined, expand: 'log' },
                { label: 'Mutations', value: String(keeperInfo.mutations), color: keeperInfo.mutations > 0 ? COL.amber : undefined, expand: keeperInfo.mutations > 0 ? 'mutations' : undefined },
                { label: 'Dedup blocks', value: keeperInfo.blocks.toLocaleString() },
                { label: 'Replicas in ZK', value: String(keeperInfo.registeredReplicas), expand: 'replicas' },
                ...(keeperInfo.hasQuorum ? [{ label: 'Insert quorum', value: 'active', color: COL.blue }] : []),
              ] as { label: string; value: string; color?: string; expand?: string }[]).map(({ label, value, color, expand }) => (
                <div key={label}
                  onClick={expand && onFetchZkChildren ? () => toggleExpand(expand) : undefined}
                  style={{
                    display: 'flex', justifyContent: 'space-between', padding: '2px 4px', borderRadius: 3,
                    cursor: expand && onFetchZkChildren ? 'pointer' : 'default',
                    backgroundColor: expandedPath === expand ? 'rgba(167,139,250,0.1)' : undefined,
                  }}>
                  <span style={{ color: COL.muted }}>{label}</span>
                  <span style={{ fontFamily: 'monospace', fontWeight: 600, color: color ?? 'var(--text-primary)' }}>{value}</span>
                </div>
              ))}
            </div>
          )}

          {/* Expanded ZK children */}
          {expandedPath && keeperInfo && (
            <div style={{ marginBottom: 6, padding: '6px 4px', borderRadius: 4, backgroundColor: 'rgba(167,139,250,0.05)', border: `1px solid ${COL.purple}20` }}>
              <div style={{ fontSize: 9, color: COL.muted, marginBottom: 4, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span>{keeperInfo.zkPath}/{expandedPath}</span>
                <span style={{ cursor: 'pointer', color: COL.muted, fontSize: 10, padding: '0 4px' }} onClick={() => setExpandedPath(null)}>x</span>
              </div>
              {zkLoading ? (
                <div style={{ fontSize: 9, color: COL.muted }}>Loading...</div>
              ) : zkChildren.length === 0 ? (
                <div style={{ fontSize: 9, color: COL.muted }}>Empty</div>
              ) : (
                <div style={{ maxHeight: 160, overflowY: 'auto', fontSize: 9 }}>
                  <table style={{ width: '100%', borderCollapse: 'separate', borderSpacing: 0 }}>
                    <thead>
                      <tr>
                        <th style={{ textAlign: 'left', color: COL.muted, fontWeight: 500, padding: '1px 4px', position: 'sticky', top: 0, background: 'var(--bg-card)' }}>Name</th>
                        <th style={{ textAlign: 'left', color: COL.muted, fontWeight: 500, padding: '1px 4px', position: 'sticky', top: 0, background: 'var(--bg-card)' }}>Value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {zkChildren.slice(-50).map((child) => {
                        const lines = child.value.split('\n').filter(Boolean);
                        const summary = lines.slice(0, 3).join(' | ');
                        return (
                          <tr key={child.name} style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                            <td style={{ padding: '2px 4px', fontFamily: 'monospace', color: COL.purple, whiteSpace: 'nowrap' }}>{child.name}</td>
                            <td title={child.value} style={{ padding: '2px 4px', fontFamily: 'monospace', color: 'var(--text-primary)', cursor: 'help', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{summary}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                  {zkChildren.length > 50 && (
                    <div style={{ fontSize: 8, color: COL.muted, marginTop: 2 }}>Showing last 50 of {zkChildren.length}</div>
                  )}
                </div>
              )}
            </div>
          )}

          {/* Connection + ZK path — expandable detail */}
          {(keeperConnections.length > 0 || keeperInfo) && (
            <div
              onClick={() => setKeeperExpanded(!keeperExpanded)}
              style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 9, cursor: 'pointer', userSelect: 'none', color: COL.muted, marginTop: 2 }}
            >
              <span style={{ fontSize: 7 }}>{keeperExpanded ? '▾' : '▸'}</span>
              {keeperConnections.length > 0 && (
                <span style={{ width: 5, height: 5, borderRadius: '50%', backgroundColor: hasKeeperIssues ? COL.red : COL.green, flexShrink: 0 }} />
              )}
              <span>Connection details</span>
            </div>
          )}
          {keeperExpanded && (
            <div style={{ marginTop: 4, padding: '4px 8px', borderRadius: 4, backgroundColor: 'rgba(167,139,250,0.04)', fontSize: 9 }}>
              {keeperConnections.map((c, i) => {
                const isExpired = c.isExpired === 1;
                return (
                  <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 2 }}>
                    <span style={{ width: 5, height: 5, borderRadius: '50%', backgroundColor: isExpired ? COL.red : COL.green, flexShrink: 0 }} />
                    <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{c.host}:{c.port}</span>
                    {isExpired && <span style={{ color: COL.red, fontWeight: 600 }}>EXPIRED</span>}
                  </div>
                );
              })}
              {keeperInfo && (
                <div style={{ fontSize: 8, fontFamily: 'monospace', color: `${COL.muted}90`, wordBreak: 'break-all', marginTop: 2 }}>
                  {keeperInfo.zkPath}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Distribution Queue — primary content */}
      {hasDistQueue && (
        <div>
          <div title="Files queued for async delivery to remote shards via the Distributed table" style={{ fontSize: 10, fontWeight: 500, color: COL.muted, marginBottom: 4, cursor: 'help' }}>
            Distribution Queue
          </div>
          {distQueueAgg.map((entry) => {
            const hasErrors = entry.totalErrors > 0 || entry.totalBroken > 0;
            const statusColor = entry.blocked ? COL.red : hasErrors ? COL.amber : COL.green;
            return (
              <div key={entry.key} style={{
                display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, padding: '3px 0',
              }}>
                <span style={{ width: 5, height: 5, borderRadius: '50%', backgroundColor: statusColor, flexShrink: 0 }} />
                <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{entry.key}</span>
                <span style={{ fontFamily: 'monospace', color: COL.muted }}>
                  {entry.totalFiles} file{entry.totalFiles !== 1 ? 's' : ''}
                  {entry.totalBytes > 0 ? ` · ${formatBytes(entry.totalBytes)}` : ''}
                </span>
                {entry.blocked && <span style={{ fontSize: 8, padding: '0 4px', borderRadius: 2, backgroundColor: `${COL.red}30`, color: COL.red, fontWeight: 600 }}>BLOCKED</span>}
                {entry.totalErrors > 0 && (
                  <span
                    className="tooltip-trigger tooltip-wrap"
                    data-tooltip={entry.lastException?.slice(0, 120) || `${entry.totalErrors} send error(s)`}
                    style={{ color: COL.amber, cursor: 'help', textDecoration: 'underline dotted', fontSize: 10 }}
                  >
                    {entry.totalErrors} err
                  </span>
                )}
                {entry.totalBroken > 0 && <span style={{ color: COL.red }}>{entry.totalBroken} broken</span>}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

// ── Main Component ──

export interface ReplicationTopologyProps {
  database: string;
  table: string;
}

export const ReplicationTopology: React.FC<ReplicationTopologyProps> = ({ database, table }) => {
  const services = useClickHouseServices();
  const { activeProfileId, profiles } = useConnectionStore();
  const { detected } = useClusterStore();
  const activeProfile = profiles.find(p => p.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;
  const theme = useThemeDetection();

  const [data, setData] = useState<ReplicationTopologyData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!services || !isConnected || !detected) return;
    let cancelled = false;
    // Only show loading spinner on first load — keep old data visible during table switch
    if (!data) setLoading(true);
    setError(null);

    const svc = new ReplicationService(services.adapter);
    const target = `${database}.${table}`;

    const fetchTopology = () => {
      svc.getTopology(database, table).then(result => {
        if (!cancelled) { setData(result); setDisplayedTable(target); }
      }).catch(err => {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      }).finally(() => {
        if (!cancelled) setLoading(false);
      });
    };

    fetchTopology();
    const interval = setInterval(fetchTopology, 15_000);

    return () => { cancelled = true; clearInterval(interval); };
  }, [services, isConnected, detected, database, table]);

  // Track which table we're currently displaying vs fetching
  const [displayedTable, setDisplayedTable] = useState(`${database}.${table}`);
  const fetching = displayedTable !== `${database}.${table}`;

  const fullName = `${database}.${table}`;

  // Callback to fetch ZK children for a sub-path (used by KeeperPanel)
  const fetchZkChildren = useCallback(async (subPath: string): Promise<ZkChildNode[]> => {
    if (!services || !data?.keeperInfo) return [];
    const zkPath = `${data.keeperInfo.zkPath}/${subPath}`;
    const raw = await services.adapter.executeQuery<Record<string, unknown>>(
      tagQuery(buildQuery(GET_ZK_CHILDREN, { path: zkPath }), sourceTag('replication', 'zk-browse'))
    );
    return raw.map(mapZkChildNode);
  }, [services, data?.keeperInfo]);

  // First load only — no data yet
  if (loading && !data) return <div className="card" style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, color: COL.muted }}>Loading topology for {fullName}...</div>;
  if (error && !data) return <div className="card" style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, color: COL.red }}>Failed to load topology: {error}</div>;
  if (!data || data.shards.length === 0) return <div className="card" style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, color: COL.muted }}>No replica data found for {fullName}</div>;

  const camDistance = Math.max(4, data.shards.length * 2 + 2);

  return (
    <div className="card" style={{ display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      {/* Title bar with engine context + topology badges */}
      <div style={{ padding: '10px 14px', borderBottom: `1px solid ${COL.border}`, flexShrink: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <span style={{ fontSize: 12, fontWeight: 600, fontFamily: 'monospace', color: 'var(--text-primary)' }}>
            {fullName}
          </span>
          {/* Topology badges — short labels, details on hover */}
          <span style={{
            padding: '2px 8px', borderRadius: 4, fontSize: 9, fontWeight: 600,
            backgroundColor: COL.blueBg, color: COL.blue, border: `1px solid ${COL.blue}40`,
          }}>
            REPLICATED
          </span>
          {data.shards.length > 1 && (
            <span style={{
              padding: '2px 8px', borderRadius: 4, fontSize: 9, fontWeight: 600,
              backgroundColor: COL.purpleBg, color: COL.purple, border: `1px solid ${COL.purple}40`,
            }}>
              {data.shards.length} SHARDS
            </span>
          )}
          {data.engineInfo?.distributedTable ? (
            <span
              title={`Via ${data.engineInfo.distributedTable}${data.engineInfo.shardingKey ? ` · shard by ${data.engineInfo.shardingKey}` : ''}`}
              style={{
                padding: '2px 8px', borderRadius: 4, fontSize: 9, fontWeight: 600,
                backgroundColor: COL.greenBg, color: COL.green, border: `1px solid ${COL.green}40`,
                cursor: 'help',
              }}
            >
              DISTRIBUTED
            </span>
          ) : data.shards.length > 1 ? (
            <span
              title="No Distributed table — INSERTs go to the connected shard only"
              style={{
                padding: '2px 8px', borderRadius: 4, fontSize: 9, fontWeight: 600,
                backgroundColor: COL.amberBg, color: COL.amber, border: `1px solid ${COL.amber}40`,
                cursor: 'help',
              }}
            >
              NOT DISTRIBUTED
            </span>
          ) : null}
          <span style={{ flex: 1 }} />
          <span style={{
            padding: '2px 10px', borderRadius: 4, fontSize: 10, fontWeight: 700,
            color: '#f0883e', backgroundColor: 'rgba(240,136,62,0.1)',
            border: '1px solid rgba(240,136,62,0.3)',
            textTransform: 'uppercase', letterSpacing: '0.5px',
          }}>
            Experimental
          </span>
        </div>
        <div style={{ display: 'flex', gap: 12, marginTop: 4, flexWrap: 'wrap', fontSize: 10 }}>
          {data.engineInfo && (
            <>
              <span style={{ color: COL.muted }}>
                Engine: <span style={{ color: 'var(--text-primary)', fontFamily: 'monospace' }}>{data.engineInfo.engine}</span>
              </span>
              {data.engineInfo.partitionKey && (
                <span style={{ color: COL.muted }}>
                  Partition: <span style={{ color: 'var(--text-primary)', fontFamily: 'monospace' }}>{data.engineInfo.partitionKey}</span>
                </span>
              )}
              {data.engineInfo.sortingKey && (
                <span style={{ color: COL.muted }}>
                  Order: <span style={{ color: 'var(--text-primary)', fontFamily: 'monospace' }}>{data.engineInfo.sortingKey}</span>
                </span>
              )}
              {data.engineInfo.shardingKey && (
                <span style={{ color: COL.muted }}>
                  Shard key: <span style={{ color: 'var(--text-primary)', fontFamily: 'monospace' }}>{data.engineInfo.shardingKey}</span>
                </span>
              )}
            </>
          )}
          <span style={{ color: COL.muted }}>
            {data.shards.reduce((n, s) => n + s.replicas.length, 0)} replicas · {formatBytes(data.totalBytes)}
          </span>
        </div>
      </div>

      {/* 3D Topology */}
      <div style={{ height: 500, position: 'relative' }}>
        {fetching && (
          <div style={{
            position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, zIndex: 10,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            backgroundColor: 'rgba(0,0,0,0.3)', backdropFilter: 'blur(2px)',
            fontSize: 11, color: 'rgba(255,255,255,0.7)', pointerEvents: 'none',
          }}>
            Loading {fullName}...
          </div>
        )}
        <ErrorBoundary3D>
          <Canvas
            camera={{ position: [0, camDistance * 0.7, camDistance], fov: 45 }}
            shadows
            gl={{ antialias: true }}
            style={{ background: theme === 'dark' ? '#0c0c1a' : '#303055' }}
          >
            <ambientLight intensity={theme === 'dark' ? 0.5 : 0.7} />
            <directionalLight position={[5, 8, 5]} intensity={0.6} castShadow />
            <ReplicaScene data={data} theme={theme} />
            <OrbitControls
              enablePan enableZoom enableRotate
              maxPolarAngle={Math.PI / 2.1} minDistance={2} maxDistance={20}
            />
          </Canvas>
        </ErrorBoundary3D>
      </div>

      {/* 2D Detail Panels — compact, pinned to bottom */}
      <div style={{ flexShrink: 0, borderTop: `1px solid ${COL.border}`, display: 'flex' }}>
        <div style={{ flex: '1 1 0', padding: '12px 16px' }}>
          <DataDistributionBar shards={data.shards} totalBytes={data.totalBytes} partitionDist={data.partitionDist} />
        </div>
        <div style={{ width: 1, backgroundColor: COL.border, flexShrink: 0 }} />
        <div style={{ flex: '1 1 0', padding: '12px 16px' }}>
          <SyncStatus shards={data.shards} logHead={data.logHead} />
        </div>
        <div style={{ width: 1, backgroundColor: COL.border, flexShrink: 0 }} />
        <div style={{ flex: '1 1 0', padding: '12px 16px' }}>
          <ReplicationQueuePanel queueEntries={data.queueEntries} />
        </div>
        <div style={{ width: 1, backgroundColor: COL.border, flexShrink: 0 }} />
        <div style={{ flex: '1 1 0', padding: '12px 16px' }}>
          <KeeperPanel
            keeperInfo={data.keeperInfo}
            keeperConnections={data.keeperConnections}
            distributionQueue={data.distributionQueue}
            onFetchZkChildren={fetchZkChildren}
          />
        </div>
      </div>

    </div>
  );
};

export default ReplicationTopology;
