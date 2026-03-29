/**
 * ReplicationTopology — per-table 3D + 2D visualization showing shard/replica layout,
 * ZooKeeper connections, data distribution, and queue health.
 *
 * The 3D scene reuses the glass-box visual language from ClusterTopology.
 * Below it, compact 2D panels show data distribution, keeper sync state, and recovery guides.
 */

import React, { useEffect, useState, useMemo, useRef } from 'react';
import { Canvas, useFrame } from '@react-three/fiber';
import { Html, OrbitControls } from '@react-three/drei';
import * as THREE from 'three';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useConnectionStore } from '../../stores/connectionStore';
import { useClusterStore } from '../../stores/clusterStore';
import { ReplicationService, classifyReplicaHealth } from '@tracehouse/core';
import type {
  ShardPartitionDist,
  TopologyShard,
  TopologyReplica,
  ReplicationTopologyData,
  TopologyQueueEntry,
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
  theme: 'dark' | 'light';
}> = ({ position, logHead, theme }) => {
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

  return (
    <group position={[position[0], 0, position[2]]}>
      <mesh
        ref={meshRef} position={[0, position[1], 0]}
        onPointerOver={(e) => { e.stopPropagation(); setHovered(true); document.body.style.cursor = 'pointer'; }}
        onPointerOut={(e) => { e.stopPropagation(); setHovered(false); document.body.style.cursor = 'auto'; }}
      >
        <boxGeometry args={size} />
        <meshPhysicalMaterial
          color={color.main} metalness={0.1} roughness={0.05}
          transmission={0.5} thickness={1.5} transparent
          opacity={hovered ? 0.85 : 0.65}
          emissive={color.main} emissiveIntensity={hovered ? 0.4 : 0.15}
          clearcoat={1} clearcoatRoughness={0.1} ior={1.5}
        />
      </mesh>
      <lineSegments position={[0, position[1], 0]}>
        <primitive object={edgesGeometry} attach="geometry" />
        <lineBasicMaterial color={color.edge} transparent opacity={hovered ? 1 : 0.7} />
      </lineSegments>
      <mesh position={[0, 0.04, 0]}>
        <boxGeometry args={[size[0] * 1.08, 0.06, size[2] * 1.08]} />
        <meshBasicMaterial color={color.main} transparent opacity={0.8} />
      </mesh>

      {/* Always-visible keeper label */}
      <Html
        position={[0, position[1] + size[1] / 2 + 0.15, 0]}
        center style={{ pointerEvents: 'none', whiteSpace: 'nowrap' }}
      >
        <div style={{ textAlign: 'center', transform: 'scale(0.8)' }}>
          <div style={{ fontSize: 9, fontFamily: 'ui-monospace, monospace', fontWeight: 600, color: color.glow, textShadow: `0 0 6px ${color.main}` }}>
            Keeper
          </div>
          <div style={{ fontSize: 8, color: 'rgba(167,139,250,0.7)' }}>
            log: {logHead.toLocaleString()}
          </div>
        </div>
      </Html>

      {hovered && (
        <Html position={[0, position[1] + 0.5, 0.3]} center style={{ pointerEvents: 'none', zIndex: 1000 }}>
          <div style={{
            background: theme === 'light' ? 'rgba(255,255,255,0.95)' : 'rgba(15,23,42,0.92)',
            padding: '10px 14px', borderRadius: 4,
            border: `1px solid ${color.edge}50`, minWidth: 140,
            backdropFilter: 'blur(12px)', transform: 'scale(0.85)',
          }}>
            <div style={{ fontSize: 12, fontWeight: 600, fontFamily: 'monospace', color: color.glow, marginBottom: 4 }}>
              Keeper
            </div>
            <HoverRow label="Log head" value={logHead.toLocaleString()} theme={theme} />
          </div>
        </Html>
      )}
    </group>
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

  return { shardLayouts, keeperPos, totalWidth, nodeSize };
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

      <KeeperNode3D position={layout.keeperPos} logHead={data.logHead} theme={theme} />

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
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
        {allReplicas.map(r => {
          const isBehind = r.info.absolute_delay > 0;
          const behind = logHead - r.info.log_pointer;
          const color = isBehind ? COL.amber : COL.green;
          const bgColor = isBehind ? COL.amberBg : COL.greenBg;
          return (
            <span key={r.info.replica_name} style={{
              display: 'inline-flex', alignItems: 'center', gap: 5,
              padding: '3px 8px', borderRadius: 4, fontSize: 10,
              backgroundColor: bgColor, border: `1px solid ${color}30`,
            }}>
              <span style={{ width: 6, height: 6, borderRadius: '50%', backgroundColor: color, flexShrink: 0 }} />
              <span style={{ fontFamily: 'monospace', color: COL.primary }}>{r.info.hostname}</span>
              {isBehind && (
                <span style={{ color: COL.amber, fontFamily: 'monospace', fontSize: 9 }}>
                  {formatDelay(r.info.absolute_delay)}{behind > 0 ? ` (log -${behind})` : ''}
                </span>
              )}
            </span>
          );
        })}
      </div>
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
    svc.getTopology(database, table).then(result => {
      if (!cancelled) { setData(result); setDisplayedTable(target); }
    }).catch(err => {
      if (!cancelled) setError(err instanceof Error ? err.message : String(err));
    }).finally(() => {
      if (!cancelled) setLoading(false);
    });

    return () => { cancelled = true; };
  }, [services, isConnected, detected, database, table]);

  // Track which table we're currently displaying vs fetching
  const [displayedTable, setDisplayedTable] = useState(`${database}.${table}`);
  const fetching = displayedTable !== `${database}.${table}`;

  const fullName = `${database}.${table}`;

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
          <span style={{ position: 'relative', fontSize: 12, fontWeight: 600, fontFamily: 'monospace', color: 'var(--text-primary)' }}>
            {fullName}
            <span style={{
              position: 'absolute', top: -6, right: -18,
              fontSize: 7, fontWeight: 700, color: '#f0883e',
              background: 'var(--bg-tertiary)', border: '1px solid rgba(240,136,62,0.3)',
              borderRadius: 3, padding: '0 3px', lineHeight: '12px',
              textTransform: 'uppercase', letterSpacing: '0.3px',
            }}>exp</span>
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
      <div style={{ flexShrink: 0, borderTop: `1px solid ${COL.border}`, padding: '10px 14px', display: 'flex', gap: 14 }}>
        <div style={{ flex: '1 1 0' }}>
          <DataDistributionBar shards={data.shards} totalBytes={data.totalBytes} partitionDist={data.partitionDist} />
        </div>
        <div style={{ flex: '1 1 0' }}>
          <SyncStatus shards={data.shards} logHead={data.logHead} />
        </div>
      </div>

    </div>
  );
};

export default ReplicationTopology;
