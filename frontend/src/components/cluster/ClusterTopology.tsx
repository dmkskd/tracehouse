/**
 * ClusterTopology — 3D visualization of cluster nodes grouped by shard,
 * plus Keeper/ZooKeeper nodes. Uses the same glass-box style as the
 * database explorer hierarchy view.
 */

import React, { useState, useMemo, useRef, useEffect } from 'react';
import { Canvas, useFrame } from '@react-three/fiber';
import { Html, OrbitControls } from '@react-three/drei';
import * as THREE from 'three';
import { shortenHostname } from '@tracehouse/core';
import { ErrorBoundary3D } from '../3d/ErrorBoundary3D';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';
import { useThemeDetection } from '../../hooks/useThemeDetection';
import { formatBytes } from '../../utils/formatters';
import { computeLayout } from './clusterLayout';

// ── Types ──

interface NodeRow {
  shard_num: number;
  replica_num: number;
  host_name: string;
  host_address: string;
  port: number;
  is_local: number;
  errors_count: number;
  slowdowns_count: number;
  estimated_recovery_time: number;
}

interface KeeperNode {
  host: string;
  port: number;
  index: number;
  connected_time: string;
  is_expired: number;
  keeper_api_version: number;
}

export interface HostMetrics {
  hostname: string;
  uptime: number;
  version: string;
  mem_total: number;
  mem_free: number;
  load_1m: number;
  /** @deprecated Use effective_cores instead. Raw host core count from OSUserTimeCPU metrics. */
  cpu_cores_host: number;
  /** CGroupMaxCPU value (0 if no cgroup limit). */
  cgroup_cpu: number;
  /** NumberOfCPUCores from async metrics (host-level). */
  os_cpu_cores: number;
  /** CGroupMemoryLimit/CGroupMemoryTotal (0 or very large if no limit). */
  cgroup_mem_limit: number;
  /** CGroupMemoryUsed (0 if not available). */
  cgroup_mem_used: number;
  [key: string]: unknown;
}

/** Derive the effective core count from a HostMetrics row. */
export function getEffectiveCores(m: HostMetrics): { effective: number; host: number; isCgroupLimited: boolean } {
  const cgroupCpu = Number(m.cgroup_cpu) || 0;
  const hostCores = Number(m.cpu_cores_host) || Number(m.os_cpu_cores) || 0;
  if (cgroupCpu > 0 && cgroupCpu < hostCores) {
    return { effective: Math.round(cgroupCpu), host: hostCores, isCgroupLimited: true };
  }
  return { effective: hostCores, host: hostCores, isCgroupLimited: false };
}
function getEffectiveMemory(m: HostMetrics): { effective: number; host: number; used: number; isCgroupLimited: boolean } {
  const cgroupMem = Number(m.cgroup_mem_limit) || 0;
  const cgroupUsed = Number(m.cgroup_mem_used) || 0;
  const hostMem = Number(m.mem_total) || 0;
  const hostFree = Number(m.mem_free) || 0;
  // CGroupMemoryLimit/Total returns a very large number when no limit is set
  if (cgroupMem > 0 && cgroupMem < 1e18 && cgroupMem < hostMem) {
    // In containers, use CGroupMemoryUsed if available, otherwise fall back to total - free
    const used = cgroupUsed > 0 ? cgroupUsed : Math.max(0, cgroupMem - hostFree);
    return { effective: cgroupMem, host: hostMem, used, isCgroupLimited: true };
  }
  const used = Math.max(0, hostMem - hostFree);
  return { effective: hostMem, host: hostMem, used, isCgroupLimited: false };
}

interface ClusterTopologyProps {
  clusterName: string;
  nodes: NodeRow[];
  keeperNodes: KeeperNode[];
  hostMetrics?: HostMetrics[];
}

// ── Colors ──

const COLORS = {
  local:   { main: '#58a6ff', edge: '#79b8ff', glow: '#58a6ff' },
  healthy: { main: '#22c55e', edge: '#4ade80', glow: '#22c55e' },
  error:   { main: '#ef4444', edge: '#f87171', glow: '#ef4444' },
  keeper:  { main: '#a78bfa', edge: '#c4b5fd', glow: '#a78bfa' },
  keeperExpired: { main: '#ef4444', edge: '#f87171', glow: '#ef4444' },
  shard:   { main: '#334155', edge: '#94a3b8', glow: '#64748b' },
};

// ── Hover card row ──

const HoverRow: React.FC<{ label: string; value: string; theme: 'dark' | 'light'; color?: string }> = ({ label, value, theme, color }) => (
  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
    <span style={{ color: theme === 'light' ? 'rgba(0,0,0,0.45)' : 'rgba(255,255,255,0.45)', fontSize: 10 }}>{label}</span>
    <span style={{ color: color ?? (theme === 'light' ? 'rgba(0,0,0,0.85)' : 'rgba(255,255,255,0.85)'), fontSize: 11, fontWeight: 500, fontFamily: 'ui-monospace, monospace' }}>{value}</span>
  </div>
);

// ── 3D Node Box ──

interface NodeBoxProps {
  position: [number, number, number];
  size: [number, number, number];
  color: { main: string; edge: string; glow: string };
  label: string;
  sublabel: string;
  isLocal?: boolean;
  metrics?: HostMetrics;
  keeperInfo?: KeeperNode;
  errorCount?: number;
  theme: 'dark' | 'light';
}

function formatUptime(seconds: number): string {
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  if (d > 0) return `${d}d ${h}h`;
  const m = Math.floor((seconds % 3600) / 60);
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

function formatMemGB(bytes: number): string {
  return formatBytes(bytes);
}

const NodeBox: React.FC<NodeBoxProps> = ({ position, size, color, label, sublabel, isLocal, metrics, keeperInfo, errorCount, theme }) => {
  const meshRef = useRef<THREE.Mesh>(null);
  const [hovered, setHovered] = useState(false);

  const edgesGeometry = useMemo(() => {
    const geo = new THREE.BoxGeometry(size[0] * 1.002, size[1] * 1.002, size[2] * 1.002);
    return new THREE.EdgesGeometry(geo, 1);
  }, [size]);

  useEffect(() => () => { edgesGeometry.dispose(); }, [edgesGeometry]);

  useFrame((state) => {
    if (!meshRef.current) return;
    const t = state.clock.elapsedTime;
    meshRef.current.position.y = position[1] + Math.sin(t * 0.6 + position[0] * 2) * 0.04;
  });

  return (
    <group position={[position[0], 0, position[2]]}>
      <mesh
        ref={meshRef}
        position={[0, position[1], 0]}
        onPointerOver={(e) => { e.stopPropagation(); setHovered(true); document.body.style.cursor = 'pointer'; }}
        onPointerOut={(e) => { e.stopPropagation(); setHovered(false); document.body.style.cursor = 'auto'; }}
        castShadow
        receiveShadow
      >
        <boxGeometry args={size} />
        <meshPhysicalMaterial
          color={color.main}
          metalness={0.1}
          roughness={0.05}
          transmission={0.5}
          thickness={1.5}
          transparent
          opacity={hovered ? 0.85 : 0.65}
          emissive={color.main}
          emissiveIntensity={hovered ? 0.4 : (isLocal ? 0.25 : 0.15)}
          clearcoat={1}
          clearcoatRoughness={0.1}
          ior={1.5}
        />
      </mesh>

      <lineSegments position={[0, position[1], 0]}>
        <primitive object={edgesGeometry} attach="geometry" />
        <lineBasicMaterial color={color.edge} transparent opacity={hovered ? 1 : 0.7} />
      </lineSegments>

      {/* Health bar at base */}
      <mesh position={[0, 0.04, 0]}>
        <boxGeometry args={[size[0] * 1.08, 0.06, size[2] * 1.08]} />
        <meshBasicMaterial color={color.main} transparent opacity={0.8} />
      </mesh>

      {/* Glow on hover */}
      {hovered && (
        <mesh position={[0, 0.02, 0]} rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[size[0] * 2, size[2] * 2]} />
          <meshBasicMaterial color={color.glow} transparent opacity={0.12} />
        </mesh>
      )}

      {/* Hover card with details */}
      {hovered && (
        <Html
          position={[0, position[1] + size[1] / 2 + 0.2, size[2] / 2 + 0.3]}
          center
          style={{ pointerEvents: 'none', zIndex: 1000 }}
        >
          <div style={{
            background: theme === 'light' ? 'rgba(255,255,255,0.95)' : 'rgba(15,23,42,0.92)',
            padding: '10px 14px',
            borderRadius: 4,
            border: `1px solid ${color.edge}50`,
            boxShadow: `0 0 16px ${color.main}20`,
            minWidth: 170,
            backdropFilter: 'blur(12px)',
            transform: 'scale(0.85)',
          }}>
            <div style={{
              fontSize: 12, fontWeight: 600, fontFamily: 'ui-monospace, monospace',
              color: color.glow, marginBottom: 6, paddingBottom: 6,
              borderBottom: `1px solid ${color.edge}30`,
            }}>
              {isLocal ? '● ' : ''}{label}
              <div style={{
                color: theme === 'light' ? 'rgba(0,0,0,0.4)' : 'rgba(255,255,255,0.4)',
                fontSize: 9, fontWeight: 400, marginTop: 1,
              }}>
                {sublabel}
              </div>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
              {metrics && (
                <>
                  <HoverRow label="Version" value={String(metrics.version)} theme={theme} />
                  <HoverRow label="Uptime" value={formatUptime(Number(metrics.uptime))} theme={theme} />
                  {(() => {
                    const cores = getEffectiveCores(metrics);
                    const coreLabel = cores.isCgroupLimited
                      ? `${cores.effective} vCPUs (host: ${cores.host})`
                      : String(cores.effective || '—');
                    return <HoverRow label="CPU Cores" value={coreLabel} theme={theme} />;
                  })()}
                  <HoverRow label="Load 1m" value={Number(metrics.load_1m).toFixed(2)} theme={theme} />
                  {(() => {
                    const mem = getEffectiveMemory(metrics);
                    const memLabel = mem.isCgroupLimited
                      ? `${formatMemGB(mem.used)} / ${formatMemGB(mem.effective)} (host: ${formatMemGB(mem.host)})`
                      : `${formatMemGB(mem.used)} / ${formatMemGB(mem.effective)}`;
                    return <HoverRow label="Memory" value={memLabel} theme={theme} />;
                  })()}
                </>
              )}
              {keeperInfo && (
                <>
                  <HoverRow label="Host" value={`${shortenHostname(keeperInfo.host)}:${keeperInfo.port}`} theme={theme} />
                  <HoverRow label="Status" value={Number(keeperInfo.is_expired) ? 'EXPIRED' : 'Connected'} theme={theme} color={Number(keeperInfo.is_expired) ? '#ef4444' : '#22c55e'} />
                  <HoverRow label="Connected" value={String(keeperInfo.connected_time)} theme={theme} />
                  <HoverRow label="API Version" value={String(keeperInfo.keeper_api_version)} theme={theme} />
                </>
              )}
              {(errorCount ?? 0) > 0 && (
                <HoverRow label="Errors" value={String(errorCount)} theme={theme} color="#ef4444" />
              )}
            </div>
          </div>
        </Html>
      )}
    </group>
  );
};

// ── Shard enclosure (wireframe box around a group of replicas) ──

const ShardEnclosure: React.FC<{
  position: [number, number, number];
  size: [number, number, number];
  label: string;
  theme: 'dark' | 'light';
}> = ({ position, size, label, theme }) => {
  const fillColor = theme === 'light' ? '#4a6080' : '#1a3050';
  const borderColor = theme === 'light' ? '#6b9fd4' : '#4a90d9';

  const y = 0.04;
  const hw = size[0] / 2;
  const hd = size[2] / 2;

  // Single border
  const borderPoints = useMemo(() => new Float32Array([
    -hw, 0, -hd,  hw, 0, -hd,
     hw, 0, -hd,  hw, 0,  hd,
     hw, 0,  hd, -hw, 0,  hd,
    -hw, 0,  hd, -hw, 0, -hd,
  ]), [hw, hd]);

  // Canvas texture for the shard label
  const labelTexture = useMemo(() => {
    const canvas = document.createElement('canvas');
    canvas.width = 1024;
    canvas.height = 256;
    const ctx = canvas.getContext('2d')!;
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    const textStr = label.toUpperCase();
    const textFill = theme === 'light' ? '#90b8e0' : '#80c0ff';

    ctx.shadowColor = theme === 'dark' ? 'rgba(100,180,255,0.7)' : 'rgba(0,0,0,0)';
    ctx.shadowBlur = 30;
    ctx.font = 'bold 140px ui-monospace, "SF Mono", "Cascadia Code", monospace';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = textFill;
    ctx.fillText(textStr, canvas.width / 2, canvas.height / 2);
    ctx.shadowBlur = 0;
    ctx.fillText(textStr, canvas.width / 2, canvas.height / 2);

    const tex = new THREE.CanvasTexture(canvas);
    tex.anisotropy = 16;
    tex.minFilter = THREE.LinearMipmapLinearFilter;
    tex.magFilter = THREE.LinearFilter;
    return tex;
  }, [label, theme]);

  useEffect(() => () => { labelTexture.dispose(); }, [labelTexture]);

  // Label sits in the +Z padding area (nodes are shifted to -Z, so this is clear)
  const labelW = size[0] * 0.85;
  const labelPlaneH = 0.45;
  const labelZ = hd - labelPlaneH / 2 - 0.1;

  return (
    <group position={[position[0], y, position[2]]}>
      {/* Floor fill */}
      <mesh rotation={[-Math.PI / 2, 0, 0]} receiveShadow>
        <planeGeometry args={[size[0], size[2]]} />
        <meshBasicMaterial color={fillColor} transparent opacity={0.5} depthWrite={false} />
      </mesh>

      {/* Single border */}
      <lineSegments>
        <bufferGeometry>
          <bufferAttribute attach="attributes-position" count={8} array={borderPoints} itemSize={3} />
        </bufferGeometry>
        <lineBasicMaterial color={borderColor} transparent opacity={0.9} />
      </lineSegments>

      {/* Shard name on the floor — in the clear area past the nodes */}
      <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, 0.008, labelZ]}>
        <planeGeometry args={[labelW, labelPlaneH]} />
        <meshBasicMaterial map={labelTexture} transparent opacity={1} depthWrite={false} />
      </mesh>
    </group>
  );
};

// ── Keeper Enclosure (same glass-floor style, keeper-themed) ──

const KeeperEnclosure: React.FC<{
  position: [number, number, number];
  size: [number, number, number];
  label: string;
  theme: 'dark' | 'light';
}> = ({ position, size, label, theme }) => {
  const fillColor = theme === 'light' ? '#4a3870' : '#1a1040';
  const borderColor = theme === 'light' ? '#b49ce0' : '#7c5cbf';

  const y = 0.04;
  const hw = size[0] / 2;
  const hd = size[2] / 2;

  const borderPoints = useMemo(() => new Float32Array([
    -hw, 0, -hd,  hw, 0, -hd,
     hw, 0, -hd,  hw, 0,  hd,
     hw, 0,  hd, -hw, 0,  hd,
    -hw, 0,  hd, -hw, 0, -hd,
  ]), [hw, hd]);

  const labelTexture = useMemo(() => {
    const canvas = document.createElement('canvas');
    canvas.width = 1024;
    canvas.height = 256;
    const ctx = canvas.getContext('2d')!;
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    const textStr = label.toUpperCase();
    const textFill = theme === 'light' ? '#b49ce0' : '#c4b5fd';

    ctx.shadowColor = theme === 'dark' ? 'rgba(167,139,250,0.7)' : 'rgba(0,0,0,0)';
    ctx.shadowBlur = 30;
    ctx.font = 'bold 120px ui-monospace, "SF Mono", "Cascadia Code", monospace';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = textFill;
    ctx.fillText(textStr, canvas.width / 2, canvas.height / 2);
    ctx.shadowBlur = 0;
    ctx.fillText(textStr, canvas.width / 2, canvas.height / 2);

    const tex = new THREE.CanvasTexture(canvas);
    tex.anisotropy = 16;
    tex.minFilter = THREE.LinearMipmapLinearFilter;
    tex.magFilter = THREE.LinearFilter;
    return tex;
  }, [label, theme]);

  useEffect(() => () => { labelTexture.dispose(); }, [labelTexture]);

  const labelW = size[0] * 0.85;
  const labelPlaneH = 0.45;
  const labelZ = hd - labelPlaneH / 2 - 0.1;

  return (
    <group position={[position[0], y, position[2]]}>
      <mesh rotation={[-Math.PI / 2, 0, 0]} receiveShadow>
        <planeGeometry args={[size[0], size[2]]} />
        <meshBasicMaterial color={fillColor} transparent opacity={0.5} depthWrite={false} />
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

// ── Connection lines between keeper and shards ──

const ConnectionLines: React.FC<{
  shardPositions: [number, number, number][];
  keeperPositions: [number, number, number][];
  theme: 'dark' | 'light';
}> = ({ shardPositions, keeperPositions, theme }) => {
  const lines = useMemo(() => {
    const result: [THREE.Vector3, THREE.Vector3][] = [];
    for (const sp of shardPositions) {
      for (const kp of keeperPositions) {
        result.push([
          new THREE.Vector3(sp[0], 0.1, sp[2]),
          new THREE.Vector3(kp[0], 0.1, kp[2]),
        ]);
      }
    }
    return result;
  }, [shardPositions, keeperPositions]);

  const lineColor = theme === 'light' ? '#94a3b8' : '#475569';

  return (
    <>
      {lines.map((pair, i) => {
        const points = [pair[0], pair[1]];
        const geometry = new THREE.BufferGeometry().setFromPoints(points);
        return (
          <lineSegments key={i}>
            <primitive object={geometry} attach="geometry" />
            <lineDashedMaterial
              color={lineColor}
              transparent
              opacity={0.25}
              dashSize={0.15}
              gapSize={0.1}
            />
          </lineSegments>
        );
      })}
    </>
  );
};

// ── Ground plane ──

const GROUND_COLORS = {
  dark: { ground: '#0c0c1a', grid1: '#1a1a3a', grid2: '#0f0f2a' },
  light: { ground: '#303055', grid1: '#40406a', grid2: '#353560' },
};

const Ground: React.FC<{ size: number; theme: 'dark' | 'light' }> = ({ size, theme }) => {
  const colors = GROUND_COLORS[theme];
  const materialProps = theme === 'light'
    ? { metalness: 0.4, roughness: 0.6 }
    : { metalness: 0.8, roughness: 0.4 };

  return (
    <group>
      <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, 0, 0]} receiveShadow>
        <planeGeometry args={[size * 2.5, size * 2.5]} />
        <meshStandardMaterial color={colors.ground} {...materialProps} />
      </mesh>
      <gridHelper args={[size * 2, Math.floor(size * 2), colors.grid1, colors.grid2]} position={[0, 0.01, 0]} />
    </group>
  );
};

// ── Scene content ──

const ClusterScene: React.FC<{
  clusterName: string;
  nodes: NodeRow[];
  keeperNodes: KeeperNode[];
  hostMetrics?: HostMetrics[];
  theme: 'dark' | 'light';
}> = ({ nodes, keeperNodes: keepers, hostMetrics, theme }) => {
  const layout = useMemo(() => computeLayout(nodes, keepers), [nodes, keepers]);
  const nodeSize: [number, number, number] = [0.7, 0.4, 0.4];
  const keeperSize: [number, number, number] = [0.6, 0.3, 0.3];

  // Build hostname → metrics lookup (index by both full and short hostname)
  const metricsMap = useMemo(() => {
    const map = new Map<string, HostMetrics>();
    for (const m of (hostMetrics ?? [])) {
      const h = String(m.hostname);
      map.set(h, m);
      const short = shortenHostname(h);
      if (short !== h) map.set(short, m);
    }
    return map;
  }, [hostMetrics]);

  const shardCenters = layout.shardGroups.map(sg => sg.center);
  const keeperPositions = layout.keeperNodes.map(k => k.position);

  return (
    <>
      {/* Ground */}
      <Ground size={layout.totalWidth + 6} theme={theme} />

      {/* Shard enclosures + replica nodes */}
      {layout.shardGroups.map((sg) => (
        <React.Fragment key={sg.shardNum}>
          <ShardEnclosure
            position={sg.center}
            size={sg.enclosureSize}
            label={`Shard ${sg.shardNum}`}
            theme={theme}
          />
          {sg.replicas.map((r) => {
            const hasErrors = Number(r.node.errors_count) > 0;
            const isLocal = !!Number(r.node.is_local);
            const color = hasErrors ? COLORS.error : isLocal ? COLORS.local : COLORS.healthy;
            const shortName = shortenHostname(r.node.host_name);
            const nodeMetrics = metricsMap.get(r.node.host_name) ?? metricsMap.get(shortName);
            return (
              <NodeBox
                key={`${r.node.host_name}-${r.node.replica_num}`}
                position={r.position}
                size={nodeSize}
                color={color}
                label={shortName}
                sublabel={`replica ${r.node.replica_num}`}
                isLocal={isLocal}
                metrics={nodeMetrics}
                errorCount={Number(r.node.errors_count)}
                theme={theme}
              />
            );
          })}
        </React.Fragment>
      ))}

      {/* Keeper enclosure + nodes */}
      {layout.keeperEnclosure && (
        <KeeperEnclosure
          position={layout.keeperEnclosure.center}
          size={layout.keeperEnclosure.size}
          label="Keeper"
          theme={theme}
        />
      )}
      {layout.keeperNodes.map((k) => {
        const expired = !!Number(k.node.is_expired);
        const color = expired ? COLORS.keeperExpired : COLORS.keeper;
        return (
          <NodeBox
            key={`keeper-${k.node.index}`}
            position={k.position}
            size={keeperSize}
            color={color}
            label={shortenHostname(k.node.host)}
            sublabel={expired ? 'expired' : 'keeper'}
            keeperInfo={k.node}
            theme={theme}
          />
        );
      })}

      {/* Connection lines from shards to keepers */}
      {keepers.length > 0 && shardCenters.length > 0 && (
        <ConnectionLines
          shardPositions={shardCenters}
          keeperPositions={keeperPositions}
          theme={theme}
        />
      )}

    </>
  );
};

// ── 2D Fallback ──

const Fallback2DNode: React.FC<{
  label: string;
  sublabel: string;
  borderColor: string;
  isLocal?: boolean;
  metrics?: HostMetrics;
  keeperInfo?: KeeperNode;
  errorCount?: number;
}> = ({ label, sublabel, borderColor, isLocal, metrics, keeperInfo, errorCount }) => {
  const [hovered, setHovered] = useState(false);

  return (
    <div
      style={{
        padding: '8px 14px', borderRadius: 6, border: `2px solid ${borderColor}`,
        background: isLocal ? `${borderColor}10` : 'var(--bg-card)', textAlign: 'center',
        position: 'relative', cursor: 'default',
      }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <div style={{ fontSize: 11, fontWeight: 600, fontFamily: 'monospace', color: 'var(--text-primary)' }}>
        {isLocal ? '● ' : ''}{label}
      </div>
      <div style={{ fontSize: 9, color: 'var(--text-muted)' }}>{sublabel}</div>
      {hovered && (metrics || keeperInfo || (errorCount ?? 0) > 0) && (
        <div style={{
          position: 'absolute', bottom: '100%', left: '50%', transform: 'translateX(-50%)',
          marginBottom: 6, background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)',
          borderRadius: 6, padding: '8px 12px', zIndex: 100, minWidth: 170,
          boxShadow: '0 4px 12px rgba(0,0,0,0.15)', pointerEvents: 'none', whiteSpace: 'nowrap',
        }}>
          <div style={{ fontSize: 11, fontWeight: 600, fontFamily: 'monospace', color: borderColor, marginBottom: 4, paddingBottom: 4, borderBottom: '1px solid var(--border-secondary)' }}>
            {label}
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2, fontSize: 10 }}>
            {metrics && (
              <>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <span style={{ color: 'var(--text-muted)' }}>Version</span>
                  <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{String(metrics.version)}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <span style={{ color: 'var(--text-muted)' }}>Uptime</span>
                  <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{formatUptime(Number(metrics.uptime))}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <span style={{ color: 'var(--text-muted)' }}>CPU Cores</span>
                  <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>
                    {(() => {
                      const cores = getEffectiveCores(metrics);
                      return cores.isCgroupLimited
                        ? <>{cores.effective} <span style={{ fontSize: 8, color: '#60a5fa' }}>cgroup:{cores.effective}/{cores.host}</span></>
                        : String(cores.effective || '—');
                    })()}
                  </span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <span style={{ color: 'var(--text-muted)' }}>Load 1m</span>
                  <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{Number(metrics.load_1m).toFixed(2)}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <span style={{ color: 'var(--text-muted)' }}>Memory</span>
                  <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>
                    {(() => {
                      const mem = getEffectiveMemory(metrics);
                      return mem.isCgroupLimited
                        ? <>{formatMemGB(mem.used)} / {formatMemGB(mem.effective)} <span style={{ fontSize: 8, color: '#60a5fa' }}>cgroup:{formatMemGB(mem.effective)}/{formatMemGB(mem.host)}</span></>
                        : <>{formatMemGB(mem.used)} / {formatMemGB(mem.effective)}</>;
                    })()}
                  </span>
                </div>
              </>
            )}
            {keeperInfo && (
              <>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <span style={{ color: 'var(--text-muted)' }}>Host</span>
                  <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{shortenHostname(keeperInfo.host)}:{keeperInfo.port}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <span style={{ color: 'var(--text-muted)' }}>Status</span>
                  <span style={{ fontFamily: 'monospace', color: Number(keeperInfo.is_expired) ? '#ef4444' : '#22c55e' }}>{Number(keeperInfo.is_expired) ? 'EXPIRED' : 'Connected'}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <span style={{ color: 'var(--text-muted)' }}>Connected</span>
                  <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{String(keeperInfo.connected_time)}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                  <span style={{ color: 'var(--text-muted)' }}>API Version</span>
                  <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{String(keeperInfo.keeper_api_version)}</span>
                </div>
              </>
            )}
            {(errorCount ?? 0) > 0 && (
              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                <span style={{ color: 'var(--text-muted)' }}>Errors</span>
                <span style={{ fontFamily: 'monospace', color: '#ef4444' }}>{String(errorCount)}</span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

const Fallback2D: React.FC<ClusterTopologyProps> = ({ nodes, keeperNodes, hostMetrics }) => {
  const shards = new Map<number, NodeRow[]>();
  for (const n of nodes) {
    const s = Number(n.shard_num);
    if (!shards.has(s)) shards.set(s, []);
    shards.get(s)!.push(n);
  }

  // Build hostname → metrics lookup
  const metricsMap = new Map<string, HostMetrics>();
  for (const m of (hostMetrics ?? [])) {
    const h = String(m.hostname);
    metricsMap.set(h, m);
    const short = shortenHostname(h);
    if (short !== h) metricsMap.set(short, m);
  }

  return (
    <div style={{ padding: 24 }}>
      <div style={{ display: 'flex', gap: 20, justifyContent: 'center', flexWrap: 'wrap', marginBottom: keeperNodes.length > 0 ? 24 : 0 }}>
        {[...shards.entries()].sort(([a], [b]) => a - b).map(([shardNum, replicas]) => (
          <div key={shardNum} style={{ border: '1px dashed var(--border-secondary)', borderRadius: 10, padding: 14, minWidth: 140 }}>
            <div style={{ fontSize: 9, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', textAlign: 'center', marginBottom: 10 }}>
              Shard {shardNum}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8, alignItems: 'center' }}>
              {replicas.sort((a, b) => Number(a.replica_num) - Number(b.replica_num)).map((n) => {
                const hasErrors = Number(n.errors_count) > 0;
                const isLocal = !!Number(n.is_local);
                const borderColor = hasErrors ? '#ef4444' : isLocal ? '#58a6ff' : '#22c55e';
                const shortName = shortenHostname(n.host_name);
                const nodeMetrics = metricsMap.get(n.host_name) ?? metricsMap.get(shortName);
                return (
                  <Fallback2DNode
                    key={`${n.host_name}-${n.replica_num}`}
                    label={shortName}
                    sublabel={`replica ${n.replica_num}`}
                    borderColor={borderColor}
                    isLocal={isLocal}
                    metrics={nodeMetrics}
                    errorCount={Number(n.errors_count)}
                  />
                );
              })}
            </div>
          </div>
        ))}
      </div>
      {keeperNodes.length > 0 && (
        <div style={{ display: 'flex', gap: 20, justifyContent: 'center', flexWrap: 'wrap' }}>
          <div style={{ border: '1px dashed #7c5cbf', borderRadius: 10, padding: 14, minWidth: 140 }}>
            <div style={{ fontSize: 9, fontWeight: 600, color: '#a78bfa', textTransform: 'uppercase', letterSpacing: '1px', textAlign: 'center', marginBottom: 10 }}>
              Keeper
            </div>
            <div style={{ display: 'flex', flexDirection: 'row', gap: 12, justifyContent: 'center', flexWrap: 'wrap' }}>
              {keeperNodes.map((k) => {
                const expired = !!Number(k.is_expired);
                return (
                  <Fallback2DNode
                    key={`keeper-${k.index}`}
                    label={`${shortenHostname(k.host)}:${k.port}`}
                    sublabel={expired ? 'expired' : 'keeper'}
                    borderColor={expired ? '#ef4444' : '#a78bfa'}
                    keeperInfo={k}
                  />
                );
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// ── Main export ──

export const ClusterTopology: React.FC<ClusterTopologyProps> = (props) => {
  const theme = useThemeDetection();
  const { clusterName, nodes, keeperNodes, hostMetrics } = props;
  const { preferredViewMode: viewMode } = useUserPreferenceStore();

  // Camera position based on layout size
  const camDistance = Math.max(4, nodes.length * 0.8 + keeperNodes.length * 0.5);

  return (
    <div className="card">
      <div className="card-header flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span>Cluster Topology:</span>
          <span style={{ fontFamily: 'ui-monospace, monospace', color: 'var(--text-muted)' }}>{clusterName}</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          {/* Legend */}
          <div style={{ display: 'flex', gap: 12, fontSize: 10, color: 'var(--text-muted)' }}>
            {[
              { color: COLORS.local.main, label: 'This node' },
              { color: COLORS.healthy.main, label: 'Healthy' },
              { color: COLORS.error.main, label: 'Errors' },
              ...(keeperNodes.length > 0 ? [{ color: COLORS.keeper.main, label: 'Keeper' }] : []),
            ].map(({ color, label }) => (
              <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <span style={{ width: 7, height: 7, borderRadius: '50%', background: color }} />
                {label}
              </div>
            ))}
          </div>
        </div>
      </div>
      <div style={{ height: 400, position: 'relative' }}>
        {viewMode === '2d' ? (
          <Fallback2D {...props} />
        ) : (
          <ErrorBoundary3D fallback2D={<Fallback2D {...props} />}>
            <Canvas
              shadows
              dpr={[1, 2]}
              gl={{ antialias: true, powerPreference: 'high-performance' }}
              camera={{ position: [0, camDistance * 0.7, camDistance], fov: 45 }}
            >
              {/* Lighting */}
              <ambientLight intensity={theme === 'light' ? 0.6 : 0.3} />
              <directionalLight position={[5, 8, 5]} intensity={theme === 'light' ? 0.8 : 0.5} castShadow />
              <directionalLight position={[-3, 4, -3]} intensity={0.2} />

              {/* Controls */}
              <OrbitControls
                enablePan
                enableZoom
                enableRotate
                maxPolarAngle={Math.PI / 2.1}
                minDistance={2}
                maxDistance={20}
                target={[0, 0.5, 0]}
              />

              {/* Scene */}
              <ClusterScene
                clusterName={clusterName}
                nodes={nodes}
                keeperNodes={keeperNodes}
                hostMetrics={hostMetrics}
                theme={theme}
              />
            </Canvas>
          </ErrorBoundary3D>
        )}
      </div>
    </div>
  );
};
