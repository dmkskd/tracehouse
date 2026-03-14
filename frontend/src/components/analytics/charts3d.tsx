/**
 * Shared 3D chart components (Three.js via @react-three/fiber).
 *
 * Extracted from QueryExplorer so they can be reused in DashboardViewer.
 */

import React, { useState, useEffect, useMemo, useRef } from 'react';
import { Canvas, useThree } from '@react-three/fiber';
import { OrbitControls, RoundedBox, Html } from '@react-three/drei';
import * as THREE from 'three';
import type { ChartType } from './metaLanguage';
import type { ChartDataPoint, DrillDownEvent, GroupedChartData } from './charts';
import { GROUP_COLORS } from './charts';

// ─── Theme helpers ───

/** Detect current theme and re-render on change */
function use3DTheme(): 'dark' | 'light' {
  const [theme, setTheme] = useState<'dark' | 'light'>(() =>
    typeof document !== 'undefined' && document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark'
  );
  useEffect(() => {
    const obs = new MutationObserver(() => {
      setTheme(document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark');
    });
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    return () => obs.disconnect();
  }, []);
  return theme;
}

/** Lighten a hex color by mixing with white. amt 0-1 */
function lightenHex(hex: string, amt: number): string {
  const h = hex.replace('#', '');
  const r = parseInt(h.substring(0, 2), 16);
  const g = parseInt(h.substring(2, 4), 16);
  const b = parseInt(h.substring(4, 6), 16);
  const lr = Math.round(r + (255 - r) * amt);
  const lg = Math.round(g + (255 - g) * amt);
  const lb = Math.round(b + (255 - b) * amt);
  return `#${lr.toString(16).padStart(2,'0')}${lg.toString(16).padStart(2,'0')}${lb.toString(16).padStart(2,'0')}`;
}

/** Pastel-ify chart colors for light mode 3D */
function themed3DColor(color: string, theme: 'dark' | 'light'): string {
  return theme === 'light' ? lightenHex(color, 0.35) : color;
}

const LIGHT_3D = {
  bg: 'linear-gradient(180deg, #f8fafc 0%, #e2e8f0 100%)',
  ground: '#e2e8f0',
  groundOpacity: 0.6,
  ambient: 0.8,
  directional: 1.0,
  point: 0.4,
  metalness: 0.05,
  roughness: 0.4,
  labelColor: '#334155',
  gridPrimary: '#cbd5e1',
  gridSecondary: '#e2e8f0',
};

const DARK_3D = {
  bg: 'linear-gradient(180deg, #0f172a 0%, #020617 100%)',
  ground: '#1a1a2e',
  groundOpacity: 0.5,
  ambient: 0.4,
  directional: 0.8,
  point: 0.3,
  metalness: 0.1,
  roughness: 0.6,
  labelColor: 'var(--text-muted)',
  gridPrimary: '#1e293b',
  gridSecondary: '#0f172a',
};

// ─── Camera & controls ───

function useAutoFitCamera(sceneWidth: number, sceneHeight: number, elevationRatio = 0.4, lookAtY?: number) {
  const { camera, size } = useThree();
  useEffect(() => {
    if (!(camera instanceof THREE.PerspectiveCamera)) return;
    const fovRad = (camera.fov * Math.PI) / 180;
    const aspect = size.width / size.height;
    const distForHeight = (sceneHeight / 2) / Math.tan(fovRad / 2);
    const distForWidth = (sceneWidth / 2) / (Math.tan(fovRad / 2) * aspect);
    const dist = Math.max(distForHeight, distForWidth) * 1.15;
    const y = dist * elevationRatio;
    const z = dist * Math.sqrt(1 - elevationRatio * elevationRatio);
    const targetY = lookAtY ?? sceneHeight * 0.25;
    camera.position.set(0, y, z);
    camera.lookAt(0, targetY, 0);
    camera.updateProjectionMatrix();
  }, [camera, size, sceneWidth, sceneHeight, elevationRatio, lookAtY]);
}

function AutoRotate({ speed = 0.15 }: { speed?: number }) {
  const controlsRef = useRef<any>(null);
  return (
    <OrbitControls
      ref={controlsRef}
      enablePan
      enableZoom
      enableRotate
      autoRotate
      autoRotateSpeed={speed}
      maxPolarAngle={Math.PI / 2.1}
    />
  );
}

// ─── 3D Scene components ───

function Bar3DScene({ data, onDrillDown }: { data: ChartDataPoint[]; title?: string; onDrillDown?: (e: DrillDownEvent) => void }) {
  const theme = use3DTheme();
  const t = theme === 'light' ? LIGHT_3D : DARK_3D;
  const maxVal = Math.max(...data.map(d => d.value), 1);
  const spacing = 1.8;
  const barWidth = 0.8;
  const offsetX = -(data.length - 1) * spacing / 2;

  const sceneWidth = data.length * spacing + 2;
  const sceneHeight = 5;
  useAutoFitCamera(sceneWidth, sceneHeight, 0.65);

  return (
    <>
      <ambientLight intensity={t.ambient} />
      <directionalLight position={[5, 8, 5]} intensity={t.directional} />
      <pointLight position={[-5, 5, -5]} intensity={t.point} />
      {data.map((d, i) => {
        const h = Math.max(0.05, (d.value / maxVal) * 4);
        return (
          <group key={i} position={[offsetX + i * spacing, h / 2, 0]}>
            <RoundedBox args={[barWidth, h, barWidth]} radius={0.05} smoothness={4}
              onClick={onDrillDown ? (e: { stopPropagation: () => void }) => { e.stopPropagation(); onDrillDown({ label: d.label, value: d.value }); } : undefined}
              onPointerOver={onDrillDown ? () => { document.body.style.cursor = 'pointer'; } : undefined}
              onPointerOut={onDrillDown ? () => { document.body.style.cursor = 'auto'; } : undefined}
            >
              <meshStandardMaterial color={themed3DColor(d.color, theme)} metalness={t.metalness} roughness={t.roughness} />
            </RoundedBox>
            <Html position={[0, h / 2 + 0.3, 0]} center style={{ pointerEvents: 'none' }}>
              <div style={{ fontSize: 9, color: t.labelColor, whiteSpace: 'nowrap', fontFamily: "'Share Tech Mono',monospace" }}>
                {d.value.toLocaleString()}
              </div>
            </Html>
            <Html position={[0, -h / 2 - 0.4, 0]} center style={{ pointerEvents: 'none' }}>
              <div style={{ fontSize: 8, color: t.labelColor, whiteSpace: 'nowrap', maxWidth: 60, overflow: 'hidden', textOverflow: 'ellipsis', textAlign: 'center' }}>
                {d.label.length > 10 ? d.label.slice(0, 10) + '…' : d.label}
              </div>
            </Html>
          </group>
        );
      })}
      <gridHelper args={[data.length * spacing + 4, 20, t.gridPrimary, t.gridSecondary]} position={[0, -0.01, 0]} />
      <AutoRotate speed={0.15} />
    </>
  );
}

function Line3DScene({ data }: { data: ChartDataPoint[]; title?: string }) {
  const theme = use3DTheme();
  const t = theme === 'light' ? LIGHT_3D : DARK_3D;
  const maxVal = Math.max(...data.map(d => d.value), 1);
  const minVal = Math.min(...data.map(d => d.value));
  const range = maxVal - minVal || 1;
  const totalWidth = 10;
  const spacing = data.length > 1 ? totalWidth / (data.length - 1) : 1;
  const startX = -totalWidth / 2;
  const maxHeight = 4;
  const baseY = 0.5; // lift line above floor
  const lineColor = theme === 'light' ? '#818cf8' : '#6366f1';
  const axisColor = theme === 'light' ? '#94a3b8' : '#475569';

  const sceneWidth = totalWidth + 2;
  useAutoFitCamera(sceneWidth, maxHeight + 2, 0.4);

  const points = useMemo(() =>
    data.map((d, i) => new THREE.Vector3(startX + i * spacing, ((d.value - minVal) / range) * maxHeight + baseY, 0)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [data, startX, spacing, minVal, range, maxHeight]
  );

  // Piecewise-linear tube for a 3D line with presence
  const curve = useMemo(() => {
    const path = new THREE.CurvePath<THREE.Vector3>();
    for (let i = 0; i < points.length - 1; i++) {
      path.add(new THREE.LineCurve3(points[i], points[i + 1]));
    }
    return path;
  }, [points]);

  // Adaptive sphere size: smaller when many points
  const sphereRadius = data.length <= 10 ? 0.15 : data.length <= 20 ? 0.10 : 0.06;

  // Key indices that get value labels: first, last, min, max + sampled
  const labelIndices = useMemo(() => {
    const set = new Set<number>();
    set.add(0);
    set.add(data.length - 1);
    // Global min/max
    let minIdx = 0, maxIdx = 0;
    for (let i = 1; i < data.length; i++) {
      if (data[i].value < data[minIdx].value) minIdx = i;
      if (data[i].value > data[maxIdx].value) maxIdx = i;
    }
    set.add(minIdx);
    set.add(maxIdx);
    // Sample evenly for the rest
    const targetLabels = Math.min(12, data.length);
    const step = Math.max(1, Math.floor(data.length / targetLabels));
    for (let i = 0; i < data.length; i += step) set.add(i);
    return set;
  }, [data]);

  // X-axis label indices (show all if <=15, otherwise every nth)
  const xLabelIndices = useMemo(() => {
    if (data.length <= 15) return data.map((_, i) => i);
    const step = Math.ceil(data.length / 12);
    const indices: number[] = [];
    for (let i = 0; i < data.length; i += step) indices.push(i);
    return indices;
  }, [data.length]);

  return (
    <>
      <ambientLight intensity={t.ambient} />
      <directionalLight position={[5, 8, 5]} intensity={t.directional} />
      <directionalLight position={[-5, 5, 10]} intensity={t.point} color="#6366f1" />

      {/* Grid floor */}
      <gridHelper args={[20, 20, t.gridPrimary, t.gridSecondary]} position={[0, -0.01, 0]} />

      {/* Y-axis line */}
      <line>
        <bufferGeometry>
          <bufferAttribute
            attach="attributes-position"
            count={2}
            array={new Float32Array([startX - 0.3, baseY, 0, startX - 0.3, maxHeight + baseY, 0])}
            itemSize={3}
          />
        </bufferGeometry>
        <lineBasicMaterial color={axisColor} />
      </line>

      {/* X-axis line */}
      <line>
        <bufferGeometry>
          <bufferAttribute
            attach="attributes-position"
            count={2}
            array={new Float32Array([startX - 0.3, baseY, 0, startX + totalWidth + 0.3, baseY, 0])}
            itemSize={3}
          />
        </bufferGeometry>
        <lineBasicMaterial color={axisColor} />
      </line>

      {/* Y-axis reference lines and labels */}
      {[0, 0.25, 0.5, 0.75, 1].map((pct, i) => {
        const y = pct * maxHeight + baseY;
        const value = Math.round(minVal + pct * range);
        return (
          <group key={`yref-${i}`}>
            <line>
              <bufferGeometry>
                <bufferAttribute
                  attach="attributes-position"
                  count={2}
                  array={new Float32Array([startX - 0.3, y, 0, startX + totalWidth + 0.3, y, 0])}
                  itemSize={3}
                />
              </bufferGeometry>
              <lineBasicMaterial color={t.gridPrimary} transparent opacity={0.5} />
            </line>
            <Html position={[startX - 0.8, y, 0]} center style={{ pointerEvents: 'none' }}>
              <div style={{ fontSize: 9, color: t.labelColor, whiteSpace: 'nowrap', fontFamily: "'Share Tech Mono',monospace" }}>
                {value.toLocaleString()}
              </div>
            </Html>
          </group>
        );
      })}

      {/* Tube line */}
      <mesh>
        <tubeGeometry args={[curve, points.length * 4, 0.03, 6, false]} />
        <meshStandardMaterial color={lineColor} emissive={lineColor} emissiveIntensity={0.4} />
      </mesh>

      {/* Data point spheres + selective labels */}
      {data.map((d, i) => {
        const y = ((d.value - minVal) / range) * maxHeight + baseY;
        const showLabel = labelIndices.has(i);
        return (
          <group key={i} position={[startX + i * spacing, y, 0]}>
            <mesh>
              <sphereGeometry args={[sphereRadius, 12, 12]} />
              <meshStandardMaterial color={d.color || lineColor} emissive={d.color || lineColor} emissiveIntensity={theme === 'light' ? 0.2 : 0.4} metalness={0.5} roughness={0.3} />
            </mesh>
            {showLabel && (
              <Html position={[0, sphereRadius + 0.25, 0]} center style={{ pointerEvents: 'none' }}>
                <div style={{ fontSize: 9, color: t.labelColor, whiteSpace: 'nowrap', fontFamily: "'Share Tech Mono',monospace", fontWeight: 600 }}>
                  {d.value.toLocaleString()}
                </div>
              </Html>
            )}
          </group>
        );
      })}

      {/* X-axis labels */}
      {xLabelIndices.map(i => {
        const d = data[i];
        if (!d) return null;
        return (
          <Html key={`xlabel-${i}`} position={[startX + i * spacing, 0.1, 0.3]} center style={{ pointerEvents: 'none' }}>
            <div style={{ fontSize: 8, color: t.labelColor, whiteSpace: 'nowrap', fontFamily: "'Share Tech Mono',monospace", maxWidth: 50, overflow: 'hidden', textOverflow: 'ellipsis', textAlign: 'center' }}>
              {d.label}
            </div>
          </Html>
        );
      })}

      <AutoRotate speed={0.12} />
    </>
  );
}

// ─── Multi-series ridge / waterfall line chart ───

function GroupedLine3DScene({ data }: { data: GroupedChartData[] }) {
  const theme = use3DTheme();
  const t = theme === 'light' ? LIGHT_3D : DARK_3D;

  // Extract series names and build per-series point arrays
  const { seriesData, globalMin, globalMax } = useMemo(() => {
    if (!data.length) return { seriesNames: [] as string[], seriesData: [] as { name: string; color: string; points: { label: string; value: number }[] }[], globalMin: 0, globalMax: 1 };
    const names = data[0].groups.map(g => g.name);
    const sd = names.map((name, gi) => ({
      name,
      color: data[0].groups[gi]?.color || '#6366f1',
      points: data.map(d => ({
        label: d.label,
        value: d.groups.find(g => g.name === name)?.value ?? 0,
      })),
    }));
    let min = Infinity, max = -Infinity;
    for (const s of sd) for (const p of s.points) { if (p.value < min) min = p.value; if (p.value > max) max = p.value; }
    if (min === max) { min = 0; max = max || 1; }
    return { seriesNames: names, seriesData: sd, globalMin: min, globalMax: max };
  }, [data]);

  const totalWidth = 10;
  const maxHeight = 4;
  const baseY = 0.3;
  const laneSpacing = 2.2; // Z gap between series
  const totalDepth = (seriesData.length - 1) * laneSpacing;
  const range = globalMax - globalMin || 1;

  useAutoFitCamera(Math.max(totalWidth, totalDepth) + 4, maxHeight + 3, 0.5);

  return (
    <>
      <ambientLight intensity={t.ambient} />
      <directionalLight position={[5, 8, 5]} intensity={t.directional} />
      <directionalLight position={[-5, 5, 10]} intensity={t.point} color="#6366f1" />

      {/* Grid floor */}
      <gridHelper args={[Math.max(totalWidth, totalDepth) + 6, 20, t.gridPrimary, t.gridSecondary]} position={[0, -0.01, -totalDepth / 2]} />

      {/* Y-axis reference lines */}
      {[0, 0.25, 0.5, 0.75, 1].map((pct, i) => {
        const y = pct * maxHeight + baseY;
        const value = Math.round(globalMin + pct * range);
        const startX = -totalWidth / 2;
        return (
          <group key={`yref-${i}`}>
            <line>
              <bufferGeometry>
                <bufferAttribute attach="attributes-position" count={2}
                  array={new Float32Array([startX - 0.3, y, 0, startX - 0.3, y, -totalDepth])}
                  itemSize={3} />
              </bufferGeometry>
              <lineBasicMaterial color={t.gridPrimary} transparent opacity={0.3} />
            </line>
            <Html position={[startX - 0.9, y, 0.2]} center style={{ pointerEvents: 'none' }}>
              <div style={{ fontSize: 9, color: t.labelColor, whiteSpace: 'nowrap', fontFamily: "'Share Tech Mono',monospace" }}>
                {value.toLocaleString()}
              </div>
            </Html>
          </group>
        );
      })}

      {/* Each series as a ridge line */}
      {seriesData.map((series, si) => {
        const z = -si * laneSpacing;
        const startX = -totalWidth / 2;
        const spacing = data.length > 1 ? totalWidth / (data.length - 1) : 1;

        const points = series.points.map((p, i) =>
          new THREE.Vector3(startX + i * spacing, ((p.value - globalMin) / range) * maxHeight + baseY, z)
        );

        // Build tube curve
        const path = new THREE.CurvePath<THREE.Vector3>();
        for (let i = 0; i < points.length - 1; i++) {
          path.add(new THREE.LineCurve3(points[i], points[i + 1]));
        }

        // Build filled "ribbon" shape — a semi-transparent curtain from line down to floor
        const ribbonPositions: number[] = [];
        const ribbonIndices: number[] = [];
        for (let i = 0; i < points.length; i++) {
          const p = points[i];
          // top vertex
          ribbonPositions.push(p.x, p.y, p.z);
          // bottom vertex (floor)
          ribbonPositions.push(p.x, baseY * 0.5, p.z);
        }
        for (let i = 0; i < points.length - 1; i++) {
          const topA = i * 2, botA = i * 2 + 1, topB = (i + 1) * 2, botB = (i + 1) * 2 + 1;
          ribbonIndices.push(topA, botA, topB);
          ribbonIndices.push(botA, botB, topB);
        }

        const themedColor = themed3DColor(series.color, theme);

        return (
          <group key={series.name}>
            {/* Filled ribbon/curtain */}
            <mesh>
              <bufferGeometry>
                <bufferAttribute attach="attributes-position" count={ribbonPositions.length / 3}
                  array={new Float32Array(ribbonPositions)} itemSize={3} />
                <bufferAttribute attach="index" count={ribbonIndices.length}
                  array={new Uint16Array(ribbonIndices)} itemSize={1} />
              </bufferGeometry>
              <meshStandardMaterial
                color={themedColor} transparent opacity={0.18}
                side={THREE.DoubleSide} depthWrite={false}
              />
            </mesh>

            {/* Tube line */}
            {points.length >= 2 && (
              <mesh>
                <tubeGeometry args={[path, points.length * 4, 0.04, 6, false]} />
                <meshStandardMaterial color={themedColor} emissive={themedColor} emissiveIntensity={0.5}
                  metalness={t.metalness} roughness={t.roughness} />
              </mesh>
            )}

            {/* Series label at end of line */}
            <Html position={[totalWidth / 2 + 0.6, points[points.length - 1]?.y ?? baseY, z]} center style={{ pointerEvents: 'none' }}>
              <div style={{
                fontSize: 10, fontWeight: 600, color: themedColor, whiteSpace: 'nowrap',
                fontFamily: "'Share Tech Mono',monospace",
                textShadow: theme === 'dark' ? `0 0 6px ${themedColor}` : 'none',
              }}>
                {series.name.length > 16 ? series.name.slice(0, 16) + '…' : series.name}
              </div>
            </Html>
          </group>
        );
      })}

      {/* X-axis labels along front series */}
      {(() => {
        const startX = -totalWidth / 2;
        const spacing = data.length > 1 ? totalWidth / (data.length - 1) : 1;
        const step = Math.max(1, Math.ceil(data.length / 12));
        const indices: number[] = [];
        for (let i = 0; i < data.length; i++) { if (i % step === 0 || i === data.length - 1) indices.push(i); }
        return indices.map(i => (
          <Html key={`xlabel-${i}`} position={[startX + i * spacing, 0.05, 0.5]} center style={{ pointerEvents: 'none' }}>
            <div style={{ fontSize: 8, color: t.labelColor, whiteSpace: 'nowrap', fontFamily: "'Share Tech Mono',monospace",
              maxWidth: 55, overflow: 'hidden', textOverflow: 'ellipsis', textAlign: 'center' }}>
              {data[i].label}
            </div>
          </Html>
        ));
      })()}

      <AutoRotate speed={0.15} />
    </>
  );
}

function PieSlice3D({ s, i, sliceHeight, radius, innerRadius, theme, t, onDrillDown }: {
  s: { label: string; value: number; color: string; startAngle: number; angle: number; pct: number; midAngle: number };
  i: number; sliceHeight: number; radius: number; innerRadius: number; theme: 'dark' | 'light'; t: typeof LIGHT_3D;
  onDrillDown?: (e: DrillDownEvent) => void;
}) {
  const [hovered, setHovered] = useState(false);
  const meshRef = useRef<THREE.Mesh>(null);

  if (s.angle < 0.01) return null;

  const shape = new THREE.Shape();
  const segments = Math.max(8, Math.round(s.angle * 20));
  shape.moveTo(Math.cos(s.startAngle) * innerRadius, Math.sin(s.startAngle) * innerRadius);
  shape.lineTo(Math.cos(s.startAngle) * radius, Math.sin(s.startAngle) * radius);
  for (let j = 1; j <= segments; j++) {
    const a = s.startAngle + (j / segments) * s.angle;
    shape.lineTo(Math.cos(a) * radius, Math.sin(a) * radius);
  }
  shape.lineTo(Math.cos(s.startAngle + s.angle) * innerRadius, Math.sin(s.startAngle + s.angle) * innerRadius);
  for (let j = segments - 1; j >= 0; j--) {
    const a = s.startAngle + (j / segments) * s.angle;
    shape.lineTo(Math.cos(a) * innerRadius, Math.sin(a) * innerRadius);
  }
  shape.closePath();

  const labelR = (radius + innerRadius) / 2;
  const lx = Math.cos(s.midAngle) * labelR;
  const lz = Math.sin(s.midAngle) * labelR;
  const liftY = hovered ? 0.25 : 0;

  return (
    <group>
      <mesh
        ref={meshRef}
        rotation={[-Math.PI / 2, 0, 0]}
        position={[0, i * 0.01 + liftY, 0]}
        onPointerOver={(e) => { e.stopPropagation(); setHovered(true); document.body.style.cursor = 'pointer'; }}
        onPointerOut={() => { setHovered(false); document.body.style.cursor = 'auto'; }}
        onClick={onDrillDown ? (e: { stopPropagation: () => void }) => { e.stopPropagation(); onDrillDown({ label: s.label, value: s.value }); } : undefined}
      >
        <extrudeGeometry args={[shape, { depth: sliceHeight, bevelEnabled: false }]} />
        <meshStandardMaterial
          color={themed3DColor(s.color, theme)}
          metalness={t.metalness}
          roughness={hovered ? t.roughness * 0.5 : t.roughness}
          emissive={hovered ? themed3DColor(s.color, theme) : '#000000'}
          emissiveIntensity={hovered ? 0.3 : 0}
        />
      </mesh>
      <Html position={[lx, sliceHeight + 0.15 + liftY, -lz]} center style={{ pointerEvents: 'none' }}>
        <div style={{
          position: 'relative', display: 'flex', flexDirection: 'column', alignItems: 'center',
          transition: 'all 0.2s ease',
          transform: hovered ? 'scale(1.15)' : 'scale(1)',
        }}>
          {(s.pct >= 4 || hovered) && (
            <div style={{
              display: 'flex', flexDirection: 'column', alignItems: 'center',
              background: 'rgba(0,0,0,0.45)',
              backdropFilter: 'blur(4px)',
              borderRadius: 6,
              padding: '3px 8px',
              border: hovered ? `1px solid rgba(255,255,255,0.3)` : '1px solid rgba(255,255,255,0.1)',
            }}>
              <div style={{
                fontSize: hovered ? 12 : (s.pct >= 12 ? 11 : 9),
                fontWeight: 600, color: '#fff',
                fontFamily: "'Inter', -apple-system, sans-serif",
                textShadow: hovered ? `0 0 8px ${s.color}, 0 0 16px ${s.color}` : 'none',
                whiteSpace: 'nowrap', lineHeight: 1.2,
              }}>
                {s.label}
              </div>
              <div style={{
                fontSize: hovered ? 14 : (s.pct >= 12 ? 13 : 11),
                fontWeight: 700, color: '#fff',
                fontFamily: "'Share Tech Mono', monospace",
                textShadow: hovered ? `0 0 8px ${s.color}, 0 0 16px ${s.color}` : 'none',
                whiteSpace: 'nowrap',
              }}>
                {s.value.toLocaleString()}
              </div>
              {hovered && (
                <div style={{ fontSize: 10, fontWeight: 500, color: 'rgba(255,255,255,0.85)', fontFamily: "'Share Tech Mono', monospace", whiteSpace: 'nowrap' }}>
                  {s.pct.toFixed(1)}%
                </div>
              )}
            </div>
          )}
        </div>
      </Html>
    </group>
  );
}

function Pie3DScene({ data, onDrillDown }: { data: ChartDataPoint[]; title?: string; onDrillDown?: (e: DrillDownEvent) => void }) {
  const theme = use3DTheme();
  const t = theme === 'light' ? LIGHT_3D : DARK_3D;
  const total = data.reduce((s, d) => s + d.value, 0);
  const radius = 2;
  const innerRadius = 0.7;
  const sliceHeight = 0.5;

  useAutoFitCamera(radius * 2 + 1, radius * 2 + 1, 0.7);

  const slices = useMemo(() => {
    let startAngle = 0;
    return data.map(d => {
      const angle = total > 0 ? (d.value / total) * Math.PI * 2 : 0;
      const pct = total > 0 ? (d.value / total) * 100 : 0;
      const sa = startAngle;
      const midAngle = sa + angle / 2;
      startAngle += angle;
      return { ...d, startAngle: sa, angle, pct, midAngle };
    });
  }, [data, total]);

  return (
    <>
      <ambientLight intensity={t.ambient} />
      <directionalLight position={[5, 8, 5]} intensity={t.directional} />
      {slices.map((s, i) => (
        <PieSlice3D key={i} s={s} i={i} sliceHeight={sliceHeight} radius={radius} innerRadius={innerRadius} theme={theme} t={t} onDrillDown={onDrillDown} />
      ))}
      <Html position={[0, sliceHeight / 2 + 0.1, 0]} center style={{ pointerEvents: 'none' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 20, fontWeight: 700, color: t.labelColor, fontFamily: "'Share Tech Mono', monospace", lineHeight: 1.2 }}>
            {total.toLocaleString()}
          </div>
          <div style={{ fontSize: 10, color: '#64748b', fontFamily: "'Inter', -apple-system, sans-serif", textTransform: 'uppercase', letterSpacing: '1px' }}>
            total
          </div>
        </div>
      </Html>
      <AutoRotate speed={0.2} />
    </>
  );
}

// ─── 3D Grouped / Stacked Bar Scenes ───

function flattenGrouped3D(data: GroupedChartData[]): { labels: string[]; groupNames: string[]; colorMap: Record<string, string>; matrix: number[][] } {
  const labels = data.map(d => d.label);
  const groupNames = [...new Set(data.flatMap(d => d.groups.map(g => g.name)))];
  const colorMap: Record<string, string> = {};
  data.forEach(d => d.groups.forEach(g => { if (!colorMap[g.name]) colorMap[g.name] = g.color; }));
  groupNames.forEach((name, i) => { if (!colorMap[name]) colorMap[name] = GROUP_COLORS[i % GROUP_COLORS.length]; });
  const matrix = data.map(d => groupNames.map(name => d.groups.find(g => g.name === name)?.value ?? 0));
  return { labels, groupNames, colorMap, matrix };
}

function GroupedBar3DScene({ data, orientation = 'horizontal', onDrillDown }: { data: GroupedChartData[]; orientation?: 'horizontal' | 'vertical'; onDrillDown?: (e: DrillDownEvent) => void }) {
  const theme = use3DTheme();
  const t = theme === 'light' ? LIGHT_3D : DARK_3D;
  const { labels, groupNames, colorMap, matrix } = useMemo(() => flattenGrouped3D(data), [data]);
  const globalMax = Math.max(...matrix.flat(), 1);
  const horiz = orientation === 'horizontal';
  const groupSpacing = 0.5;
  const barWidth = 0.6;
  const clusterWidth = groupNames.length * (barWidth + 0.1) - 0.1;
  const labelSpacing = clusterWidth + groupSpacing;
  const maxBarLen = 4;
  const sceneSpan = labels.length * labelSpacing + 2;

  // For horizontal: bars grow along +X, labels stacked along Y (centered at origin)
  // For vertical: clusters along X, bars grow along +Y
  const offsetLabel = -(labels.length - 1) * labelSpacing / 2;

  useAutoFitCamera(horiz ? maxBarLen + 4 : sceneSpan, horiz ? sceneSpan : 5, horiz ? 0.15 : 0.6, horiz ? 0 : undefined);

  return (
    <>
      <ambientLight intensity={t.ambient} />
      <directionalLight position={[5, 8, 5]} intensity={t.directional} />
      <pointLight position={[-5, 5, -5]} intensity={t.point} />
      {labels.map((label, li) => (
        <group key={li}>
          {groupNames.map((gName, gi) => {
            const val = matrix[li][gi];
            const len = Math.max(0.05, (val / globalMax) * maxBarLen);
            if (horiz) {
              // Y position: spread labels top-to-bottom centered at 0
              const yCenter = offsetLabel + li * labelSpacing;
              const yOff = -yCenter + gi * (barWidth + 0.1) - clusterWidth / 2 + barWidth / 2;
              return (
                <group key={gi} position={[len / 2 - maxBarLen / 2, yOff, 0]}>
                  <RoundedBox args={[len, barWidth, barWidth]} radius={0.05} smoothness={4}
                    onClick={onDrillDown ? (e: { stopPropagation: () => void }) => { e.stopPropagation(); onDrillDown({ label, value: val }); } : undefined}
                    onPointerOver={onDrillDown ? () => { document.body.style.cursor = 'pointer'; } : undefined}
                    onPointerOut={onDrillDown ? () => { document.body.style.cursor = 'auto'; } : undefined}>
                    <meshStandardMaterial color={themed3DColor(colorMap[gName], theme)} metalness={t.metalness} roughness={t.roughness} />
                  </RoundedBox>
                </group>
              );
            }
            const x = offsetLabel + li * labelSpacing + gi * (barWidth + 0.1) - clusterWidth / 2 + barWidth / 2;
            return (
              <group key={gi} position={[x, len / 2, 0]}>
                <RoundedBox args={[barWidth, len, barWidth]} radius={0.05} smoothness={4}
                  onClick={onDrillDown ? (e: { stopPropagation: () => void }) => { e.stopPropagation(); onDrillDown({ label, value: val }); } : undefined}
                  onPointerOver={onDrillDown ? () => { document.body.style.cursor = 'pointer'; } : undefined}
                  onPointerOut={onDrillDown ? () => { document.body.style.cursor = 'auto'; } : undefined}>
                  <meshStandardMaterial color={themed3DColor(colorMap[gName], theme)} metalness={t.metalness} roughness={t.roughness} />
                </RoundedBox>
              </group>
            );
          })}
          {horiz ? (
            <Html position={[-maxBarLen / 2 - 0.4, -(offsetLabel + li * labelSpacing), 0]} center style={{ pointerEvents: 'none' }}>
              <div style={{ fontSize: 8, color: t.labelColor, whiteSpace: 'nowrap', maxWidth: 80, overflow: 'hidden', textOverflow: 'ellipsis', textAlign: 'right', fontFamily: "'Share Tech Mono',monospace" }}>
                {label.length > 14 ? label.slice(0, 14) + '…' : label}
              </div>
            </Html>
          ) : (
            <Html position={[offsetLabel + li * labelSpacing, -0.4, 0]} center style={{ pointerEvents: 'none' }}>
              <div style={{ fontSize: 8, color: t.labelColor, whiteSpace: 'nowrap', maxWidth: 70, overflow: 'hidden', textOverflow: 'ellipsis', textAlign: 'center', fontFamily: "'Share Tech Mono',monospace" }}>
                {label.length > 12 ? label.slice(0, 12) + '…' : label}
              </div>
            </Html>
          )}
        </group>
      ))}
      <gridHelper args={[Math.max(sceneSpan, maxBarLen + 4) + 2, 20, t.gridPrimary, t.gridSecondary]} position={[0, -0.01, 0]} />
      <AutoRotate speed={0.15} />
    </>
  );
}

function StackedBar3DScene({ data, orientation = 'horizontal', onDrillDown }: { data: GroupedChartData[]; orientation?: 'horizontal' | 'vertical'; onDrillDown?: (e: DrillDownEvent) => void }) {
  const theme = use3DTheme();
  const t = theme === 'light' ? LIGHT_3D : DARK_3D;
  const { labels, groupNames, colorMap, matrix } = useMemo(() => flattenGrouped3D(data), [data]);
  const stackTotals = matrix.map(row => row.reduce((s, v) => s + v, 0));
  const globalMax = Math.max(...stackTotals, 1);
  const horiz = orientation === 'horizontal';
  const spacing = 1.8;
  const barWidth = 0.8;
  const maxBarLen = 4;
  const gap = 0.04;
  const offsetLabel = -(labels.length - 1) * spacing / 2;
  const sceneSpan = labels.length * spacing + 2;

  useAutoFitCamera(horiz ? maxBarLen + 4 : sceneSpan, horiz ? sceneSpan : 5, horiz ? 0.15 : 0.6, horiz ? 0 : undefined);

  return (
    <>
      <ambientLight intensity={t.ambient} />
      <directionalLight position={[5, 8, 5]} intensity={t.directional} />
      <pointLight position={[-5, 5, -5]} intensity={t.point} />
      {labels.map((label, li) => {
        let segOffset = 0;
        return (
          <group key={li}>
            {groupNames.map((gName, gi) => {
              const val = matrix[li][gi];
              const len = Math.max(0.02, (val / globalMax) * maxBarLen);
              const pos = segOffset + len / 2;
              segOffset += len + gap;
              if (horiz) {
                const y = -(offsetLabel + li * spacing);
                return (
                  <group key={gi} position={[pos - maxBarLen / 2, y, 0]}>
                    <RoundedBox args={[len, barWidth, barWidth]} radius={0.03} smoothness={4}
                      onClick={onDrillDown ? (e: { stopPropagation: () => void }) => { e.stopPropagation(); onDrillDown({ label, value: val }); } : undefined}
                      onPointerOver={onDrillDown ? () => { document.body.style.cursor = 'pointer'; } : undefined}
                      onPointerOut={onDrillDown ? () => { document.body.style.cursor = 'auto'; } : undefined}>
                      <meshStandardMaterial color={themed3DColor(colorMap[gName], theme)} metalness={t.metalness} roughness={t.roughness} />
                    </RoundedBox>
                  </group>
                );
              }
              return (
                <group key={gi} position={[offsetLabel + li * spacing, pos, 0]}>
                  <RoundedBox args={[barWidth, len, barWidth]} radius={0.03} smoothness={4}
                    onClick={onDrillDown ? (e: { stopPropagation: () => void }) => { e.stopPropagation(); onDrillDown({ label, value: val }); } : undefined}
                    onPointerOver={onDrillDown ? () => { document.body.style.cursor = 'pointer'; } : undefined}
                    onPointerOut={onDrillDown ? () => { document.body.style.cursor = 'auto'; } : undefined}>
                    <meshStandardMaterial color={themed3DColor(colorMap[gName], theme)} metalness={t.metalness} roughness={t.roughness} />
                  </RoundedBox>
                </group>
              );
            })}
            {horiz ? (
              <Html position={[-maxBarLen / 2 - 0.4, -(offsetLabel + li * spacing), 0]} center style={{ pointerEvents: 'none' }}>
                <div style={{ fontSize: 8, color: t.labelColor, whiteSpace: 'nowrap', maxWidth: 80, overflow: 'hidden', textOverflow: 'ellipsis', textAlign: 'right', fontFamily: "'Share Tech Mono',monospace" }}>
                  {label.length > 14 ? label.slice(0, 14) + '…' : label}
                </div>
              </Html>
            ) : (
              <Html position={[offsetLabel + li * spacing, -0.4, 0]} center style={{ pointerEvents: 'none' }}>
                <div style={{ fontSize: 8, color: t.labelColor, whiteSpace: 'nowrap', maxWidth: 70, overflow: 'hidden', textOverflow: 'ellipsis', textAlign: 'center', fontFamily: "'Share Tech Mono',monospace" }}>
                  {label.length > 12 ? label.slice(0, 12) + '…' : label}
                </div>
              </Html>
            )}
          </group>
        );
      })}
      <gridHelper args={[Math.max(sceneSpan, maxBarLen + 4) + 2, 20, t.gridPrimary, t.gridSecondary]} position={[0, -0.01, 0]} />
      <AutoRotate speed={0.15} />
    </>
  );
}

// ─── Exported Canvas wrapper ───

export const Chart3DCanvas: React.FC<{
  data: ChartDataPoint[];
  groupedData?: GroupedChartData[];
  type: ChartType;
  orientation?: 'horizontal' | 'vertical';
  title?: string;
  description?: string;
  isFullscreen?: boolean;
  onToggleFullscreen?: () => void;
  onDrillDown?: (e: DrillDownEvent) => void;
}> = ({ data, groupedData, type, orientation, title, description, isFullscreen, onToggleFullscreen, onDrillDown }) => {
  const theme = use3DTheme();
  const bg = theme === 'light' ? LIGHT_3D.bg : DARK_3D.bg;
  const labelColor = theme === 'light' ? LIGHT_3D.labelColor : DARK_3D.labelColor;
  const cameraPos: [number, number, number] = [0, 4, 12];

  return (
    <div style={{
      width: '100%', height: '100%',
      minHeight: isFullscreen ? '100vh' : 300,
      borderRadius: isFullscreen ? 0 : 8,
      overflow: 'hidden', background: bg, position: 'relative',
    }}>
      {(title || description) && (
        <div style={{
          position: 'absolute', top: isFullscreen ? 24 : 16, left: isFullscreen ? 28 : 20,
          zIndex: 10, pointerEvents: 'none', maxWidth: '50%',
        }}>
          {title && (
            <div style={{
              fontSize: isFullscreen ? 20 : 15, fontWeight: 700, color: labelColor,
              fontFamily: "'Inter', 'SF Pro Display', -apple-system, sans-serif",
              letterSpacing: '-0.01em', lineHeight: 1.3,
            }}>{title}</div>
          )}
          {description && (
            <div style={{
              fontSize: isFullscreen ? 13 : 11,
              color: theme === 'light' ? '#64748b' : 'rgba(148,163,184,0.7)',
              fontFamily: "'Inter', -apple-system, sans-serif", marginTop: 4, lineHeight: 1.4,
            }}>{description}</div>
          )}
        </div>
      )}

      {/* Legend — top right (hidden for line/area and grouped/stacked charts which have their own legend) */}
      {data.length > 0 && !(type === 'line' || type === 'area' || type === 'grouped_line' || type === 'grouped_bar' || type === 'stacked_bar') && (
        <div style={{
          position: 'absolute', top: isFullscreen ? 24 : 16, right: isFullscreen ? 28 : 20, zIndex: 10,
          background: theme === 'light' ? 'rgba(255,255,255,0.45)' : 'rgba(15,23,42,0.4)',
          backdropFilter: 'blur(12px)', borderRadius: 8, padding: isFullscreen ? '12px 18px' : '8px 12px',
          border: `1px solid ${theme === 'light' ? 'rgba(0,0,0,0.04)' : 'rgba(255,255,255,0.04)'}`,
          maxHeight: isFullscreen ? '70vh' : '80%', overflowY: 'auto',
        }}>
          {data.slice(0, isFullscreen ? 30 : 15).map((d, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: isFullscreen ? 10 : 8, padding: isFullscreen ? '3px 0' : '2px 0', fontSize: isFullscreen ? 13 : 11 }}>
              <div style={{ width: isFullscreen ? 12 : 10, height: isFullscreen ? 12 : 10, borderRadius: 2, background: d.color, flexShrink: 0 }} />
              <span style={{
                color: theme === 'light' ? '#475569' : '#94a3b8',
                fontFamily: "'Inter', -apple-system, sans-serif",
                whiteSpace: 'nowrap',
              }}>{d.label}</span>
              <span style={{
                color: '#64748b', fontFamily: "'Share Tech Mono', monospace",
                fontSize: isFullscreen ? 12 : 10, marginLeft: 'auto', paddingLeft: 12,
              }}>{d.value.toLocaleString()}</span>
            </div>
          ))}
          {data.length > (isFullscreen ? 30 : 15) && (
            <div style={{ fontSize: 10, color: theme === 'light' ? '#94a3b8' : '#475569', fontStyle: 'italic', marginTop: 4 }}>
              +{data.length - (isFullscreen ? 30 : 15)} more
            </div>
          )}
        </div>
      )}

      {/* Compact summary for line/area charts */}
      {data.length > 0 && (type === 'line' || type === 'area') && (
        <div style={{
          position: 'absolute', bottom: isFullscreen ? 24 : 12, left: isFullscreen ? 28 : 20, zIndex: 10,
          background: theme === 'light' ? 'rgba(255,255,255,0.85)' : 'rgba(15,23,42,0.8)',
          backdropFilter: 'blur(8px)', borderRadius: 6, padding: '6px 12px',
          border: `1px solid ${theme === 'light' ? 'rgba(0,0,0,0.06)' : 'rgba(255,255,255,0.06)'}`,
          display: 'flex', gap: 16, fontSize: 11,
          fontFamily: "'Share Tech Mono', monospace",
        }}>
          <span style={{ color: theme === 'light' ? '#475569' : '#94a3b8' }}>
            min <span style={{ color: theme === 'light' ? '#334155' : '#e2e8f0', fontWeight: 600 }}>{Math.min(...data.map(d => d.value)).toLocaleString()}</span>
          </span>
          <span style={{ color: theme === 'light' ? '#475569' : '#94a3b8' }}>
            max <span style={{ color: theme === 'light' ? '#334155' : '#e2e8f0', fontWeight: 600 }}>{Math.max(...data.map(d => d.value)).toLocaleString()}</span>
          </span>
          <span style={{ color: theme === 'light' ? '#475569' : '#94a3b8' }}>
            {data.length} pts
          </span>
        </div>
      )}

      {/* Fullscreen toggle — bottom right */}
      {onToggleFullscreen && (
        <button
          onClick={onToggleFullscreen}
          title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}
          style={{
            position: 'absolute', bottom: isFullscreen ? 24 : 12, right: isFullscreen ? 28 : 16, zIndex: 10,
            width: 32, height: 32, borderRadius: 6,
            border: `1px solid ${theme === 'light' ? 'rgba(0,0,0,0.08)' : 'rgba(255,255,255,0.08)'}`,
            background: theme === 'light' ? 'rgba(255,255,255,0.8)' : 'rgba(15,23,42,0.7)',
            backdropFilter: 'blur(8px)', cursor: 'pointer',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            color: labelColor, fontSize: 14, transition: 'all .15s',
          }}
        >
          {isFullscreen ? '⤓' : '⤢'}
        </button>
      )}

      {/* Legend for grouped/stacked charts — show series names + colors */}
      {(type === 'grouped_line' || type === 'grouped_bar' || type === 'stacked_bar') && groupedData && groupedData.length > 0 && (
        <div style={{
          position: 'absolute', top: isFullscreen ? 24 : 16, right: isFullscreen ? 28 : 20, zIndex: 10,
          background: theme === 'light' ? 'rgba(255,255,255,0.45)' : 'rgba(15,23,42,0.4)',
          backdropFilter: 'blur(12px)', borderRadius: 8, padding: isFullscreen ? '12px 18px' : '8px 12px',
          border: `1px solid ${theme === 'light' ? 'rgba(0,0,0,0.04)' : 'rgba(255,255,255,0.04)'}`,
          maxHeight: isFullscreen ? '70vh' : '80%', overflowY: 'auto',
        }}>
          {groupedData[0].groups.map((g, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '2px 0', fontSize: 11 }}>
              <div style={{ width: 10, height: 10, borderRadius: 2, background: g.color, flexShrink: 0 }} />
              <span style={{ color: theme === 'light' ? '#475569' : '#94a3b8', fontFamily: "'Inter', -apple-system, sans-serif", whiteSpace: 'nowrap' }}>
                {g.name}
              </span>
            </div>
          ))}
        </div>
      )}

      <Canvas camera={{ position: cameraPos, fov: 50 }} style={{ width: '100%', height: '100%' }}>
        {type === 'bar' && <Bar3DScene data={data.slice(0, 30)} title={title} onDrillDown={onDrillDown} />}
        {type === 'line' && <Line3DScene data={data.slice(0, 60)} title={title} />}
        {type === 'grouped_line' && groupedData && groupedData.length > 0 && <GroupedLine3DScene data={groupedData.slice(0, 60)} />}
        {type === 'grouped_bar' && groupedData && groupedData.length > 0 && <GroupedBar3DScene data={groupedData.slice(0, 20)} orientation={orientation} onDrillDown={onDrillDown} />}
        {type === 'stacked_bar' && groupedData && groupedData.length > 0 && <StackedBar3DScene data={groupedData.slice(0, 20)} orientation={orientation} onDrillDown={onDrillDown} />}
        {(type === 'pie' || type === 'area') && <Pie3DScene data={data.slice(0, 15)} title={title} onDrillDown={onDrillDown} />}
      </Canvas>
    </div>
  );
};
