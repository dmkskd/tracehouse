/**
 * QueryXRay3D — 3D resource corridor with log event scrubber.
 *
 * Renders a 3D "corridor" where:
 *   X = time (seconds)
 *   Y = CPU cores used (width of corridor)
 *   Z = Memory MB (height of corridor)
 *
 * Inner traces show I/O Wait, Read throughput, and Network.
 * A slider scrubs through text_log events, highlighting each event's
 * time position with a vertical marker in the 3D scene.
 */

import React, { useMemo, useState, useEffect, useRef, useCallback } from 'react';
import { Canvas, useThree } from '@react-three/fiber';
import { OrbitControls, Text, Line, Html } from '@react-three/drei';
import * as THREE from 'three';
import type { TraceLog } from '@tracehouse/core';
import { useProcessSamples } from './useProcessSamples';
import type { ProcessSample } from './useProcessSamples';
import { useTraceSampleCounts, hasTraceSamplesInRange, useTimeScopedFlamegraph } from './useHotFunctions';
import { SpeedscopeViewer } from '../tracing/SpeedscopeViewer';

/* ── Constants ──────────────────────────────────────────────────────── */

const RUNWAY_X = 22;       // world X extent for time axis
const MAX_Y = 5;           // max Y for CPU
const MAX_Z = 7;           // max Z for memory

/* ── Types ──────────────────────────────────────────────────────────── */

interface LogEvent {
  t: number;           // seconds from query start
  source: string;      // logger name
  message: string;     // truncated message
  level: string;
}

interface XRaySceneProps {
  samples: ProcessSample[];
  highlightTime: number | null;  // seconds from start, null = no highlight
  highlightLabel?: string;       // optional label at crosshair
  sampleCounts?: Map<number, number> | null;  // profiler sample counts per second
  sampleOffset?: number;  // offset between queryStartTime and first process sample
  onShowFlamegraphForT?: (t: number) => void;
  // Stacked per-host breakdown mode
  stackedView?: boolean;
  hostSamples?: Map<string, ProcessSample[]>;
  hosts?: string[];
}

/* ── Helpers ─────────────────────────────────────────────────────────── */

function mapT(t: number, maxT: number): number {
  return maxT > 0 ? (t / maxT) * RUNWAY_X : 0;
}

function mapCpu(v: number, maxCpu: number): number {
  return maxCpu > 0 ? (v / maxCpu) * MAX_Y : 0;
}

function mapMem(v: number, maxMem: number): number {
  return maxMem > 0 ? (v / maxMem) * MAX_Z : 0;
}

const fmtMB = (mb: number) =>
  mb < 1024 ? `${mb.toFixed(0)} MB` : `${(mb / 1024).toFixed(1)} GB`;

/** Simple moving average to smooth jagged samples */
function smooth(values: number[], window = 3): number[] {
  const half = Math.floor(window / 2);
  return values.map((_, i) => {
    let sum = 0, count = 0;
    for (let j = Math.max(0, i - half); j <= Math.min(values.length - 1, i + half); j++) {
      sum += values[j];
      count++;
    }
    return sum / count;
  });
}

/* ── Camera ──────────────────────────────────────────────────────────── */

// Center of the corridor — orbit pivot point
const CENTER_X = RUNWAY_X / 2;
const CENTER_Y = MAX_Y / 2;
const CENTER_Z = MAX_Z / 2;

const CameraSetup: React.FC = () => {
  const { camera } = useThree();
  useMemo(() => {
    (camera as THREE.PerspectiveCamera).fov = 40;
    (camera as THREE.PerspectiveCamera).updateProjectionMatrix();
    camera.position.set(8, 16, 26);
    camera.lookAt(CENTER_X, CENTER_Y, CENTER_Z);
  }, [camera]);
  return null;
};

/* ── Cage + axis grid ──────────────────────────────────────────────── */

const CageAndAxes: React.FC<{ maxT: number; maxCpu: number; maxMem: number }> = ({ maxT, maxCpu, maxMem }) => {
  const cageColor = '#1e2540';
  const cageBright = '#2a3355';

  // Cage: 12 edges of a bounding box (0,0,0) to (RUNWAY_X, MAX_Y, MAX_Z)
  const cageEdges: [THREE.Vector3, THREE.Vector3][] = useMemo(() => {
    const X = RUNWAY_X, Y = MAX_Y, Z = MAX_Z;
    return [
      // Bottom face (z=0)
      [new THREE.Vector3(0, 0, 0), new THREE.Vector3(X, 0, 0)],
      [new THREE.Vector3(0, Y, 0), new THREE.Vector3(X, Y, 0)],
      [new THREE.Vector3(0, 0, 0), new THREE.Vector3(0, Y, 0)],
      [new THREE.Vector3(X, 0, 0), new THREE.Vector3(X, Y, 0)],
      // Top face (z=MAX_Z)
      [new THREE.Vector3(0, 0, Z), new THREE.Vector3(X, 0, Z)],
      [new THREE.Vector3(0, Y, Z), new THREE.Vector3(X, Y, Z)],
      [new THREE.Vector3(0, 0, Z), new THREE.Vector3(0, Y, Z)],
      [new THREE.Vector3(X, 0, Z), new THREE.Vector3(X, Y, Z)],
      // Verticals (z-axis)
      [new THREE.Vector3(0, 0, 0), new THREE.Vector3(0, 0, Z)],
      [new THREE.Vector3(X, 0, 0), new THREE.Vector3(X, 0, Z)],
      [new THREE.Vector3(0, Y, 0), new THREE.Vector3(0, Y, Z)],
      [new THREE.Vector3(X, Y, 0), new THREE.Vector3(X, Y, Z)],
    ];
  }, []);

  // Grid lines on the floor and back wall
  const gridLines = useMemo(() => {
    const lines: [THREE.Vector3, THREE.Vector3][] = [];
    // Floor grid: time × CPU
    const nTime = 10;
    for (let i = 1; i < nTime; i++) {
      const x = (i / nTime) * RUNWAY_X;
      lines.push([new THREE.Vector3(x, 0, 0), new THREE.Vector3(x, MAX_Y, 0)]);
    }
    // Back wall grid: time × memory (at y=0)
    for (let i = 1; i < nTime; i++) {
      const x = (i / nTime) * RUNWAY_X;
      lines.push([new THREE.Vector3(x, 0, 0), new THREE.Vector3(x, 0, MAX_Z)]);
    }
    // Memory ticks on back wall
    for (let i = 1; i < 4; i++) {
      const z = (i / 4) * MAX_Z;
      lines.push([new THREE.Vector3(0, 0, z), new THREE.Vector3(RUNWAY_X, 0, z)]);
    }
    // CPU ticks on floor
    for (let i = 1; i < 4; i++) {
      const y = (i / 4) * MAX_Y;
      lines.push([new THREE.Vector3(0, y, 0), new THREE.Vector3(RUNWAY_X, y, 0)]);
    }
    return lines;
  }, []);

  // Time labels — along the FRONT bottom edge (y=MAX_Y, z=0) so they face the camera
  const timeLabels = useMemo(() => {
    const labels: { x: number; text: string }[] = [];
    const nTime = 5;
    for (let i = 0; i <= nTime; i++) {
      const t = (i / nTime) * maxT;
      labels.push({ x: (i / nTime) * RUNWAY_X, text: `${t.toFixed(0)}s` });
    }
    return labels;
  }, [maxT]);

  // Memory labels — along the left front vertical (x=0, y=MAX_Y)
  const memLabels = useMemo(() => {
    const labels: { z: number; text: string }[] = [];
    for (let i = 0; i <= 4; i++) {
      const mem = (i / 4) * maxMem;
      labels.push({ z: (i / 4) * MAX_Z, text: fmtMB(mem) });
    }
    return labels;
  }, [maxMem]);

  // CPU labels — along the left front bottom (x=0, z=0)
  const cpuLabels = useMemo(() => {
    const labels: { y: number; text: string }[] = [];
    for (let i = 0; i <= 4; i++) {
      const cpu = (i / 4) * maxCpu;
      labels.push({ y: (i / 4) * MAX_Y, text: `${cpu.toFixed(1)}` });
    }
    return labels;
  }, [maxCpu]);

  return (
    <group>
      {/* Cage edges */}
      {cageEdges.map(([a, b], i) => (
        <Line key={`cage-${i}`} points={[a, b]} color={cageBright} lineWidth={1.5} />
      ))}
      {/* Inner grid */}
      {gridLines.map(([a, b], i) => (
        <Line key={`grid-${i}`} points={[a, b]} color={cageColor} lineWidth={0.8} />
      ))}

      {/* Time tick labels — front top edge */}
      {timeLabels.map(({ x, text }, i) => (
        <Html key={`t${i}`} position={[x, MAX_Y + 0.3, 0]} center style={{ pointerEvents: 'none' }}>
          <span style={{ fontFamily: 'monospace', fontSize: 10, color: '#778', whiteSpace: 'nowrap' }}>{text}</span>
        </Html>
      ))}

      {/* Memory tick labels — back-left vertical edge (z-axis at y=0, x=0) */}
      {memLabels.map(({ z, text }, i) => (
        <Html key={`m${i}`} position={[-0.3, -0.3, z]} center style={{ pointerEvents: 'none' }}>
          <span style={{ fontFamily: 'monospace', fontSize: 10, color: '#6688cc', whiteSpace: 'nowrap' }}>{text}</span>
        </Html>
      ))}
      <Html position={[-0.3, -0.3, MAX_Z + 0.5]} center style={{ pointerEvents: 'none' }}>
        <span style={{ fontFamily: 'monospace', fontSize: 11, color: '#6688cc', whiteSpace: 'nowrap', fontWeight: 600 }}>Memory</span>
      </Html>

      {/* CPU tick labels — front-left bottom edge (y-axis at z=0, x=0) */}
      {cpuLabels.map(({ y, text }, i) => (
        <Html key={`c${i}`} position={[-0.3, y, -0.3]} center style={{ pointerEvents: 'none' }}>
          <span style={{ fontFamily: 'monospace', fontSize: 10, color: '#ccaa44', whiteSpace: 'nowrap' }}>{text}</span>
        </Html>
      ))}
      <Html position={[-0.3, MAX_Y + 0.5, -0.3]} center style={{ pointerEvents: 'none' }}>
        <span style={{ fontFamily: 'monospace', fontSize: 11, color: '#ccaa44', whiteSpace: 'nowrap', fontWeight: 600 }}>cores</span>
      </Html>
    </group>
  );
};

/* ── Build wall geometry helper ──────────────────────────────────────── */

function buildWallGeo(
  samples: ProcessSample[],
  getEdge: (s: ProcessSample, i: number) => [number, number, number, number, number, number],
  baseColor: THREE.Color, intensityFn: (s: ProcessSample) => number,
): THREE.BufferGeometry | null {
  const n = samples.length;
  if (n < 2) return null;
  const geo = new THREE.BufferGeometry();
  const positions: number[] = [];
  const colors: number[] = [];
  const indices: number[] = [];
  for (let i = 0; i < n; i++) {
    const [x1, y1, z1, x2, y2, z2] = getEdge(samples[i], i);
    const intensity = intensityFn(samples[i]);
    const c = baseColor.clone().multiplyScalar(0.3 + intensity * 0.7);
    positions.push(x1, y1, z1, x2, y2, z2);
    colors.push(c.r, c.g, c.b, c.r, c.g, c.b);
  }
  for (let i = 0; i < n - 1; i++) {
    const a = i * 2, b = a + 1, c = a + 2, d = a + 3;
    indices.push(a, c, b, b, c, d);
  }
  geo.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
  geo.setAttribute('color', new THREE.Float32BufferAttribute(colors, 3));
  geo.setIndex(indices);
  geo.computeVertexNormals();
  return geo;
}

/* ── Index-based wall geometry builder ──────────────────────────────── */

function buildWallGeoIdx(
  n: number,
  getEdge: (i: number) => [number, number, number, number, number, number],
  baseColor: THREE.Color,
  intensityFn: (i: number) => number,
): THREE.BufferGeometry | null {
  if (n < 2) return null;
  const positions: number[] = [], colors: number[] = [], indices: number[] = [];
  for (let i = 0; i < n; i++) {
    const [x1, y1, z1, x2, y2, z2] = getEdge(i);
    const c = baseColor.clone().multiplyScalar(0.3 + intensityFn(i) * 0.7);
    positions.push(x1, y1, z1, x2, y2, z2);
    colors.push(c.r, c.g, c.b, c.r, c.g, c.b);
  }
  for (let i = 0; i < n - 1; i++) {
    const a = i * 2;
    indices.push(a, a + 2, a + 1, a + 1, a + 2, a + 3);
  }
  const geo = new THREE.BufferGeometry();
  geo.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
  geo.setAttribute('color', new THREE.Float32BufferAttribute(colors, 3));
  geo.setIndex(indices);
  geo.computeVertexNormals();
  return geo;
}

/* ── Stacked corridor (per-host breakdown) ──────────────────────────── */

const HOST_COLORS = [
  new THREE.Color(0xddaa33),  // gold
  new THREE.Color(0x5577dd),  // blue
  new THREE.Color(0x33bbaa),  // teal
  new THREE.Color(0xbb55cc),  // purple
  new THREE.Color(0x55cc55),  // green
  new THREE.Color(0xdd5555),  // red
  new THREE.Color(0xff8844),  // orange
  new THREE.Color(0x88aadd),  // light blue
];

/* ── Split view: per-host lanes ────────────────────────────────────── */

const SplitCorridorMesh: React.FC<{
  hostSamples: Map<string, ProcessSample[]>;
  hosts: string[];
  maxT: number;
  maxCpu: number;
  maxMem: number;
}> = ({ hostSamples, hosts, maxT, maxCpu, maxMem }) => {
  const N = hosts.length;
  const laneGap = 0.3;
  const usableY = MAX_Y - laneGap * (N - 1);
  const laneW = usableY / N;

  const lanes = useMemo(() => {
    return hosts.map((host, hi) => {
      const samples = hostSamples.get(host) || [];
      const n = samples.length;
      if (n < 2) return null;

      const color = HOST_COLORS[hi % HOST_COLORS.length];
      const laneY = hi * (laneW + laneGap);
      const sCpu = smooth(samples.map(s => s.d_cpu_cores), 5);
      const sMem = smooth(samples.map(s => s.memory_mb), 5);
      const cpuInLane = (v: number) => maxCpu > 0 ? (v / maxCpu) * laneW : 0;

      // Back wall: y=laneY, z=0..mem
      const backGeo = buildWallGeoIdx(n, i => {
        const x = mapT(samples[i].t, maxT);
        return [x, laneY, 0, x, laneY, mapMem(sMem[i], maxMem)];
      }, color, i => maxMem > 0 ? sMem[i] / maxMem : 0);

      // Front wall: y=laneY+cpu, z=0..mem
      const frontGeo = buildWallGeoIdx(n, i => {
        const x = mapT(samples[i].t, maxT);
        const y = laneY + cpuInLane(sCpu[i]);
        return [x, y, 0, x, y, mapMem(sMem[i], maxMem)];
      }, color, i => maxCpu > 0 ? sCpu[i] / maxCpu : 0);

      // Floor: z=0, y=laneY..laneY+cpu
      const floorGeo = buildWallGeoIdx(n, i => {
        const x = mapT(samples[i].t, maxT);
        return [x, laneY, 0, x, laneY + cpuInLane(sCpu[i]), 0];
      }, color, () => 0.3);

      // Ceiling: z=mem, y=laneY..laneY+cpu
      const ceilingGeo = buildWallGeoIdx(n, i => {
        const x = mapT(samples[i].t, maxT);
        const mem = mapMem(sMem[i], maxMem);
        return [x, laneY, mem, x, laneY + cpuInLane(sCpu[i]), mem];
      }, color, i => maxMem > 0 ? sMem[i] / maxMem : 0);

      // Edge lines
      const frontTop: THREE.Vector3[] = [];
      const backTop: THREE.Vector3[] = [];
      const frontBot: THREE.Vector3[] = [];
      for (let i = 0; i < n; i++) {
        const x = mapT(samples[i].t, maxT);
        const cpu = cpuInLane(sCpu[i]);
        const mem = mapMem(sMem[i], maxMem);
        frontTop.push(new THREE.Vector3(x, laneY + cpu, mem));
        backTop.push(new THREE.Vector3(x, laneY, mem));
        frontBot.push(new THREE.Vector3(x, laneY + cpu, 0));
      }

      const peakMem = Math.max(...samples.map(s => s.memory_mb), 0);
      const peakCpu = Math.max(...samples.map(s => s.d_cpu_cores), 0);

      return { host, color, backGeo, frontGeo, floorGeo, ceilingGeo,
        frontTop, backTop, frontBot, laneY, peakMem, peakCpu };
    });
  }, [hosts, hostSamples, maxT, maxCpu, maxMem, laneW, laneGap]);

  return (
    <group>
      {lanes.map((lane, i) => {
        if (!lane) return null;
        const hex = '#' + lane.color.getHexString();
        return (
          <group key={i}>
            {lane.backGeo && <mesh geometry={lane.backGeo}>
              <meshStandardMaterial vertexColors transparent opacity={0.55} side={THREE.DoubleSide} depthWrite={false} />
            </mesh>}
            {lane.frontGeo && <mesh geometry={lane.frontGeo}>
              <meshStandardMaterial vertexColors transparent opacity={0.45} side={THREE.DoubleSide} depthWrite={false} />
            </mesh>}
            {lane.floorGeo && <mesh geometry={lane.floorGeo}>
              <meshStandardMaterial vertexColors transparent opacity={0.3} side={THREE.DoubleSide} depthWrite={false} />
            </mesh>}
            {lane.ceilingGeo && <mesh geometry={lane.ceilingGeo}>
              <meshStandardMaterial vertexColors transparent opacity={0.4} side={THREE.DoubleSide} depthWrite={false} />
            </mesh>}
            {lane.frontTop.length > 1 && <Line points={lane.frontTop} color={hex} lineWidth={2.5} />}
            {lane.backTop.length > 1 && <Line points={lane.backTop} color={hex} lineWidth={2} />}
            {lane.frontBot.length > 1 && <Line points={lane.frontBot} color={hex} lineWidth={1.5} />}
            {/* Lane separator */}
            <Line points={[
              new THREE.Vector3(0, lane.laneY, 0),
              new THREE.Vector3(RUNWAY_X, lane.laneY, 0),
            ]} color="#1e2540" lineWidth={1} />
          </group>
        );
      })}
      {/* Host labels at the front of each lane */}
      {lanes.map((lane, i) => {
        if (!lane) return null;
        const hex = '#' + lane.color.getHexString();
        return (
          <Html key={`lbl-${i}`} position={[RUNWAY_X + 0.5, lane.laneY + laneW / 2, 0]}
            style={{ pointerEvents: 'none' }}>
            <div style={{
              fontFamily: 'monospace', fontSize: 10, fontWeight: 600,
              color: hex, whiteSpace: 'nowrap', lineHeight: 1.4,
              textShadow: '0 0 6px rgba(0,0,0,0.9)',
            }}>
              <div>{lane.host.length > 22 ? lane.host.slice(0, 10) + '…' + lane.host.slice(-10) : lane.host}</div>
              <div style={{ fontSize: 9, opacity: 0.7 }}>{fmtMB(lane.peakMem)} · {lane.peakCpu.toFixed(1)}c</div>
            </div>
          </Html>
        );
      })}
    </group>
  );
};

/* ── Corridor ──────────────────────────────────────────────────────── */

const CorridorMesh: React.FC<{
  samples: ProcessSample[];
  maxT: number;
  maxCpu: number;
  maxMem: number;
  smoothCpu: number[];
  smoothMem: number[];
}> = ({ samples, maxT, maxCpu, maxMem, smoothCpu, smoothMem }) => {
  const n = samples.length;
  const memColor = useMemo(() => new THREE.Color(0x5577dd), []);
  const cpuColor = useMemo(() => new THREE.Color(0xddaa33), []);
  const floorColor = useMemo(() => new THREE.Color(0x222240), []);

  // Ceiling: back wall (y=0, z=mem) to front wall (y=cpu, z=mem)
  const ceilingGeo = useMemo(() => buildWallGeo(
    samples,
    (_, i) => {
      const x = mapT(samples[i].t, maxT);
      const cpu = mapCpu(smoothCpu[i], maxCpu);
      const mem = mapMem(smoothMem[i], maxMem);
      return [x, 0, mem, x, cpu, mem];
    },
    memColor, s => s.memory_mb / maxMem,
  ), [samples, maxT, maxCpu, maxMem, smoothCpu, smoothMem, memColor]);

  // Back wall: (y=0, z=0) to (y=0, z=mem)
  const backGeo = useMemo(() => buildWallGeo(
    samples,
    (_, i) => {
      const x = mapT(samples[i].t, maxT);
      const mem = mapMem(smoothMem[i], maxMem);
      return [x, 0, 0, x, 0, mem];
    },
    cpuColor, s => s.d_cpu_cores / maxCpu,
  ), [samples, maxT, maxCpu, maxMem, smoothMem, cpuColor]);

  // Front wall: (y=cpu, z=0) to (y=cpu, z=mem)
  const frontGeo = useMemo(() => buildWallGeo(
    samples,
    (_, i) => {
      const x = mapT(samples[i].t, maxT);
      const cpu = mapCpu(smoothCpu[i], maxCpu);
      const mem = mapMem(smoothMem[i], maxMem);
      return [x, cpu, 0, x, cpu, mem];
    },
    cpuColor, s => s.d_cpu_cores / maxCpu,
  ), [samples, maxT, maxCpu, maxMem, smoothCpu, smoothMem, cpuColor]);

  // Floor: (y=0, z=0) to (y=cpu, z=0)
  const floorGeo = useMemo(() => buildWallGeo(
    samples,
    (_, i) => {
      const x = mapT(samples[i].t, maxT);
      const cpu = mapCpu(smoothCpu[i], maxCpu);
      return [x, 0, 0, x, cpu, 0];
    },
    floorColor, () => 0.5,
  ), [samples, maxT, maxCpu, maxMem, smoothCpu, floorColor]);

  // Edge outlines — the 4 profile edges that define corridor cross-section shape
  const edges = useMemo(() => {
    if (n < 2) return { cpuTop: [] as THREE.Vector3[], cpuBot: [] as THREE.Vector3[], memBack: [] as THREE.Vector3[], memFront: [] as THREE.Vector3[] };
    const cpuTop: THREE.Vector3[] = [];
    const cpuBot: THREE.Vector3[] = [];
    const memBack: THREE.Vector3[] = [];
    const memFront: THREE.Vector3[] = [];
    for (let i = 0; i < n; i++) {
      const x = mapT(samples[i].t, maxT);
      const cpu = mapCpu(smoothCpu[i], maxCpu);
      const mem = mapMem(smoothMem[i], maxMem);
      cpuTop.push(new THREE.Vector3(x, cpu, mem));  // front-top edge
      cpuBot.push(new THREE.Vector3(x, cpu, 0));    // front-bottom edge
      memBack.push(new THREE.Vector3(x, 0, mem));   // back-top edge
      memFront.push(new THREE.Vector3(x, 0, 0));    // back-bottom (floor-back)
    }
    return { cpuTop, cpuBot, memBack, memFront };
  }, [samples, n, maxT, maxCpu, maxMem, smoothCpu, smoothMem]);

  // End caps — close the corridor at first and last sample
  const endCapGeos = useMemo(() => {
    if (n < 2) return null;
    const caps: THREE.BufferGeometry[] = [];
    for (const idx of [0, n - 1]) {
      const x = mapT(samples[idx].t, maxT);
      const cpu = mapCpu(smoothCpu[idx], maxCpu);
      const mem = mapMem(smoothMem[idx], maxMem);
      // Quad: (x,0,0) → (x,cpu,0) → (x,cpu,mem) → (x,0,mem)
      const geo = new THREE.BufferGeometry();
      const verts = new Float32Array([
        x, 0, 0,
        x, cpu, 0,
        x, cpu, mem,
        x, 0, mem,
      ]);
      geo.setAttribute('position', new THREE.Float32BufferAttribute(verts, 3));
      geo.setIndex([0, 1, 2, 0, 2, 3]);
      geo.computeVertexNormals();
      caps.push(geo);
    }
    return caps;
  }, [samples, n, maxT, maxCpu, maxMem, smoothCpu, smoothMem]);

  if (!ceilingGeo || !backGeo || !frontGeo || !floorGeo) return null;

  const wallMat = (opacity: number) => (
    <meshStandardMaterial vertexColors transparent opacity={opacity} side={THREE.DoubleSide} depthWrite={false} />
  );

  return (
    <group>
      <mesh geometry={ceilingGeo}>{wallMat(0.55)}</mesh>
      <mesh geometry={backGeo}>{wallMat(0.5)}</mesh>
      <mesh geometry={frontGeo}>{wallMat(0.45)}</mesh>
      <mesh geometry={floorGeo}>{wallMat(0.3)}</mesh>

      {/* End caps to close the corridor */}
      {endCapGeos && endCapGeos.map((geo, i) => (
        <mesh key={`cap-${i}`} geometry={geo}>
          <meshStandardMaterial color="#2a2a50" transparent opacity={0.4} side={THREE.DoubleSide} depthWrite={false} />
        </mesh>
      ))}

      {/* Bright edge outlines for definition */}
      {edges.cpuTop.length > 1 && <>
        <Line points={edges.cpuTop} color="#FECB52" lineWidth={2.5} />
        <Line points={edges.cpuBot} color="#aa8830" lineWidth={1.5} />
        <Line points={edges.memBack} color="#636EFA" lineWidth={2.5} />
      </>}
    </group>
  );
};

/* ── Inner trace lines ──────────────────────────────────────────────── */

const InnerTraces: React.FC<{
  samples: ProcessSample[];
  maxT: number;
  maxCpu: number;
  maxMem: number;
  smoothCpu: number[];
  smoothMem: number[];
}> = ({ samples, maxT, maxCpu, smoothCpu }) => {
  const n = samples.length;

  // Smooth the inner metrics too
  const smoothIo = useMemo(() => smooth(samples.map(s => s.d_io_wait_s), 5), [samples]);
  const smoothRead = useMemo(() => smooth(samples.map(s => s.d_read_mb), 5), [samples]);
  const smoothNet = useMemo(() => smooth(samples.map(s => (s.d_net_send_kb + s.d_net_recv_kb)), 5), [samples]);

  const maxIo = useMemo(() => Math.max(...smoothIo, 0.001), [smoothIo]);
  const maxRead = useMemo(() => Math.max(...smoothRead, 0.001), [smoothRead]);
  const maxNet = useMemo(() => Math.max(...smoothNet, 0), [smoothNet]);

  // Inner traces run along the corridor at fixed height fractions
  // Y position: proportional to CPU width for "inside corridor" feel
  // Z position: fixed fraction of corridor height for readability
  const ioPoints = useMemo(() =>
    samples.map((s, i) => {
      const x = mapT(s.t, maxT);
      const cpu = mapCpu(smoothCpu[i], maxCpu);
      const yNorm = smoothIo[i] / maxIo;
      return new THREE.Vector3(x, cpu * (0.2 + yNorm * 0.15), MAX_Z * 0.5);
    }),
    [samples, maxT, maxCpu, smoothCpu, smoothIo, maxIo]
  );

  const readPoints = useMemo(() =>
    samples.map((s, i) => {
      const x = mapT(s.t, maxT);
      const cpu = mapCpu(smoothCpu[i], maxCpu);
      const yNorm = smoothRead[i] / maxRead;
      return new THREE.Vector3(x, cpu * (0.5 + yNorm * 0.15), MAX_Z * 0.25);
    }),
    [samples, maxT, maxCpu, smoothCpu, smoothRead, maxRead]
  );

  const netPoints = useMemo(() =>
    maxNet > 0
      ? samples.map((s, i) => {
          const x = mapT(s.t, maxT);
          const cpu = mapCpu(smoothCpu[i], maxCpu);
          return new THREE.Vector3(x, cpu * 0.4, MAX_Z * 0.75);
        })
      : [],
    [samples, maxT, maxCpu, smoothCpu, maxNet]
  );

  if (n < 2) return null;

  return (
    <group>
      <Line points={ioPoints} color="#7B83FF" lineWidth={5} />
      <Line points={readPoints} color="#00DD99" lineWidth={4} />
      {netPoints.length > 1 && (
        <Line points={netPoints} color="#33DDFF" lineWidth={3} />
      )}
    </group>
  );
};

/* ── Time slice marker ────────────────────────────────────────────── */

const TimeHighlight: React.FC<{
  samples: ProcessSample[];
  highlightTime: number | null;
  highlightLabel?: string;
  maxT: number;
  maxCpu: number;
  maxMem: number;
}> = ({ samples, highlightTime, highlightLabel, maxT, maxCpu, maxMem: _maxMem }) => {
  // Translucent vertical slice that cuts through the cage at the highlighted time —
  // like a CT-scan plane, fitting the X-Ray theme.
  const sliceGeo = useMemo(() => {
    if (highlightTime === null || samples.length === 0) return null;
    const x = mapT(highlightTime, maxT);
    // Quad spanning the full cage cross-section at this x
    const geo = new THREE.BufferGeometry();
    const verts = new Float32Array([
      x, 0, 0,
      x, MAX_Y, 0,
      x, MAX_Y, MAX_Z,
      x, 0, MAX_Z,
    ]);
    geo.setAttribute('position', new THREE.Float32BufferAttribute(verts, 3));
    geo.setIndex([0, 1, 2, 0, 2, 3]);
    return geo;
  }, [highlightTime, maxT, samples.length]);

  if (highlightTime === null || samples.length === 0 || !sliceGeo) return null;

  const x = mapT(highlightTime, maxT);

  // Find nearest sample for label positioning
  let nearestIdx = 0;
  let minDist = Infinity;
  for (let i = 0; i < samples.length; i++) {
    const d = Math.abs(samples[i].t - highlightTime);
    if (d < minDist) { minDist = d; nearestIdx = i; }
  }
  const s = samples[nearestIdx];
  const cpu = mapCpu(s.d_cpu_cores, maxCpu);

  // Thin bright edge line along the bottom of the slice for definition
  const edgeLine = [
    new THREE.Vector3(x, 0, 0),
    new THREE.Vector3(x, MAX_Y, 0),
  ];

  return (
    <group>
      {/* Translucent slice plane */}
      <mesh geometry={sliceGeo}>
        <meshBasicMaterial
          color="#FECB52"
          transparent
          opacity={0.06}
          side={THREE.DoubleSide}
          depthWrite={false}
        />
      </mesh>
      {/* Bright edge on the floor for readability */}
      <Line points={edgeLine} color="#FECB52" lineWidth={2} transparent opacity={0.7} />
      {/* Label */}
      {highlightLabel && (
        <Text
          position={[x, Math.max(cpu, MAX_Y * 0.3) + 0.4, MAX_Z + 0.3]}
          fontSize={0.28}
          color="#FECB52"
          anchorX="center"
          anchorY="bottom"
          outlineWidth={0.02}
          outlineColor="#000000"
        >
          {highlightLabel}
        </Text>
      )}
    </group>
  );
};

/* ── Hover tooltip ─────────────────────────────────────────────────── */

const HoverTooltip: React.FC<{
  samples: ProcessSample[];
  maxT: number;
  maxCpu: number;
  maxMem: number;
  smoothCpu: number[];
  smoothMem: number[];
  sampleCounts?: Map<number, number> | null;
  sampleOffset?: number;
  onShowFlamegraphForT?: (t: number) => void;
}> = ({ samples, maxT, maxCpu, maxMem, smoothCpu, smoothMem, sampleCounts, sampleOffset = 0, onShowFlamegraphForT }) => {
  const meshRef = useRef<THREE.Mesh>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const [hoverPos, setHoverPos] = useState<THREE.Vector3 | null>(null);

  const handlePointerMove = useCallback((e: { point: THREE.Vector3 }) => {
    const xFrac = e.point.x / RUNWAY_X;
    if (xFrac < 0 || xFrac > 1) { setHoverIdx(null); return; }
    const idx = Math.round(xFrac * (samples.length - 1));
    const clamped = Math.max(0, Math.min(samples.length - 1, idx));
    setHoverIdx(clamped);
    setHoverPos(e.point.clone());
  }, [samples.length]);

  const handlePointerLeave = useCallback(() => {
    setHoverIdx(null);
    setHoverPos(null);
  }, []);

  const handleClick = useCallback(() => {
    if (hoverIdx === null || !onShowFlamegraphForT) return;
    const s = samples[hoverIdx];
    if (!s || !sampleCounts) return;
    if (hasTraceSamplesInRange(sampleCounts, s.t + sampleOffset, s.t + sampleOffset + 1)) {
      onShowFlamegraphForT(s.t);
    }
  }, [hoverIdx, samples, sampleCounts, onShowFlamegraphForT]);

  const s = hoverIdx !== null ? samples[hoverIdx] : null;

  return (
    <group>
      {/* Invisible plane for raycasting — covers the full corridor volume */}
      <mesh
        ref={meshRef}
        position={[RUNWAY_X / 2, MAX_Y / 2, MAX_Z / 2]}
        onPointerMove={handlePointerMove}
        onPointerLeave={handlePointerLeave}
        onClick={handleClick}
      >
        <planeGeometry args={[RUNWAY_X, MAX_Y]} />
        <meshBasicMaterial transparent opacity={0} side={THREE.DoubleSide} />
      </mesh>

      {/* Vertical time indicator line */}
      {hoverIdx !== null && s && (() => {
        const x = mapT(s.t, maxT);
        const cpu = mapCpu(smoothCpu[hoverIdx], maxCpu);
        const mem = mapMem(smoothMem[hoverIdx], maxMem);
        return (
          <>
            <Line
              points={[
                new THREE.Vector3(x, 0, 0),
                new THREE.Vector3(x, 0, mem),
              ]}
              color="#ffffff"
              lineWidth={1}
              transparent
              opacity={0.4}
            />
            <Line
              points={[
                new THREE.Vector3(x, 0, 0),
                new THREE.Vector3(x, cpu, 0),
              ]}
              color="#ffffff"
              lineWidth={1}
              transparent
              opacity={0.4}
            />
          </>
        );
      })()}

      {/* Html tooltip overlay */}
      {hoverIdx !== null && s && hoverPos && (
        <Html
          position={[mapT(s.t, maxT), mapCpu(smoothCpu[hoverIdx], maxCpu) + 0.3, mapMem(smoothMem[hoverIdx], maxMem)]}
          center
          style={{ pointerEvents: 'none' }}
        >
          <div style={{
            background: 'rgba(10, 10, 30, 0.92)',
            border: '1px solid rgba(100, 110, 250, 0.5)',
            borderRadius: 5,
            padding: '6px 10px',
            fontSize: 11,
            fontFamily: 'monospace',
            color: '#ddd',
            whiteSpace: 'nowrap',
            lineHeight: 1.6,
            minWidth: 140,
          }}>
            <div style={{ color: '#888', borderBottom: '1px solid #333', paddingBottom: 3, marginBottom: 3 }}>
              t = {s.t.toFixed(1)}s
            </div>
            <div><span style={{ color: '#636EFA' }}>Memory:</span> {fmtMB(s.memory_mb)}</div>
            <div><span style={{ color: '#FECB52' }}>CPU:</span> {s.d_cpu_cores.toFixed(2)} cores</div>
            <div><span style={{ color: '#7B83FF' }}>I/O Wait:</span> {s.d_io_wait_s.toFixed(3)}s</div>
            <div><span style={{ color: '#00DD99' }}>read_bytes:</span> {s.d_read_mb.toFixed(1)} MB/s</div>
            {s.d_net_send_kb > 0 && <div><span style={{ color: '#33DDFF' }}>Net Send:</span> {s.d_net_send_kb.toFixed(1)} KB</div>}
            {s.d_net_recv_kb > 0 && <div><span style={{ color: '#33DDFF' }}>Net Recv:</span> {s.d_net_recv_kb.toFixed(1)} KB</div>}
            <div><span style={{ color: '#aaa' }}>Threads:</span> {s.thread_count}</div>
            {sampleCounts && hasTraceSamplesInRange(sampleCounts, s.t + sampleOffset, s.t + sampleOffset + 1) && (
              <div style={{
                borderTop: '1px solid #333', paddingTop: 3, marginTop: 3,
                color: '#636EFA', fontSize: 10,
              }}>
                Click for Flamegraph
              </div>
            )}
          </div>
        </Html>
      )}
    </group>
  );
};

/* ── 3D Scene ──────────────────────────────────────────────────────── */

const XRayScene: React.FC<XRaySceneProps> = ({ samples, highlightTime, highlightLabel, sampleCounts, sampleOffset = 0, onShowFlamegraphForT, stackedView, hostSamples, hosts }) => {
  const maxT = useMemo(() => Math.max(...samples.map(s => s.t), 1), [samples]);
  const maxCpu = useMemo(() => Math.max(...samples.map(s => s.d_cpu_cores), 1), [samples]);
  const maxMem = useMemo(() => Math.max(...samples.map(s => s.memory_mb), 1), [samples]);

  // Smooth CPU and memory for corridor shape — removes sample-to-sample jitter
  const smoothCpu = useMemo(() => smooth(samples.map(s => s.d_cpu_cores), 5), [samples]);
  const smoothMem = useMemo(() => smooth(samples.map(s => s.memory_mb), 5), [samples]);

  return (
    <>
      <CameraSetup />
      <OrbitControls enableDamping dampingFactor={0.1} target={[CENTER_X, CENTER_Y, CENTER_Z]} minPolarAngle={0} maxPolarAngle={Math.PI} makeDefault />

      {/* Lighting — brighter for better surface visibility */}
      <ambientLight intensity={0.6} />
      <directionalLight position={[12, 15, 10]} intensity={1.0} color="#fffaf0" />
      <directionalLight position={[-8, 8, -5]} intensity={0.4} color="#aabbff" />

      <CageAndAxes maxT={maxT} maxCpu={maxCpu} maxMem={maxMem} />
      {stackedView && hostSamples && hosts && hosts.length > 1 ? (
        <SplitCorridorMesh
          hostSamples={hostSamples}
          hosts={hosts}
          maxT={maxT}
          maxCpu={maxCpu}
          maxMem={maxMem}
        />
      ) : (
        <CorridorMesh samples={samples} maxT={maxT} maxCpu={maxCpu} maxMem={maxMem}
          smoothCpu={smoothCpu} smoothMem={smoothMem} />
      )}
      {!stackedView && <InnerTraces samples={samples} maxT={maxT} maxCpu={maxCpu} maxMem={maxMem}
        smoothCpu={smoothCpu} smoothMem={smoothMem} />}
      <TimeHighlight
        samples={samples}
        highlightTime={highlightTime}
        highlightLabel={highlightLabel}
        maxT={maxT}
        maxCpu={maxCpu}
        maxMem={maxMem}
      />
      <HoverTooltip
        samples={samples}
        maxT={maxT}
        maxCpu={maxCpu}
        maxMem={maxMem}
        smoothCpu={smoothCpu}
        smoothMem={smoothMem}
        sampleCounts={sampleCounts}
        sampleOffset={sampleOffset}
        onShowFlamegraphForT={onShowFlamegraphForT}
      />
    </>
  );
};

/* ── Scrubber (React UI outside Canvas) ───────────────────────────────── */

type ScrubberMode = 'time' | 'logs';

const MODE_META: Record<ScrubberMode, { icon: string; label: string; color: string }> = {
  time: { icon: '⏱', label: 'Time', color: '#636EFA' },
  logs: { icon: '☰', label: 'Logs', color: '#FECB52' },
};

const pillStyle = (active: boolean, color: string): React.CSSProperties => ({
  padding: '3px 10px',
  borderRadius: 12,
  fontSize: 10,
  fontWeight: 600,
  letterSpacing: '0.4px',
  cursor: 'pointer',
  border: `1px solid ${active ? color + '66' : '#2a2a3a'}`,
  background: active ? `${color}1a` : 'rgba(255,255,255,0.02)',
  color: active ? color : '#555',
  transition: 'all 0.2s ease',
  userSelect: 'none',
  whiteSpace: 'nowrap',
  backdropFilter: 'blur(4px)',
});

const navBtnStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.03)',
  border: '1px solid #2a2a3a',
  color: '#777',
  borderRadius: 6,
  padding: '4px 10px',
  cursor: 'pointer',
  fontSize: 13,
  fontWeight: 500,
  lineHeight: 1,
  transition: 'all 0.15s ease',
};

/* Custom range slider styles injected once */
const SLIDER_STYLE_ID = 'xray-slider-styles';
function ensureSliderStyles() {
  let el = document.getElementById(SLIDER_STYLE_ID) as HTMLStyleElement | null;
  const css = `
    .xray-scrubber-slider {
      -webkit-appearance: none;
      appearance: none;
      width: 100%;
      height: 6px;
      border-radius: 3px;
      background: linear-gradient(90deg, rgba(255,255,255,0.06) 0%, rgba(255,255,255,0.1) 100%);
      outline: none;
      cursor: pointer;
      margin: 0;
    }
    .xray-scrubber-slider::-webkit-slider-thumb {
      -webkit-appearance: none;
      appearance: none;
      width: 14px;
      height: 14px;
      border-radius: 50%;
      background: var(--xray-accent);
      border: 2px solid rgba(0,0,0,0.4);
      box-shadow: 0 0 8px var(--xray-accent-glow), 0 1px 3px rgba(0,0,0,0.5);
      cursor: pointer;
      transition: transform 0.1s ease, box-shadow 0.15s ease;
    }
    .xray-scrubber-slider::-webkit-slider-thumb:hover {
      transform: scale(1.2);
      box-shadow: 0 0 14px var(--xray-accent-glow), 0 1px 4px rgba(0,0,0,0.6);
    }
    .xray-scrubber-slider::-webkit-slider-thumb:active {
      transform: scale(1.1);
    }
    .xray-scrubber-slider::-moz-range-thumb {
      width: 14px;
      height: 14px;
      border-radius: 50%;
      background: var(--xray-accent);
      border: 2px solid rgba(0,0,0,0.4);
      box-shadow: 0 0 8px var(--xray-accent-glow), 0 1px 3px rgba(0,0,0,0.5);
      cursor: pointer;
    }
    .xray-scrubber-slider::-moz-range-track {
      height: 6px;
      border-radius: 3px;
      background: linear-gradient(90deg, rgba(255,255,255,0.06) 0%, rgba(255,255,255,0.1) 100%);
    }
  `;
  if (!el) {
    el = document.createElement('style');
    el.id = SLIDER_STYLE_ID;
    document.head.appendChild(el);
  }
  el.textContent = css;
}

const levelColors: Record<string, string> = {
  Fatal: '#ef4444', Critical: '#ef4444', Error: '#ef4444',
  Warning: '#f59e0b', Notice: '#3b82f6', Information: '#3b82f6',
  Debug: '#6b7280', Trace: '#4b5563',
};

const Scrubber: React.FC<{
  mode: ScrubberMode;
  onModeChange: (m: ScrubberMode) => void;
  logEvents: LogEvent[];
  samples: ProcessSample[];
  activeIdx: number;
  onChange: (idx: number) => void;
}> = ({ mode, onModeChange, logEvents, samples, activeIdx, onChange }) => {
  const items = mode === 'logs' ? logEvents : samples;
  const count = items.length;
  const sliderRef = useRef<HTMLInputElement>(null);

  // Inject custom slider CSS
  useEffect(() => { ensureSliderStyles(); }, []);

  if (count === 0) return null;

  const clampedIdx = Math.max(0, Math.min(count - 1, activeIdx));
  const progressPct = count > 1 ? (clampedIdx / (count - 1)) * 100 : 0;
  const currentT = mode === 'logs'
    ? (logEvents[clampedIdx]?.t ?? 0)
    : (samples[clampedIdx]?.t ?? 0);

  // Detail line content depends on mode
  const detailContent = (() => {
    if (mode === 'logs') {
      const evt = logEvents[clampedIdx];
      if (!evt) return null;
      return (
        <div style={{ display: 'flex', gap: 8, alignItems: 'baseline', minWidth: 0, overflow: 'hidden' }}>
          <span style={{ color: '#FECB52', flexShrink: 0 }}>{evt.t.toFixed(1)}s</span>
          <span style={{
            color: levelColors[evt.level] || '#888',
            flexShrink: 0, fontSize: 10, textTransform: 'uppercase',
          }}>{evt.level}</span>
          <span style={{ color: '#aaa', flexShrink: 0 }}>
            {evt.source.split('(')[0].trim()}
          </span>
          <span style={{
            color: '#555', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          }}>{evt.message}</span>
        </div>
      );
    }
    // Time mode — show sample metrics
    const s = samples[clampedIdx];
    if (!s) return null;
    return (
      <div style={{ display: 'flex', gap: 14, alignItems: 'baseline', minWidth: 0, overflow: 'hidden' }}>
        <span style={{ color: '#636EFA', flexShrink: 0 }}>{s.t.toFixed(0)}s</span>
        <span style={{ color: '#5577dd' }}>Mem {fmtMB(s.memory_mb)}</span>
        <span style={{ color: '#ddaa33' }}>CPU {s.d_cpu_cores.toFixed(1)}</span>
        <span style={{ color: '#7B83FF' }}>IO {s.d_io_wait_s.toFixed(2)}s</span>
        <span style={{ color: '#00DD99' }}>read_bytes {s.d_read_mb.toFixed(0)} MB/s</span>
        <span style={{ color: '#aaa' }}>{s.thread_count} thr</span>
      </div>
    );
  })();

  const accentColor = MODE_META[mode].color;
  const accentGlow = accentColor + '66';

  // Build track background with filled portion
  const trackBg = `linear-gradient(90deg, ${accentColor}44 0%, ${accentColor}88 ${progressPct}%, rgba(255,255,255,0.08) ${progressPct}%)`;

  return (
    <div style={{
      padding: '8px 14px 10px',
      background: 'linear-gradient(180deg, rgba(15,15,30,0.97) 0%, rgba(8,8,18,0.99) 100%)',
      borderTop: '1px solid rgba(100,110,250,0.12)',
      fontFamily: 'monospace',
      fontSize: 11,
      overflow: 'hidden',
    }}>
      {/* Top row: mode pills + time + nav buttons */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6, minWidth: 0 }}>
        {/* Mode selector pills */}
        <div style={{ display: 'flex', gap: 5, flexShrink: 0 }}>
          {(Object.keys(MODE_META) as ScrubberMode[]).map(m => (
            <span
              key={m}
              style={pillStyle(mode === m, MODE_META[m].color)}
              onClick={() => onModeChange(m)}
            >
              {MODE_META[m].icon} {MODE_META[m].label}
              <span style={{ opacity: 0.5, marginLeft: 4, fontSize: 9 }}>
                {m === 'logs' ? logEvents.length : samples.length}
              </span>
            </span>
          ))}
        </div>

        {/* Time indicator */}
        <span style={{
          color: accentColor,
          fontSize: 13,
          fontWeight: 700,
          flexShrink: 0,
          minWidth: 48,
          textAlign: 'right',
          textShadow: `0 0 10px ${accentGlow}`,
        }}>
          {currentT.toFixed(mode === 'logs' ? 1 : 0)}s
        </span>

        {/* Spacer */}
        <span style={{ flex: 1 }} />

        {/* Nav buttons */}
        <div style={{ display: 'flex', gap: 4, flexShrink: 0 }}>
          <button onClick={() => onChange(Math.max(0, activeIdx - 1))} style={navBtnStyle}>‹</button>
          <button onClick={() => onChange(Math.min(count - 1, activeIdx + 1))} style={navBtnStyle}>›</button>
        </div>
      </div>

      {/* Slider on its own row for full width */}
      <div style={{
        position: 'relative',
        marginBottom: 6,
        paddingTop: 2,
        // Pass accent color to CSS via custom properties
        ['--xray-accent' as string]: accentColor,
        ['--xray-accent-glow' as string]: accentGlow,
      }}>
        <input
          ref={sliderRef}
          type="range"
          className="xray-scrubber-slider"
          min={0}
          max={count - 1}
          value={clampedIdx}
          onChange={e => onChange(parseInt(e.target.value))}
          style={{
            width: '100%',
            background: trackBg,
          }}
        />
      </div>

      {/* Detail row */}
      <div style={{ lineHeight: 1.5, minHeight: 18, display: 'flex', alignItems: 'center', gap: 10 }}>
        {detailContent}
      </div>
    </div>
  );
};

/* ── Host tab pill ────────────────────────────────────────────────────── */

const HostTab: React.FC<{
  label: string;
  title?: string;
  active: boolean;
  onClick: () => void;
}> = ({ label, title, active, onClick }) => (
  <span
    title={title}
    onClick={onClick}
    style={{
      padding: '3px 10px',
      borderRadius: 12,
      fontSize: 10,
      fontWeight: 600,
      letterSpacing: '0.4px',
      cursor: 'pointer',
      border: `1px solid ${active ? '#636EFA66' : '#2a2a3a'}`,
      background: active ? '#636EFA1a' : 'rgba(255,255,255,0.02)',
      color: active ? '#636EFA' : '#555',
      transition: 'all 0.2s ease',
      userSelect: 'none',
      whiteSpace: 'nowrap',
    }}
  >
    {label}
  </span>
);

/* ── Main component ──────────────────────────────────────────────────── */

export interface QueryXRay3DProps {
  queryId: string;
  logs: TraceLog[];
  queryStartTime?: string;
}

export const QueryXRay3D: React.FC<QueryXRay3DProps> = ({
  queryId,
  logs,
  queryStartTime,
}) => {
  const { samples: allSamples, hostSamples, hosts, isLoading: isLoadingSamples, error, fetch: fetchSamples } = useProcessSamples(queryId);
  const { sampleCounts, fetch: fetchSampleCounts } = useTraceSampleCounts(queryId, queryStartTime, queryStartTime);
  const timeScopedFlamegraph = useTimeScopedFlamegraph();
  const [showFlamegraphPopup, setShowFlamegraphPopup] = useState(false);
  const [selectedHost, setSelectedHost] = useState<string | null>(null);
  const [stackedView, setStackedView] = useState(false);
  const [scrubberMode, setScrubberMode] = useState<ScrubberMode>('time');
  const [scrubberIdx, setScrubberIdx] = useState(0);

  // Active samples: "All" (aggregated) or per-host filtered
  const samples = useMemo(() => {
    if (selectedHost === null) return allSamples;
    return hostSamples.get(selectedHost) || [];
  }, [selectedHost, allSamples, hostSamples]);

  // Fetch samples and probe trace_log on mount
  useEffect(() => {
    fetchSamples();
    fetchSampleCounts();
  }, [fetchSamples, fetchSampleCounts]);

  // Convert TraceLog[] to LogEvent[] relative to query start
  const logEvents = useMemo(() => {
    if (!logs.length || !samples.length) return [];

    const maxT = samples[samples.length - 1].t;

    // Parse all log timestamps
    const logTimes = logs.map(log =>
      new Date(log.event_time_microseconds || log.event_time).getTime()
    );

    // Use the first log timestamp as reference, then scale to sample range.
    // Log timestamps are absolute; sample t values are relative to query start.
    // The first log typically fires at query start, so first_log_time ≈ t=0.
    const firstLogMs = Math.min(...logTimes);
    const lastLogMs = Math.max(...logTimes);
    const logSpanMs = lastLogMs - firstLogMs;

    // If we have a queryStartTime, use it to align logs to sample timeline.
    // Otherwise, scale log span to fit within sample range.
    const queryStartMs = queryStartTime ? new Date(queryStartTime).getTime() : null;

    const events: LogEvent[] = [];

    for (let i = 0; i < logs.length; i++) {
      const log = logs[i];
      let elapsed: number;

      if (queryStartMs) {
        elapsed = (logTimes[i] - queryStartMs) / 1000;
      } else if (logSpanMs > 0) {
        elapsed = ((logTimes[i] - firstLogMs) / logSpanMs) * maxT;
      } else {
        elapsed = maxT / 2;
      }

      // Skip if far outside sample range
      if (elapsed < -2 || elapsed > maxT + 2) continue;
      elapsed = Math.max(0, Math.min(maxT, elapsed));

      events.push({
        t: elapsed,
        source: log.source,
        message: (log.message || '').slice(0, 120),
        level: log.level || 'Debug',
      });
    }

    return events.sort((a, b) => a.t - b.t);
  }, [logs, samples, queryStartTime]);

  // Compute highlight time + label from scrubber state
  const scrubberItems = scrubberMode === 'logs' ? logEvents : samples;
  const clampedIdx = Math.max(0, Math.min(scrubberItems.length - 1, scrubberIdx));

  const highlightTime = scrubberItems.length > 0
    ? (scrubberMode === 'logs' ? logEvents[clampedIdx]?.t : samples[clampedIdx]?.t) ?? null
    : null;

  const highlightLabel = scrubberMode === 'logs' && logEvents[clampedIdx]
    ? logEvents[clampedIdx].source.split('(')[0].trim().slice(0, 25)
    : undefined;

  // Offset between queryStartTime and the first process sample.
  // Process sample t=0 corresponds to queryStartTime + sampleOffset seconds.
  // The probe's t_second values are relative to queryStartTime, so we add this offset
  // when mapping scrubber positions to probe keys and absolute times.
  const sampleOffset = samples.length > 0 ? samples[0].elapsed : 0;

  // Compute absolute time window for current scrubber position (for flamegraph popup)
  // Only returns non-null when we know there are profiler samples in this window.
  // Works in both time and logs mode.
  const currentTimeWindow = useMemo(() => {
    if (samples.length === 0 || !queryStartTime || !sampleCounts) return null;
    const startMs = new Date(queryStartTime).getTime();
    let currentT: number;
    let nextT: number;
    if (scrubberMode === 'logs') {
      currentT = logEvents[clampedIdx]?.t ?? 0;
      nextT = currentT + 1;
    } else {
      currentT = samples[clampedIdx]?.t ?? 0;
      nextT = clampedIdx < samples.length - 1 ? samples[clampedIdx + 1].t : currentT + 1;
    }
    // Convert from process-sample-relative t to queryStartTime-relative t_second
    const probeFrom = currentT + sampleOffset;
    const probeTo = nextT + sampleOffset;
    if (!hasTraceSamplesInRange(sampleCounts, probeFrom, probeTo)) return null;
    return {
      from: new Date(startMs + probeFrom * 1000).toISOString(),
      to: new Date(startMs + probeTo * 1000).toISOString(),
      label: `${currentT.toFixed(0)}s – ${nextT.toFixed(0)}s`,
    };
  }, [scrubberMode, samples, logEvents, clampedIdx, queryStartTime, sampleCounts, sampleOffset]);

  // Open flamegraph popup for current time window
  const handleShowFlamegraph = useCallback(() => {
    if (!currentTimeWindow) return;
    timeScopedFlamegraph.clear();
    setShowFlamegraphPopup(true);
    timeScopedFlamegraph.fetch(queryId, currentTimeWindow.from, currentTimeWindow.to, queryStartTime);
  }, [queryId, queryStartTime, currentTimeWindow, timeScopedFlamegraph]);

  // Open flamegraph for a specific t value (from hover card click)
  // t is process-sample-relative; add sampleOffset to get queryStartTime-relative
  const handleShowFlamegraphForT = useCallback((t: number) => {
    if (!queryStartTime || !sampleCounts) return;
    const startMs = new Date(queryStartTime).getTime();
    const absT = t + sampleOffset;
    const from = new Date(startMs + absT * 1000).toISOString();
    const to = new Date(startMs + (absT + 1) * 1000).toISOString();
    timeScopedFlamegraph.clear();
    setShowFlamegraphPopup(true);
    timeScopedFlamegraph.fetch(queryId, from, to, queryStartTime);
  }, [queryId, queryStartTime, sampleCounts, sampleOffset, timeScopedFlamegraph]);

  // Esc closes flamegraph popup (capture phase to prevent parent modal from closing)
  useEffect(() => {
    if (!showFlamegraphPopup) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopImmediatePropagation();
        e.preventDefault();
        setShowFlamegraphPopup(false);
      }
    };
    window.addEventListener('keydown', onKey, true); // capture phase
    return () => window.removeEventListener('keydown', onKey, true);
  }, [showFlamegraphPopup]);

  // Reset index on mode change
  const handleModeChange = useCallback((m: ScrubberMode) => {
    setScrubberMode(m);
    setScrubberIdx(0);
  }, []);

  // Keyboard navigation
  useEffect(() => {
    const maxIdx = scrubberItems.length - 1;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'ArrowLeft') {
        setScrubberIdx(prev => Math.max(0, prev - 1));
        e.preventDefault();
      } else if (e.key === 'ArrowRight') {
        setScrubberIdx(prev => Math.min(maxIdx, prev + 1));
        e.preventDefault();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [scrubberItems.length]);

  // Loading state
  if (isLoadingSamples) {
    return (
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '100%', minHeight: 400, color: 'var(--text-muted)',
      }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{
            width: 24, height: 24, borderWidth: 2,
            borderStyle: 'solid', borderColor: 'var(--text-muted) transparent var(--text-muted) transparent',
            borderRadius: '50%', animation: 'spin 1s linear infinite',
            margin: '0 auto 12px',
          }} />
          Loading process samples...
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '100%', minHeight: 400, padding: 40,
      }}>
        <div style={{ textAlign: 'center', maxWidth: 500 }}>
          <div style={{ fontSize: 14, color: '#ef4444', marginBottom: 8 }}>
            Failed to load process samples
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 16, fontFamily: 'monospace' }}>
            {error}
          </div>
          <div style={{ fontSize: 12, color: '#888', lineHeight: 1.6 }}>
            This requires <code style={{ background: '#1a1a2e', padding: '1px 4px', borderRadius: 3 }}>
            tracehouse.processes_history</code> — see{' '}
            <code style={{ background: '#1a1a2e', padding: '1px 4px', borderRadius: 3 }}>
            infra/scripts/setup_sampling.sh</code>
          </div>
          <button
            onClick={fetchSamples}
            style={{
              marginTop: 12, padding: '6px 16px', background: '#1a1a2e',
              border: '1px solid #333', borderRadius: 4, color: '#ccc',
              cursor: 'pointer', fontSize: 12,
            }}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  // No data
  if (samples.length < 2) {
    return (
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '100%', minHeight: 400, padding: 40,
      }}>
        <div style={{ textAlign: 'center', maxWidth: 400 }}>
          <div style={{ fontSize: 14, color: 'var(--text-secondary)', marginBottom: 8 }}>
            No Process Samples
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.6 }}>
            This query ran too fast for the process sampler to capture data,
            or the sampler wasn't active when this query executed.
          </div>
        </div>
      </div>
    );
  }

  // Summary stats
  const peakMem = Math.max(...samples.map(s => s.peak_memory_mb));
  const peakCpu = Math.max(...samples.map(s => s.d_cpu_cores));
  const duration = samples[samples.length - 1].t;
  const totalRows = Math.max(...samples.map(s => s.read_rows));

  const multiHost = hosts.length > 1;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Host tab bar — only shown for multi-host (distributed) queries */}
      {multiHost && (
        <div style={{
          padding: '4px 16px',
          background: 'var(--bg-secondary, #111)',
          borderBottom: '1px solid var(--border-accent, #333)',
          display: 'flex',
          gap: 6,
          fontSize: 10,
          fontFamily: 'monospace',
          flexShrink: 0,
          alignItems: 'center',
        }}>
          <HostTab
            label="All"
            active={selectedHost === null && !stackedView}
            onClick={() => { setSelectedHost(null); setStackedView(false); setScrubberIdx(0); }}
          />
          <HostTab
            label="Split"
            title="Show each host in its own lane for direct comparison"
            active={selectedHost === null && stackedView}
            onClick={() => { setSelectedHost(null); setStackedView(true); setScrubberIdx(0); }}
          />
          <span style={{ color: '#333', margin: '0 2px' }}>│</span>
          {hosts.map(h => (
            <HostTab
              key={h}
              label={h.length > 18 ? h.slice(0, 8) + '...' + h.slice(-8) : h}
              title={h}
              active={selectedHost === h}
              onClick={() => { setSelectedHost(h); setStackedView(false); setScrubberIdx(0); }}
            />
          ))}
          <span style={{ color: '#555', marginLeft: 4 }}>{hosts.length} hosts</span>
        </div>
      )}

      {/* Summary bar */}
      <div style={{
        padding: '6px 16px',
        background: 'var(--bg-secondary, #111)',
        borderBottom: '1px solid var(--border-accent, #333)',
        display: 'flex',
        gap: 16,
        fontSize: 11,
        fontFamily: 'monospace',
        color: '#888',
        flexShrink: 0,
      }}>
        <span>{duration.toFixed(1)}s</span>
        <span style={{ color: '#636EFA' }}>{fmtMB(peakMem)} peak mem</span>
        <span style={{ color: '#FECB52' }}>{peakCpu.toFixed(1)} peak cores</span>
        <span>{totalRows.toLocaleString()} rows</span>
        <span>{samples.length} samples</span>
        {logEvents.length > 0 && <span>{logEvents.length} log events</span>}
      </div>

      {/* 3D Canvas */}
      <div style={{ flex: 1, minHeight: 0, position: 'relative' }}>
        <Canvas
          gl={{ antialias: true, alpha: false }}
          style={{ background: '#0a0a1a' }}
        >
          <XRayScene
            samples={samples}
            highlightTime={highlightTime}
            highlightLabel={highlightLabel}
            sampleCounts={sampleCounts}
            sampleOffset={sampleOffset}
            onShowFlamegraphForT={handleShowFlamegraphForT}
            stackedView={stackedView && selectedHost === null}
            hostSamples={hostSamples}
            hosts={hosts}
          />
        </Canvas>
        {/* Host color legend — shown in stacked mode */}
        {stackedView && selectedHost === null && multiHost && (
          <div style={{
            position: 'absolute', top: 8, right: 8, zIndex: 10,
            background: 'rgba(10, 10, 30, 0.85)',
            border: '1px solid rgba(100, 110, 250, 0.25)',
            borderRadius: 6, padding: '6px 10px',
            fontFamily: 'monospace', fontSize: 10,
          }}>
            {hosts.map((h, i) => {
              const hSamples = hostSamples.get(h) || [];
              const hPeakMem = Math.max(...hSamples.map(s => s.memory_mb), 0);
              const hPeakCpu = Math.max(...hSamples.map(s => s.d_cpu_cores), 0);
              return (
                <div key={h} style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: i < hosts.length - 1 ? 4 : 0 }}>
                  <span style={{
                    width: 10, height: 10, borderRadius: 2, flexShrink: 0,
                    background: '#' + HOST_COLORS[i % HOST_COLORS.length].getHexString(),
                  }} />
                  <span style={{ color: '#aaa', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 150 }}>
                    {h.length > 20 ? h.slice(0, 10) + '…' + h.slice(-8) : h}
                  </span>
                  <span style={{ color: '#666', flexShrink: 0 }}>
                    {fmtMB(hPeakMem)} · {hPeakCpu.toFixed(1)}c
                  </span>
                </div>
              );
            })}
          </div>
        )}
        {/* Flamegraph button — floats above the scrubber in the canvas area */}
        {currentTimeWindow && (
          <button
            onClick={handleShowFlamegraph}
            style={{
              position: 'absolute',
              bottom: 8,
              left: 16,
              padding: '4px 10px',
              fontSize: 10,
              fontFamily: 'monospace',
              borderRadius: 4,
              border: '1px solid #636EFA55',
              background: 'rgba(10,10,26,0.9)',
              color: '#636EFA',
              cursor: 'pointer',
              zIndex: 10,
              whiteSpace: 'nowrap',
            }}
          >
            Flamegraph
          </button>
        )}
      </div>

      {/* Scrubber */}
      <Scrubber
        mode={scrubberMode}
        onModeChange={handleModeChange}
        logEvents={logEvents}
        samples={samples}
        activeIdx={scrubberIdx}
        onChange={setScrubberIdx}
      />

      {/* Time-scoped flamegraph popup */}
      {showFlamegraphPopup && (
        <div style={{
          position: 'fixed',
          top: 0, left: 0, right: 0, bottom: 0,
          zIndex: 9999,
          display: 'flex',
          flexDirection: 'column',
          background: '#000',
        }}>
          <div style={{
            padding: '10px 16px',
            background: '#111',
            borderBottom: '1px solid #333',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}>
            <span style={{ color: '#ccc', fontSize: 13, fontFamily: 'monospace' }}>
              Flamegraph @ {currentTimeWindow?.label}
            </span>
            <button
              onClick={() => setShowFlamegraphPopup(false)}
              style={{
                padding: '4px 12px', fontSize: 12, borderRadius: 4,
                border: '1px solid #444', background: '#222', color: '#ccc', cursor: 'pointer',
              }}
            >
              Close (Esc)
            </button>
          </div>
          <div style={{ flex: 1, minHeight: 0 }}>
            <SpeedscopeViewer
              folded={timeScopedFlamegraph.folded}
              isLoading={timeScopedFlamegraph.isLoading}
              error={timeScopedFlamegraph.error}
              unavailableReason={timeScopedFlamegraph.unavailableReason}
              onRefresh={() => {
                if (currentTimeWindow) {
                  timeScopedFlamegraph.fetch(queryId, currentTimeWindow.from, currentTimeWindow.to, queryStartTime);
                }
              }}
            />
          </div>
        </div>
      )}
    </div>
  );
};
