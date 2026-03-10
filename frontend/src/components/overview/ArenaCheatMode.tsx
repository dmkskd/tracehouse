/**
 * ArenaCheatMode — FPS "cheat" mode for the 3D Resource Arena
 *
 * Activates with 'X' key. Flips the camera 180° so blocks fly TOWARD you.
 * Click to shoot lasers that destroy blocks with particle explosions.
 * WASD to move, mouse to aim. Pure fun.
 */

import React, { useRef, useState, useCallback, useEffect } from 'react';
import { useFrame, useThree } from '@react-three/fiber';
import * as THREE from 'three';
import {
  Ts, Cs, MIN_DIM, LANE_GAP, DECK_GAP,
  DECK_ORDER, type Deck, type BlockEntry,
} from './arena-types';

/* ── Types ──────────────────────────────────────────── */

/** Per-block health + cheat speed stored outside React state for perf */
export interface BlockCheatInfo {
  maxHp: number;
  hp: number;
  speedMultiplier: number;  // random 5-8x for cheat mode
  xOffset: number;          // extra X offset that accumulates over time
  damageFlash: number;      // timestamp of last hit (for flash effect)
}

export interface CheatModeState {
  active: boolean;
  score: number;
  cpuFreed: number;   // total CPU cores freed
  memFreed: number;   // total bytes freed
  combo: number;
  maxCombo: number;
  destroyedIds: Set<string>;
  lastHitTime: number;
  /** Floating reward text popups */
  rewardPopups: RewardPopup[];
}

interface RewardPopup {
  id: number;
  text: string;
  color: string;
  birth: number; // Date.now()
  x: number; // screen % position
  y: number;
}

/* ── Constants ──────────────────────────────────────── */

const LASER_SPEED = 45;
const LASER_LENGTH = 1.2;
const LASER_LIFETIME = 2.0;
const COMBO_TIMEOUT = 1500;
const PARTICLE_COUNT = 28;
const PARTICLE_LIFETIME = 1.2;
const PARTICLE_SPEED = 8;
const REWARD_LIFETIME = 1500; // ms

/** In cheat mode blocks rush toward camera at this multiplier of normal Ts */
export const CHEAT_SPEED_BASE = 6;  // 6-10x faster (randomized per block)
export const CHEAT_SPEED_VARIANCE = 4;

/* ── Shared geometries ──────────────────────────────── */

const _laserGeo = new THREE.CylinderGeometry(0.02, 0.02, LASER_LENGTH, 6);
_laserGeo.rotateX(Math.PI / 2);

const _particleGeo = new THREE.BoxGeometry(0.06, 0.06, 0.06);

/* ── Laser projectile ───────────────────────────────── */

interface Laser {
  id: number;
  position: THREE.Vector3;
  direction: THREE.Vector3;
  birth: number;
  color: THREE.Color;
  alive: boolean;
}

/* ── Explosion particle ─────────────────────────────── */

interface Particle {
  position: THREE.Vector3;
  velocity: THREE.Vector3;
  birth: number;
  color: THREE.Color;
  scale: number;
  alive: boolean;
}

/* ── Hit flash state ────────────────────────────────── */

interface HitFlash {
  position: THREE.Vector3;
  birth: number;
  color: THREE.Color;
  alive: boolean;
}

/* ── Block HP computation ───────────────────────────── */

/** Compute hit points for a block: bigger blocks need more shots */
export function computeBlockHp(entry: BlockEntry): number {
  const cpuFactor = Math.max(1, entry.cpu);  // 1 core = 1 HP base
  const memGB = entry.mem / 1073741824;
  const memFactor = Math.max(0, Math.log2(1 + memGB) * 0.5);
  // 1-2 shots for small, 3-5 for medium, 6+ for heavy
  return Math.max(1, Math.round(cpuFactor + memFactor));
}

/** Get or create cheat info for a block */
export function getOrCreateCheatInfo(
  map: Map<string, BlockCheatInfo>,
  entry: BlockEntry,
): BlockCheatInfo {
  let info = map.get(entry.id);
  if (!info) {
    const hp = computeBlockHp(entry);
    info = {
      maxHp: hp,
      hp,
      speedMultiplier: CHEAT_SPEED_BASE + Math.random() * CHEAT_SPEED_VARIANCE,
      xOffset: 0,
      damageFlash: 0,
    };
    map.set(entry.id, info);
  }
  return info;
}

/* ── FPS Camera Controller ──────────────────────────── */

export function FPSCamera({ active }: { active: boolean }) {
  const { camera, gl } = useThree();
  const euler = useRef(new THREE.Euler(0, 0, 0, 'YXZ'));
  const velocity = useRef(new THREE.Vector3());
  const keys = useRef(new Set<string>());
  const isLocked = useRef(false);
  const savedState = useRef<{ pos: THREE.Vector3; quat: THREE.Quaternion } | null>(null);

  useEffect(() => {
    if (!active) {
      if (savedState.current) {
        camera.position.copy(savedState.current.pos);
        camera.quaternion.copy(savedState.current.quat);
        savedState.current = null;
      }
      if (document.pointerLockElement) {
        document.exitPointerLock();
      }
      isLocked.current = false;
      return;
    }

    savedState.current = {
      pos: camera.position.clone(),
      quat: camera.quaternion.clone(),
    };

    // Stand at ~30s into the past (-X side), face +X (toward NOW).
    // Blocks naturally drift toward -X as they age, so they fly toward us.
    camera.position.set(-120 * Ts, 1.5, -1.5);
    camera.lookAt(10, 1.2, -1.5); // look toward NOW (+X)
    euler.current.setFromQuaternion(camera.quaternion, 'YXZ');

    const onMouseMove = (e: MouseEvent) => {
      if (!document.pointerLockElement) return;
      const sensitivity = 0.002;
      euler.current.y -= e.movementX * sensitivity;
      euler.current.x -= e.movementY * sensitivity;
      euler.current.x = Math.max(-Math.PI / 2.5, Math.min(Math.PI / 2.5, euler.current.x));
      camera.quaternion.setFromEuler(euler.current);
    };

    const onKeyDown = (e: KeyboardEvent) => { keys.current.add(e.key.toLowerCase()); };
    const onKeyUp = (e: KeyboardEvent) => { keys.current.delete(e.key.toLowerCase()); };
    const onLockChange = () => { isLocked.current = !!document.pointerLockElement; };

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('keydown', onKeyDown);
    document.addEventListener('keyup', onKeyUp);
    document.addEventListener('pointerlockchange', onLockChange);

    const canvas = gl.domElement;
    const requestLock = () => {
      if (!document.pointerLockElement) canvas.requestPointerLock();
    };
    canvas.addEventListener('click', requestLock);
    setTimeout(() => canvas.requestPointerLock(), 100);

    return () => {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('keydown', onKeyDown);
      document.removeEventListener('keyup', onKeyUp);
      document.removeEventListener('pointerlockchange', onLockChange);
      canvas.removeEventListener('click', requestLock);
      if (document.pointerLockElement) document.exitPointerLock();
    };
  }, [active, camera, gl]);

  useFrame((_, delta) => {
    if (!active) return;

    const speed = 8;
    const dir = new THREE.Vector3();
    const right = new THREE.Vector3();
    const forward = new THREE.Vector3();

    camera.getWorldDirection(forward);
    forward.y = 0;
    forward.normalize();
    right.crossVectors(forward, new THREE.Vector3(0, 1, 0)).normalize();

    if (keys.current.has('w')) dir.add(forward);
    if (keys.current.has('s')) dir.sub(forward);
    if (keys.current.has('a')) dir.sub(right);
    if (keys.current.has('d')) dir.add(right);
    if (keys.current.has(' ')) dir.y += 1;
    if (keys.current.has('shift')) dir.y -= 1;

    if (dir.length() > 0) {
      dir.normalize();
      velocity.current.lerp(dir.multiplyScalar(speed), 0.15);
    } else {
      velocity.current.multiplyScalar(0.85);
    }

    camera.position.add(velocity.current.clone().multiplyScalar(delta));
    camera.position.y = Math.max(0.5, Math.min(8, camera.position.y));
  });

  return null;
}

/* ── Laser System (3D scene component) ──────────────── */

function LaserSystem({ lasers }: { lasers: Laser[] }) {
  const meshRefs = useRef<Map<number, THREE.Mesh>>(new Map());
  const matRefs = useRef<Map<number, THREE.MeshBasicMaterial>>(new Map());

  useFrame(() => {
    const now = performance.now() / 1000;
    for (const laser of lasers) {
      const mesh = meshRefs.current.get(laser.id);
      const mat = matRefs.current.get(laser.id);
      if (!mesh || !mat) continue;

      if (!laser.alive) { mesh.visible = false; continue; }

      mesh.visible = true;
      mesh.position.copy(laser.position);
      mesh.quaternion.setFromUnitVectors(new THREE.Vector3(0, 0, 1), laser.direction);

      const age = now - laser.birth;
      const fade = Math.max(0, 1 - age / LASER_LIFETIME);
      mat.opacity = fade * 0.9;
    }
  });

  return (
    <>
      {lasers.map(laser => (
        <mesh
          key={laser.id}
          ref={el => { if (el) meshRefs.current.set(laser.id, el); else meshRefs.current.delete(laser.id); }}
          geometry={_laserGeo}
          raycast={() => {}}
        >
          <meshBasicMaterial
            ref={el => { if (el) matRefs.current.set(laser.id, el); else matRefs.current.delete(laser.id); }}
            color={laser.color}
            transparent opacity={0.9} depthWrite={false}
          />
        </mesh>
      ))}
    </>
  );
}

/* ── Explosion Particles (3D scene component) ───────── */

function ExplosionSystem({ particles }: { particles: Particle[] }) {
  const meshRefs = useRef<Map<number, THREE.Mesh>>(new Map());
  const matRefs = useRef<Map<number, THREE.MeshBasicMaterial>>(new Map());

  useFrame((_, delta) => {
    const now = performance.now() / 1000;
    for (let i = 0; i < particles.length; i++) {
      const p = particles[i];
      const mesh = meshRefs.current.get(i);
      const mat = matRefs.current.get(i);
      if (!mesh || !mat || !p.alive) { if (mesh) mesh.visible = false; continue; }

      const age = now - p.birth;
      if (age > PARTICLE_LIFETIME) { p.alive = false; mesh.visible = false; continue; }

      mesh.visible = true;
      p.velocity.y -= 9.8 * delta;
      p.position.add(p.velocity.clone().multiplyScalar(delta));
      mesh.position.copy(p.position);
      mesh.rotation.x += delta * 8;
      mesh.rotation.z += delta * 5;

      const life = 1 - age / PARTICLE_LIFETIME;
      const s = p.scale * life;
      mesh.scale.set(s, s, s);
      mat.opacity = life * 0.8;
    }
  });

  return (
    <>
      {particles.map((p, i) => (
        <mesh
          key={i}
          ref={el => { if (el) meshRefs.current.set(i, el); else meshRefs.current.delete(i); }}
          geometry={_particleGeo}
          raycast={() => {}}
        >
          <meshBasicMaterial
            ref={el => { if (el) matRefs.current.set(i, el); else matRefs.current.delete(i); }}
            color={p.color} transparent opacity={0.8} depthWrite={false}
          />
        </mesh>
      ))}
    </>
  );
}

/* ── Hit Flash Ring ─────────────────────────────────── */

const _ringGeo = new THREE.RingGeometry(0.1, 0.5, 16);

function HitFlashSystem({ flashes }: { flashes: HitFlash[] }) {
  const meshRefs = useRef<Map<number, THREE.Mesh>>(new Map());
  const matRefs = useRef<Map<number, THREE.MeshBasicMaterial>>(new Map());

  useFrame(() => {
    const now = performance.now() / 1000;
    for (let i = 0; i < flashes.length; i++) {
      const f = flashes[i];
      const mesh = meshRefs.current.get(i);
      const mat = matRefs.current.get(i);
      if (!mesh || !mat || !f.alive) { if (mesh) mesh.visible = false; continue; }

      const age = now - f.birth;
      if (age > 0.4) { f.alive = false; mesh.visible = false; continue; }

      mesh.visible = true;
      mesh.position.copy(f.position);
      const expand = 1 + age * 8;
      mesh.scale.set(expand, expand, expand);
      mat.opacity = (1 - age / 0.4) * 0.6;
    }
  });

  return (
    <>
      {flashes.map((f, i) => (
        <mesh
          key={i}
          ref={el => { if (el) meshRefs.current.set(i, el); else meshRefs.current.delete(i); }}
          geometry={_ringGeo}
          raycast={() => {}}
        >
          <meshBasicMaterial
            ref={el => { if (el) matRefs.current.set(i, el); else matRefs.current.delete(i); }}
            color={f.color} transparent opacity={0.6} depthWrite={false} side={THREE.DoubleSide}
          />
        </mesh>
      ))}
    </>
  );
}

/* ── Muzzle flash light ─────────────────────────────── */

function MuzzleFlash({ active, position }: { active: boolean; position: THREE.Vector3 }) {
  const ref = useRef<THREE.PointLight>(null);
  const birthRef = useRef(0);

  useFrame(() => {
    if (!ref.current) return;
    const now = performance.now() / 1000;
    if (active) birthRef.current = now;
    const age = now - birthRef.current;
    ref.current.intensity = age < 0.08 ? 3 * (1 - age / 0.08) : 0;
  });

  return <pointLight ref={ref} position={position.toArray()} color="#00ffff" distance={8} />;
}

/* ── Shooting logic hook ────────────────────────────── */

export function useShootingSystem() {
  const lasersRef = useRef<Laser[]>([]);
  const particlesRef = useRef<Particle[]>([]);
  const flashesRef = useRef<HitFlash[]>([]);
  const nextId = useRef(0);
  const [, forceUpdate] = useState(0);

  const shoot = useCallback((origin: THREE.Vector3, direction: THREE.Vector3) => {
    const laser: Laser = {
      id: nextId.current++,
      position: origin.clone(),
      direction: direction.clone().normalize(),
      birth: performance.now() / 1000,
      color: new THREE.Color('#00ffff'),
      alive: true,
    };
    lasersRef.current.push(laser);
    lasersRef.current = lasersRef.current.filter(l => l.alive);
    forceUpdate(v => v + 1);
  }, []);

  const spawnExplosion = useCallback((position: THREE.Vector3, color: string, big = false) => {
    const now = performance.now() / 1000;
    const baseColor = new THREE.Color(color);
    const count = big ? PARTICLE_COUNT * 2 : PARTICLE_COUNT;
    const speed = big ? PARTICLE_SPEED * 1.5 : PARTICLE_SPEED;

    for (let i = 0; i < count; i++) {
      const vel = new THREE.Vector3(
        (Math.random() - 0.5) * 2,
        Math.random() * 1.5 + 0.5,
        (Math.random() - 0.5) * 2,
      ).normalize().multiplyScalar(speed * (0.3 + Math.random() * 0.7));

      const pColor = baseColor.clone();
      pColor.offsetHSL((Math.random() - 0.5) * 0.1, 0, (Math.random() - 0.5) * 0.2);

      particlesRef.current.push({
        position: position.clone().add(new THREE.Vector3(
          (Math.random() - 0.5) * 0.3,
          (Math.random() - 0.5) * 0.3,
          (Math.random() - 0.5) * 0.3,
        )),
        velocity: vel,
        birth: now,
        color: pColor,
        scale: big ? 0.8 + Math.random() * 2.0 : 0.5 + Math.random() * 1.5,
        alive: true,
      });
    }

    flashesRef.current.push({
      position: position.clone(),
      birth: now,
      color: baseColor,
      alive: true,
    });

    particlesRef.current = particlesRef.current.filter(p => p.alive);
    flashesRef.current = flashesRef.current.filter(f => f.alive);
    forceUpdate(v => v + 1);
  }, []);

  /** Small spark effect for damage (not kill) */
  const spawnSpark = useCallback((position: THREE.Vector3, color: string) => {
    const now = performance.now() / 1000;
    const baseColor = new THREE.Color(color);
    for (let i = 0; i < 6; i++) {
      const vel = new THREE.Vector3(
        (Math.random() - 0.5) * 2,
        Math.random() * 2,
        (Math.random() - 0.5) * 2,
      ).normalize().multiplyScalar(3 + Math.random() * 2);

      particlesRef.current.push({
        position: position.clone(),
        velocity: vel,
        birth: now,
        color: baseColor.clone().offsetHSL(0, 0, 0.2),
        scale: 0.3 + Math.random() * 0.5,
        alive: true,
      });
    }
    forceUpdate(v => v + 1);
  }, []);

  const updateLasers = useCallback((delta: number) => {
    for (const laser of lasersRef.current) {
      if (!laser.alive) continue;
      laser.position.add(laser.direction.clone().multiplyScalar(LASER_SPEED * delta));
      if (laser.position.length() > 50) laser.alive = false;
    }
  }, []);

  return {
    lasers: lasersRef.current,
    particles: particlesRef.current,
    flashes: flashesRef.current,
    shoot,
    spawnExplosion,
    spawnSpark,
    updateLasers,
    lasersRef,
  };
}

/* ── Cheat mode state hook ─────────────────────────── */

export function useCheatMode(opts: {
  containerRef: React.RefObject<HTMLElement | null>;
  clearSelection: () => void;
}) {
  const [cheatState, setCheatState] = useState<CheatModeState>({
    active: false, score: 0, cpuFreed: 0, memFreed: 0, combo: 0, maxCombo: 0,
    destroyedIds: new Set(), lastHitTime: 0, rewardPopups: [],
  });
  const cheatInfoMapRef = useRef<Map<string, BlockCheatInfo>>(new Map());
  const shootingSystem = useShootingSystem();
  const rewardIdRef = useRef(0);
  const [aimedEntry, setAimedEntry] = useState<BlockEntry | null>(null);

  const toggleCheatMode = useCallback(() => {
    setCheatState(prev => {
      if (prev.active) {
        cheatInfoMapRef.current.clear();
        return { active: false, score: 0, cpuFreed: 0, memFreed: 0, combo: 0, maxCombo: 0, destroyedIds: new Set(), lastHitTime: 0, rewardPopups: [] };
      }
      opts.clearSelection();
      const el = opts.containerRef.current;
      if (el && !document.fullscreenElement) {
        el.requestFullscreen().catch(() => {});
      }
      return { ...prev, active: true, score: 0, cpuFreed: 0, memFreed: 0, combo: 0, maxCombo: 0, destroyedIds: new Set(), lastHitTime: 0, rewardPopups: [] };
    });
  }, [opts]);

  // Clean expired reward popups periodically
  useEffect(() => {
    if (!cheatState.active) return;
    const iv = setInterval(() => {
      const now = Date.now();
      setCheatState(prev => {
        const fresh = prev.rewardPopups.filter(p => now - p.birth < 1500);
        if (fresh.length === prev.rewardPopups.length) return prev;
        return { ...prev, rewardPopups: fresh };
      });
    }, 500);
    return () => clearInterval(iv);
  }, [cheatState.active]);

  const handleAimUpdate = useCallback((entry: BlockEntry | null) => {
    setAimedEntry(entry);
  }, []);

  const handleCheatHit = useCallback((entry: BlockEntry, hitPosition: THREE.Vector3, killed: boolean) => {
    const now = Date.now();
    const isLive = entry.endTime === null;

    if (killed) {
      setCheatState(prev => {
        if (prev.destroyedIds.has(entry.id)) return prev;
        const newDestroyed = new Set(prev.destroyedIds);
        newDestroyed.add(entry.id);

        if (!isLive) return { ...prev, destroyedIds: newDestroyed };

        const comboAlive = now - prev.lastHitTime < 1500;
        const newCombo = comboAlive ? prev.combo + 1 : 1;
        const comboMultiplier = Math.min(newCombo, 10);

        const cpuBonus = Math.round(entry.cpu * 50);
        const memGB = entry.mem / 1073741824;
        const memBonus = Math.round(Math.log2(1 + memGB) * 30);
        const baseScore = 100 + cpuBonus + memBonus;
        const points = baseScore * comboMultiplier;

        const cpuStr = entry.cpu.toFixed(1);
        const memStr = memGB >= 1 ? `${memGB.toFixed(1)} GB` : `${(entry.mem / 1048576).toFixed(0)} MB`;
        const rewardText = `+${points} · freed ${cpuStr} CPU · ${memStr} RAM`;
        const popup = {
          id: rewardIdRef.current++,
          text: rewardText,
          color: '#4ade80',
          birth: now,
          x: 40 + Math.random() * 20,
          y: 55 + Math.random() * 10,
        };

        return {
          ...prev,
          destroyedIds: newDestroyed,
          score: prev.score + points,
          cpuFreed: prev.cpuFreed + entry.cpu,
          memFreed: prev.memFreed + entry.mem,
          combo: newCombo,
          maxCombo: Math.max(prev.maxCombo, newCombo),
          lastHitTime: now,
          rewardPopups: [...prev.rewardPopups, popup],
        };
      });

      shootingSystem.spawnExplosion(hitPosition, entry.color, true);
    } else {
      shootingSystem.spawnSpark(hitPosition, entry.color);
    }
  }, [shootingSystem]);

  /**
   * Call from the parent's keydown handler.
   * Returns true if the key was consumed by cheat mode (so parent should skip its own handling).
   */
  const handleKeyDown = useCallback((e: KeyboardEvent): boolean => {
    if (cheatState.active) {
      if (e.key === 'x') { toggleCheatMode(); return true; }
      if (e.key === 'Escape' && !document.pointerLockElement) { toggleCheatMode(); return true; }
      return true; // swallow all keys while cheat is active
    }
    if (e.key === 'x') { toggleCheatMode(); return true; }
    return false;
  }, [cheatState.active, toggleCheatMode]);

  return {
    cheatState,
    cheatInfoMap: cheatInfoMapRef.current,
    shootingSystem,
    aimedEntry,
    toggleCheatMode,
    handleCheatHit,
    handleAimUpdate,
    handleKeyDown,
  };
}

/* ── Collision detection (AABB raycasting) ──────────── */

function CheatModeShootingBridge({ entries, cheatState, cheatInfoMap, onHit, onAimUpdate }: {
  entries: BlockEntry[];
  cheatState: CheatModeState;
  cheatInfoMap: Map<string, BlockCheatInfo>;
  onHit: (entry: BlockEntry, position: THREE.Vector3, killed: boolean) => void;
  onAimUpdate: (entry: BlockEntry | null) => void;
}) {
  const { camera } = useThree();
  const raycaster = useRef(new THREE.Raycaster());
  const lastShootRef = useRef(0);

  // Build AABB for each visible block (same math as LiveBlock + cheat offset)
  const getBlockAABBs = useCallback((): { entry: BlockEntry; box: THREE.Box3 }[] => {
    const now = Date.now();
    const results: { entry: BlockEntry; box: THREE.Box3 }[] = [];

    for (const entry of entries) {
      if (cheatState.destroyedIds.has(entry.id)) continue;

      const running = entry.endTime === null;
      const baseY = DECK_ORDER.indexOf(entry.deck as Deck) * DECK_GAP;
      const laneZ = -entry.lane * LANE_GAP;
      const cpuH = Math.max(MIN_DIM, entry.cpu * Cs);
      const memGB = entry.mem / 1073741824;
      const memD = Math.max(MIN_DIM * 3, Math.log2(1 + memGB) * 0.4);
      const ci = cheatInfoMap.get(entry.id);

      const GAP = 0.03;
      let rightX = (running ? 0 : -((now - entry.endTime!) / 1000) * Ts) - GAP;
      let leftX = -((now - entry.startTime) / 1000) * Ts + GAP;
      if (ci) { rightX += ci.xOffset; leftX += ci.xOffset; }

      const width = Math.max(MIN_DIM, rightX - leftX);
      const centerX = (leftX + rightX) / 2;
      const py = baseY + cpuH / 2;

      results.push({
        entry,
        box: new THREE.Box3(
          new THREE.Vector3(centerX - width / 2, py - cpuH / 2, laneZ - memD / 2),
          new THREE.Vector3(centerX + width / 2, py + cpuH / 2, laneZ + memD / 2),
        ),
      });
    }
    return results;
  }, [entries, cheatState.destroyedIds, cheatInfoMap]);

  // Aim detection + shooting
  useEffect(() => {
    if (!cheatState.active) return;

    const onMouseDown = (e: MouseEvent) => {
      if (e.button !== 0 || !document.pointerLockElement) return;
      const now = performance.now();
      if (now - lastShootRef.current < 100) return;
      lastShootRef.current = now;

      raycaster.current.setFromCamera(new THREE.Vector2(0, 0), camera);
      const ray = raycaster.current.ray;
      const aabbs = getBlockAABBs();

      let closestDist = Infinity;
      let closestHit: { entry: BlockEntry; point: THREE.Vector3 } | null = null;
      for (const { entry, box } of aabbs) {
        const hitPoint = new THREE.Vector3();
        if (ray.intersectBox(box, hitPoint)) {
          const dist = hitPoint.distanceTo(camera.position);
          if (dist < closestDist) {
            closestDist = dist;
            closestHit = { entry, point: hitPoint };
          }
        }
      }

      if (closestHit) {
        const ci = getOrCreateCheatInfo(cheatInfoMap, closestHit.entry);
        ci.hp--;
        ci.damageFlash = performance.now() / 1000;
        const killed = ci.hp <= 0;
        onHit(closestHit.entry, closestHit.point, killed);
      }
    };

    window.addEventListener('mousedown', onMouseDown);
    return () => window.removeEventListener('mousedown', onMouseDown);
  }, [cheatState.active, camera, getBlockAABBs, onHit, cheatInfoMap]);

  // Aim tracking — update which block is under crosshair each frame
  useFrame(() => {
    if (!cheatState.active) return;
    raycaster.current.setFromCamera(new THREE.Vector2(0, 0), camera);
    const ray = raycaster.current.ray;
    const aabbs = getBlockAABBs();

    let closestDist = Infinity;
    let aimed: BlockEntry | null = null;
    for (const { entry, box } of aabbs) {
      const hitPoint = new THREE.Vector3();
      if (ray.intersectBox(box, hitPoint)) {
        const dist = hitPoint.distanceTo(camera.position);
        if (dist < closestDist) { closestDist = dist; aimed = entry; }
      }
    }
    onAimUpdate(aimed);
  });

  return null;
}

/* ── Cheat Scene Elements (all 3D cheat stuff) ─────── */

export function CheatSceneElements({ entries, cheatState, cheatInfoMap, shooting, onCheatHit, onAimUpdate }: {
  entries: BlockEntry[];
  cheatState: CheatModeState;
  cheatInfoMap: Map<string, BlockCheatInfo>;
  shooting: ReturnType<typeof useShootingSystem>;
  onCheatHit: (entry: BlockEntry, position: THREE.Vector3, killed: boolean) => void;
  onAimUpdate: (entry: BlockEntry | null) => void;
}) {
  const { camera } = useThree();
  const muzzlePos = useRef(new THREE.Vector3());
  const [muzzleActive, setMuzzleActive] = useState(false);

  // Update muzzle position to camera + forward offset
  useFrame(() => {
    const dir = new THREE.Vector3();
    camera.getWorldDirection(dir);
    muzzlePos.current.copy(camera.position).add(dir.multiplyScalar(0.5));
  });

  // Shoot laser on click
  useEffect(() => {
    const onMouseDown = (e: MouseEvent) => {
      if (e.button !== 0 || !document.pointerLockElement) return;

      const dir = new THREE.Vector3();
      camera.getWorldDirection(dir);
      const origin = camera.position.clone().add(dir.clone().multiplyScalar(0.5));
      shooting.shoot(origin, dir);
      setMuzzleActive(true);
      setTimeout(() => setMuzzleActive(false), 80);
    };

    window.addEventListener('mousedown', onMouseDown);
    return () => window.removeEventListener('mousedown', onMouseDown);
  }, [shooting, camera]);

  // Update laser positions each frame
  useFrame((_, delta) => {
    shooting.updateLasers(delta);
  });

  return (
    <>
      <FPSCamera active={cheatState.active} />
      <LaserSystem lasers={shooting.lasers} />
      <ExplosionSystem particles={shooting.particles} />
      <HitFlashSystem flashes={shooting.flashes} />
      <MuzzleFlash active={muzzleActive} position={muzzlePos.current} />
      <CheatModeShootingBridge
        entries={entries}
        cheatState={cheatState}
        cheatInfoMap={cheatInfoMap}
        onHit={onCheatHit}
        onAimUpdate={onAimUpdate}
      />
    </>
  );
}

/* ── Format helpers for rewards ─────────────────────── */

function fmtBytes(b: number): string {
  if (b < 1024) return `${b} B`;
  if (b < 1048576) return `${(b / 1024).toFixed(0)} KB`;
  if (b < 1073741824) return `${(b / 1048576).toFixed(1)} MB`;
  return `${(b / 1073741824).toFixed(2)} GB`;
}

/* ── HUD Overlay (DOM) ──────────────────────────────── */

const panelFont = "'JetBrains Mono', 'Fira Code', 'SF Mono', monospace";

export function CheatHUD({ state, onExit, aimedEntry, cheatInfoMap }: {
  state: CheatModeState;
  onExit: () => void;
  aimedEntry?: BlockEntry | null;
  cheatInfoMap?: Map<string, BlockCheatInfo>;
}) {
  const [locked, setLocked] = useState(false);

  useEffect(() => {
    if (!state.active) return;
    const update = () => setLocked(!!document.pointerLockElement);
    update();
    document.addEventListener('pointerlockchange', update);
    return () => document.removeEventListener('pointerlockchange', update);
  }, [state.active]);

  if (!state.active) return null;

  const comboFresh = Date.now() - state.lastHitTime < COMBO_TIMEOUT;
  const now = Date.now();

  return (
    <div style={{
      position: 'absolute', inset: 0, zIndex: 30, pointerEvents: 'none',
      fontFamily: panelFont,
    }}>
      {/* Crosshair */}
      <div style={{
        position: 'absolute', top: '50%', left: '50%',
        transform: 'translate(-50%, -50%)',
      }}>
        <div style={{
          width: 40, height: 40,
          border: '1.5px solid rgba(0,255,255,0.4)',
          borderRadius: '50%', position: 'absolute', top: -20, left: -20,
          animation: 'cheat-crosshair-spin 4s linear infinite',
        }} />
        <div style={{
          width: 4, height: 4, background: '#00ffff', borderRadius: '50%',
          position: 'absolute', top: -2, left: -2,
          boxShadow: '0 0 8px #00ffff, 0 0 16px #00ffff40',
        }} />
        <div style={{ position: 'absolute', top: -14, left: -0.5, width: 1, height: 10, background: 'rgba(0,255,255,0.5)' }} />
        <div style={{ position: 'absolute', top: 5, left: -0.5, width: 1, height: 10, background: 'rgba(0,255,255,0.5)' }} />
        <div style={{ position: 'absolute', top: -0.5, left: -14, width: 10, height: 1, background: 'rgba(0,255,255,0.5)' }} />
        <div style={{ position: 'absolute', top: -0.5, left: 5, width: 10, height: 1, background: 'rgba(0,255,255,0.5)' }} />
      </div>

      {/* Aimed target info */}
      {aimedEntry && (() => {
        const isLive = aimedEntry.endTime === null;
        const ci = cheatInfoMap?.get(aimedEntry.id);
        const hp = ci?.hp ?? 1;
        const maxHp = ci?.maxHp ?? 1;
        const hpPct = Math.max(0, hp / maxHp * 100);
        const memGB = aimedEntry.mem / 1073741824;
        const memStr = memGB >= 1 ? `${memGB.toFixed(1)} GB` : `${(aimedEntry.mem / 1048576).toFixed(0)} MB`;
        const cpuStr = aimedEntry.cpu.toFixed(1);
        const reward = isLive ? 100 + Math.round(aimedEntry.cpu * 50) + Math.round(Math.log2(1 + memGB) * 30) : 0;
        return (
          <div style={{
            position: 'absolute', top: '55%', left: '50%',
            transform: 'translateX(-50%)',
            textAlign: 'center', minWidth: 200,
          }}>
            {/* Target name */}
            <div style={{
              fontSize: 10, letterSpacing: 1.5, fontWeight: 700,
              color: isLive ? '#ff4444' : 'rgba(255,255,255,0.3)',
              marginBottom: 4,
            }}>
              {isLive ? '● LIVE' : '○ DEAD'} &middot; {aimedEntry.kind}
            </div>
            <div style={{
              fontSize: 11, color: 'rgba(255,255,255,0.6)',
              overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              maxWidth: 240, margin: '0 auto',
            }}>
              {aimedEntry.tableHint !== 'unknown' ? aimedEntry.tableHint : aimedEntry.label}
            </div>
            {/* HP bar */}
            <div style={{
              width: 160, height: 6, borderRadius: 3, margin: '4px auto',
              background: 'rgba(255,255,255,0.1)', overflow: 'hidden',
            }}>
              <div style={{
                height: '100%', borderRadius: 3,
                width: `${hpPct}%`,
                background: hpPct > 50 ? '#4ade80' : hpPct > 25 ? '#fbbf24' : '#ef4444',
                transition: 'width 0.1s',
                boxShadow: `0 0 6px ${hpPct > 50 ? '#4ade80' : '#ef4444'}60`,
              }} />
            </div>
            <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.35)' }}>
              HP {hp}/{maxHp} &middot; {cpuStr} CPU &middot; {memStr} RAM
            </div>
            {/* Reward preview */}
            {isLive && (
              <div style={{
                fontSize: 12, fontWeight: 700, letterSpacing: 1,
                color: '#4ade80', marginTop: 2,
                textShadow: '0 0 8px rgba(74,222,128,0.4)',
              }}>
                +{reward} pts &middot; free {cpuStr} CPU + {memStr}
              </div>
            )}
            {!isLive && (
              <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.2)', marginTop: 2 }}>
                no reward (already finished)
              </div>
            )}
          </div>
        );
      })()}

      {/* Score panel */}
      <div style={{ position: 'absolute', top: 50, right: 20, textAlign: 'right' }}>
        <div style={{ fontSize: 11, color: 'rgba(0,255,255,0.5)', letterSpacing: 2, fontWeight: 700 }}>SCORE</div>
        <div style={{
          fontSize: 36, color: '#00ffff', fontWeight: 700,
          textShadow: '0 0 20px rgba(0,255,255,0.5), 0 0 40px rgba(0,255,255,0.2)',
          fontVariantNumeric: 'tabular-nums',
        }}>{state.score.toLocaleString()}</div>

        {/* CPU freed */}
        <div style={{ marginTop: 8, display: 'flex', alignItems: 'baseline', justifyContent: 'flex-end', gap: 6 }}>
          <span style={{ fontSize: 9, color: 'rgba(96,165,250,0.5)', letterSpacing: 1.5, fontWeight: 700 }}>CPU FREED</span>
          <span style={{
            fontSize: 20, color: '#60a5fa', fontWeight: 700, fontVariantNumeric: 'tabular-nums',
            textShadow: '0 0 12px rgba(96,165,250,0.4)',
          }}>{state.cpuFreed.toFixed(1)}</span>
          <span style={{ fontSize: 10, color: 'rgba(96,165,250,0.4)' }}>cores</span>
        </div>

        {/* Memory freed */}
        <div style={{ marginTop: 4, display: 'flex', alignItems: 'baseline', justifyContent: 'flex-end', gap: 6 }}>
          <span style={{ fontSize: 9, color: 'rgba(167,139,250,0.5)', letterSpacing: 1.5, fontWeight: 700 }}>MEM FREED</span>
          <span style={{
            fontSize: 20, color: '#a78bfa', fontWeight: 700, fontVariantNumeric: 'tabular-nums',
            textShadow: '0 0 12px rgba(167,139,250,0.4)',
          }}>{fmtBytes(state.memFreed)}</span>
        </div>
      </div>

      {/* Combo */}
      {state.combo > 1 && comboFresh && (
        <div style={{
          position: 'absolute', top: '35%', left: '50%',
          transform: 'translate(-50%, -50%)', textAlign: 'center',
          animation: 'cheat-combo-pop 0.3s ease-out',
        }}>
          <div style={{
            fontSize: 28, fontWeight: 900, letterSpacing: 3,
            color: state.combo >= 5 ? '#ff4444' : state.combo >= 3 ? '#ffaa00' : '#00ffff',
            textShadow: `0 0 20px ${state.combo >= 5 ? '#ff4444' : '#00ffff'}80`,
          }}>
            {state.combo}x COMBO
          </div>
          {state.combo >= 5 && (
            <div style={{ fontSize: 12, color: '#ff4444', letterSpacing: 4, marginTop: 4 }}>
              UNSTOPPABLE
            </div>
          )}
        </div>
      )}

      {/* Max combo */}
      {state.maxCombo > 1 && (
        <div style={{ position: 'absolute', top: 110, right: 20, fontSize: 10, color: 'rgba(255,170,0,0.5)', letterSpacing: 1 }}>
          MAX COMBO: {state.maxCombo}x
        </div>
      )}

      {/* Reward popups — float up from center */}
      {state.rewardPopups.map(popup => {
        const age = now - popup.birth;
        if (age > REWARD_LIFETIME) return null;
        const progress = age / REWARD_LIFETIME;
        const opacity = progress < 0.2 ? progress / 0.2 : 1 - (progress - 0.2) / 0.8;
        const yOffset = -40 * progress; // float upward
        return (
          <div
            key={popup.id}
            style={{
              position: 'absolute',
              left: `${popup.x}%`, top: `${popup.y}%`,
              transform: `translate(-50%, ${yOffset}px)`,
              fontSize: 14, fontWeight: 700, letterSpacing: 1,
              color: popup.color,
              opacity,
              textShadow: `0 0 10px ${popup.color}80`,
              whiteSpace: 'nowrap',
            }}
          >
            {popup.text}
          </div>
        );
      })}

      {/* Controls hint */}
      <div style={{
        position: 'absolute', bottom: 16, left: '50%',
        transform: 'translateX(-50%)',
        display: 'flex', gap: 16,
        fontSize: 10, color: 'rgba(255,255,255,0.25)', letterSpacing: 1,
      }}>
        <span><b style={{ color: 'rgba(255,255,255,0.4)' }}>WASD</b> move</span>
        <span><b style={{ color: 'rgba(255,255,255,0.4)' }}>MOUSE</b> aim</span>
        <span><b style={{ color: 'rgba(255,255,255,0.4)' }}>CLICK</b> shoot</span>
        <span><b style={{ color: 'rgba(255,255,255,0.4)' }}>SPACE</b> up</span>
        <span><b style={{ color: 'rgba(255,255,255,0.4)' }}>SHIFT</b> down</span>
        <span>
          <button onClick={onExit} style={{
            background: 'rgba(255,0,0,0.15)', border: '1px solid rgba(255,0,0,0.3)',
            color: 'rgba(255,100,100,0.7)', padding: '2px 8px', borderRadius: 3,
            fontFamily: panelFont, fontSize: 10, cursor: 'pointer', pointerEvents: 'auto', letterSpacing: 1,
          }}>ESC EXIT</button>
        </span>
      </div>

      {/* Activation banner */}
      <div style={{
        position: 'absolute', top: 14, left: '50%', transform: 'translateX(-50%)',
        fontSize: 10, letterSpacing: 4, fontWeight: 700,
        color: 'rgba(255,0,0,0.6)',
        animation: 'cheat-banner-glow 2s ease infinite',
      }}>
        ▸ CHEAT MODE ACTIVE ◂
      </div>

      {/* Vignette */}
      <div style={{
        position: 'absolute', inset: 0, pointerEvents: 'none',
        background: 'radial-gradient(ellipse at center, transparent 50%, rgba(0,0,0,0.4) 100%)',
      }} />

      {/* Scan lines */}
      <div style={{
        position: 'absolute', inset: 0, pointerEvents: 'none',
        backgroundImage: 'repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,0,0,0.03) 2px, rgba(0,0,0,0.03) 4px)',
      }} />

      {/* Click to resume when pointer lock lost */}
      {!locked && (
        <div style={{
          position: 'absolute', inset: 0,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          background: 'rgba(0,0,0,0.5)', pointerEvents: 'none', zIndex: 40,
        }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{
              fontSize: 18, color: '#00ffff', fontWeight: 700, letterSpacing: 3,
              textShadow: '0 0 20px rgba(0,255,255,0.5)', marginBottom: 8,
            }}>PAUSED</div>
            <div style={{ fontSize: 12, color: 'rgba(255,255,255,0.5)', letterSpacing: 1 }}>
              click to resume &middot; press <b style={{ color: 'rgba(255,255,255,0.7)' }}>X</b> or <b style={{ color: 'rgba(255,255,255,0.7)' }}>ESC</b> to exit
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* ── CSS Animations ─────────────────────────────────── */

export function injectCheatStyles() {
  if (typeof document === 'undefined') return;
  if (document.getElementById('arena-cheat-styles')) return;

  const style = document.createElement('style');
  style.id = 'arena-cheat-styles';
  style.textContent = `
    @keyframes cheat-crosshair-spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }
    @keyframes cheat-combo-pop {
      0% { transform: translate(-50%, -50%) scale(1.8); opacity: 0; }
      50% { transform: translate(-50%, -50%) scale(0.95); opacity: 1; }
      100% { transform: translate(-50%, -50%) scale(1); opacity: 1; }
    }
    @keyframes cheat-banner-glow {
      0%, 100% { opacity: 0.5; text-shadow: 0 0 10px rgba(255,0,0,0.3); }
      50% { opacity: 1; text-shadow: 0 0 20px rgba(255,0,0,0.6), 0 0 40px rgba(255,0,0,0.2); }
    }
  `;
  document.head.appendChild(style);
}

injectCheatStyles();
