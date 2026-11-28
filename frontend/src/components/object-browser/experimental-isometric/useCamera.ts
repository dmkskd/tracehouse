import { useCallback, useLayoutEffect, useRef, useState, type MouseEvent, type PointerEvent } from 'react';
import type { Rect } from './layout';

export const CAMERA_HOME = { tilt: 54, rotation: -45 };
export const MIN_ZOOM = .12, MAX_ZOOM = 4;
const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value));
const angle = (degrees: number) => ((degrees + 180) % 360 + 360) % 360 - 180;
export interface CameraState {
  scale: number; tilt: number; rotation: number; panX: number; panY: number;
}
interface Point { x: number; y: number }
interface Size { width: number; height: number }

/** Keep perspective finite even for large catalogs. The same distance is used by
 * the CSS camera, fitting and selection framing; layout remains in model space. */
export const cameraDistance = (width: number, height: number) => Math.max(2400, Math.hypot(width, height) * 1.6);

export function projectPoint(width: number, height: number, tilt: number, rotation: number, x: number, y: number, z = 0) {
  const rx = tilt * Math.PI / 180, rz = rotation * Math.PI / 180;
  const cx = x - width / 2, cy = y - height / 2;
  const px = Math.cos(rz) * cx - Math.sin(rz) * cy;
  const py = Math.cos(rx) * (Math.sin(rz) * cx + Math.cos(rz) * cy) - Math.sin(rx) * z;
  const pz = Math.sin(rx) * (Math.sin(rz) * cx + Math.cos(rz) * cy) + Math.cos(rx) * z;
  const distance = cameraDistance(width, height), factor = tilt || rotation ? distance / (distance - pz) : 1;
  return { x: px * factor, y: py * factor };
}

/** Bounds of the perspective projection, including extruded platform/tile faces. */
export function projectedBounds(width: number, height: number, tilt: number, rotation: number) {
  const points = [0, width].flatMap(x => [0, height].flatMap(y => [-12, 32].map(z => projectPoint(width, height, tilt, rotation, x, y, z))));
  const left = Math.min(...points.map(p => p.x)), top = Math.min(...points.map(p => p.y));
  return { left, top, width: Math.max(...points.map(p => p.x)) - left, height: Math.max(...points.map(p => p.y)) - top };
}

export function orbitCamera(camera: CameraState, dx: number, dy: number): CameraState {
  return { ...camera, rotation: angle(camera.rotation - dx * .35), tilt: clamp(camera.tilt - dy * .3, 20, 72) };
}

/** Keep the ground point at the viewport centre fixed, including after panning. */
export function orbitCameraAtCentre(camera: CameraState, dx: number, dy: number, width: number, height: number, size: Size): CameraState {
  const anchor = groundPoint(width, height, camera, size, { x: size.width / 2, y: size.height / 2 });
  const next = orbitCamera(camera, dx, dy);
  const projected = projectPoint(width, height, next.tilt, next.rotation, anchor.x, anchor.y);
  return { ...next, panX: -projected.x * next.scale, panY: -projected.y * next.scale };
}

/** Zoom about the pointer's position, preserving its model-space anchor after pan. */
export function zoomCamera(camera: CameraState, factor: number, point: Point, size: Size): CameraState {
  const scale = clamp(camera.scale * factor, MIN_ZOOM, MAX_ZOOM), ratio = scale / camera.scale;
  const x = point.x - size.width / 2, y = point.y - size.height / 2;
  return { ...camera, scale, panX: x - (x - camera.panX) * ratio, panY: y - (y - camera.panY) * ratio };
}

export function fitCamera(camera: CameraState, width: number, height: number, size: Size): CameraState {
  const bounds = projectedBounds(width, height, camera.tilt, camera.rotation);
  const scale = clamp(Math.min(1, (size.width - 48) / bounds.width, (size.height - 48) / bounds.height), MIN_ZOOM, MAX_ZOOM);
  return { ...camera, panX: -(bounds.left + bounds.width / 2) * scale, panY: -(bounds.top + bounds.height / 2) * scale, scale };
}

/** Frame the start of a long catalog at a readable scale. Orbit anchoring is
 * computed separately from the displayed view when a rotation gesture begins. */
export function homeCamera(width: number, height: number, size: Size): CameraState {
  const fitted = fitCamera({ ...CAMERA_HOME, scale: 1, panX: 0, panY: 0 }, width, height, size);
  const scale = Math.max(1, fitted.scale);
  const anchor = projectPoint(width, height, CAMERA_HOME.tilt, CAMERA_HOME.rotation, width / 2, Math.min(height / 2, 450));
  return { ...fitted, scale, panX: -anchor.x * scale, panY: -anchor.y * scale };
}

/** Inverse ground-plane projection for the overview outline and orbit anchor. */
export function groundPoint(width: number, height: number, camera: CameraState, size: Size, point: Point): Point {
  const u = (point.x - size.width / 2 - camera.panX) / camera.scale;
  const v = (point.y - size.height / 2 - camera.panY) / camera.scale;
  const tilt = camera.tilt * Math.PI / 180, rotation = camera.rotation * Math.PI / 180;
  const denominator = 1 + v * Math.tan(tilt) / cameraDistance(width, height);
  const x = u / denominator, y = v / (Math.cos(tilt) * denominator);
  return { x: width / 2 + Math.cos(rotation) * x + Math.sin(rotation) * y,
    y: height / 2 - Math.sin(rotation) * x + Math.cos(rotation) * y };
}

export function useCamera(width: number, height: number) {
  const viewport = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState<Size>({ width: 800, height: 500 });
  const [camera, setCamera] = useState<CameraState>({ ...CAMERA_HOME, scale: 1, panX: 0, panY: 0 });
  const [dragging, setDragging] = useState(false);
  const initialized = useRef(false);
  const points = useRef(new Map<number, Point>());
  const gesture = useRef<{ origin: Point; camera: CameraState; pan: boolean; moved: boolean; distance?: number } | null>(null);
  const suppressed = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useLayoutEffect(() => {
    const element = viewport.current;
    if (!element) return;
    const measure = () => {
      const next = { width: element.clientWidth, height: element.clientHeight };
      if (!next.width || !next.height) return;
      setSize(previous => previous.width === next.width && previous.height === next.height ? previous : next);
      if (!initialized.current) { initialized.current = true;setCamera(homeCamera(width, height, next)); }
    };
    const frame = requestAnimationFrame(measure);
    const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(measure);
    observer?.observe(element);window.addEventListener('resize', measure);
    return () => { cancelAnimationFrame(frame);observer?.disconnect();window.removeEventListener('resize', measure);clearTimeout(timer.current); };
  }, [width, height]);

  useLayoutEffect(() => {
    const element = viewport.current;
    if (!element) return;
    const wheel = (event: WheelEvent) => {
      event.preventDefault();
      const rect = element.getBoundingClientRect();
      const delta = event.deltaY * (event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? size.height : 1);
      setCamera(c => zoomCamera(c, Math.exp(-delta * .0015), { x: event.clientX - rect.left, y: event.clientY - rect.top }, size));
    };
    element.addEventListener('wheel', wheel, { passive: false });
    return () => element.removeEventListener('wheel', wheel);
  }, [size]);

  const onPointerDown = (event: PointerEvent<HTMLDivElement>) => {
    if (event.button !== 0 && event.button !== 1) return;
    if (event.button === 1) event.preventDefault();
    clearTimeout(timer.current);suppressed.current = false;
    points.current.set(event.pointerId, { x: event.clientX, y: event.clientY });
    const [a, b] = [...points.current.values()];
    if (b) {
      gesture.current = { origin: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 }, camera, pan: true, moved: true, distance: Math.max(1, Math.hypot(b.x - a.x, b.y - a.y)) };
      event.currentTarget.setPointerCapture(event.pointerId);setDragging(true);
    } else gesture.current = { origin: a, camera, pan: event.shiftKey || event.button === 1, moved: false };
  };
  const onPointerMove = (event: PointerEvent<HTMLDivElement>) => {
    const start = gesture.current;
    if (!start || !points.current.has(event.pointerId)) return;
    points.current.set(event.pointerId, { x: event.clientX, y: event.clientY });
    const [a, b] = [...points.current.values()];
    const point = b ? { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 } : a;
    const dx = point.x - start.origin.x, dy = point.y - start.origin.y;
    if (!start.moved && Math.abs(dx) + Math.abs(dy) <= 4) return;
    start.moved = true;setDragging(true);event.currentTarget.setPointerCapture(event.pointerId);
    if (start.distance && b) {
      const rect = event.currentTarget.getBoundingClientRect();
      const zoomed = zoomCamera(start.camera, Math.hypot(b.x - a.x, b.y - a.y) / start.distance,
        { x: start.origin.x - rect.left, y: start.origin.y - rect.top }, size);
      setCamera({ ...zoomed, panX: zoomed.panX + dx, panY: zoomed.panY + dy });
    } else setCamera(start.pan ? { ...start.camera, panX: start.camera.panX + dx, panY: start.camera.panY + dy } : orbitCameraAtCentre(start.camera, dx, dy, width, height, size));
  };
  const endGesture = () => {
    if (!gesture.current) return;
    suppressed.current = gesture.current.moved;
    gesture.current = null;points.current.clear();setDragging(false);
    timer.current = setTimeout(() => { suppressed.current = false; }, 0);
  };
  const suppressClick = (event: MouseEvent<HTMLDivElement>) => {
    if (suppressed.current) { event.preventDefault();event.stopPropagation(); }
  };
  const centre = { x: size.width / 2, y: size.height / 2 };
  const reveal = useCallback((rect: Rect) => setCamera(c => {
    const projected = projectPoint(width, height, c.tilt, c.rotation, rect.x + rect.width / 2, rect.y + rect.height / 2, 26);
    const px = projected.x * c.scale, py = projected.y * c.scale;
    const sx = size.width / 2 + c.panX + px, sy = size.height / 2 + c.panY + py;
    return sx >= 64 && sx <= size.width - 64 && sy >= 64 && sy <= size.height - 64 ? c : { ...c, panX: -px, panY: -py };
  }), [width, height, size]);
  return {
    viewport, ...camera, dragging, reveal,
    overviewCamera: camera,
    centreOn: (x: number, y: number) => setCamera(c => {
      const point = projectPoint(width, height, c.tilt, c.rotation, x, y);
      return { ...c, panX: -point.x * c.scale, panY: -point.y * c.scale };
    }),
    reset: () => setCamera(homeCamera(width, height, size)),
    zoomIn: () => setCamera(c => zoomCamera(c, 1.2, centre, size)),
    zoomOut: () => setCamera(c => zoomCamera(c, 1 / 1.2, centre, size)),
    frame: { width: size.width, height: size.height },
    transform: `translate(${size.width / 2 + camera.panX}px, ${size.height / 2 + camera.panY}px) scale(${camera.scale}) perspective(${cameraDistance(width, height)}px) rotateX(${camera.tilt}deg) rotateZ(${camera.rotation}deg) translate(${-width / 2}px, ${-height / 2}px)`,
    pointerHandlers: { onPointerDown, onPointerMove, onPointerUp: endGesture, onPointerCancel: endGesture,
      onLostPointerCapture: (event: PointerEvent<HTMLDivElement>) => { if (event.target === event.currentTarget) endGesture(); },
      onClickCapture: suppressClick, onDoubleClickCapture: suppressClick },
  };
}
