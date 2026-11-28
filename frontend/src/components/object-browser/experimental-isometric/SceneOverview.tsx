import { useRef, type PointerEvent } from 'react';
import type { SpatialScene } from './layout';
import { groundPoint, type CameraState } from './useCamera';

interface Props {
  scene: SpatialScene;
  camera: CameraState;
  frame: { width: number; height: number };
  onNavigate: (x: number, y: number) => void;
}

/** Flat model-space map with an axis-aligned approximation of the viewport. */
export function SceneOverview({ scene, camera, frame, onNavigate }: Props) {
  const drag = useRef<{ pointer: number; offsetX: number; offsetY: number } | null>(null);
  // Match the map's inner dimensions; omit object marks that would become noise.
  const mapScale = Math.min(158 / (scene.width + 40), 140 / (scene.height + 40));
  const mapPoint = (event: PointerEvent<SVGSVGElement>) => {
    const matrix = event.currentTarget.getScreenCTM();
    return matrix ? new DOMPoint(event.clientX, event.clientY).matrixTransform(matrix.inverse()) : null;
  };
  const navigate = (x: number, y: number) => onNavigate(Math.max(0, Math.min(scene.width, x)), Math.max(0, Math.min(scene.height, y)));
  const finishDrag = (event: PointerEvent<SVGSVGElement>) => {
    if (drag.current?.pointer !== event.pointerId) return;
    drag.current = null;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId);
  };
  const corners = [{ x: 0, y: 0 }, { x: frame.width, y: 0 }, { x: frame.width, y: frame.height }, { x: 0, y: frame.height }]
    .map(point => groundPoint(scene.width, scene.height, camera, frame, point));
  const left = Math.min(...corners.map(point => point.x));
  const top = Math.min(...corners.map(point => point.y));
  const width = Math.max(...corners.map(point => point.x)) - left;
  const height = Math.max(...corners.map(point => point.y)) - top;
  return <div className="spatial-overview" title="Click or drag to pan · Arrow keys to move">
    <svg viewBox={`-20 -20 ${scene.width + 40} ${scene.height + 40}`} preserveAspectRatio="xMidYMid meet"
      role="button" tabIndex={0} aria-label="Catalog minimap. Click or drag to pan; arrow keys to move."
      onKeyDown={event => {
        const delta = { ArrowLeft: [-100, 0], ArrowRight: [100, 0], ArrowUp: [0, -100], ArrowDown: [0, 100] }[event.key];
        if (!delta) return;
        event.preventDefault();
        const centre = groundPoint(scene.width, scene.height, camera, frame, { x: frame.width / 2, y: frame.height / 2 });
        onNavigate(centre.x + delta[0], centre.y + delta[1]);
      }}
      onPointerDown={event => {
        if (event.button !== 0 || drag.current) return;
        const point = mapPoint(event);
        if (!point) return;
        event.preventDefault();event.currentTarget.focus();
        const centre = groundPoint(scene.width, scene.height, camera, frame, { x: frame.width / 2, y: frame.height / 2 });
        const inside = (event.target as Element).classList.contains('spatial-map-viewport');
        drag.current = { pointer: event.pointerId, offsetX: inside ? centre.x - point.x : 0, offsetY: inside ? centre.y - point.y : 0 };
        event.currentTarget.setPointerCapture(event.pointerId);
        if (!inside) navigate(point.x, point.y);
      }}
      onPointerMove={event => {
        const start = drag.current;
        if (!start || start.pointer !== event.pointerId) return;
        const point = mapPoint(event);
        if (point) navigate(point.x + start.offsetX, point.y + start.offsetY);
      }}
      onPointerUp={finishDrag} onPointerCancel={finishDrag} onLostPointerCapture={() => { drag.current = null; }}>
      {scene.groups.map(group => <rect key={group.group.id} x={group.x} y={group.y} width={group.width} height={group.height}
        className="spatial-map-group" data-kind={group.group.kind} />)}
      {scene.tiles.filter(tile => tile.width * mapScale >= 3 && tile.height * mapScale >= 2).map(tile => <rect key={tile.key} x={tile.x} y={tile.y} width={tile.width} height={tile.height} className="spatial-map-tile" />)}
      <rect x={left} y={top} width={width} height={height} className="spatial-map-viewport" />
    </svg>
  </div>;
}
