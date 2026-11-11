import React, { useEffect, useRef, useState } from 'react';
import { DefaultZIndexes, ZIndexLayer, usePlotArea, useXAxisDomain } from 'recharts';
import { zoomSelection, type TimeWindow } from '@tracehouse/core';

/** Pointer movement below this is a click (drill-down, event marker), not a zoom. */
const DRAG_THRESHOLD_PX = 5;

interface Geometry {
  left: number;
  top: number;
  width: number;
  height: number;
  startMs: number;
  endMs: number;
}

interface Drag {
  anchorX: number;
  currentX: number;
}

/** The click that ends a drag must not reach chart click handlers such as drill-down. */
function swallowNextClick() {
  const swallow = (event: MouseEvent) => {
    event.stopPropagation();
    event.preventDefault();
  };
  window.addEventListener('click', swallow, { capture: true, once: true });
  // No click follows a mouseup outside the chart; drop the listener once this task ends.
  setTimeout(() => window.removeEventListener('click', swallow, { capture: true }), 0);
}

/**
 * Drag across a recharts chart whose x axis is epoch ms to select a time range.
 * Must be rendered inside a recharts cartesian chart.
 */
export const ChartTimeZoom: React.FC<{ onZoom: (window: TimeWindow) => void }> = ({ onZoom }) => {
  const anchorRef = useRef<SVGGElement>(null);
  const xDomain = useXAxisDomain() as unknown[] | undefined;
  const plotArea = usePlotArea();
  const [drag, setDrag] = useState<Drag | null>(null);

  const startMs = xDomain?.[0];
  const endMs = xDomain?.[xDomain.length - 1];
  const geometry: Geometry | null = plotArea && typeof startMs === 'number' && typeof endMs === 'number' && endMs > startMs
    ? { left: plotArea.x, top: plotArea.y, width: plotArea.width, height: plotArea.height, startMs, endMs }
    : null;

  // Native listeners read the latest geometry and callback without re-registering.
  const geometryRef = useRef<Geometry | null>(geometry);
  const onZoomRef = useRef(onZoom);
  useEffect(() => {
    geometryRef.current = geometry;
    onZoomRef.current = onZoom;
  });

  useEffect(() => {
    const svg = anchorRef.current?.ownerSVGElement;
    if (!svg) return;
    const svgX = (clientX: number) => clientX - svg.getBoundingClientRect().left;
    const toMs = (x: number, g: Geometry) => g.startMs + ((x - g.left) / g.width) * (g.endMs - g.startMs);
    const clampX = (x: number, g: Geometry) => Math.max(g.left, Math.min(g.left + g.width, x));

    const onMouseDown = (event: MouseEvent) => {
      const g = geometryRef.current;
      if (!g || event.button !== 0) return;
      const rect = svg.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      if (x < g.left || x > g.left + g.width || y < g.top || y > g.top + g.height) return;
      // Keeps the drag from selecting page text.
      event.preventDefault();
      let current: Drag = { anchorX: x, currentX: x };
      setDrag(current);

      const onMouseMove = (moveEvent: MouseEvent) => {
        current = { ...current, currentX: clampX(svgX(moveEvent.clientX), g) };
        setDrag(current);
      };
      const onMouseUp = () => {
        window.removeEventListener('mousemove', onMouseMove);
        window.removeEventListener('mouseup', onMouseUp);
        setDrag(null);
        if (Math.abs(current.currentX - current.anchorX) < DRAG_THRESHOLD_PX) return;
        swallowNextClick();
        const selected = zoomSelection(toMs(current.anchorX, g), toMs(current.currentX, g), [g.startMs, g.endMs]);
        if (selected) onZoomRef.current(selected);
      };
      window.addEventListener('mousemove', onMouseMove);
      window.addEventListener('mouseup', onMouseUp);
    };

    svg.addEventListener('mousedown', onMouseDown);
    return () => svg.removeEventListener('mousedown', onMouseDown);
  }, []);

  const selection = drag && geometry && Math.abs(drag.currentX - drag.anchorX) >= DRAG_THRESHOLD_PX
    ? { x: Math.min(drag.anchorX, drag.currentX), width: Math.abs(drag.currentX - drag.anchorX) }
    : null;

  return (
    <>
      <g ref={anchorRef} />
      {selection && geometry && (
        <ZIndexLayer zIndex={DefaultZIndexes.cursorLine}>
          <rect
            x={selection.x} y={geometry.top} width={selection.width} height={geometry.height}
            fill="rgba(99,102,241,0.14)" stroke="rgba(99,102,241,0.6)" strokeWidth={1}
            pointerEvents="none"
          />
        </ZIndexLayer>
      )}
    </>
  );
};
