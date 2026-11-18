import { useRef, useState, type MouseEvent, type PointerEvent, type RefObject } from 'react';

/** Pan through native scroll containers so all diagram content remains reachable. */
export function useResourceFlowViewport(verticalViewport?: RefObject<HTMLDivElement>) {
  const viewport = useRef<HTMLDivElement>(null);
  const [zoom, setZoom] = useState(1);
  const [dragging, setDragging] = useState(false);
  const drag = useRef<{ x: number; y: number; left: number; top: number; moved: boolean }>();
  const suppressClick = useRef(false);
  const panHandlers = {
    onPointerDown(event: PointerEvent<HTMLDivElement>) {
      if (event.button !== 0 || event.pointerType === 'touch') return;
      const el = viewport.current;
      if (!el) return;
      // Prevent native SVG/text selection before the pan threshold is reached.
      event.preventDefault();
      suppressClick.current = false;
      drag.current = { x: event.clientX, y: event.clientY, left: el.scrollLeft,
        top: (verticalViewport?.current ?? el).scrollTop, moved: false };
    },
    onPointerMove(event: PointerEvent<HTMLDivElement>) {
      const start = drag.current, el = viewport.current;
      if (!start || !el) return;
      const dx = event.clientX-start.x, dy = event.clientY-start.y;
      if (!start.moved && Math.hypot(dx,dy) < 5) return;
      if (!start.moved) {
        start.moved = true; setDragging(true); el.setPointerCapture?.(event.pointerId);
        const selection = window.getSelection();
        if (selection?.anchorNode && el.contains(selection.anchorNode)) selection.removeAllRanges();
      }
      event.preventDefault();
      suppressClick.current = true;
      el.scrollLeft = start.left-dx;
      (verticalViewport?.current ?? el).scrollTop = start.top-dy;
    },
    onPointerUp(event: PointerEvent<HTMLDivElement>) {
      drag.current = undefined; setDragging(false);
      if (viewport.current?.hasPointerCapture?.(event.pointerId)) viewport.current.releasePointerCapture(event.pointerId);
    },
    onPointerCancel() { drag.current = undefined; setDragging(false); suppressClick.current = false; },
    onPointerLeave() { if (!drag.current?.moved) drag.current = undefined; },
    onLostPointerCapture() { drag.current = undefined; setDragging(false); },
    onClickCapture(event: MouseEvent<HTMLDivElement>) {
      if (suppressClick.current && event.detail !== 0) {
        event.preventDefault(); event.stopPropagation(); suppressClick.current = false;
      }
    },
  };
  return { viewport, zoom, dragging, panHandlers,
    zoomIn: () => setZoom(value => Math.min(2,value+.25)),
    zoomOut: () => setZoom(value => Math.max(.5,value-.25)),
    resetZoom: () => setZoom(1) };
}
