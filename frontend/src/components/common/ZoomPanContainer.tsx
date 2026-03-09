/**
 * ZoomPanContainer — Reusable wrapper that adds zoom/pan controls to any content.
 * Supports mouse wheel zoom, button controls, and click-drag panning.
 */
import React, { useState, useRef, useCallback, useEffect } from 'react';

interface ZoomPanContainerProps {
  children: React.ReactNode;
  /** Minimum zoom scale (default: 0.2) */
  minScale?: number;
  /** Maximum zoom scale (default: 5) */
  maxScale?: number;
  /** Initial zoom scale (default: 1) */
  initialScale?: number;
  /** Height of the container (default: 100%) */
  height?: number | string;
  /** Whether to show the zoom controls overlay (default: true) */
  showControls?: boolean;
  /** Zoom step for buttons (default: 0.25) */
  zoomStep?: number;
}

export const ZoomPanContainer: React.FC<ZoomPanContainerProps> = ({
  children,
  minScale = 0.2,
  maxScale = 5,
  initialScale = 1,
  height = '100%',
  showControls = true,
  zoomStep = 0.25,
}) => {
  const [scale, setScale] = useState(initialScale);
  const [translate, setTranslate] = useState({ x: 0, y: 0 });
  const containerRef = useRef<HTMLDivElement>(null);
  const isDragging = useRef(false);
  const dragStart = useRef({ x: 0, y: 0 });
  const translateStart = useRef({ x: 0, y: 0 });

  const clampScale = useCallback((s: number) => Math.min(maxScale, Math.max(minScale, s)), [minScale, maxScale]);

  // Wheel zoom centered on cursor
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const handler = (e: WheelEvent) => {
      e.preventDefault();
      const rect = el.getBoundingClientRect();
      const cursorX = e.clientX - rect.left;
      const cursorY = e.clientY - rect.top;

      const zoomFactor = e.deltaY < 0 ? 1.15 : 1 / 1.15;
      const newScale = clampScale(scale * zoomFactor);
      if (newScale === scale) return;

      // Adjust translate so the point under cursor stays fixed
      const ratio = newScale / scale;
      const newTx = cursorX - ratio * (cursorX - translate.x);
      const newTy = cursorY - ratio * (cursorY - translate.y);

      setScale(newScale);
      setTranslate({ x: newTx, y: newTy });
    };
    el.addEventListener('wheel', handler, { passive: false });
    return () => el.removeEventListener('wheel', handler);
  }, [scale, translate, clampScale]);

  // Drag to pan
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    // Only pan on middle-click or when holding space (we'll just use any click for simplicity)
    // Ignore if the click is on an interactive element
    if ((e.target as HTMLElement).closest('button, a, input, select')) return;
    isDragging.current = true;
    dragStart.current = { x: e.clientX, y: e.clientY };
    translateStart.current = { ...translate };
    e.preventDefault();
  }, [translate]);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (!isDragging.current) return;
    const dx = e.clientX - dragStart.current.x;
    const dy = e.clientY - dragStart.current.y;
    setTranslate({
      x: translateStart.current.x + dx,
      y: translateStart.current.y + dy,
    });
  }, []);

  const handleMouseUp = useCallback(() => {
    isDragging.current = false;
  }, []);

  const handleZoomIn = useCallback(() => {
    const newScale = clampScale(scale + zoomStep);
    // Zoom toward center of container
    const el = containerRef.current;
    if (el) {
      const rect = el.getBoundingClientRect();
      const cx = rect.width / 2;
      const cy = rect.height / 2;
      const ratio = newScale / scale;
      setTranslate({ x: cx - ratio * (cx - translate.x), y: cy - ratio * (cy - translate.y) });
    }
    setScale(newScale);
  }, [scale, translate, clampScale, zoomStep]);

  const handleZoomOut = useCallback(() => {
    const newScale = clampScale(scale - zoomStep);
    const el = containerRef.current;
    if (el) {
      const rect = el.getBoundingClientRect();
      const cx = rect.width / 2;
      const cy = rect.height / 2;
      const ratio = newScale / scale;
      setTranslate({ x: cx - ratio * (cx - translate.x), y: cy - ratio * (cy - translate.y) });
    }
    setScale(newScale);
  }, [scale, translate, clampScale, zoomStep]);

  const handleReset = useCallback(() => {
    setScale(initialScale);
    setTranslate({ x: 0, y: 0 });
  }, [initialScale]);

  const handleFitWidth = useCallback(() => {
    const el = containerRef.current;
    if (!el) return;
    // Find the inner content's natural width
    const inner = el.querySelector('[data-zoom-content]') as HTMLElement | null;
    if (!inner) { handleReset(); return; }
    const contentWidth = inner.scrollWidth;
    const containerWidth = el.clientWidth;
    if (contentWidth <= 0) return;
    const fitScale = clampScale(containerWidth / contentWidth * 0.95);
    setScale(fitScale);
    setTranslate({ x: 0, y: 0 });
  }, [clampScale, handleReset]);

  const pct = Math.round(scale * 100);

  return (
    <div
      ref={containerRef}
      style={{
        position: 'relative',
        height,
        overflow: 'hidden',
        cursor: isDragging.current ? 'grabbing' : 'grab',
      }}
      onMouseDown={handleMouseDown}
      onMouseMove={handleMouseMove}
      onMouseUp={handleMouseUp}
      onMouseLeave={handleMouseUp}
    >
      {/* Transformed content */}
      <div
        data-zoom-content
        style={{
          transformOrigin: '0 0',
          transform: `translate(${translate.x}px, ${translate.y}px) scale(${scale})`,
          willChange: 'transform',
        }}
      >
        {children}
      </div>

      {/* Zoom controls overlay */}
      {showControls && (
        <div style={{
          position: 'absolute',
          bottom: 12,
          right: 12,
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          background: 'color-mix(in srgb, var(--bg-secondary), transparent 15%)',
          backdropFilter: 'blur(12px)',
          WebkitBackdropFilter: 'blur(12px)',
          border: '1px solid var(--border-primary)',
          borderRadius: 8,
          padding: '4px 6px',
          zIndex: 10,
          boxShadow: '0 2px 8px rgba(0,0,0,0.3)',
        }}>
          <ZoomButton onClick={handleZoomOut} title="Zoom out" disabled={scale <= minScale}>−</ZoomButton>
          <span
            onClick={handleReset}
            title="Reset zoom"
            style={{
              fontSize: 10,
              color: 'var(--text-muted)',
              minWidth: 38,
              textAlign: 'center',
              cursor: 'pointer',
              userSelect: 'none',
              fontFamily: 'monospace',
            }}
          >
            {pct}%
          </span>
          <ZoomButton onClick={handleZoomIn} title="Zoom in" disabled={scale >= maxScale}>+</ZoomButton>
          <div style={{ width: 1, height: 16, background: 'var(--border-primary)', margin: '0 4px' }} />
          <ZoomButton onClick={handleFitWidth} title="Fit to width">⤢</ZoomButton>
        </div>
      )}
    </div>
  );
};

const ZoomButton: React.FC<{
  onClick: () => void;
  title: string;
  disabled?: boolean;
  children: React.ReactNode;
}> = ({ onClick, title, disabled, children }) => (
  <button
    onClick={(e) => { e.stopPropagation(); onClick(); }}
    title={title}
    disabled={disabled}
    style={{
      width: 26,
      height: 26,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      background: 'transparent',
      border: 'none',
      borderRadius: 4,
      color: disabled ? 'var(--text-disabled)' : 'var(--text-secondary)',
      cursor: disabled ? 'default' : 'pointer',
      fontSize: 14,
      fontWeight: 600,
      lineHeight: 1,
      padding: 0,
      transition: 'color 0.1s ease, background 0.1s ease',
    }}
    onMouseEnter={(e) => { if (!disabled) e.currentTarget.style.background = 'var(--bg-hover)'; }}
    onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
  >
    {children}
  </button>
);
