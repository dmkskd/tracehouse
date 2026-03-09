/**
 * TimelineNavigator - Mini timeline for navigating through a longer time range
 * 
 * Shows a 2-hour overview with metric data and a draggable viewport window
 * representing the current view window that can be dragged to navigate time.
 * Mirrors the metric mode from the main chart (Memory, CPU, Network, Disk).
 */
import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import type { TimeseriesPoint } from '@tracehouse/core';

export type MetricMode = 'memory' | 'cpu' | 'network' | 'disk';

const METRIC_CONFIG: Record<MetricMode, { label: string; color: string; lightColor: string }> = {
  memory: { label: 'Memory', color: '#58a6ff', lightColor: '#a0cfff' },
  cpu: { label: 'CPU', color: '#3fb950', lightColor: '#7dd98a' },
  network: { label: 'Network', color: '#d29922', lightColor: '#e8c060' },
  disk: { label: 'Disk I/O', color: '#bc8cff', lightColor: '#d4b8ff' },
};

interface TimelineNavigatorProps {
  /** Timeseries data for the extended range */
  data: TimeseriesPoint[];
  /** Current metric mode to display */
  metricMode: MetricMode;
  /** Start of the navigator range (ms) */
  rangeStartMs: number;
  /** End of the navigator range (ms) */
  rangeEndMs: number;
  /** Current viewport start (ms) */
  viewportStartMs: number;
  /** Current viewport end (ms) */
  viewportEndMs: number;
  /** Callback when viewport is dragged to a new position */
  onViewportChange: (newEndMs: number) => void;
  /** Height of the navigator */
  height?: number;
  /** Whether data is loading */
  isLoading?: boolean;
  /** Total RAM in bytes (for memory percentage) */
  totalRam?: number;
  /** Number of CPU cores (for CPU percentage) */
  cpuCores?: number;
  /** Callback when drag ends — commit the final viewport position */
  onDragEnd?: (endMs: number) => void;
}

const fmtTime = (ms: number): string => new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

export const TimelineNavigator: React.FC<TimelineNavigatorProps> = ({
  data,
  metricMode,
  rangeStartMs,
  rangeEndMs,
  viewportStartMs,
  viewportEndMs,
  onViewportChange,
  height = 60,
  isLoading = false,
  totalRam = 0,
  cpuCores = 0,
  onDragEnd,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [isDragging, setIsDragging] = useState(false);
  const dragStateRef = useRef<{
    lastX: number;
    currentEndMs: number;
    viewportWidth: number;
    frozenRangeMs: number;      // locked at drag start so delta calc stays stable
    frozenContainerWidth: number; // container width at drag start
  } | null>(null);

  const [hoverInfo, setHoverInfo] = useState<{ x: number; y: number; value: string } | null>(null);
  const viewportEndMsRef = useRef(viewportEndMs);
  viewportEndMsRef.current = viewportEndMs;

  const cfg = METRIC_CONFIG[metricMode];
  const rangeMs = rangeEndMs - rangeStartMs || 1;
  const viewportWidthMs = viewportEndMs - viewportStartMs;

  // Parse data to ms timestamps
  const dataPoints = useMemo(() => {
    return data.map(p => {
      const normalized = p.t.replace(' ', 'T') + (p.t.includes('Z') || p.t.includes('+') ? '' : 'Z');
      return { ms: new Date(normalized).getTime(), v: p.v };
    }).filter(p => p.ms >= rangeStartMs && p.ms <= rangeEndMs);
  }, [data, rangeStartMs, rangeEndMs]);

  // Calculate max Y for scaling
  const maxY = useMemo(() => {
    if (dataPoints.length === 0) return 1;
    return Math.max(...dataPoints.map(p => p.v)) * 1.1 || 1;
  }, [dataPoints]);

  // Convert ms to X position (0-100%)
  const msToPercent = useCallback((ms: number) => {
    return ((ms - rangeStartMs) / rangeMs) * 100;
  }, [rangeStartMs, rangeMs]);

  // Convert X position to ms
  const percentToMs = useCallback((percent: number) => {
    return rangeStartMs + (percent / 100) * rangeMs;
  }, [rangeStartMs, rangeMs]);

  // Build SVG path for area chart
  const areaPath = useMemo(() => {
    if (dataPoints.length < 2) return '';
    const chartHeight = height - 20; // Leave room for labels
    const points = dataPoints.map(p => {
      const x = msToPercent(p.ms);
      const y = chartHeight - (p.v / maxY) * chartHeight;
      return `${x},${y}`;
    });
    const firstX = msToPercent(dataPoints[0].ms);
    const lastX = msToPercent(dataPoints[dataPoints.length - 1].ms);
    return `M${firstX},${chartHeight} L${points.join(' L')} L${lastX},${chartHeight} Z`;
  }, [dataPoints, maxY, height, msToPercent]);

  // Viewport position as percentages (clamped to stay visible)
  const rawLeftPercent = msToPercent(viewportStartMs);
  const rawWidthPercent = (viewportWidthMs / rangeMs) * 100;
  
  // Clamp to 0-100 range but ensure at least some visibility
  const viewportLeftPercent = Math.max(0, Math.min(100 - 2, rawLeftPercent));
  const viewportWidthPercent = Math.max(2, Math.min(100 - viewportLeftPercent, rawWidthPercent));

  // Handle mouse events for dragging
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    if (!containerRef.current) return;
    const rect = containerRef.current.getBoundingClientRect();
    const x = e.clientX - rect.left;
    
    // Lock rangeMs and container width at drag start for stable pixel→ms conversion
    dragStateRef.current = {
      lastX: x,
      currentEndMs: viewportEndMsRef.current,
      viewportWidth: viewportWidthMs,
      frozenRangeMs: rangeMs,
      frozenContainerWidth: rect.width,
    };
    setIsDragging(true);
    e.preventDefault();
  }, [viewportWidthMs, rangeMs]);

  const handleMouseMove = useCallback((e: MouseEvent) => {
    const dragState = dragStateRef.current;
    if (!isDragging || !dragState) return;

    const currentX = e.clientX - (containerRef.current?.getBoundingClientRect().left ?? 0);
    const deltaX = currentX - dragState.lastX;
    dragState.lastX = currentX;

    // Use frozen values from drag start — immune to range extension feedback loops
    const deltaPx = deltaX / dragState.frozenContainerWidth;
    const deltaMs = deltaPx * dragState.frozenRangeMs;

    const newEndMs = dragState.currentEndMs + deltaMs;
    dragState.currentEndMs = newEndMs;

    // Parent handles clamping (future, etc.) and range extension
    onViewportChange(newEndMs);
  }, [isDragging, onViewportChange]);

  const handleMouseUp = useCallback(() => {
    if (onDragEnd && dragStateRef.current) {
      onDragEnd(dragStateRef.current.currentEndMs);
    }
    setIsDragging(false);
    dragStateRef.current = null;
  }, [onDragEnd]);

  // Handle click to jump to position
  const handleClick = useCallback((e: React.MouseEvent) => {
    if (isDragging) return; // Don't jump if we were dragging
    if (!containerRef.current) return;
    
    const rect = containerRef.current.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const clickPercent = (x / rect.width) * 100;
    const clickMs = percentToMs(clickPercent);
    
    // Center the viewport on the click position
    let newEndMs = clickMs + viewportWidthMs / 2;
    
    // Clamp to valid range
    const minEndMs = rangeStartMs + viewportWidthMs;
    const maxEndMs = rangeEndMs;
    newEndMs = Math.max(minEndMs, Math.min(maxEndMs, newEndMs));
    
    onViewportChange(newEndMs);
    if (onDragEnd) onDragEnd(newEndMs); // Click = immediate commit
  }, [isDragging, percentToMs, viewportWidthMs, rangeStartMs, rangeEndMs, onViewportChange, onDragEnd]);

  // Global mouse event listeners for dragging
  useEffect(() => {
    if (isDragging) {
      window.addEventListener('mousemove', handleMouseMove);
      window.addEventListener('mouseup', handleMouseUp);
      return () => {
        window.removeEventListener('mousemove', handleMouseMove);
        window.removeEventListener('mouseup', handleMouseUp);
      };
    }
  }, [isDragging, handleMouseMove, handleMouseUp]);

  // Time labels
  const timeLabels = useMemo(() => {
    const labels: { ms: number; percent: number }[] = [];
    const step = rangeMs / 4; // 4 labels
    for (let i = 0; i <= 4; i++) {
      const ms = rangeStartMs + step * i;
      labels.push({ ms, percent: msToPercent(ms) });
    }
    return labels;
  }, [rangeStartMs, rangeMs, msToPercent]);

  const chartHeight = height - 20;

  // Handle mouse hover to show percentage tooltip
  const handleMouseMoveHover = useCallback((e: React.MouseEvent) => {
    if (!containerRef.current || dataPoints.length === 0) {
      setHoverInfo(null);
      return;
    }
    
    const rect = containerRef.current.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const hoverPercent = (x / rect.width) * 100;
    const hoverMs = percentToMs(hoverPercent);
    
    // Find closest data point
    let closest = dataPoints[0];
    let minDist = Math.abs(dataPoints[0].ms - hoverMs);
    for (const p of dataPoints) {
      const dist = Math.abs(p.ms - hoverMs);
      if (dist < minDist) {
        minDist = dist;
        closest = p;
      }
    }
    
    // Calculate percentage based on metric mode
    let valueStr = '';
    if (metricMode === 'memory' && totalRam > 0) {
      const pct = (closest.v / totalRam) * 100;
      valueStr = `${pct.toFixed(0)}%`;
    } else if (metricMode === 'cpu' && cpuCores > 0) {
      const cpuFullUs = cpuCores * 1_000_000;
      const pct = (closest.v / cpuFullUs) * 100;
      valueStr = `${pct.toFixed(0)}%`;
    } else {
      // Fallback for network/disk - just show raw value
      setHoverInfo(null);
      return;
    }
    
    setHoverInfo({ x: e.clientX - rect.left, y: e.clientY - rect.top, value: valueStr });
  }, [dataPoints, percentToMs, metricMode, totalRam, cpuCores]);

  const handleMouseLeave = useCallback(() => {
    setHoverInfo(null);
  }, []);

  return (
    <div 
      ref={containerRef}
      style={{
        position: 'relative',
        height,
        background: 'var(--bg-tertiary)',
        borderRadius: 8,
        border: '1px solid var(--border-primary)',
        overflow: 'hidden',
        cursor: isDragging ? 'grabbing' : 'pointer',
        userSelect: 'none',
      }}
      onClick={handleClick}
      onMouseMove={handleMouseMoveHover}
      onMouseLeave={handleMouseLeave}
    >
      {/* Loading overlay */}
      {isLoading && (
        <div style={{
          position: 'absolute',
          inset: 0,
          background: 'rgba(0,0,0,0.3)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 10,
        }}>
          <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>Loading...</span>
        </div>
      )}

      {/* Metric area chart */}
      <svg 
        width="100%" 
        height={chartHeight} 
        viewBox={`0 0 100 ${chartHeight}`} 
        preserveAspectRatio="none"
        style={{ display: 'block' }}
      >
        <defs>
          <linearGradient id={`navGradient-${metricMode}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={cfg.color} stopOpacity="0.4" />
            <stop offset="100%" stopColor={cfg.color} stopOpacity="0.05" />
          </linearGradient>
        </defs>
        {areaPath && (
          <path d={areaPath} fill={`url(#navGradient-${metricMode})`} stroke={cfg.color} strokeWidth="0.5" />
        )}
      </svg>

      {/* Viewport window (draggable) */}
      <div
        style={{
          position: 'absolute',
          top: 0,
          left: `${viewportLeftPercent}%`,
          width: `${viewportWidthPercent}%`,
          height: chartHeight,
          background: 'rgba(88, 166, 255, 0.15)',
          borderLeft: '2px solid #58a6ff',
          borderRight: '2px solid #58a6ff',
          cursor: isDragging ? 'grabbing' : 'grab',
          transition: isDragging ? 'none' : 'left 0.1s ease-out',
        }}
        onMouseDown={handleMouseDown}
      >
        {/* Grip indicator */}
        <div style={{
          position: 'absolute',
          top: '50%',
          left: '50%',
          transform: 'translate(-50%, -50%)',
          display: 'flex',
          gap: 2,
        }}>
          {[0, 1, 2].map(i => (
            <div key={i} style={{
              width: 3,
              height: 12,
              background: 'rgba(88, 166, 255, 0.5)',
              borderRadius: 1,
            }} />
          ))}
        </div>
      </div>

      {/* Dimmed areas outside viewport */}
      <div style={{
        position: 'absolute',
        top: 0,
        left: 0,
        width: `${Math.max(0, viewportLeftPercent)}%`,
        height: chartHeight,
        background: 'var(--overlay-dim, rgba(0, 0, 0, 0.4))',
        pointerEvents: 'none',
      }} />
      <div style={{
        position: 'absolute',
        top: 0,
        right: 0,
        width: `${Math.max(0, 100 - viewportLeftPercent - viewportWidthPercent)}%`,
        height: chartHeight,
        background: 'var(--overlay-dim, rgba(0, 0, 0, 0.4))',
        pointerEvents: 'none',
      }} />

      {/* Time labels */}
      <div style={{
        position: 'absolute',
        bottom: 0,
        left: 0,
        right: 0,
        height: 20,
        display: 'flex',
        alignItems: 'center',
        background: 'var(--bg-secondary)',
        borderTop: '1px solid var(--border-primary)',
      }}>
        {timeLabels.map((label, i) => (
          <div
            key={i}
            style={{
              position: 'absolute',
              left: `${label.percent}%`,
              transform: 'translateX(-50%)',
              fontSize: 9,
              color: 'var(--text-muted)',
              whiteSpace: 'nowrap',
            }}
          >
            {fmtTime(label.ms)}
          </div>
        ))}
      </div>

      {/* Current viewport time indicator */}
      <div style={{
        position: 'absolute',
        top: 2,
        left: `${viewportLeftPercent + viewportWidthPercent / 2}%`,
        transform: 'translateX(-50%)',
        fontSize: 9,
        color: '#58a6ff',
        fontWeight: 600,
        background: 'rgba(13, 17, 23, 0.9)',
        padding: '1px 6px',
        borderRadius: 4,
        whiteSpace: 'nowrap',
        pointerEvents: 'none',
      }}>
        {fmtTime(viewportStartMs)} - {fmtTime(viewportEndMs)}
      </div>

      {/* Hover tooltip showing percentage */}
      {hoverInfo && (
        <div style={{
          position: 'absolute',
          top: Math.max(2, hoverInfo.y - 24),
          left: Math.min(Math.max(hoverInfo.x, 20), containerRef.current?.clientWidth ? containerRef.current.clientWidth - 30 : hoverInfo.x),
          transform: 'translateX(-50%)',
          fontSize: 10,
          color: cfg.lightColor,
          fontWeight: 600,
          background: 'rgba(13, 17, 23, 0.9)',
          padding: '2px 6px',
          borderRadius: 4,
          pointerEvents: 'none',
          zIndex: 20,
          whiteSpace: 'nowrap',
        }}>
          {hoverInfo.value}
        </div>
      )}
    </div>
  );
};

export default TimelineNavigator;
