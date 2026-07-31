/**
 * TimelineNavigator - Mini timeline for navigating through a longer time range
 * 
 * Shows a longer overview with metric data and a draggable viewport window
 * representing the current view window that can be dragged to navigate time.
 * Mirrors the metric mode from the main chart (Memory, CPU, Network, Disk).
 */
import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import type { OperationalEvent, TimeseriesPoint } from '@tracehouse/core';
import { formatBytes } from '../../utils/formatters';
import {
  EVENT_SEVERITY_COLORS,
  clusterTimelineEvents,
} from '../timeline/timeline-event-model';
import {
  clampNavigatorDragX,
  navigatorByteScaleCeiling,
  navigatorEdgeScrollVelocity,
  navigatorPercentScaleCeiling,
} from '../timeline/timeline-navigator-buffer';

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
  /** Optional series used only to keep the Y scale stable between shapes. */
  scaleData?: TimeseriesPoint[];
  /** Area for absolute values; delta renders signed, zero-centred bars. */
  variant?: 'area' | 'delta';
  /** Width of each downsampled metric bucket in milliseconds. */
  bucketMs?: number;
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
  /** Filtered operational events for the overview range. */
  events?: OperationalEvent[];
  selectedEventId?: string | null;
  /** Select and navigate to an event marker. */
  onEventSelect?: (event: OperationalEvent) => void;
}

const fmtTime = (ms: number): string => new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

export const TimelineNavigator: React.FC<TimelineNavigatorProps> = ({
  data,
  scaleData,
  variant = 'area',
  bucketMs,
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
  events = [],
  selectedEventId,
  onEventSelect,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(1000);
  const [isDragging, setIsDragging] = useState(false);
  const didDragRef = useRef(false);
  const suppressClickUntilRef = useRef(0);
  const dragStateRef = useRef<{
    lastX: number;
    pointerX: number;
    currentEndMs: number;
    frozenRangeMs: number;      // locked at drag start so delta calc stays stable
    frozenContainerWidth: number; // container width at drag start
  } | null>(null);

  const [hoverInfo, setHoverInfo] = useState<{
    x: number;
    y: number;
    value: string;
    color?: string;
  } | null>(null);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return;
    const update = () => setContainerWidth(element.clientWidth || 1000);
    update();
    const observer = new ResizeObserver(update);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

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

  const scalePoints = useMemo(() => {
    if (!scaleData) return dataPoints;
    return scaleData.map(p => {
      const normalized = p.t.replace(' ', 'T') + (p.t.includes('Z') || p.t.includes('+') ? '' : 'Z');
      return { ms: new Date(normalized).getTime(), v: p.v };
    }).filter(p => p.ms >= rangeStartMs && p.ms <= rangeEndMs);
  }, [scaleData, dataPoints, rangeStartMs, rangeEndMs]);

  // Calculate max Y for scaling
  const maxY = useMemo(() => {
    const rawMax = variant === 'delta'
      ? Math.max(0, ...dataPoints.map(point => Math.abs(point.v)))
      : Math.max(0, ...scalePoints.map(point => point.v));
    if (rawMax === 0) return 1;

    const capacity = metricMode === 'cpu' && cpuCores > 0
      ? cpuCores * 1_000_000
      : metricMode === 'memory' && totalRam > 0
        ? totalRam
        : 0;
    if (capacity > 0) {
      const percent = (rawMax / capacity) * 100;
      return capacity * navigatorPercentScaleCeiling(percent) / 100;
    }
    if (metricMode === 'network' || metricMode === 'disk') {
      return navigatorByteScaleCeiling(rawMax);
    }
    return rawMax * 1.1;
  }, [variant, dataPoints, scalePoints, metricMode, cpuCores, totalRam]);

  // Convert ms to X position (0-100%)
  const msToPercent = useCallback((ms: number) => {
    return ((ms - rangeStartMs) / rangeMs) * 100;
  }, [rangeStartMs, rangeMs]);

  // Convert X position to ms
  const percentToMs = useCallback((percent: number) => {
    return rangeStartMs + (percent / 100) * rangeMs;
  }, [rangeStartMs, rangeMs]);

  // Build touching step-filled buckets. Each contiguous run becomes its own
  // sub-path, so missing buckets stay empty instead of becoming diagonal data.
  const areaPaths = useMemo(() => {
    if (variant !== 'area' || dataPoints.length === 0) return [];
    const chartHeight = height - 20; // Leave room for labels
    const sorted = [...dataPoints].sort((left, right) => left.ms - right.ms);
    const positiveIntervals = sorted
      .slice(1)
      .map((point, index) => point.ms - sorted[index].ms)
      .filter(interval => interval > 0)
      .sort((left, right) => left - right);
    const inferredBucketMs = positiveIntervals.length > 0
      ? positiveIntervals[Math.floor(positiveIntervals.length / 2)]
      : rangeMs;
    const effectiveBucketMs = bucketMs && bucketMs > 0 ? bucketMs : inferredBucketMs;

    const segments: typeof sorted[] = [];
    for (const point of sorted) {
      const segment = segments[segments.length - 1];
      const previous = segment?.[segment.length - 1];
      if (!segment || (previous && point.ms - previous.ms > effectiveBucketMs * 1.5)) {
        segments.push([point]);
      } else {
        segment.push(point);
      }
    }

    const xAt = (ms: number) => msToPercent(Math.max(rangeStartMs, Math.min(rangeEndMs, ms)));
    const yAt = (value: number) => chartHeight - (value / maxY) * chartHeight;

    return segments.map(segment => {
      const startX = xAt(segment[0].ms);
      const endX = xAt(segment[segment.length - 1].ms + effectiveBucketMs);
      if (endX <= startX) return '';

      let path = `M${startX},${chartHeight} L${startX},${yAt(segment[0].v)}`;
      for (let index = 1; index < segment.length; index += 1) {
        const boundaryX = xAt(segment[index].ms);
        path += ` L${boundaryX},${yAt(segment[index - 1].v)}`;
        path += ` L${boundaryX},${yAt(segment[index].v)}`;
      }
      path += ` L${endX},${yAt(segment[segment.length - 1].v)}`;
      path += ` L${endX},${chartHeight} Z`;
      return path;
    }).filter(Boolean);
  }, [variant, dataPoints, maxY, height, msToPercent, rangeStartMs, rangeEndMs, rangeMs, bucketMs]);

  const deltaBars = useMemo(() => {
    if (variant !== 'delta' || dataPoints.length === 0) return [];
    const chartHeight = height - 20;
    const zeroY = chartHeight / 2;
    const width = Math.max(0.05, Math.min(1.5, (100 / dataPoints.length) * 0.82));
    return dataPoints.map(point => {
      const magnitude = Math.abs(point.v) / maxY;
      const barHeight = magnitude * zeroY;
      return {
        x: msToPercent(point.ms) - width / 2,
        y: point.v >= 0 ? zeroY - barHeight : zeroY,
        width,
        height: Math.max(point.v === 0 ? 0 : 0.35, barHeight),
        color: point.v >= 0 ? '#d29922' : '#38bdf8',
        opacity: 0.25 + Math.sqrt(magnitude) * 0.75,
      };
    });
  }, [variant, dataPoints, height, maxY, msToPercent]);

  const scaleValues = useMemo(() => {
    const formatScaleValue = (value: number): string => {
      const sign = value < 0 ? '−' : '';
      const absolute = Math.abs(value);
      if (metricMode === 'cpu' && cpuCores > 0) {
        const percent = (absolute / (cpuCores * 1_000_000)) * 100;
        return `${sign}${percent.toFixed(percent > 0 && percent < 10 ? 1 : 0)}%`;
      }
      if (metricMode === 'memory' && totalRam > 0) {
        const percent = (absolute / totalRam) * 100;
        return `${sign}${percent.toFixed(percent > 0 && percent < 10 ? 1 : 0)}%`;
      }
      if (metricMode === 'cpu') {
        const cores = absolute / 1_000_000;
        return `${sign}${cores.toFixed(cores > 0 && cores < 10 ? 1 : 0)}c`;
      }
      return `${sign}${formatBytes(absolute)}`;
    };

    return {
      max: formatScaleValue(maxY),
      min: formatScaleValue(variant === 'delta' ? -maxY : 0),
    };
  }, [maxY, variant, metricMode, cpuCores, totalRam]);

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
      pointerX: x,
      currentEndMs: viewportEndMs,
      frozenRangeMs: rangeMs,
      frozenContainerWidth: rect.width,
    };
    didDragRef.current = false;
    setIsDragging(true);
    e.preventDefault();
  }, [viewportEndMs, rangeMs]);

  const handleMouseMove = useCallback((e: MouseEvent) => {
    const dragState = dragStateRef.current;
    if (!isDragging || !dragState) return;

    const rawX = e.clientX - (containerRef.current?.getBoundingClientRect().left ?? 0);
    const currentX = clampNavigatorDragX(rawX, dragState.frozenContainerWidth);
    const deltaX = currentX - dragState.lastX;
    dragState.lastX = currentX;
    // Edge velocity deliberately keeps the raw coordinate so holding beyond
    // an edge can still accelerate scrolling without corrupting drag deltas.
    dragState.pointerX = rawX;
    if (Math.abs(deltaX) > 1) didDragRef.current = true;

    // Use frozen values from drag start — immune to range extension feedback loops
    const deltaPx = deltaX / dragState.frozenContainerWidth;
    const deltaMs = deltaPx * dragState.frozenRangeMs;

    const newEndMs = Math.min(dragState.currentEndMs + deltaMs, Date.now());
    dragState.currentEndMs = newEndMs;

    // Parent handles clamping (future, etc.) and range extension
    onViewportChange(newEndMs);
  }, [isDragging, onViewportChange]);

  const handleMouseUp = useCallback(() => {
    if (onDragEnd && dragStateRef.current) {
      onDragEnd(dragStateRef.current.currentEndMs);
    }
    if (didDragRef.current) suppressClickUntilRef.current = Date.now() + 250;
    didDragRef.current = false;
    setIsDragging(false);
    dragStateRef.current = null;
  }, [onDragEnd]);

  // Handle click to jump to position
  const handleClick = useCallback((e: React.MouseEvent) => {
    if (isDragging) return; // Don't jump if we were dragging
    if (Date.now() < suppressClickUntilRef.current) return;
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

  // Keep panning while the pointer is held at either edge. The parent shifts
  // the fixed-duration range and prefetches adjacent chunks as time advances.
  useEffect(() => {
    if (!isDragging) return;
    let frame = 0;
    let previousFrame = performance.now();
    const tick = (now: number) => {
      const dragState = dragStateRef.current;
      if (!dragState) return;
      const elapsedSeconds = Math.min((now - previousFrame) / 1000, 0.05);
      previousFrame = now;

      const velocity = navigatorEdgeScrollVelocity(
        dragState.pointerX,
        dragState.frozenContainerWidth,
        dragState.frozenRangeMs,
      );
      if (velocity !== 0) {
        dragState.currentEndMs = Math.min(
          dragState.currentEndMs + velocity * elapsedSeconds,
          Date.now(),
        );
        didDragRef.current = true;
        onViewportChange(dragState.currentEndMs);
      }
      frame = requestAnimationFrame(tick);
    };

    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [isDragging, onViewportChange]);

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
  const eventClusters = useMemo(
    () => clusterTimelineEvents(events, rangeStartMs, rangeEndMs, containerWidth, 9),
    [events, rangeStartMs, rangeEndMs, containerWidth],
  );

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
    
    if (variant === 'delta') {
      const sign = closest.v > 0 ? '+' : closest.v < 0 ? '−' : '';
      const absolute = Math.abs(closest.v);
      let value: string;
      if (metricMode === 'cpu') {
        const cores = absolute / 1_000_000;
        value = `${sign}${cores.toFixed(cores >= 10 ? 0 : 1)} cores`;
      } else if (metricMode === 'memory') {
        const gib = absolute / (1024 ** 3);
        const mib = absolute / (1024 ** 2);
        value = gib >= 0.1
          ? `${sign}${gib.toFixed(gib >= 10 ? 0 : 1)} GiB`
          : `${sign}${mib.toFixed(mib >= 10 ? 0 : 1)} MiB`;
      } else {
        value = `${sign}${absolute.toLocaleString()}`;
      }
      setHoverInfo({
        x: e.clientX - rect.left,
        y: e.clientY - rect.top,
        value,
        color: closest.v >= 0 ? '#d29922' : '#38bdf8',
      });
      return;
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
  }, [dataPoints, percentToMs, variant, metricMode, totalRam, cpuCores]);

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
      {/* Non-blocking background-load indicator */}
      {isLoading && (
        <div style={{
          position: 'absolute',
          top: 4,
          right: 6,
          background: 'var(--bg-secondary)',
          border: '1px solid var(--border-primary)',
          borderRadius: 4,
          padding: '1px 6px',
          zIndex: 10,
          pointerEvents: 'none',
        }}>
          <span style={{ color: 'var(--text-muted)', fontSize: 10 }}>Loading history…</span>
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
        {areaPaths.map((path, index) => (
          <path
            key={index}
            d={path}
            fill={`url(#navGradient-${metricMode})`}
            stroke={cfg.color}
            strokeWidth="2"
            vectorEffect="non-scaling-stroke"
          />
        ))}
        {variant === 'delta' && (
          <>
            <line
              x1="0"
              x2="100"
              y1={chartHeight / 2}
              y2={chartHeight / 2}
              stroke="var(--border-primary)"
              strokeWidth="0.45"
              vectorEffect="non-scaling-stroke"
            />
            {deltaBars.map((bar, index) => (
              <rect
                key={index}
                x={bar.x}
                y={bar.y}
                width={bar.width}
                height={bar.height}
                fill={bar.color}
                opacity={bar.opacity}
              />
            ))}
          </>
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

      {/* Minimal adaptive scale values. */}
      <span
        data-testid="navigator-scale-max"
        style={{
          position: 'absolute',
          top: 3,
          left: 6,
          zIndex: 9,
          color: 'var(--text-muted)',
          fontSize: 9,
          lineHeight: 1,
          pointerEvents: 'none',
        }}
      >
        {scaleValues.max}
      </span>
      <span
        data-testid="navigator-scale-min"
        style={{
          position: 'absolute',
          top: Math.max(3, chartHeight - 12),
          left: 6,
          zIndex: 9,
          color: 'var(--text-muted)',
          fontSize: 9,
          lineHeight: 1,
          pointerEvents: 'none',
        }}
      >
        {scaleValues.min}
      </span>

      {/* Operational event ticks — clustered only when markers would overlap. */}
      {eventClusters.map(cluster => {
        const left = Math.max(0, Math.min(100, msToPercent(cluster.occurredAtMs)));
        const selected = cluster.events.some(event => event.id === selectedEventId);
        return (
          <button
            key={cluster.id}
            title={cluster.events.length === 1
              ? `${cluster.primaryEvent.title}\n${new Date(cluster.primaryEvent.occurred_at).toLocaleString()}`
              : `${cluster.events.length} events near ${new Date(cluster.occurredAtMs).toLocaleString()}`}
            aria-label={cluster.events.length === 1
              ? cluster.primaryEvent.title
              : `${cluster.events.length} operational events`}
            onMouseDown={event => event.stopPropagation()}
            onClick={event => {
              event.stopPropagation();
              onEventSelect?.(cluster.primaryEvent);
            }}
            style={{
              position: 'absolute',
              top: 4,
              left: `${left}%`,
              zIndex: 8,
              width: 2,
              height: chartHeight - 8,
              padding: 0,
              transform: 'translateX(-50%)',
              border: 'none',
              borderRadius: 1,
              background: EVENT_SEVERITY_COLORS[cluster.severity],
              opacity: selected ? 1 : 0.55,
              boxShadow: selected
                ? `0 0 0 1px ${EVENT_SEVERITY_COLORS[cluster.severity]}88`
                : 'none',
              cursor: onEventSelect ? 'pointer' : 'default',
            }}
          >
            <span style={{
              position: 'absolute',
              top: -1,
              left: '50%',
              width: cluster.events.length > 1 ? 8 : 6,
              height: cluster.events.length > 1 ? 8 : 6,
              transform: 'translate(-50%, -50%)',
              borderRadius: '50%',
              background: EVENT_SEVERITY_COLORS[cluster.severity],
              border: '1px solid var(--bg-secondary)',
            }} />
          </button>
        );
      })}

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
          left: Math.min(
            Math.max(hoverInfo.x, 20),
            containerWidth > 0 ? containerWidth - 30 : hoverInfo.x,
          ),
          transform: 'translateX(-50%)',
          fontSize: 10,
          color: hoverInfo.color ?? cfg.lightColor,
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
