import React from 'react';
import type { MergeEvent } from '@tracehouse/core';

export interface MergeTimelineProps {
  events: MergeEvent[];
  width?: number;
  rowHeight?: number;
  className?: string;
  style?: React.CSSProperties;
}

// Colors for merge levels (L0, L1, L2, etc.) - matches 3D visualization
const LEVEL_COLORS = [
  '#ef4444', // L0 - Red (unmerged, needs attention)
  '#a855f7', // L1 - Purple
  '#8b5cf6', // L2 - Violet
  '#22c55e', // L3 - Green
  '#14b8a6', // L4 - Teal
  '#f59e0b', // L5+ - Amber/Orange
];

function getLevelColor(level: number): string {
  return LEVEL_COLORS[Math.min(level, LEVEL_COLORS.length - 1)];
}

export function MergeTimeline({
  events,
  width = 600,
  rowHeight = 28,
  className,
  style,
}: MergeTimelineProps) {
  if (events.length === 0) {
    return (
      <svg
        width={width}
        height={rowHeight}
        viewBox={`0 0 ${width} ${rowHeight}`}
        className={className}
        style={style}
        role="img"
        aria-label="Empty merge timeline"
      >
        <text x={width / 2} y={rowHeight / 2} textAnchor="middle" dominantBaseline="central" fill="currentColor">
          No merge events
        </text>
      </svg>
    );
  }

  // Group events by level
  const levelMap = new Map<number, MergeEvent[]>();
  for (const ev of events) {
    const list = levelMap.get(ev.level) ?? [];
    list.push(ev);
    levelMap.set(ev.level, list);
  }

  const levels = Array.from(levelMap.keys()).sort((a, b) => a - b);

  // Compute time range from event_time and duration_ms
  // event_time is when the merge COMPLETED, so start = event_time - duration_ms
  const timestamps: { start: number; end: number }[] = [];
  for (const ev of events) {
    const endTime = new Date(ev.event_time).getTime();
    const startTime = endTime - ev.duration_ms;
    // Validate the times are reasonable
    if (!isNaN(endTime) && !isNaN(startTime) && startTime <= endTime) {
      timestamps.push({ start: startTime, end: endTime });
    }
  }

  if (timestamps.length === 0) {
    return (
      <svg
        width={width}
        height={rowHeight}
        viewBox={`0 0 ${width} ${rowHeight}`}
        className={className}
        style={style}
        role="img"
        aria-label="Empty merge timeline"
      >
        <text x={width / 2} y={rowHeight / 2} textAnchor="middle" dominantBaseline="central" fill="currentColor">
          No valid merge events
        </text>
      </svg>
    );
  }

  const minTime = Math.min(...timestamps.map((t) => t.start));
  const maxTime = Math.max(...timestamps.map((t) => t.end));
  const timeSpan = maxTime - minTime || 1;

  const labelWidth = 40;
  const chartWidth = width - labelWidth;
  const height = levels.length * rowHeight;

  function timeToX(t: number): number {
    // Clamp to valid range
    const normalized = Math.max(0, Math.min(1, (t - minTime) / timeSpan));
    return labelWidth + normalized * chartWidth;
  }

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className={className}
      style={style}
      role="img"
      aria-label="Merge timeline Gantt chart"
    >
      {levels.map((level, rowIdx) => {
        const y = rowIdx * rowHeight;
        const levelEvents = levelMap.get(level)!;
        const levelColor = getLevelColor(level);

        return (
          <g key={level}>
            {/* Level label */}
            <text
              x={labelWidth - 4}
              y={y + rowHeight / 2}
              textAnchor="end"
              dominantBaseline="central"
              fill="currentColor"
              style={{ fontSize: 11 }}
            >
              L{level}
            </text>

            {/* Row separator */}
            <line x1={labelWidth} y1={y + rowHeight} x2={width} y2={y + rowHeight} stroke="currentColor" opacity={0.1} />

            {/* Event bars */}
            {levelEvents.map((ev, evIdx) => {
              const endTime = new Date(ev.event_time).getTime();
              const startTime = endTime - ev.duration_ms;
              
              // Skip invalid events
              if (isNaN(endTime) || isNaN(startTime) || startTime > endTime) {
                return null;
              }
              
              const barStart = timeToX(startTime);
              const barEnd = timeToX(endTime);
              const barWidth = Math.max(barEnd - barStart, 2);

              return (
                <rect
                  key={evIdx}
                  x={barStart}
                  y={y + 4}
                  width={barWidth}
                  height={rowHeight - 8}
                  rx={2}
                  fill={levelColor}
                  opacity={0.7}
                >
                  <title>{`${ev.part_name} (${ev.duration_ms}ms)`}</title>
                </rect>
              );
            })}
          </g>
        );
      })}
    </svg>
  );
}
