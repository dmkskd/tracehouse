/**
 * Sparkline — tiny inline SVG trend for table cells.
 *
 * Pure SVG, no recharts dependency. Renders an array of y-values
 * as a compact line/area with an optional reference line.
 */

import React from 'react';

export interface SparklineProps {
  data: number[];
  width?: number;
  height?: number;
  color?: string;
  /** Horizontal reference line (e.g. 0 for delta charts) */
  referenceValue?: number;
  /** Fill area under the line */
  fill?: boolean;
}

const DEFAULT_COLOR = '#6366f1';

export const Sparkline: React.FC<SparklineProps> = ({
  data,
  width = 80,
  height = 22,
  color = DEFAULT_COLOR,
  referenceValue,
  fill = false,
}) => {
  if (!data || data.length < 2) {
    return <span style={{ color: 'var(--text-muted)', fontSize: 10 }}>—</span>;
  }

  const pad = 1;
  const w = width - pad * 2;
  const h = height - pad * 2;
  const min = Math.min(...data, referenceValue ?? Infinity);
  const max = Math.max(...data, referenceValue ?? -Infinity);
  const range = max - min || 1;

  const points = data.map((v, i) => {
    const x = pad + (i / (data.length - 1)) * w;
    const y = pad + h - ((v - min) / range) * h;
    return `${x},${y}`;
  });

  const polyline = points.join(' ');

  // Area path: line path → bottom-right → bottom-left → close
  const areaPath = fill
    ? `M${points[0]} ${points.slice(1).map(p => `L${p}`).join(' ')} L${pad + w},${pad + h} L${pad},${pad + h} Z`
    : undefined;

  const refY = referenceValue != null
    ? pad + h - ((referenceValue - min) / range) * h
    : undefined;

  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      {refY != null && (
        <line
          x1={pad} y1={refY} x2={pad + w} y2={refY}
          stroke="var(--text-muted)" strokeWidth={0.5} strokeDasharray="2,2"
        />
      )}
      {areaPath && (
        <path d={areaPath} fill={color} opacity={0.15} />
      )}
      <polyline
        points={polyline}
        fill="none"
        stroke={color}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      {/* Dot on last value */}
      <circle
        cx={pad + w}
        cy={pad + h - ((data[data.length - 1] - min) / range) * h}
        r={2}
        fill={color}
      />
    </svg>
  );
};
