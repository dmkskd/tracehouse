import React from 'react';
import { radarShapeLayout } from './radarModel';

export const RadarShape: React.FC<{
  values: number[];
  labels?: string[];
  color: string;
  size?: number | string;
  title?: string;
  showValues?: boolean;
  rawValues?: string[];
  variant?: 'cell' | 'chart';
}> = ({ values, labels = [], color, size = 52, title, showValues = false, rawValues = [], variant = showValues ? 'chart' : 'cell' }) => {
  const layout = radarShapeLayout(values, labels, variant);
  const viewBox = layout.viewBox;

  return (
    <svg width={size} height={size} viewBox={viewBox} role="img" aria-label={title ?? 'Radar shape'} style={{ userSelect: 'none', overflow: 'visible', maxWidth: '100%', maxHeight: '100%' }}>
      {title ? <title>{title}</title> : null}
      <circle cx={layout.center.x} cy={layout.center.y} r={layout.radius + 1} fill="var(--bg-secondary, #f8fafc)" stroke="var(--border-primary, #d1d5db)" />
      {layout.spokes.map((point, i) => (
        <line
          key={i}
          x1={layout.center.x}
          y1={layout.center.y}
          x2={point.x}
          y2={point.y}
          stroke="var(--border-primary, #d1d5db)"
          strokeWidth="1"
        />
      ))}
      <polygon points={layout.polygonPoints} fill={`${color}30`} stroke={color} strokeWidth="2" />
      {layout.labels.map((label, i) => (
        <text
          key={`${label.label}-${i}`}
          x={label.x}
          y={label.y}
          textAnchor={label.anchor}
          dominantBaseline={label.baseline}
          fill="var(--text-muted, #94a3b8)"
          fontSize="6"
          fontWeight="700"
          letterSpacing="0"
          stroke="var(--bg-primary, #ffffff)"
          strokeWidth="2.5"
          paintOrder="stroke"
        >
          {showValues ? (
            <>
              <tspan x={label.x} dy="0">{`${label.label} ${values[i]?.toFixed(2) ?? '0.00'}`}</tspan>
              {rawValues[i] ? <tspan x={label.x} dy="8" fontSize="5" fontWeight="600">{rawValues[i]}</tspan> : null}
            </>
          ) : label.label}
        </text>
      ))}
    </svg>
  );
};
