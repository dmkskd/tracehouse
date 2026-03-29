/**
 * GaugeBar — inline horizontal gauge for table cells.
 *
 * Renders a colored bar proportional to value/max with an optional label.
 * Integrates with @rag coloring when a ragColor is provided.
 */

import React from 'react';

export interface GaugeBarProps {
  value: number;
  max: number;
  /** RAG color from getRagColor(), or falls back to default blue */
  ragColor?: string;
  unit?: string;
  /** Bar height in px */
  height?: number;
}

const DEFAULT_COLOR = '#6366f1';
const BG_COLOR = 'rgba(148, 163, 184, 0.15)';

export const GaugeBar: React.FC<GaugeBarProps> = ({
  value,
  max,
  ragColor,
  unit,
  height = 16,
}) => {
  const pct = max > 0 ? Math.max(0, Math.min(100, (value / max) * 100)) : 0;
  const color = ragColor || DEFAULT_COLOR;
  const label = unit ? `${value} ${unit}` : String(value);

  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 6, minWidth: 100,
    }}>
      <div style={{
        flex: 1, height, background: BG_COLOR, borderRadius: 3,
        overflow: 'hidden', position: 'relative',
      }}>
        <div style={{
          width: `${pct}%`, height: '100%', background: color,
          borderRadius: 3, transition: 'width 0.3s ease',
        }} />
      </div>
      <span style={{
        fontSize: 11, fontVariantNumeric: 'tabular-nums',
        color: ragColor || 'var(--text-secondary)', whiteSpace: 'nowrap',
        minWidth: 36, textAlign: 'right',
      }}>
        {label}
      </span>
    </div>
  );
};
