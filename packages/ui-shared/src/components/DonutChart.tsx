import React from 'react';

export interface DonutSegment {
  label: string;
  value: number;
  color: string;
}

export interface DonutChartProps {
  segments: DonutSegment[];
  size?: number;
  strokeWidth?: number;
  className?: string;
  style?: React.CSSProperties;
  /** Format function for tooltip values. Defaults to showing raw number. */
  formatValue?: (value: number) => string;
}

function polarToCartesian(cx: number, cy: number, r: number, angleDeg: number) {
  const rad = ((angleDeg - 90) * Math.PI) / 180;
  return { x: cx + r * Math.cos(rad), y: cy + r * Math.sin(rad) };
}

function describeArc(cx: number, cy: number, r: number, startAngle: number, endAngle: number): string {
  // Handle full circle case
  if (endAngle - startAngle >= 359.999) {
    const mid = startAngle + 180;
    const s = polarToCartesian(cx, cy, r, startAngle);
    const m = polarToCartesian(cx, cy, r, mid);
    return [
      `M ${s.x} ${s.y}`,
      `A ${r} ${r} 0 1 1 ${m.x} ${m.y}`,
      `A ${r} ${r} 0 1 1 ${s.x} ${s.y}`,
    ].join(' ');
  }

  const start = polarToCartesian(cx, cy, r, startAngle);
  const end = polarToCartesian(cx, cy, r, endAngle);
  const largeArc = endAngle - startAngle > 180 ? 1 : 0;
  return `M ${start.x} ${start.y} A ${r} ${r} 0 ${largeArc} 1 ${end.x} ${end.y}`;
}

/** Default format function - formats bytes to human readable */
function defaultFormatValue(value: number): string {
  if (value === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(value) / Math.log(k));
  const idx = Math.min(i, sizes.length - 1);
  return `${parseFloat((value / Math.pow(k, idx)).toFixed(2))} ${sizes[idx]}`;
}

export function DonutChart({
  segments,
  size = 120,
  strokeWidth = 20,
  className,
  style,
  formatValue = defaultFormatValue,
}: DonutChartProps) {
  const cx = size / 2;
  const cy = size / 2;
  const r = (size - strokeWidth) / 2;

  const total = segments.reduce((sum, s) => sum + Math.max(0, s.value), 0);

  if (total === 0 || segments.length === 0) {
    return (
      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${size} ${size}`}
        className={className}
        style={style}
        role="img"
        aria-label="Empty donut chart"
      >
        <circle cx={cx} cy={cy} r={r} fill="none" stroke="currentColor" strokeWidth={strokeWidth} opacity={0.2} />
      </svg>
    );
  }

  let currentAngle = 0;
  const arcs = segments
    .filter((s) => s.value > 0)
    .map((segment) => {
      const sweep = (segment.value / total) * 360;
      const startAngle = currentAngle;
      const endAngle = currentAngle + sweep;
      currentAngle = endAngle;
      return { segment, startAngle, endAngle };
    });

  return (
    <svg
      width={size}
      height={size}
      viewBox={`0 0 ${size} ${size}`}
      className={className}
      style={style}
      role="img"
      aria-label="Donut chart"
    >
      {arcs.map(({ segment, startAngle, endAngle }, i) => (
        <path
          key={i}
          d={describeArc(cx, cy, r, startAngle, endAngle)}
          fill="none"
          stroke={segment.color}
          strokeWidth={strokeWidth}
          strokeLinecap="butt"
        >
          <title>{`${segment.label}: ${formatValue(segment.value)}`}</title>
        </path>
      ))}
    </svg>
  );
}
