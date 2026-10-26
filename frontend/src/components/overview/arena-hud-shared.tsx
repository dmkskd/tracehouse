/**
 * Small components shared by the ResourceArena3D overlays (hover card, HUD,
 * inspector): sparkline and mini bar.
 */

export function Sparkline({ data, color, width = 140, height = 32, label, currentValue }: {
  data: number[];
  color: string;
  width?: number;
  height?: number;
  label: string;
  currentValue: string;
}) {
  if (data.length < 2) {
    return (
      <div style={{ width, height: height + 20, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <span style={{ fontSize: 8, color: 'rgba(255,255,255,0.15)', letterSpacing: 1 }}>collecting...</span>
      </div>
    );
  }

  const max = Math.max(...data) || 1;
  const min = Math.min(...data);
  const range = max - min || 1;
  const pad = 2;
  const plotW = width - pad * 2;
  const plotH = height - pad * 2;

  const points = data.map((v, i) => {
    const x = pad + (i / (data.length - 1)) * plotW;
    const y = pad + plotH - ((v - min) / range) * plotH;
    return `${x},${y}`;
  });

  const fillPoints = [
    `${pad},${height - pad}`,
    ...points,
    `${pad + plotW},${height - pad}`,
  ].join(' ');

  return (
    <div style={{ marginBottom: 2 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 1 }}>
        <span style={{ fontSize: 8, color: 'rgba(255,255,255,0.3)', letterSpacing: 1.5, fontWeight: 700 }}>{label}</span>
        <span style={{ fontSize: 11, color, fontWeight: 600, fontVariantNumeric: 'tabular-nums',
          filter: `drop-shadow(0 0 6px ${color}80)`,
        }}>{currentValue}</span>
      </div>
      <svg width={width} height={height} style={{ display: 'block' }}>
        <defs>
          <linearGradient id={`spark-fill-${label}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.15} />
            <stop offset="100%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        <polygon points={fillPoints} fill={`url(#spark-fill-${label})`} />
        <polyline points={points.join(' ')} fill="none" stroke={color} strokeWidth={1.5}
          strokeLinejoin="round" strokeLinecap="round"
          style={{ filter: `drop-shadow(0 0 6px ${color}90) drop-shadow(0 0 2px ${color})` }} />
        {/* Current value dot — glowing */}
        {data.length > 0 && (() => {
          const lastX = pad + plotW;
          const lastY = pad + plotH - ((data[data.length - 1] - min) / range) * plotH;
          return <circle cx={lastX} cy={lastY} r={3} fill={color}
            style={{ filter: `drop-shadow(0 0 8px ${color}) drop-shadow(0 0 3px ${color})` }} />;
        })()}
      </svg>
    </div>
  );
}

/** Tiny inline bar for the hover card */
export function MiniBar({ ratio, color }: { ratio: number; color: string }) {
  const pct = Math.max(2, Math.min(100, ratio * 100));
  return (
    <div style={{
      width: 40, height: 4, borderRadius: 2,
      background: 'rgba(255,255,255,0.06)', overflow: 'hidden', display: 'inline-block',
      verticalAlign: 'middle', marginLeft: 4,
    }}>
      <div style={{
        height: '100%', borderRadius: 2, width: `${pct}%`,
        background: color, boxShadow: `0 0 4px ${color}40`,
      }} />
    </div>
  );
}
