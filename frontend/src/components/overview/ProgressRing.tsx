/**
 * ProgressRing - Circular progress indicator with percentage
 */

interface ProgressRingProps {
  pct: number;
  size?: number;
  stroke?: number;
  color?: string;
  bgColor?: string;
  label?: string;
  showPercent?: boolean;
  className?: string;
}

export function ProgressRing({
  pct,
  size = 40,
  stroke = 4,
  color = '#3b82f6',
  bgColor = 'rgba(148, 163, 184, 0.2)',
  label,
  showPercent = true,
  className = '',
}: ProgressRingProps) {
  const normalizedPct = Math.max(0, Math.min(100, pct));
  const radius = (size - stroke) / 2;
  const circumference = radius * 2 * Math.PI;
  const offset = circumference - (normalizedPct / 100) * circumference;

  return (
    <div className={`relative inline-flex items-center justify-center ${className}`}>
      <svg width={size} height={size} className="transform -rotate-90">
        {/* Background circle */}
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke={bgColor}
          strokeWidth={stroke}
        />
        {/* Progress circle */}
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke={color}
          strokeWidth={stroke}
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap="round"
          className="transition-all duration-300"
        />
      </svg>
      {/* Center label */}
      <div className="absolute inset-0 flex items-center justify-center">
        {label ? (
          <span className="text-xs font-mono" style={{ color: 'var(--text-secondary)' }}>{label}</span>
        ) : showPercent ? (
          <span className="text-xs font-mono" style={{ color: 'var(--text-secondary)' }}>
            {normalizedPct.toFixed(0)}%
          </span>
        ) : null}
      </div>
    </div>
  );
}

export default ProgressRing;
