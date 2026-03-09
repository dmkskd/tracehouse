/**
 * MiniBar - Small horizontal progress bar for table cells
 */

interface MiniBarProps {
  value: number;
  max: number;
  color?: string;
  width?: number;
  height?: number;
  showLabel?: boolean;
  className?: string;
}

export function MiniBar({
  value,
  max,
  color = '#3b82f6',
  width = 60,
  height = 6,
  showLabel = false,
  className = '',
}: MiniBarProps) {
  const percentage = max > 0 ? Math.min(100, (value / max) * 100) : 0;

  return (
    <div className={`flex items-center gap-2 ${className}`}>
      <div
        className="rounded-full overflow-hidden"
        style={{
          width,
          height,
          backgroundColor: 'var(--bg-tertiary)',
        }}
      >
        <div
          className="h-full rounded-full transition-all duration-300"
          style={{
            width: `${percentage}%`,
            backgroundColor: color,
          }}
        />
      </div>
      {showLabel && (
        <span className="text-xs font-mono" style={{ color: 'var(--text-tertiary)' }}>
          {percentage.toFixed(0)}%
        </span>
      )}
    </div>
  );
}

export default MiniBar;
