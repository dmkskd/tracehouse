/**
 * StatusDot - Connection/health status indicator dot
 */

import { OVERVIEW_COLORS } from '../../styles/overviewColors';

type StatusType = 'ok' | 'warn' | 'crit' | 'idle' | 'polling' | 'error';

interface StatusDotProps {
  status: StatusType;
  size?: number;
  pulse?: boolean;
  label?: string;
  className?: string;
}

const STATUS_COLORS: Record<StatusType, string> = {
  ok: OVERVIEW_COLORS.ok,
  warn: OVERVIEW_COLORS.warn,
  crit: OVERVIEW_COLORS.crit,
  idle: OVERVIEW_COLORS.textDim,
  polling: OVERVIEW_COLORS.ok,
  error: OVERVIEW_COLORS.crit,
};

export function StatusDot({
  status,
  size = 8,
  pulse = false,
  label,
  className = '',
}: StatusDotProps) {
  const color = STATUS_COLORS[status];
  const shouldPulse = pulse || status === 'polling';

  return (
    <div className={`flex items-center gap-2 ${className}`}>
      <div className="relative">
        <div
          className="rounded-full"
          style={{
            width: size,
            height: size,
            backgroundColor: color,
          }}
        />
        {shouldPulse && (
          <div
            className="absolute inset-0 rounded-full animate-ping"
            style={{
              backgroundColor: color,
              opacity: 0.4,
            }}
          />
        )}
      </div>
      {label && (
        <span className="text-xs" style={{ color: 'var(--text-tertiary)' }}>{label}</span>
      )}
    </div>
  );
}

export default StatusDot;
