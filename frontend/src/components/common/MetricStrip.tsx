import React from 'react';

interface MetricStripProps {
  children: React.ReactNode;
  ariaLabel: string;
  style?: React.CSSProperties;
}

interface MetricStripItemProps {
  label: string;
  value: React.ReactNode;
  color?: string;
  indicatorColor?: string;
  title?: string;
  barPercentage?: number;
}

/**
 * Compact, responsive summary used above data-heavy views.
 *
 * Items deliberately stay on one line and the strip wraps between items on
 * narrow screens, keeping the summary substantially shorter than a card grid.
 */
export const MetricStrip: React.FC<MetricStripProps> = ({
  children,
  ariaLabel,
  style,
}) => (
  <div
    role="group"
    aria-label={ariaLabel}
    style={{
      display: 'flex',
      alignItems: 'center',
      flexWrap: 'wrap',
      gap: '8px 18px',
      padding: '8px 16px',
      border: '1px solid var(--border-primary)',
      borderRadius: 8,
      background: 'var(--bg-card)',
      ...style,
    }}
  >
    {children}
  </div>
);

export const MetricStripItem: React.FC<MetricStripItemProps> = ({
  label,
  value,
  color,
  indicatorColor,
  title,
  barPercentage,
}) => {
  const safeBarPercentage =
    barPercentage === undefined
      ? undefined
      : Math.min(Math.max(barPercentage, 0), 100);

  return (
    <span
      title={title}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
        minWidth: 0,
        whiteSpace: 'nowrap',
      }}
    >
      {indicatorColor && (
        <span
          aria-hidden="true"
          style={{
            width: 7,
            height: 7,
            flexShrink: 0,
            borderRadius: 2,
            background: indicatorColor,
          }}
        />
      )}
      <span
        style={{
          color: color ?? 'var(--text-primary)',
          fontFamily: 'var(--font-mono, monospace)',
          fontSize: 13,
          fontWeight: 600,
        }}
      >
        {value}
      </span>
      {safeBarPercentage !== undefined && (
        <span
          aria-hidden="true"
          style={{
            width: 56,
            height: 5,
            flexShrink: 0,
            overflow: 'hidden',
            borderRadius: 3,
            background: 'var(--bg-tertiary)',
          }}
        >
          <span
            style={{
              display: 'block',
              width: `${safeBarPercentage}%`,
              height: '100%',
              borderRadius: 3,
              background: indicatorColor ?? color ?? '#3b82f6',
            }}
          />
        </span>
      )}
      <span
        style={{
          color: 'var(--text-muted)',
          fontSize: 10,
          letterSpacing: '0.04em',
          textTransform: 'uppercase',
        }}
      >
        {label}
      </span>
    </span>
  );
};

export const MetricStripDivider: React.FC = () => (
  <span
    aria-hidden="true"
    style={{
      width: 1,
      height: 24,
      flexShrink: 0,
      background: 'var(--border-primary)',
    }}
  />
);
