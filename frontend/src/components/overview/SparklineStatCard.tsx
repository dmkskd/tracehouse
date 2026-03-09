/**
 * SparklineStatCard - Stat card with inline SVG sparkline
 *
 * Shows the current value prominently with a tiny area chart
 * drawn from the last N metrics snapshots, giving instant trend context.
 */

import React, { useMemo } from 'react';

export interface BreakdownSegment {
  label: string;
  value: number;   // percentage
  color: string;
}

interface SparklineStatCardProps {
  label: string;
  value: string | number;
  subtitle?: string;
  color: string;          // hex color for the sparkline + accent
  sparklineData: number[]; // raw values (most recent last)
  warn?: boolean;
  /** Optional thin breakdown bar (e.g. queries vs merges vs other) */
  breakdown?: BreakdownSegment[];
  /** When true, segment values are raw percentages of the full bar (e.g. CPU %) */
  breakdownRawPct?: boolean;
}

const W = 120;
const H = 32;
const PAD = 1;

function buildPath(data: number[], filled: boolean): string {
  if (data.length < 2) return '';
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;

  const points = data.map((v, i) => {
    const x = PAD + (i / (data.length - 1)) * (W - PAD * 2);
    const y = H - PAD - ((v - min) / range) * (H - PAD * 2);
    return `${x},${y}`;
  });

  if (filled) {
    return `M${PAD},${H} L${points.join(' L')} L${W - PAD},${H} Z`;
  }
  return `M${points.join(' L')}`;
}

export const SparklineStatCard: React.FC<SparklineStatCardProps> = ({
  label,
  value,
  subtitle,
  color,
  sparklineData,
  warn,
  breakdown,
  breakdownRawPct,
}) => {
  const linePath = useMemo(() => buildPath(sparklineData, false), [sparklineData]);

  const accentColor = warn ? '#ef4444' : color;

  return (
    <div
      style={{
        background: 'var(--bg-card)',
        border: '1px solid var(--border-primary)',
        borderRadius: 10,
        padding: '14px 16px 10px',
        position: 'relative',
        overflow: 'hidden',
        borderLeft: 'none',
      }}
    >
      {/* Sparkline background */}
      {sparklineData.length >= 2 && (
        <svg
          viewBox={`0 0 ${W} ${H}`}
          preserveAspectRatio="none"
          style={{
            position: 'absolute',
            bottom: 0,
            right: 0,
            width: '60%',
            height: '60%',
            opacity: 0.5,
            pointerEvents: 'none',
          }}
        >
          <path d={linePath} fill="none" stroke={accentColor} strokeWidth={1.2} strokeLinejoin="round" strokeLinecap="round" opacity={0.5} />
        </svg>
      )}

      {/* Value */}
      <div style={{ position: 'relative', zIndex: 1 }}>
        <div
          style={{
            fontSize: 26,
            fontWeight: 700,
            fontFamily: 'monospace',
            color: 'var(--text-primary)',
            lineHeight: 1.1,
          }}
        >
          {value}
        </div>
        <div
          style={{
            fontSize: 11,
            fontWeight: 500,
            color: 'var(--text-muted)',
            marginTop: 4,
          }}
        >
          {label}
        </div>
        {subtitle && (
          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>
            {subtitle}
          </div>
        )}
        {/* Inline breakdown bar */}
        {breakdown && breakdown.length > 0 && (() => {
          const visible = breakdown.filter(s => s.value >= 0.5);
          const total = breakdown.reduce((sum, s) => sum + s.value, 0);
          return (
            <div style={{ marginTop: 8 }}>
              <div style={{ height: 6, borderRadius: 3, overflow: 'hidden', display: 'flex', background: 'var(--bg-tertiary)' }}>
                {visible.map((seg, i) => {
                  const w = breakdownRawPct
                    ? Math.min(seg.value, 100)
                    : (total > 0 ? (seg.value / total) * 100 : 0);
                  return (
                    <div
                      key={i}
                      className="relative group"
                      style={{ width: `${w}%`, height: '100%', background: seg.color, transition: 'width 0.3s' }}
                    >
                      <div
                        className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1 px-2 py-0.5 rounded text-xs whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10"
                        style={{ background: 'var(--bg-secondary)', color: 'var(--text-primary)', fontSize: 10 }}
                      >
                        {seg.label}: {seg.value.toFixed(1)}%
                      </div>
                    </div>
                  );
                })}
              </div>
              <div style={{ display: 'flex', gap: 6, marginTop: 3, flexWrap: 'wrap' }}>
                {visible.filter(s => s.value >= 1.5).map((seg, i) => (
                  <span key={i} style={{ display: 'inline-flex', alignItems: 'center', gap: 3, fontSize: 9, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>
                    <span style={{ width: 5, height: 5, borderRadius: '50%', background: seg.color, flexShrink: 0 }} />
                    {seg.label} {seg.value.toFixed(1)}%
                  </span>
                ))}
              </div>
            </div>
          );
        })()}
      </div>
    </div>
  );
};
