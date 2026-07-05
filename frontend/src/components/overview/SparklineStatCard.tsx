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
  variant?: 'sparkline' | 'heat' | 'tank' | 'pulse';
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
  variant = 'sparkline',
  breakdown,
  breakdownRawPct,
}) => {
  const linePath = useMemo(() => buildPath(sparklineData, false), [sparklineData]);
  const areaPath = useMemo(() => buildPath(sparklineData, true), [sparklineData]);
  const heatValues = useMemo(() => sparklineData.slice(-28), [sparklineData]);
  const latest = sparklineData.at(-1) ?? 0;
  const peak = sparklineData.length > 0 ? Math.max(...sparklineData) : 0;

  const accentColor = warn ? '#ef4444' : color;
  const heatMax = heatValues.length > 0 ? Math.max(...heatValues) : 0;

  return (
    <div
      style={{
        background: 'var(--bg-card)',
        border: '1px solid var(--border-primary)',
        borderRadius: 10,
        padding: '13px 16px 12px',
        position: 'relative',
        overflow: 'hidden',
        minHeight: 96,
      }}
    >
      <div style={{ position: 'relative', zIndex: 1 }}>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            gap: 10,
            fontSize: 12,
            fontWeight: 650,
            color: 'var(--text-muted)',
          }}
        >
          <span>{label}</span>
          {variant === 'pulse' && (
            <span style={{ width: 9, height: 9, borderRadius: '50%', background: accentColor, boxShadow: `0 0 0 4px ${accentColor}1f` }} />
          )}
          {variant === 'tank' && peak > 0 && (
            <span style={{ fontFamily: 'monospace', color: 'var(--text-muted)' }}>peak {peak.toFixed(1)}%</span>
          )}
        </div>

        <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginTop: 8 }}>
          <div
            style={{
              fontSize: 27,
              fontWeight: 750,
              fontFamily: 'monospace',
              color: 'var(--text-primary)',
              lineHeight: 1.05,
              letterSpacing: 0,
              whiteSpace: 'nowrap',
            }}
          >
            {value}
          </div>
        </div>

        {subtitle && (
          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
            {subtitle}
          </div>
        )}

        {variant === 'heat' && heatValues.length >= 2 && (
          <div style={{ display: 'flex', gap: 3, height: 22, alignItems: 'end', marginTop: 9 }}>
            {heatValues.map((v, index) => {
              const intensity = heatMax > 0 ? Math.max(0.12, Math.min(1, v / heatMax)) : 0.12;
              const h = 5 + intensity * 15;
              const hot = v >= 80;
              return (
                <span
                  key={`${index}-${v}`}
                  title={`${v.toFixed(1)}`}
                  style={{
                    flex: 1,
                    minWidth: 3,
                    height: h,
                    borderRadius: 2,
                    background: hot ? '#f59e0b' : accentColor,
                    opacity: 0.28 + intensity * 0.62,
                  }}
                />
              );
            })}
          </div>
        )}

        {variant === 'tank' && (
          <div style={{ marginTop: 10 }}>
            <div style={{ height: 26, borderRadius: 5, overflow: 'hidden', border: '1px solid var(--border-secondary)', background: 'var(--bg-primary)', position: 'relative' }}>
              <div
                style={{
                  position: 'absolute',
                  left: 0,
                  right: 0,
                  bottom: 0,
                  height: `${Math.max(2, Math.min(100, latest))}%`,
                  background: `linear-gradient(180deg, ${accentColor}66, ${accentColor}cc)`,
                }}
              />
              {peak > 0 && (
                <div
                  style={{
                    position: 'absolute',
                    left: 0,
                    right: 0,
                    bottom: `${Math.min(100, peak)}%`,
                    borderTop: `1px dashed ${warn ? '#ef4444' : '#fb923c'}`,
                    opacity: 0.8,
                  }}
                />
              )}
            </div>
          </div>
        )}

        {variant === 'sparkline' && sparklineData.length >= 2 && (
          <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" style={{ width: '100%', height: 30, marginTop: 7, display: 'block' }}>
            <path d={areaPath} fill={accentColor} opacity={0.14} />
            <path d={linePath} fill="none" stroke={accentColor} strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
          </svg>
        )}

        {variant === 'pulse' && (
          <div style={{ height: 30, marginTop: 7, display: 'flex', alignItems: 'center', gap: 5 }}>
            {Array.from({ length: 16 }, (_, index) => (
              <span
                key={index}
                style={{
                  flex: 1,
                  height: index % 5 === 0 ? 18 : index % 3 === 0 ? 12 : 8,
                  borderRadius: 999,
                  background: accentColor,
                  opacity: 0.1 + (index % 5) * 0.07,
                }}
              />
            ))}
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
