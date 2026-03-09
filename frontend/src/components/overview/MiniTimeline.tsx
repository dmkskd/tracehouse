/**
 * MiniTimeline - Compact live CPU+Memory area chart with activity pulse
 *
 * Reads from metricsHistory (live polling snapshots) and renders a
 * two-layer area chart. Below the chart, colored dots show current
 * query/merge/mutation activity counts.
 */

import React, { useMemo } from 'react';
import { Link } from 'react-router-dom';
import type { ServerMetrics } from '@tracehouse/core';

interface MiniTimelineProps {
  history: Array<{ metrics: ServerMetrics; timestamp: Date }>;
  runningQueries: number;
  activeMerges: number;
  mutations?: number;
}

const W = 600;
const H = 80;
const PAD_X = 0;
const PAD_Y = 2;

const COLORS = {
  cpu: '#3B82F6',
  mem: '#8B5CF6',
  queries: '#3B82F6',
  merges: '#F59E0B',
  mutations: '#EF4444',
};

function areaPath(
  values: number[],
  maxVal: number,
): { line: string; fill: string } {
  if (values.length < 2) return { line: '', fill: '' };
  const clampMax = maxVal || 100;

  const points = values.map((v, i) => {
    const x = PAD_X + (i / (values.length - 1)) * (W - PAD_X * 2);
    const y = H - PAD_Y - (Math.min(v, clampMax) / clampMax) * (H - PAD_Y * 2);
    return { x, y };
  });

  const lineD = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x},${p.y}`).join(' ');
  const fillD = `M${PAD_X},${H} ${points.map(p => `L${p.x},${p.y}`).join(' ')} L${W - PAD_X},${H} Z`;

  return { line: lineD, fill: fillD };
}

function ActivityDot({ count, color, label }: { count: number; color: string; label: string }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
      <div style={{ position: 'relative', width: 8, height: 8 }}>
        <div
          style={{
            width: 8,
            height: 8,
            borderRadius: '50%',
            background: count > 0 ? color : 'var(--bg-tertiary)',
            transition: 'background 0.3s',
          }}
        />
        {count > 0 && (
          <div
            style={{
              position: 'absolute',
              inset: -2,
              borderRadius: '50%',
              border: `1.5px solid ${color}`,
              opacity: 0.4,
              animation: 'pulse-ring 2s ease-out infinite',
            }}
          />
        )}
      </div>
      <span style={{ fontSize: 11, fontFamily: 'monospace', color: count > 0 ? 'var(--text-primary)' : 'var(--text-muted)' }}>
        {count}
      </span>
      <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{label}</span>
    </div>
  );
}

export const MiniTimeline: React.FC<MiniTimelineProps> = ({
  history,
  runningQueries,
  activeMerges,
  mutations = 0,
}) => {
  const { cpuLine, cpuFill, memLine, memFill } = useMemo(() => {
    const cpuValues = history.map(h => h.metrics.cpu_usage);
    const memValues = history.map(h =>
      h.metrics.memory_total > 0
        ? (h.metrics.memory_used / h.metrics.memory_total) * 100
        : 0,
    );

    const cpu = areaPath(cpuValues, 100);
    const mem = areaPath(memValues, 100);
    return { cpuLine: cpu.line, cpuFill: cpu.fill, memLine: mem.line, memFill: mem.fill };
  }, [history]);

  const hasData = history.length >= 2;

  return (
    <div
      style={{
        background: 'var(--bg-card)',
        border: '1px solid var(--border-primary)',
        borderRadius: 10,
        overflow: 'hidden',
      }}
    >
      {/* Chart area */}
      <Link
        to="/timetravel"
        state={{ from: { path: '/overview', label: 'Overview' } }}
        style={{ display: 'block', textDecoration: 'none', cursor: 'pointer' }}
        title="Open Time Travel"
      >
        <div style={{ position: 'relative', height: H + 8, padding: '4px 12px 0' }}>
          {hasData ? (
            <svg
              viewBox={`0 0 ${W} ${H}`}
              preserveAspectRatio="none"
              style={{ width: '100%', height: H, display: 'block' }}
            >
              <defs>
                <linearGradient id="mini-cpu-grad" x1="0" x2="0" y1="0" y2="1">
                  <stop offset="0%" stopColor={COLORS.cpu} stopOpacity={0.2} />
                  <stop offset="100%" stopColor={COLORS.cpu} stopOpacity={0.02} />
                </linearGradient>
                <linearGradient id="mini-mem-grad" x1="0" x2="0" y1="0" y2="1">
                  <stop offset="0%" stopColor={COLORS.mem} stopOpacity={0.15} />
                  <stop offset="100%" stopColor={COLORS.mem} stopOpacity={0.02} />
                </linearGradient>
              </defs>

              {/* Memory (behind) */}
              <path d={memFill} fill="url(#mini-mem-grad)" />
              <path d={memLine} fill="none" stroke={COLORS.mem} strokeWidth={1.2} opacity={0.6} />

              {/* CPU (in front) */}
              <path d={cpuFill} fill="url(#mini-cpu-grad)" />
              <path d={cpuLine} fill="none" stroke={COLORS.cpu} strokeWidth={1.5} />
            </svg>
          ) : (
            <div style={{ height: H, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Collecting metrics...</span>
            </div>
          )}

          {/* Legend overlay */}
          {hasData && (
            <div style={{ position: 'absolute', top: 6, left: 16, display: 'flex', gap: 12 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <div style={{ width: 10, height: 2, borderRadius: 1, background: COLORS.cpu }} />
                <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>CPU</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <div style={{ width: 10, height: 2, borderRadius: 1, background: COLORS.mem, opacity: 0.6 }} />
                <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>Memory</span>
              </div>
            </div>
          )}
          {hasData && (
            <div style={{ position: 'absolute', top: 6, right: 16 }}>
              <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>Time Travel →</span>
            </div>
          )}
        </div>
      </Link>

      {/* Activity strip */}
      <div
        style={{
          display: 'flex',
          gap: 20,
          padding: '6px 16px 8px',
          borderTop: '1px solid var(--border-secondary)',
        }}
      >
        <ActivityDot count={runningQueries} color={COLORS.queries} label="queries" />
        <ActivityDot count={activeMerges} color={COLORS.merges} label="merges" />
        {mutations > 0 && (
          <ActivityDot count={mutations} color={COLORS.mutations} label="mutations" />
        )}
      </div>

      {/* Keyframe for pulse animation */}
      <style>{`
        @keyframes pulse-ring {
          0% { transform: scale(1); opacity: 0.4; }
          70% { transform: scale(1.8); opacity: 0; }
          100% { transform: scale(1.8); opacity: 0; }
        }
      `}</style>
    </div>
  );
};
