/**
 * CPUCoreMap - Per-core CPU utilization with stacked vertical bars
 */

import type { CPUCoreInfo, EngineInternalsData } from '@tracehouse/core';

interface CPUCoreMapProps {
  cores: CPUCoreInfo[];
  meta?: EngineInternalsData['cpuCoresMeta'];
  className?: string;
}

const STATE_COLORS = {
  user: '#3b82f6',    // blue-500
  system: '#ef4444',  // red-500
  iowait: '#f59e0b',  // amber-500
  idle: 'rgba(100, 116, 139, 0.15)',  // slate-500 nearly transparent
};

const STATE_LABELS = {
  user: 'User',
  system: 'System',
  iowait: 'IO Wait',
  idle: 'Idle',
};

export function CPUCoreMap({ cores, meta, className = '' }: CPUCoreMapProps) {
  // Calculate average CPU
  const avgCpu = cores.length > 0
    ? cores.reduce((sum, c) => sum + c.pct, 0) / cores.length
    : 0;

  const barHeight = 80; // pixels

  return (
    <div 
      className={`rounded-lg border ${className}`}
      style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}
    >
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>CPU Core Map</h3>
            {meta?.isCgroupLimited && (
              <span 
                title={`Container limited: ${meta.effectiveCores} vCPUs allocated (host has ${meta.hostCores} logical cores)`}
                style={{ 
                  fontSize: 9, 
                  padding: '1px 6px', 
                  borderRadius: 3, 
                  background: 'rgba(59, 130, 246, 0.15)', 
                  color: '#60a5fa',
                  fontWeight: 500,
                  letterSpacing: '0.02em',
                }}
              >
                cgroup: {meta.effectiveCores}/{meta.hostCores}
              </span>
            )}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, fontSize: 10 }}>
            <span style={{ color: 'var(--text-muted)' }}>
              Cores: <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{cores.length}</span>
            </span>
            <span style={{ color: 'var(--text-muted)' }}>
              Avg: <span style={{ fontFamily: 'monospace', color: avgCpu > 80 ? '#ef4444' : avgCpu > 50 ? '#f59e0b' : 'var(--text-primary)' }}>{avgCpu.toFixed(1)}%</span>
            </span>
          </div>
        </div>
      </div>

      <div style={{ padding: 16 }}>
        {cores.length === 0 ? (
          <div style={{ fontSize: 11, textAlign: 'center', padding: '16px 0', color: 'var(--text-muted)' }}>
            <p>Per-core metrics not available</p>
            <p style={{ fontSize: 10, marginTop: 8, color: 'var(--text-muted)' }}>
              Requires procfs metrics. Check browser console for details.
            </p>
          </div>
        ) : (
          <>
            {/* Stacked Vertical Bars */}
            <div style={{ display: 'flex', alignItems: 'flex-end', gap: 4, justifyContent: 'space-between' }}>
              {cores.map((core) => {
                const breakdown = core.breakdown || { user: 0, system: 0, iowait: 0, idle: 100 };
                return (
                  <div key={core.core} className="group" style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', flex: 1, minWidth: 0, position: 'relative' }}>
                    {/* Tooltip */}
                    <div 
                      className="opacity-0 group-hover:opacity-100"
                      style={{ 
                        position: 'absolute', 
                        bottom: '100%', 
                        left: '50%', 
                        transform: 'translateX(-50%)', 
                        marginBottom: 8, 
                        padding: '4px 8px', 
                        borderRadius: 4, 
                        fontSize: 10, 
                        whiteSpace: 'nowrap', 
                        transition: 'opacity 0.15s',
                        pointerEvents: 'none',
                        zIndex: 10,
                        background: 'var(--bg-secondary)', 
                        color: 'var(--text-primary)', 
                        border: '1px solid var(--border-primary)' 
                      }}
                    >
                      <div>Core {core.core}: {core.pct.toFixed(1)}%</div>
                      <div style={{ color: STATE_COLORS.user }}>User: {breakdown.user.toFixed(1)}%</div>
                      <div style={{ color: STATE_COLORS.system }}>System: {breakdown.system.toFixed(1)}%</div>
                      <div style={{ color: STATE_COLORS.iowait }}>IO Wait: {breakdown.iowait.toFixed(1)}%</div>
                      <div style={{ color: 'var(--text-muted)' }}>Idle: {breakdown.idle.toFixed(1)}%</div>
                    </div>
                    {/* Stacked bar container - stacks from bottom to top */}
                    <div 
                      style={{ 
                        width: '100%',
                        borderRadius: 4,
                        overflow: 'hidden',
                        display: 'flex',
                        flexDirection: 'column',
                        height: barHeight, 
                        backgroundColor: 'var(--bg-tertiary)',
                        minWidth: 12,
                        maxWidth: 32,
                      }}
                    >
                      {/* Idle (top - nearly transparent) */}
                      <div style={{ height: `${breakdown.idle}%`, backgroundColor: STATE_COLORS.idle }} />
                      {/* User */}
                      <div style={{ height: `${breakdown.user}%`, backgroundColor: STATE_COLORS.user }} />
                      {/* IO Wait */}
                      <div style={{ height: `${breakdown.iowait}%`, backgroundColor: STATE_COLORS.iowait }} />
                      {/* System (bottom) */}
                      <div style={{ height: `${breakdown.system}%`, backgroundColor: STATE_COLORS.system }} />
                    </div>
                    {/* Core number */}
                    <span style={{ fontSize: 10, fontFamily: 'monospace', marginTop: 4, color: 'var(--text-muted)' }}>
                      {core.core}
                    </span>
                  </div>
                );
              })}
            </div>

            {/* Legend */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16, marginTop: 16, paddingTop: 12, borderTop: '1px solid var(--border-secondary)' }}>
              {(['user', 'system', 'iowait', 'idle'] as const).map((state) => (
                <div key={state} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <div style={{ width: 8, height: 8, borderRadius: 2, backgroundColor: STATE_COLORS[state] }} />
                  <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>{STATE_LABELS[state]}</span>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default CPUCoreMap;
