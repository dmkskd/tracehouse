/**
 * MemoryBreakdown - Compact memory breakdown visualization
 */

import { formatBytes } from '../../utils/formatters';
import type { MemoryXRay as MemoryXRayType } from '@tracehouse/core';

interface MemoryXRayProps {
  memoryXRay: MemoryXRayType;
  className?: string;
}

export function MemoryXRay({ memoryXRay, className = '' }: MemoryXRayProps) {
  const { totalRSS, totalRAM, jemalloc, subsystems, fragmentationPct } = memoryXRay;
  const usedPct = totalRAM > 0 ? (totalRSS / totalRAM) * 100 : 0;
  const visibleSubsystems = subsystems.filter(s => s.bytes > 0);

  return (
    <div 
      className={`rounded-lg border ${className}`}
      style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}
    >
      {/* Header */}
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>Memory Breakdown</h3>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, fontSize: 10 }}>
            <span style={{ color: 'var(--text-muted)' }}>
              RSS: <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{formatBytes(totalRSS)}</span>
            </span>
            <span style={{ color: 'var(--text-muted)' }}>
              Used: <span style={{ fontFamily: 'monospace', color: usedPct > 85 ? '#ef4444' : usedPct > 70 ? '#f59e0b' : 'var(--text-primary)' }}>{usedPct.toFixed(1)}%</span>
            </span>
          </div>
        </div>
      </div>

      <div style={{ padding: 16 }}>
        {/* Stacked Bar - capped to prevent overflow from different memory accounting */}
        <div
          style={{ width: '100%', height: 16, borderRadius: 4, overflow: 'hidden', display: 'flex', marginBottom: 16, backgroundColor: 'var(--bg-tertiary)' }}
        >
          {visibleSubsystems.map((subsystem) => {
            // Cap at 100% to handle cases where subsystem tracking differs from RSS
            const widthPct = totalRSS > 0 ? Math.min((subsystem.bytes / totalRSS) * 100, 100) : 0;
            if (widthPct < 0.5) return null;
            
            return (
              <div
                key={subsystem.id}
                className="group"
                style={{ height: '100%', position: 'relative', width: `${widthPct}%`, backgroundColor: subsystem.color, maxWidth: '100%' }}
              >
                <div 
                  className="opacity-0 group-hover:opacity-100"
                  style={{ 
                    position: 'absolute', 
                    bottom: '100%', 
                    left: '50%', 
                    transform: 'translateX(-50%)', 
                    marginBottom: 4, 
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
                  {subsystem.label}: {formatBytes(subsystem.bytes)}
                </div>
              </div>
            );
          })}
        </div>

        {/* Vertical List of Subsystems */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
          {subsystems.map((subsystem, index) => {
            return (
              <div key={subsystem.id} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 11, padding: '5px 0', borderBottom: index < subsystems.length - 1 ? '1px solid var(--border-secondary)' : 'none' }}>
                <div style={{ width: 8, height: 8, borderRadius: 2, flexShrink: 0, backgroundColor: subsystem.color }} />
                <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-secondary)' }}>{subsystem.label}</span>
                <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{formatBytes(subsystem.bytes)}</span>
              </div>
            );
          })}
        </div>

        {/* jemalloc Stats */}
        <div style={{ marginTop: 16, paddingTop: 12, borderTop: '1px solid var(--border-secondary)', fontSize: 11 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <span style={{ fontWeight: 500, color: 'var(--text-secondary)' }}>jemalloc</span>
            <span style={{ fontSize: 10, color: fragmentationPct > 20 ? '#f59e0b' : 'var(--text-muted)' }}>
              Frag: <span style={{ fontFamily: 'monospace' }}>{fragmentationPct.toFixed(1)}%</span>
            </span>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px 16px', fontSize: 11 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: 'var(--text-muted)' }}>Allocated</span>
              <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{formatBytes(jemalloc.allocated)}</span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: 'var(--text-muted)' }}>Resident</span>
              <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{formatBytes(jemalloc.resident)}</span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: 'var(--text-muted)' }}>Mapped</span>
              <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{formatBytes(jemalloc.mapped)}</span>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ color: 'var(--text-muted)' }}>Retained</span>
              <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{formatBytes(jemalloc.retained)}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default MemoryXRay;
