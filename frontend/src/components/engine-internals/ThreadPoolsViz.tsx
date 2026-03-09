/**
 * ThreadPoolsViz - Thread pool utilization visualization
 */

import type { ThreadPoolInfo } from '@tracehouse/core';

interface ThreadPoolsVizProps {
  pools: ThreadPoolInfo[];
  className?: string;
}

export function ThreadPoolsViz({ pools, className = '' }: ThreadPoolsVizProps) {
  return (
    <div 
      className={`rounded-lg border ${className}`}
      style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}
    >
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
        <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>Thread Pools</h3>
      </div>

      <div style={{ padding: 16 }}>
        {pools.length === 0 ? (
          <p style={{ fontSize: 11, textAlign: 'center', padding: '16px 0', color: 'var(--text-muted)' }}>
            No thread pool data available
          </p>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
            {pools.map((pool, index) => {
              const utilization = pool.max > 0 ? (pool.active / pool.max) * 100 : 0;
              
              return (
                <div key={pool.name} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 0', borderBottom: index < pools.length - 1 ? '1px solid var(--border-secondary)' : 'none' }}>
                  <span style={{ fontSize: 11, color: 'var(--text-secondary)', whiteSpace: 'nowrap', minWidth: 120 }}>{pool.name}</span>
                  {pool.isSaturated && (
                    <span
                      style={{
                        padding: '2px 6px',
                        fontSize: 10,
                        fontWeight: 500,
                        borderRadius: 4,
                        backgroundColor: 'rgba(239, 68, 68, 0.2)',
                        color: '#ef4444',
                        flexShrink: 0,
                      }}
                    >
                      SAT
                    </span>
                  )}
                  <div
                    style={{ flex: 1, height: 8, borderRadius: 4, overflow: 'hidden', backgroundColor: 'var(--bg-tertiary)' }}
                  >
                    <div
                      style={{
                        height: '100%',
                        borderRadius: 4,
                        transition: 'all 0.3s',
                        width: `${Math.min(100, utilization)}%`,
                        backgroundColor: pool.isSaturated ? '#ef4444' : pool.color,
                      }}
                    />
                  </div>
                  <span style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-primary)', whiteSpace: 'nowrap', minWidth: 50, textAlign: 'right' }}>
                    {pool.active}/{pool.max}
                  </span>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

export default ThreadPoolsViz;
