/**
 * MonitoringCapabilitiesCard - Compact grouped view of detected monitoring capabilities
 * with screen/feature mapping from the capability registry.
 */

import { useState, useMemo } from 'react';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';
import type { MonitoringCapability } from '@tracehouse/core';
import {
  getConsumersForCapability,
  getScreenSummaries,
} from '@tracehouse/core';

const CATEGORY_COLORS: Record<MonitoringCapability['category'], string> = {
  profiling: '#3b82f6',
  tracing: '#a78bfa',
  logging: '#94a3b8',
  metrics: '#22c55e',
  introspection: '#f59e0b',
};

const CATEGORY_LABELS: Record<MonitoringCapability['category'], string> = {
  profiling: 'Profiling',
  tracing: 'Tracing',
  logging: 'Logging',
  metrics: 'Metrics',
  introspection: 'Introspection',
};

const CATEGORY_ORDER: MonitoringCapability['category'][] = [
  'profiling', 'tracing', 'metrics', 'introspection', 'logging',
];

type ViewMode = 'capabilities' | 'screens';

interface MonitoringCapabilitiesCardProps {
  className?: string;
}

export function MonitoringCapabilitiesCard({ className = '' }: MonitoringCapabilitiesCardProps) {
  const { capabilities, probeStatus, probeError } = useMonitoringCapabilitiesStore();
  const [showUnavailable, setShowUnavailable] = useState(false);
  const [viewMode, setViewMode] = useState<ViewMode>('capabilities');
  const [expandedCap, setExpandedCap] = useState<string | null>(null);

  // Compute screen availability summaries from registry + probed capabilities
  const screenSummaries = useMemo(() => {
    if (!capabilities) return [] as { screen: string; tab?: string; route?: string; required: string[]; optional: string[]; requiredMissing: string[]; optionalMissing: string[]; status: 'full' | 'degraded' | 'unavailable' }[];
    const summaries = getScreenSummaries();
    const isAvail = (id: string) => capabilities.capabilities.find(c => c.id === id)?.available ?? false;

    return summaries.map(s => {
      const requiredMissing = s.required.filter((id: string) => !isAvail(id));
      const optionalMissing = s.optional.filter((id: string) => !isAvail(id));
      const status: 'full' | 'degraded' | 'unavailable' =
        requiredMissing.length > 0 ? 'unavailable' :
        optionalMissing.length > 0 ? 'degraded' : 'full';
      return { ...s, requiredMissing, optionalMissing, status };
    }).sort((a, b) => {
      // Primary: match nav bar order, secondary: status within tab
      const TAB_ORDER: Record<string, number> = {
        'Overview': 0, 'Engine Internals': 1, 'Cluster': 2, 'Explorer': 3,
        'Time Travel': 4, 'Queries': 5, 'Merges': 6, 'Replication': 7, 'Analytics': 8,
      };
      const tabA = TAB_ORDER[a.tab ?? ''] ?? 99;
      const tabB = TAB_ORDER[b.tab ?? ''] ?? 99;
      if (tabA !== tabB) return tabA - tabB;
      const statusOrder: Record<string, number> = { full: 0, degraded: 1, unavailable: 2 };
      return statusOrder[a.status] - statusOrder[b.status];
    });
  }, [capabilities]);

  if (probeStatus === 'probing' || probeStatus === 'idle') {
    return (
      <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div style={{ padding: '8px 16px', fontSize: 11, color: 'var(--text-muted)' }}>
          {probeStatus === 'probing' ? 'Probing capabilities...' : ''}
        </div>
      </div>
    );
  }

  if (probeError || !capabilities) {
    return (
      <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div style={{ padding: '8px 16px', fontSize: 11, color: 'var(--text-muted)' }}>
          {probeError || 'Capabilities not probed'}
        </div>
      </div>
    );
  }

  // Group by category
  const grouped = new Map<MonitoringCapability['category'], { available: MonitoringCapability[]; unavailable: MonitoringCapability[] }>();
  for (const cap of capabilities.capabilities) {
    const group = grouped.get(cap.category) || { available: [], unavailable: [] };
    if (cap.available) group.available.push(cap);
    else group.unavailable.push(cap);
    grouped.set(cap.category, group);
  }

  const totalAvailable = capabilities.capabilities.filter(c => c.available).length;
  const totalUnavailable = capabilities.capabilities.filter(c => !c.available).length;

  const STATUS_COLORS = { full: '#22c55e', degraded: '#f59e0b', unavailable: '#64748b' };
  const STATUS_LABELS = { full: 'Full', degraded: 'Partial', unavailable: 'Unavailable' };

  return (
    <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
      {/* Header */}
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>Monitoring Capabilities</h3>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 10 }}>
          <span style={{ color: 'var(--text-muted)' }}>
            <span style={{ fontFamily: 'monospace', color: '#22c55e' }}>{totalAvailable}</span>/{capabilities.capabilities.length}
          </span>
          <span style={{ fontFamily: 'monospace', color: 'var(--text-muted)' }}>v{capabilities.serverVersion}</span>
        </div>
      </div>

      {/* View mode toggle */}
      <div style={{ padding: '8px 16px 4px', display: 'flex', gap: 2 }}>
        {(['capabilities', 'screens'] as const).map(mode => (
          <button
            key={mode}
            onClick={() => setViewMode(mode)}
            style={{
              background: viewMode === mode ? 'var(--bg-tertiary)' : 'none',
              border: '1px solid',
              borderColor: viewMode === mode ? 'var(--border-secondary)' : 'transparent',
              borderRadius: 4,
              padding: '2px 8px',
              fontSize: 10,
              color: viewMode === mode ? 'var(--text-primary)' : 'var(--text-muted)',
              cursor: 'pointer',
            }}
          >
            {mode === 'capabilities' ? 'By Capability' : 'By Screen'}
          </button>
        ))}
      </div>

      {viewMode === 'capabilities' && (
        <>
          {/* Grouped capabilities */}
          <div style={{ padding: '8px 16px 12px', display: 'flex', flexDirection: 'column', gap: 6 }}>
            {CATEGORY_ORDER.map(category => {
              const group = grouped.get(category);
              if (!group || (group.available.length === 0 && group.unavailable.length === 0)) return null;
              const color = CATEGORY_COLORS[category];

              return (
                <div key={category} style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                  <span style={{
                    fontSize: 9, fontWeight: 600, color, textTransform: 'uppercase',
                    letterSpacing: '0.04em', minWidth: 72, flexShrink: 0,
                  }}>
                    {CATEGORY_LABELS[category]}
                    <span style={{ color: 'var(--text-muted)', fontWeight: 400, marginLeft: 4 }}>
                      {group.available.length}/{group.available.length + group.unavailable.length}
                    </span>
                  </span>

                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
                    {group.available.map(cap => {
                      const consumers = getConsumersForCapability(cap.id);
                      const isExpanded = expandedCap === cap.id;
                      return (
                        <span key={cap.id} style={{ position: 'relative' }}>
                          <span
                            onClick={() => consumers.length > 0 && setExpandedCap(isExpanded ? null : cap.id)}
                            title={`system.${cap.id}\n${cap.description}\n${cap.detail || ''}`}
                            style={{
                              display: 'inline-flex', alignItems: 'center', gap: 3,
                              padding: '1px 6px', fontSize: 10, borderRadius: 3,
                              background: `${color}12`, border: `1px solid ${color}25`, color,
                              cursor: consumers.length > 0 ? 'pointer' : 'default',
                              whiteSpace: 'nowrap',
                            }}
                          >
                            <span style={{ width: 4, height: 4, borderRadius: '50%', background: '#22c55e', flexShrink: 0 }} />
                            {cap.label}
                            {consumers.length > 0 && (
                              <span style={{ fontSize: 8, opacity: 0.6, marginLeft: 2 }}>
                                {isExpanded ? '▾' : `·${consumers.length}`}
                              </span>
                            )}
                          </span>
                          {isExpanded && consumers.length > 0 && (
                            <div style={{
                              position: 'absolute', top: '100%', left: 0, zIndex: 20,
                              marginTop: 4, padding: '6px 10px',
                              background: 'var(--bg-elevated, var(--bg-card))',
                              border: '1px solid var(--border-secondary)',
                              borderRadius: 6, boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
                              minWidth: 200, maxWidth: 300,
                            }}>
                              <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                                Used by
                              </div>
                              {consumers.map((c, i) => (
                                <div key={i} style={{ fontSize: 10, color: 'var(--text-primary)', padding: '2px 0', display: 'flex', alignItems: 'baseline', gap: 6 }}>
                                  <span style={{ fontWeight: 500, whiteSpace: 'nowrap' }}>{c.screen}</span>
                                  <span style={{ fontSize: 9, color: 'var(--text-muted)', flex: 1 }}>{c.enables}</span>
                                </div>
                              ))}
                            </div>
                          )}
                        </span>
                      );
                    })}
                    {group.unavailable.length > 0 && !showUnavailable && (
                      <span style={{ fontSize: 10, color: 'var(--text-muted)', padding: '1px 4px', opacity: 0.5 }}>
                        +{group.unavailable.length}
                      </span>
                    )}
                    {showUnavailable && group.unavailable.map(cap => {
                      const consumers = getConsumersForCapability(cap.id);
                      const isExpanded = expandedCap === cap.id;
                      return (
                        <span key={cap.id} style={{ position: 'relative' }}>
                          <span
                            onClick={() => consumers.length > 0 && setExpandedCap(isExpanded ? null : cap.id)}
                            title={cap.description}
                            style={{
                              display: 'inline-flex', alignItems: 'center',
                              padding: '1px 6px', fontSize: 10, borderRadius: 3,
                              background: 'var(--bg-tertiary)', color: 'var(--text-muted)',
                              cursor: consumers.length > 0 ? 'pointer' : 'default',
                              whiteSpace: 'nowrap', opacity: 0.5,
                              textDecoration: 'line-through', textDecorationColor: 'var(--border-secondary)',
                            }}
                          >
                            {cap.label}
                            {consumers.length > 0 && (
                              <span style={{ fontSize: 8, marginLeft: 2, textDecoration: 'none' }}>
                                {isExpanded ? '▾' : `·${consumers.length}`}
                              </span>
                            )}
                          </span>
                          {isExpanded && consumers.length > 0 && (
                            <div style={{
                              position: 'absolute', top: '100%', left: 0, zIndex: 20,
                              marginTop: 4, padding: '6px 10px',
                              background: 'var(--bg-elevated, var(--bg-card))',
                              border: '1px solid var(--border-secondary)',
                              borderRadius: 6, boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
                              minWidth: 200, maxWidth: 300,
                            }}>
                              <div style={{ fontSize: 9, color: 'var(--accent-red, #ef4444)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
                                Blocked screens
                              </div>
                              {consumers.map((c, i) => (
                                <div key={i} style={{ fontSize: 10, color: 'var(--text-primary)', padding: '2px 0', display: 'flex', alignItems: 'baseline', gap: 6 }}>
                                  <span style={{ fontWeight: 500, whiteSpace: 'nowrap' }}>{c.screen}</span>
                                  <span style={{
                                    fontSize: 8, padding: '0 4px', borderRadius: 2,
                                    background: c.importance === 'required' ? '#ef444420' : '#f59e0b20',
                                    color: c.importance === 'required' ? '#ef4444' : '#f59e0b',
                                  }}>
                                    {c.importance}
                                  </span>
                                </div>
                              ))}
                            </div>
                          )}
                        </span>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>

          {totalUnavailable > 0 && (
            <div style={{ padding: '0 16px 8px' }}>
              <button
                onClick={() => setShowUnavailable(!showUnavailable)}
                style={{
                  background: 'none', border: 'none', cursor: 'pointer',
                  fontSize: 10, color: 'var(--text-muted)', padding: '2px 0',
                  display: 'flex', alignItems: 'center', gap: 4,
                }}
              >
                <span style={{ fontSize: 8, transition: 'transform 0.15s', transform: showUnavailable ? 'rotate(90deg)' : 'rotate(0deg)', display: 'inline-block' }}>▶</span>
                {showUnavailable ? 'Hide' : 'Show'} {totalUnavailable} unavailable
              </button>
            </div>
          )}
        </>
      )}

      {viewMode === 'screens' && (() => {
        // Compute rowSpan for each tab group so the tab name only appears once
        const tabCounts = new Map<string, number>();
        for (const s of screenSummaries) {
          const t = s.tab ?? 'Other';
          tabCounts.set(t, (tabCounts.get(t) ?? 0) + 1);
        }
        let prevTab = '';

        return (
          <div style={{ padding: '4px 12px 12px' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
              <thead>
                <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                  <th style={{ textAlign: 'left', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', width: 100 }}>Screen</th>
                  <th style={{ textAlign: 'left', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)' }}>Feature</th>
                  <th style={{ textAlign: 'left', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', width: 70 }}>Status</th>
                  <th style={{ textAlign: 'left', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)' }}>Missing</th>
                </tr>
              </thead>
              <tbody>
                {screenSummaries.map(s => {
                  const tab = s.tab ?? 'Other';
                  const isFirstInGroup = tab !== prevTab;
                  prevTab = tab;
                  return (
                    <tr key={s.screen} style={{ borderBottom: '1px solid var(--border-secondary)', opacity: s.status === 'unavailable' ? 0.5 : 1 }}>
                      {isFirstInGroup && (
                        <td
                          rowSpan={tabCounts.get(tab)}
                          style={{ padding: '3px 6px', fontWeight: 600, fontSize: 10, color: 'var(--text-secondary)', verticalAlign: 'top', borderRight: '1px solid var(--border-secondary)' }}
                        >
                          {tab}
                        </td>
                      )}
                      <td style={{ padding: '3px 6px', fontWeight: 500, color: s.status === 'unavailable' ? 'var(--text-muted)' : 'var(--text-primary)' }}>
                        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                          <span style={{ width: 5, height: 5, borderRadius: '50%', background: STATUS_COLORS[s.status], flexShrink: 0 }} />
                          {s.screen}
                        </span>
                      </td>
                      <td style={{ padding: '3px 6px' }}>
                        <span style={{
                          fontSize: 9, padding: '1px 5px', borderRadius: 3,
                          background: `${STATUS_COLORS[s.status]}15`,
                          color: STATUS_COLORS[s.status],
                          textTransform: 'uppercase', letterSpacing: '0.03em',
                        }}>
                          {STATUS_LABELS[s.status]}
                        </span>
                      </td>
                      <td style={{ padding: '3px 6px', fontSize: 10, color: 'var(--text-muted)' }}>
                        {s.requiredMissing.length > 0 && (
                          <span style={{ color: '#ef4444' }}>
                            {s.requiredMissing.map(id => capabilities.capabilities.find(c => c.id === id)?.label || id).join(', ')}
                          </span>
                        )}
                        {s.requiredMissing.length > 0 && s.optionalMissing.length > 0 && ' · '}
                        {s.optionalMissing.length > 0 && (
                          <span>{s.optionalMissing.map(id => capabilities.capabilities.find(c => c.id === id)?.label || id).join(', ')}</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        );
      })()}
    </div>
  );
}

export default MonitoringCapabilitiesCard;
