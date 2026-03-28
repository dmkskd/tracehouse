/**
 * MonitoringCapabilitiesCard - Compact grouped view of detected monitoring capabilities
 * with screen/feature mapping from the capability registry.
 */

import { useState, useMemo, useRef } from 'react';
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

const POPOVER_BASE: React.CSSProperties = {
  position: 'absolute', top: '100%', zIndex: 20,
  marginTop: 4, padding: '8px 12px',
  background: '#0d0d1a',
  border: '1px solid #5b5b80',
  borderRadius: 6, boxShadow: '0 6px 24px rgba(0,0,0,0.9)',
};
const POPOVER_LEFT: React.CSSProperties = { ...POPOVER_BASE, left: 0 };
const POPOVER_RIGHT: React.CSSProperties = { ...POPOVER_BASE, right: 0 };

/** Truncated description with hover-to-expand popover */
function ExpandableCell({ text, detail, maxChars = 28, dimmed }: { text: string; detail?: string; maxChars?: number; dimmed?: boolean }) {
  const [hover, setHover] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);
  const truncated = text.length > maxChars ? text.slice(0, maxChars) + '...' : text;
  const needsExpand = text.length > maxChars || !!detail;

  return (
    <span
      ref={ref}
      onMouseEnter={() => needsExpand && setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{ position: 'relative', cursor: needsExpand ? 'default' : undefined }}
    >
      <span style={{ fontSize: 10, color: 'var(--text-muted)', whiteSpace: 'nowrap', opacity: dimmed ? 0.5 : undefined }}>
        {truncated}
      </span>
      {hover && (
        <div style={{ ...POPOVER_LEFT, minWidth: 240, maxWidth: 360, whiteSpace: 'normal' }}>
          <div style={{ fontSize: 10, color: '#e2e8f0', lineHeight: 1.4 }}>{text}</div>
          {detail && (
            <div style={{ fontSize: 9, color: '#94a3b8', marginTop: 4, fontFamily: 'monospace' }}>{detail}</div>
          )}
        </div>
      )}
    </span>
  );
}

/** Hover-to-expand consumer/screen list */
function ExpandableConsumers({ consumers, available, dimmed }: { consumers: { screen: string; enables: string; importance: string }[]; available: boolean; dimmed?: boolean }) {
  const [hover, setHover] = useState(false);

  return (
    <span
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{ position: 'relative', cursor: 'default' }}
    >
      <span style={{ fontSize: 10, color: 'var(--text-muted)', whiteSpace: 'nowrap', opacity: dimmed ? 0.5 : undefined }}>
        {consumers.length} screen{consumers.length !== 1 ? 's' : ''}
      </span>
      {hover && (
        <div style={{ ...POPOVER_RIGHT, minWidth: 220, maxWidth: 320 }}>
          <div style={{ fontSize: 9, color: available ? '#94a3b8' : '#ef4444', marginBottom: 4, textTransform: 'uppercase', letterSpacing: '0.04em' }}>
            {available ? 'Used by' : 'Blocked screens'}
          </div>
          {consumers.map((c, i) => (
            <div key={i} style={{ padding: '3px 0', borderTop: i > 0 ? '1px solid rgba(255,255,255,0.08)' : undefined }}>
              <div style={{ fontSize: 10, color: '#e2e8f0', fontWeight: 500, display: 'flex', alignItems: 'center', gap: 6 }}>
                {c.screen}
                {!available && (
                  <span style={{
                    fontSize: 8, padding: '0 4px', borderRadius: 2,
                    background: c.importance === 'required' ? '#ef444420' : '#f59e0b20',
                    color: c.importance === 'required' ? '#ef4444' : '#f59e0b',
                  }}>
                    {c.importance}
                  </span>
                )}
              </div>
              {available && (
                <div style={{ fontSize: 9, color: '#94a3b8', marginTop: 1, lineHeight: 1.3 }}>{c.enables}</div>
              )}
            </div>
          ))}
        </div>
      )}
    </span>
  );
}

type ViewMode = 'capabilities' | 'screens';

interface MonitoringCapabilitiesCardProps {
  className?: string;
}

export function MonitoringCapabilitiesCard({ className = '' }: MonitoringCapabilitiesCardProps) {
  const { capabilities, probeStatus, probeError } = useMonitoringCapabilitiesStore();
  const [showUnavailable, setShowUnavailable] = useState(false);
  const [viewMode, setViewMode] = useState<ViewMode>('capabilities');
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

      {viewMode === 'capabilities' && (() => {
        // Build ordered flat list of capabilities grouped by category
        const capRows: { cap: MonitoringCapability; isFirstInGroup: boolean; groupSize: number }[] = [];
        for (const category of CATEGORY_ORDER) {
          const group = grouped.get(category);
          if (!group) continue;
          const caps = [...group.available, ...(showUnavailable ? group.unavailable : [])];
          if (caps.length === 0) continue;
          caps.forEach((cap, i) => {
            capRows.push({ cap, isFirstInGroup: i === 0, groupSize: caps.length });
          });
        }

        // Count hidden unavailable to show toggle
        const hiddenUnavailable = showUnavailable ? 0 : totalUnavailable;

        return (
          <>
            <div style={{ padding: '4px 12px 12px' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
                <thead>
                  <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                    <th style={{ textAlign: 'left', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', width: 90 }}>Category</th>
                    <th style={{ textAlign: 'left', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)' }}>Capability</th>
                    <th style={{ textAlign: 'center', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', width: 16 }}></th>
                    <th style={{ textAlign: 'left', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)' }}>Description</th>
                    <th style={{ textAlign: 'left', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', width: 90 }}>Source</th>
                    <th style={{ textAlign: 'left', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', width: 80 }}>Used By</th>
                    <th style={{ textAlign: 'left', padding: '4px 6px', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', width: 70 }}>TTL</th>
                  </tr>
                </thead>
                <tbody>
                  {capRows.map(({ cap, isFirstInGroup, groupSize }) => {
                    const consumers = getConsumersForCapability(cap.id);
                    const color = CATEGORY_COLORS[cap.category];
                    const dimmed = !cap.available;
                    return (
                      <tr key={cap.id} style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                        {isFirstInGroup && (
                          <td
                            rowSpan={groupSize}
                            style={{
                              padding: '3px 6px', fontWeight: 600, fontSize: 9, color,
                              verticalAlign: 'top', borderRight: '1px solid var(--border-secondary)',
                              textTransform: 'uppercase', letterSpacing: '0.04em',
                            }}
                          >
                            {CATEGORY_LABELS[cap.category]}
                          </td>
                        )}
                        <td style={{ padding: '3px 6px', fontWeight: 500, color: cap.available ? 'var(--text-primary)' : 'var(--text-muted)', opacity: dimmed ? 0.5 : undefined }}>
                          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                            <span style={{
                              width: 5, height: 5, borderRadius: '50%', flexShrink: 0,
                              background: cap.available ? '#22c55e' : '#64748b',
                            }} />
                            {cap.label}
                          </span>
                        </td>
                        <td style={{ padding: '3px 2px', textAlign: 'center' }}>
                          {!cap.available && (
                            <span style={{ fontSize: 8, color: '#64748b' }} title={cap.detail || ''}>!</span>
                          )}
                        </td>
                        <td style={{ padding: '3px 6px' }}>
                          <ExpandableCell text={cap.description} detail={cap.detail} dimmed={dimmed} />
                          {cap.detail && (
                            <div style={{ fontSize: 9, color: 'var(--text-muted)', fontFamily: 'monospace', marginTop: 1, opacity: dimmed ? 0.5 : undefined }}>
                              {cap.detail}
                            </div>
                          )}
                        </td>
                        <td style={{ padding: '3px 6px' }}>
                          {cap.source ? (
                            <ExpandableCell text={cap.source} maxChars={16} dimmed={dimmed} />
                          ) : ''}
                        </td>
                        <td style={{ padding: '3px 6px' }}>
                          {consumers.length > 0 ? (
                            <ExpandableConsumers consumers={consumers} available={cap.available} dimmed={dimmed} />
                          ) : (
                            <span style={{ fontSize: 10, color: 'var(--text-muted)', opacity: dimmed ? 0.5 : undefined }}>—</span>
                          )}
                        </td>
                        <td style={{ padding: '3px 6px', fontSize: 10, fontFamily: 'monospace', opacity: dimmed ? 0.5 : undefined }}>
                          {cap.ttl === undefined ? '' : cap.ttl === null ? (
                            <span title="No TTL configured — data retained indefinitely" style={{ color: '#f59e0b' }}>&#x221e;</span>
                          ) : (
                            <span style={{ color: 'var(--text-secondary)' }}>{cap.ttl}</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {hiddenUnavailable > 0 && (
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
                  Show {hiddenUnavailable} unavailable
                </button>
              </div>
            )}
            {showUnavailable && totalUnavailable > 0 && (
              <div style={{ padding: '0 16px 8px' }}>
                <button
                  onClick={() => setShowUnavailable(false)}
                  style={{
                    background: 'none', border: 'none', cursor: 'pointer',
                    fontSize: 10, color: 'var(--text-muted)', padding: '2px 0',
                    display: 'flex', alignItems: 'center', gap: 4,
                  }}
                >
                  <span style={{ fontSize: 8, display: 'inline-block', transform: 'rotate(90deg)' }}>▶</span>
                  Hide unavailable
                </button>
              </div>
            )}
          </>
        );
      })()}

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
