import React, { useState } from 'react';
import { useProfileEventDescriptionsStore } from '../../../../stores/profileEventDescriptionsStore';
import { formatBytes } from '../../../../stores/databaseStore';
import { PROFILE_EVENT_CATEGORIES } from '../shared/profileEventCategories';

/**
 * Performance Tab - ProfileEvents breakdown
 */
export const PerformanceTab: React.FC<{
  profileEvents: Record<string, number> | undefined;
  isLoading: boolean;
}> = ({ profileEvents, isLoading }) => {
  const [filter, setFilter] = useState('');
  const [hideZero, setHideZero] = useState(true);
  const descriptions = useProfileEventDescriptionsStore((s) => s.descriptions);

  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{
            width: 24, height: 24,
            borderWidth: 2,
            borderStyle: 'solid',
            borderColor: 'var(--border-primary)',
            borderTopColor: 'var(--text-tertiary)',
            borderRadius: '50%',
            animation: 'spin 1s linear infinite',
            margin: '0 auto 8px',
          }} />
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading performance data...</span>
        </div>
      </div>
    );
  }

  if (!profileEvents || Object.keys(profileEvents).length === 0) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <div style={{ fontSize: 14, color: 'var(--text-tertiary)' }}>No ProfileEvents data available</div>
      </div>
    );
  }

  const lowerFilter = filter.toLowerCase();

  // Group events by category
  const categorizedEvents: Record<string, { event: string; value: number }[]> = {};
  const uncategorized: { event: string; value: number }[] = [];
  let totalCount = 0;
  let shownCount = 0;

  Object.entries(profileEvents).forEach(([event, value]) => {
    if (hideZero && value === 0) return;
    totalCount++;
    if (lowerFilter && !event.toLowerCase().includes(lowerFilter)) return;
    shownCount++;
    let found = false;
    for (const [cat, config] of Object.entries(PROFILE_EVENT_CATEGORIES)) {
      if (config.events.some(e => event.includes(e) || e.includes(event))) {
        if (!categorizedEvents[cat]) categorizedEvents[cat] = [];
        categorizedEvents[cat].push({ event, value });
        found = true;
        break;
      }
    }
    if (!found) uncategorized.push({ event, value });
  });

  // Sort categorized events to match the order defined in PROFILE_EVENT_CATEGORIES
  for (const [cat, config] of Object.entries(PROFILE_EVENT_CATEGORIES)) {
    if (categorizedEvents[cat]) {
      const order = config.events;
      categorizedEvents[cat].sort((a, b) => {
        const ai = order.findIndex(e => a.event === e || a.event.includes(e) || e.includes(a.event));
        const bi = order.findIndex(e => b.event === e || b.event.includes(e) || e.includes(b.event));
        const aIdx = ai === -1 ? 999 : ai;
        const bIdx = bi === -1 ? 999 : bi;
        if (aIdx !== bIdx) return aIdx - bIdx;
        return a.event.localeCompare(b.event);
      });
    }
  }

  const fmtValue = (event: string, value: number) => {
    if (event.includes('Bytes')) return formatBytes(value);
    if (event.includes('Microseconds')) return value >= 1000000 ? `${(value / 1000000).toFixed(2)}s` : value >= 1000 ? `${(value / 1000).toFixed(1)}ms` : `${value}µs`;
    if (event.includes('Nanoseconds')) return value >= 1000000000 ? `${(value / 1000000000).toFixed(2)}s` : value >= 1000000 ? `${(value / 1000000).toFixed(1)}ms` : value >= 1000 ? `${(value / 1000).toFixed(1)}µs` : `${value}ns`;
    return value.toLocaleString();
  };

  return (
    <div style={{ padding: 20, overflow: 'auto', height: '100%' }}>
      {/* Filter bar */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16 }}>
        <input
          type="text"
          placeholder="Filter events..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{
            flex: 1,
            maxWidth: 300,
            padding: '6px 10px',
            fontSize: 12,
            fontFamily: 'monospace',
            borderRadius: 6,
            border: '1px solid var(--border-primary)',
            background: 'var(--bg-code)',
            color: 'var(--text-primary)',
            outline: 'none',
          }}
        />
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: 'var(--text-muted)', cursor: 'pointer' }}>
          <input
            type="checkbox"
            checked={hideZero}
            onChange={(e) => setHideZero(e.target.checked)}
            style={{ accentColor: '#58a6ff' }}
          />
          Hide zero
        </label>
        <span style={{ fontSize: 10, color: 'var(--text-muted)', marginLeft: 'auto' }}>
          {shownCount}{filter ? ` / ${totalCount}` : ''} events
        </span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 16 }}>
        {Object.entries(PROFILE_EVENT_CATEGORIES).map(([cat, config]) => {
          const events = categorizedEvents[cat] || [];
          if (events.length === 0) return null;
          return (
            <div key={cat} style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 8, padding: 16 }}>
              <div style={{ fontSize: 11, fontWeight: 600, color: config.color, textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ width: 8, height: 8, borderRadius: '50%', background: config.color }} />
                {config.label}
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {events.sort((a, b) => b.value - a.value).map(({ event, value }) => (
                  <div key={event} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: 11, color: 'var(--text-tertiary)', fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '60%' }} title={descriptions[event] || event}>{event}</span>
                    <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)', fontFamily: 'monospace' }}>{fmtValue(event, value)}</span>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
      {uncategorized.length > 0 && (
        <div style={{ marginTop: 16, background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 8, padding: 16 }}>
          <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 12 }}>
            Other Events ({uncategorized.length})
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 8 }}>
            {uncategorized.sort((a, b) => b.value - a.value).map(({ event, value }) => (
              <div key={event} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{ fontSize: 10, color: 'var(--text-muted)', fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '60%' }} title={descriptions[event] || event}>{event}</span>
                <span style={{ fontSize: 11, color: 'var(--text-tertiary)', fontFamily: 'monospace' }}>{fmtValue(event, value)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
