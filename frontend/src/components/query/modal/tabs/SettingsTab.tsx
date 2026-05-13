import React, { useState, useEffect } from 'react';
import type { QueryDetail as QueryDetailType } from '@tracehouse/core';

/**
 * Settings Tab - query settings and cache usage
 */
export const SettingsTab: React.FC<{
  queryDetail: QueryDetailType | null;
  isLoading: boolean;
  onFetchDefaults: (settingNames: string[]) => Promise<Record<string, { default: string; description: string }>>;
}> = ({ queryDetail, isLoading, onFetchDefaults }) => {
  const [filter, setFilter] = useState('');
  const [defaults, setDefaults] = useState<Record<string, { default: string; description: string }>>({});
  const [isLoadingDefaults, setIsLoadingDefaults] = useState(false);

  // Fetch defaults when queryDetail changes
  useEffect(() => {
    if (queryDetail?.Settings && Object.keys(queryDetail.Settings).length > 0 && Object.keys(defaults).length === 0 && !isLoadingDefaults) {
      setIsLoadingDefaults(true);
      onFetchDefaults(Object.keys(queryDetail.Settings))
        .then(setDefaults)
        .catch(() => {/* ignore errors */ })
        .finally(() => setIsLoadingDefaults(false));
    }
  }, [queryDetail?.Settings, defaults, isLoadingDefaults, onFetchDefaults]);

  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ width: 24, height: 24, borderWidth: 2, borderStyle: 'solid', borderColor: 'var(--border-primary)', borderTopColor: 'var(--text-tertiary)', borderRadius: '50%', animation: 'spin 1s linear infinite', margin: '0 auto 8px' }} />
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading...</span>
        </div>
      </div>
    );
  }

  if (!queryDetail) {
    return <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-tertiary)' }}>No data available</div>;
  }

  const settings = queryDetail.Settings || {};
  const filteredSettings = Object.entries(settings).filter(([key]) =>
    filter === '' || key.toLowerCase().includes(filter.toLowerCase())
  );

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header with cache info */}
      <div style={{ padding: 16, borderBottom: '1px solid var(--border-secondary)', background: 'var(--bg-card)' }}>
        <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
          <div style={{ background: 'var(--bg-code)', padding: '8px 12px', borderRadius: 6 }}>
            <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Query Cache</span>
            <div style={{ fontSize: 13, fontFamily: 'monospace', color: 'var(--text-primary)', marginTop: 4 }}>{queryDetail.query_cache_usage || 'None'}</div>
          </div>
          <div style={{ background: 'var(--bg-code)', padding: '8px 12px', borderRadius: 6 }}>
            <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Overridden Settings</span>
            <div style={{ fontSize: 13, fontFamily: 'monospace', color: 'var(--text-primary)', marginTop: 4 }}>{Object.keys(settings).length}</div>
          </div>
        </div>
        <input
          type="text"
          placeholder="Filter settings..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{ width: '100%', padding: '8px 12px', fontSize: 12, borderRadius: 6, border: '1px solid var(--border-primary)', background: 'var(--bg-code)', color: 'var(--text-primary)' }}
        />
      </div>
      {/* Settings table */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        {filteredSettings.length === 0 ? (
          <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)' }}>
            {Object.keys(settings).length === 0 ? 'No settings were overridden for this query' : 'No settings match filter'}
          </div>
        ) : (
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ background: 'var(--bg-card)', borderBottom: '1px solid var(--border-secondary)' }}>
                <th style={{ padding: '10px 16px', textAlign: 'left', fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Setting</th>
                <th style={{ padding: '10px 16px', textAlign: 'right', fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', width: 150 }}>Value</th>
                <th style={{ padding: '10px 16px', textAlign: 'right', fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', width: 150 }}>Default</th>
              </tr>
            </thead>
            <tbody>
              {filteredSettings.map(([key, value], i) => {
                const defaultInfo = defaults[key];
                const isChanged = defaultInfo && defaultInfo.default !== value;
                return (
                  <tr key={key} style={{ borderBottom: '1px solid var(--border-secondary)', background: i % 2 === 0 ? 'transparent' : 'var(--bg-card)' }} title={defaultInfo?.description}>
                    <td style={{ padding: '10px 16px', fontFamily: 'monospace', fontSize: 11, color: 'var(--text-secondary)' }}>{key}</td>
                    <td style={{ padding: '10px 16px', fontFamily: 'monospace', fontSize: 11, color: isChanged ? 'var(--color-warning)' : '#58a6ff', textAlign: 'right', fontWeight: 500 }} title={value}>{value}</td>
                    <td style={{ padding: '10px 16px', fontFamily: 'monospace', fontSize: 11, color: 'var(--text-muted)', textAlign: 'right' }}>
                      {isLoadingDefaults ? '...' : (defaultInfo?.default ?? '—')}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};
