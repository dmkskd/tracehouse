import React from 'react';
import type { QueryDetail as QueryDetailType } from '@tracehouse/core';

/**
 * Functions Tab - used functions, aggregates, table functions, etc.
 */
export const FunctionsTab: React.FC<{
  queryDetail: QueryDetailType | null;
  isLoading: boolean;
}> = ({ queryDetail, isLoading }) => {
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

  const sections = [
    { label: 'Functions', items: queryDetail.used_functions, color: '#58a6ff' },
    { label: 'Aggregate Functions', items: queryDetail.used_aggregate_functions, color: 'var(--color-success)' },
    { label: 'Aggregate Combinators', items: queryDetail.used_aggregate_function_combinators, color: 'var(--color-info)' },
    { label: 'Table Functions', items: queryDetail.used_table_functions, color: 'var(--color-warning)' },
    { label: 'Storages', items: queryDetail.used_storages, color: 'var(--color-memory)' },
    { label: 'Formats', items: queryDetail.used_formats, color: '#f0883e' },
    { label: 'Dictionaries', items: queryDetail.used_dictionaries, color: '#f778ba' },
  ];

  return (
    <div style={{ padding: 20, overflow: 'auto', height: '100%' }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 16 }}>
        {sections.map(({ label, items, color }) => (
          <div key={label} style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 8, padding: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 600, color, textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ width: 8, height: 8, borderRadius: '50%', background: color }} />
              {label} ({items?.length || 0})
            </div>
            {items && items.length > 0 ? (
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {items.map((item, i) => (
                  <span key={i} style={{ fontSize: 11, fontFamily: 'monospace', padding: '4px 8px', background: 'var(--bg-code)', borderRadius: 4, color: 'var(--text-secondary)' }}>{item}</span>
                ))}
              </div>
            ) : (
              <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>None</span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};
