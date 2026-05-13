import React from 'react';
import type { QueryDetail as QueryDetailType } from '@tracehouse/core';

/**
 * Objects Tab - databases, tables, columns, partitions, views
 */
export const ObjectsTab: React.FC<{
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
    { label: 'Databases', items: queryDetail.databases, color: 'var(--color-info)' },
    { label: 'Tables', items: queryDetail.tables, color: 'var(--color-success)' },
    { label: 'Columns', items: queryDetail.columns, color: 'var(--color-warning)' },
    { label: 'Partitions', items: queryDetail.partitions, color: 'var(--color-memory)' },
    { label: 'Views', items: queryDetail.views, color: '#f0883e' },
    { label: 'Projections', items: queryDetail.projections ? [queryDetail.projections] : [], color: '#f778ba' },
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
                {items.slice(0, 20).map((item, i) => (
                  <span key={i} style={{ fontSize: 11, fontFamily: 'monospace', padding: '4px 8px', background: 'var(--bg-code)', borderRadius: 4, color: 'var(--text-secondary)' }}>{item}</span>
                ))}
                {items.length > 20 && <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>+{items.length - 20} more</span>}
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
