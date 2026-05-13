import React from 'react';
import type { QueryDetail as QueryDetailType } from '@tracehouse/core';
import { ScanEfficiencyTab } from './ScanEfficiencyTab';
import { ColumnCostTab } from './ColumnCostTab';

export type AnalyticsSubTab = 'scan_efficiency' | 'column_cost';

const SUB_TABS: { key: AnalyticsSubTab; label: string }[] = [
  { key: 'scan_efficiency', label: 'Scan Efficiency' },
  { key: 'column_cost', label: 'Column Cost' },
];

interface AnalyticsTabProps {
  analyticsSubTab: AnalyticsSubTab;
  onSubTabChange: (tab: AnalyticsSubTab) => void;
  queryDetail: QueryDetailType | null;
  isLoadingDetail: boolean;
}

export const AnalyticsTab: React.FC<AnalyticsTabProps> = ({
  analyticsSubTab, onSubTabChange, queryDetail, isLoadingDetail,
}) => {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Analytics sub-tabs */}
      <div style={{ display: 'flex', gap: 0, borderBottom: '1px solid var(--border-secondary)', padding: '0 22px', flexShrink: 0 }}>
        {SUB_TABS.map((st) => (
          <button
            key={st.key}
            onClick={() => onSubTabChange(st.key)}
            style={{
              fontFamily: 'monospace',
              padding: '10px 16px',
              fontSize: 11,
              letterSpacing: '0.5px',
              border: 'none',
              borderBottom: analyticsSubTab === st.key ? '2px solid #58a6ff' : '2px solid transparent',
              background: 'transparent',
              color: analyticsSubTab === st.key ? 'var(--text-primary)' : 'var(--text-muted)',
              cursor: 'pointer',
              transition: 'all 0.15s ease',
            }}
            onMouseEnter={(e) => {
              if (analyticsSubTab !== st.key) e.currentTarget.style.color = 'var(--text-tertiary)';
            }}
            onMouseLeave={(e) => {
              if (analyticsSubTab !== st.key) e.currentTarget.style.color = 'var(--text-muted)';
            }}
          >
            {st.label}
          </button>
        ))}
      </div>
      {/* Analytics sub-tab content */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        {analyticsSubTab === 'scan_efficiency' && (
          <ScanEfficiencyTab queryDetail={queryDetail} isLoading={isLoadingDetail} />
        )}
        {analyticsSubTab === 'column_cost' && (
          <ColumnCostTab queryDetail={queryDetail} isLoading={isLoadingDetail} />
        )}
      </div>
    </div>
  );
};
