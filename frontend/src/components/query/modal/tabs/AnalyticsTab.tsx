import React from 'react';
import type { QueryDetail as QueryDetailType } from '@tracehouse/core';
import { ScanEfficiencyTab } from './ScanEfficiencyTab';
import { ColumnCostTab } from './ColumnCostTab';
import { RuntimeAnalysisTab } from './RuntimeAnalysisTab';
import { executionAnalysisSessionKey } from '../../executionAnalysisModel';

export type AnalyticsSubTab = 'scan_efficiency' | 'runtime_analysis' | 'column_cost';

const SUB_TABS: { key: AnalyticsSubTab; label: string }[] = [
  { key: 'scan_efficiency', label: 'Scan Efficiency' },
  { key: 'runtime_analysis', label: 'Explain Analyze' },
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
  const isSelectQuery = queryDetail?.query_kind?.toUpperCase() === 'SELECT';
  const visibleSubTabs = SUB_TABS.filter(
    subTab => subTab.key !== 'runtime_analysis' || isSelectQuery,
  );
  const activeSubTab = analyticsSubTab === 'runtime_analysis' && !isSelectQuery
    ? 'scan_efficiency'
    : analyticsSubTab;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Analysis sub-tabs */}
      <div style={{ display: 'flex', gap: 0, borderBottom: '1px solid var(--border-secondary)', padding: '0 22px', flexShrink: 0 }}>
        {visibleSubTabs.map((st) => (
          <button
            key={st.key}
            onClick={() => onSubTabChange(st.key)}
            style={{
              fontFamily: 'monospace',
              padding: '10px 16px',
              fontSize: 11,
              letterSpacing: '0.5px',
              border: 'none',
              borderBottom: activeSubTab === st.key ? '2px solid #58a6ff' : '2px solid transparent',
              background: 'transparent',
              color: activeSubTab === st.key ? 'var(--text-primary)' : 'var(--text-muted)',
              cursor: 'pointer',
              transition: 'all 0.15s ease',
            }}
            onMouseEnter={(e) => {
              if (activeSubTab !== st.key) e.currentTarget.style.color = 'var(--text-tertiary)';
            }}
            onMouseLeave={(e) => {
              if (activeSubTab !== st.key) e.currentTarget.style.color = 'var(--text-muted)';
            }}
          >
            {st.label}
          </button>
        ))}
      </div>
      {/* Analysis sub-tab content */}
      <div style={{
        display: activeSubTab === 'runtime_analysis' ? 'flex' : 'block',
        flex: 1,
        minHeight: 0,
        overflow: activeSubTab === 'runtime_analysis' ? 'hidden' : 'auto',
      }}>
        {activeSubTab === 'scan_efficiency' && (
          <ScanEfficiencyTab queryDetail={queryDetail} isLoading={isLoadingDetail} />
        )}
        {activeSubTab === 'runtime_analysis' && isSelectQuery && (
          <RuntimeAnalysisTab
            key={executionAnalysisSessionKey(queryDetail) ?? 'loading'}
            queryDetail={queryDetail}
            isLoading={isLoadingDetail}
          />
        )}
        {activeSubTab === 'column_cost' && (
          <ColumnCostTab queryDetail={queryDetail} isLoading={isLoadingDetail} />
        )}
      </div>
    </div>
  );
};
