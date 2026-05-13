import React from 'react';
import type { QueryDetail as QueryDetailType } from '@tracehouse/core';
import { PerformanceTab } from './PerformanceTab';
import { ObjectsTab } from './ObjectsTab';
import { FunctionsTab } from './FunctionsTab';
import { SettingsTab } from './SettingsTab';

export type DetailsSubTab = 'performance' | 'objects' | 'functions' | 'settings';

const SUB_TABS: { key: DetailsSubTab; label: string }[] = [
  { key: 'performance', label: 'Profile Events' },
  { key: 'objects', label: 'Objects' },
  { key: 'functions', label: 'Functions' },
  { key: 'settings', label: 'Settings' },
];

interface DetailsTabProps {
  detailsSubTab: DetailsSubTab;
  onSubTabChange: (tab: DetailsSubTab) => void;
  queryDetail: QueryDetailType | null;
  isLoadingDetail: boolean;
  onFetchSettingsDefaults: (settingNames: string[]) => Promise<Record<string, { default: string; description: string }>>;
}

export const DetailsTab: React.FC<DetailsTabProps> = ({
  detailsSubTab, onSubTabChange, queryDetail, isLoadingDetail, onFetchSettingsDefaults,
}) => {
  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Sub-tabs */}
      <div style={{
        padding: '12px 16px',
        borderBottom: '1px solid var(--border-secondary)',
        background: 'var(--bg-card)',
        display: 'flex',
        gap: 4,
      }}>
        {SUB_TABS.map((subTab) => (
          <button
            key={subTab.key}
            onClick={() => onSubTabChange(subTab.key)}
            style={{
              padding: '6px 14px',
              fontSize: 11,
              borderRadius: 6,
              border: detailsSubTab === subTab.key ? '1px solid var(--border-accent)' : '1px solid transparent',
              background: detailsSubTab === subTab.key ? 'rgba(88, 166, 255, 0.12)' : 'transparent',
              color: detailsSubTab === subTab.key ? 'var(--text-primary)' : 'var(--text-muted)',
              cursor: 'pointer',
              transition: 'all 0.15s ease',
              fontFamily: 'monospace',
            }}
            onMouseEnter={(e) => {
              if (detailsSubTab !== subTab.key) {
                e.currentTarget.style.color = 'var(--text-tertiary)';
                e.currentTarget.style.background = 'var(--bg-code)';
              }
            }}
            onMouseLeave={(e) => {
              if (detailsSubTab !== subTab.key) {
                e.currentTarget.style.color = 'var(--text-muted)';
                e.currentTarget.style.background = 'transparent';
              }
            }}
          >
            {subTab.label}
          </button>
        ))}
      </div>
      {/* Sub-tab content */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        {detailsSubTab === 'performance' && (
          <PerformanceTab profileEvents={queryDetail?.ProfileEvents} isLoading={isLoadingDetail} />
        )}
        {detailsSubTab === 'objects' && (
          <ObjectsTab queryDetail={queryDetail} isLoading={isLoadingDetail} />
        )}
        {detailsSubTab === 'functions' && (
          <FunctionsTab queryDetail={queryDetail} isLoading={isLoadingDetail} />
        )}
        {detailsSubTab === 'settings' && (
          <SettingsTab queryDetail={queryDetail} isLoading={isLoadingDetail} onFetchDefaults={onFetchSettingsDefaults} />
        )}
      </div>
    </div>
  );
};
