import React from 'react';
import {
  trackerFilterInputStyle,
  trackerFilterLabelStyle,
} from './trackerFilterStyles';

export const TrackerFilterBarShell: React.FC<{
  children: React.ReactNode;
  footer?: React.ReactNode;
}> = ({ children, footer }) => (
  <div
    style={{
      padding: '12px 16px',
      background: 'var(--bg-secondary)',
      border: '1px solid var(--border-primary)',
      borderRadius: 8,
    }}
  >
    <div
      style={{
        display: 'flex',
        alignItems: 'flex-end',
        gap: 10,
        flexWrap: 'wrap',
      }}
    >
      {children}
    </div>
    {footer}
  </div>
);

export const TrackerRefreshButton: React.FC<{
  isLoading?: boolean;
  onRefresh: () => void;
}> = ({ isLoading = false, onRefresh }) => (
  <button
    type="button"
    onClick={onRefresh}
    disabled={isLoading}
    style={{
      flexShrink: 0,
      padding: '6px 12px',
      color: isLoading ? 'var(--text-muted)' : 'var(--text-secondary)',
      background: 'var(--bg-tertiary)',
      border: '1px solid var(--border-primary)',
      borderRadius: 6,
      fontSize: 11,
      fontWeight: 500,
      whiteSpace: 'nowrap',
      cursor: isLoading ? 'not-allowed' : 'pointer',
    }}
  >
    {isLoading ? 'Loading…' : 'Refresh'}
  </button>
);

export const TrackerLimitInput: React.FC<{
  value?: number;
  onChange: (value: number) => void;
}> = ({ value = 100, onChange }) => {
  const commit = (input: HTMLInputElement) => {
    const parsed = Number.parseInt(input.value, 10);
    onChange(parsed > 0 ? parsed : 100);
  };

  return (
    <div style={{ width: 70 }}>
      <label style={trackerFilterLabelStyle}>Limit</label>
      <input
        key={value}
        type="number"
        defaultValue={value}
        min="1"
        max="10000"
        step="50"
        onBlur={event => commit(event.currentTarget)}
        onKeyDown={event => {
          if (event.key === 'Enter') event.currentTarget.blur();
        }}
        style={{ ...trackerFilterInputStyle, width: '100%' }}
      />
    </div>
  );
};
