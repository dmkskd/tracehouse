/**
 * ExperimentalBadge - Consistent badge for marking experimental features.
 */
import React from 'react';

interface ExperimentalBadgeProps {
  style?: React.CSSProperties;
}

export const ExperimentalBadge: React.FC<ExperimentalBadgeProps> = ({ style }) => (
  <span
    style={{
      display: 'inline-flex',
      alignItems: 'center',
      gap: 3,
      fontSize: 9,
      fontWeight: 700,
      textTransform: 'uppercase',
      letterSpacing: '0.5px',
      color: '#f0883e',
      background: 'rgba(240,136,62,0.12)',
      border: '1px solid rgba(240,136,62,0.25)',
      borderRadius: 4,
      padding: '1px 5px',
      lineHeight: '16px',
      whiteSpace: 'nowrap',
      ...style,
    }}
  >
    <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9 3H5a2 2 0 0 0-2 2v4m6-6h10a2 2 0 0 1 2 2v4M9 3v18m0 0h10a2 2 0 0 0 2-2v-4M9 21H5a2 2 0 0 1-2-2v-4m0-6v6m18-6v6" />
    </svg>
    Experimental
  </span>
);
