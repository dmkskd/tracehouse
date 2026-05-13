import React from 'react';
import { QUERY_KIND_COLORS } from './chartConstants';

export const QueryKindDot: React.FC<{ kind: string }> = ({ kind }) => {
  const k = (kind || '').toUpperCase();
  const color = QUERY_KIND_COLORS[k] || '#94a3b8';
  return (
    <span
      title={kind || 'Unknown'}
      style={{
        display: 'inline-block',
        width: 8, height: 8, borderRadius: '50%',
        background: color,
        boxShadow: `0 0 4px ${color}60`,
        flexShrink: 0,
      }}
    />
  );
};
