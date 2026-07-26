import type React from 'react';

export const trackerFilterLabelStyle: React.CSSProperties = {
  display: 'block',
  marginBottom: 4,
  color: 'var(--text-muted)',
  fontSize: 10,
  fontWeight: 500,
  letterSpacing: '0.3px',
  textTransform: 'uppercase',
};

export const trackerFilterInputStyle: React.CSSProperties = {
  padding: '6px 10px',
  color: 'var(--text-primary)',
  background: 'var(--bg-tertiary)',
  border: '1px solid var(--border-primary)',
  borderRadius: 6,
  outline: 'none',
  fontFamily: 'inherit',
  fontSize: 12,
};

export const trackerScopeOptionStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 5,
  paddingBottom: 2,
  color: 'var(--text-secondary)',
  fontSize: 11,
  lineHeight: 1.3,
  whiteSpace: 'nowrap',
  cursor: 'pointer',
  userSelect: 'none',
};
