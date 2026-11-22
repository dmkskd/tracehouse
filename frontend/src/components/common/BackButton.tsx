/**
 * BackButton - the square ← button before a page or view title, as on an Analytics dashboard.
 * The arrow is an icon; `label` names the destination for the tooltip and screen readers.
 */
import React from 'react';

export const BackButton: React.FC<{ label: string; onClick: () => void }> = ({ label, onClick }) => (
  <button
    type="button"
    className="tab dashboard-header-action dashboard-back-button"
    onClick={onClick}
    aria-label={label}
    title={label}
    style={{
      display: 'inline-flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
      width: 32, height: 32, padding: 0, borderRadius: 8, cursor: 'pointer',
      color: 'var(--text-secondary)', background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)',
      boxShadow: '0 1px 2px rgba(0, 0, 0, 0.06)',
    }}
  >
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6"
      strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M13 8H3" />
      <path d="M7.5 3.5L3 8l4.5 4.5" />
    </svg>
  </button>
);
