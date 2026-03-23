/**
 * DocsLink — small inline icon that links to the documentation page for a feature.
 * Place next to page titles: <DocsLink path="/features/merge-tracker" />
 */

import React from 'react';

const DOCS_BASE = 'https://dmkskd.github.io/tracehouse/docs';

export const DocsLink: React.FC<{ path: string }> = ({ path }) => (
  <a
    href={`${DOCS_BASE}${path}`}
    target="_blank"
    rel="noopener noreferrer"
    title="Documentation"
    style={{
      display: 'inline-flex',
      alignItems: 'center',
      justifyContent: 'center',
      color: 'var(--text-muted)',
      opacity: 0.5,
      transition: 'opacity 0.15s ease, color 0.15s ease',
    }}
    onMouseEnter={(e) => {
      e.currentTarget.style.opacity = '1';
      e.currentTarget.style.color = 'var(--text-secondary)';
    }}
    onMouseLeave={(e) => {
      e.currentTarget.style.opacity = '0.5';
      e.currentTarget.style.color = 'var(--text-muted)';
    }}
  >
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z" />
      <path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z" />
    </svg>
  </a>
);
