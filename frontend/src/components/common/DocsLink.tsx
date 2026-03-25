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
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <polyline points="14 2 14 8 20 8" />
      <line x1="16" y1="13" x2="8" y2="13" />
      <line x1="16" y1="17" x2="8" y2="17" />
    </svg>
  </a>
);
