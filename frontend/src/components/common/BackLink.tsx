/**
 * BackLink - Contextual "back" navigation shown when a user drills down
 * from one page to another (e.g. Overview → Queries).
 *
 * Reads `location.state.from` which is set by the originating Link/navigate call.
 * If no `from` state exists, renders nothing.
 */

import React from 'react';
import { useAppLocation, useNavigate } from '../../hooks/useAppLocation';
import type { LinkOrigin } from '../../hooks/useReturnToOrigin';

type FromState = LinkOrigin;

/**
 * `from` overrides the navigation state, for pages whose own URL updates clear
 * that state after arrival (they capture it on mount and pass it here).
 */
export const BackLink: React.FC<{ from?: FromState }> = ({ from: fromProp }) => {
  const location = useAppLocation();
  const navigate = useNavigate();
  const from = fromProp ?? (location.state as { from?: FromState } | null)?.from;

  if (!from) return null;

  return (
    <button
      onClick={() => navigate(from.path)}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 4,
        padding: '2px 8px',
        fontSize: 11,
        color: 'var(--text-muted)',
        background: 'transparent',
        border: '1px solid var(--border-secondary)',
        borderRadius: 4,
        cursor: 'pointer',
        fontFamily: 'inherit',
        transition: 'color 0.15s ease, border-color 0.15s ease',
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.color = 'var(--text-primary)';
        e.currentTarget.style.borderColor = 'var(--border-primary)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.color = 'var(--text-muted)';
        e.currentTarget.style.borderColor = 'var(--border-secondary)';
      }}
    >
      <span>←</span>
      <span>{from.label}</span>
    </button>
  );
};
