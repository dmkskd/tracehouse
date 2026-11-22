/**
 * BackLink - Contextual "back" navigation shown when a user drills down
 * from one page to another (e.g. Overview → Queries): a square ← before the
 * page title, as on an Analytics dashboard. The tooltip names the origin.
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
      type="button"
      className="tab dashboard-header-action dashboard-back-button"
      onClick={() => navigate(from.path)}
      aria-label={`Back to ${from.label}`}
      title={`Back to ${from.label}`}
      style={{
        display: 'inline-flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
        width: 32, height: 32, padding: 0, fontSize: 15, borderRadius: 8, cursor: 'pointer', fontFamily: 'inherit',
        color: 'var(--text-secondary)', background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)',
        boxShadow: '0 1px 2px rgba(0, 0, 0, 0.06)',
      }}
    >
      <span aria-hidden="true">←</span>
    </button>
  );
};
