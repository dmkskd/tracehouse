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
import { BackButton } from './BackButton';

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

  return <BackButton label={`Back to ${from.label}`} onClick={() => navigate(from.path)} />;
};
