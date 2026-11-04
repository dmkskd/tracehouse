/**
 * For pages opened by a link that carries `state.from` (Analytics, Overview):
 * the origin, read once on mount because the page's own URL updates drop
 * navigation state, and a function that navigates back to it.
 */

import { useCallback, useState } from 'react';
import { useAppLocation, useNavigate } from './useAppLocation';

export interface LinkOrigin {
  path: string;
  label: string;
}

export function useReturnToOrigin() {
  const location = useAppLocation();
  const navigate = useNavigate();
  const [from] = useState(() => (location.state as { from?: LinkOrigin } | null)?.from);
  const returnToOrigin = useCallback(() => {
    if (from) navigate(from.path);
  }, [from, navigate]);
  return { from, returnToOrigin };
}
