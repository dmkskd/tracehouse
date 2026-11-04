/**
 * RefreshingOverlay — wraps content that stays on screen while a slower
 * refetch is in flight. After a short CSS-driven delay it shows an
 * indeterminate bar along the top edge plus an elapsed-time pill, and
 * optionally dims the stale content. Fast refreshes finish before the delay
 * and never flicker.
 */

import React, { useEffect, useState } from 'react';

interface RefreshingOverlayProps {
  active: boolean;
  /** Dim the wrapped content while refreshing (stale data on screen). */
  dim?: boolean;
  label?: string;
  children: React.ReactNode;
}

/** Mounted only while active, so each refresh starts counting from zero. */
const ElapsedSeconds: React.FC = () => {
  const [startedAt] = useState(() => Date.now());
  const [now, setNow] = useState(startedAt);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 250);
    return () => clearInterval(timer);
  }, []);
  return <>{Math.floor((now - startedAt) / 1000)}s</>;
};

export const RefreshingOverlay: React.FC<RefreshingOverlayProps> = ({
  active,
  dim = true,
  label = 'Updating',
  children,
}) => (
  <div
    className="refreshing-overlay"
    aria-busy={active}
    data-dim={active && dim ? '' : undefined}
  >
    <div className="refreshing-overlay-content">{children}</div>
    {active && (
      <>
        <div className="refreshing-overlay-bar" />
        <div role="status" className="refreshing-overlay-pill">
          <span className="refreshing-overlay-dot" />
          {label} · <ElapsedSeconds />
        </div>
      </>
    )}
  </div>
);
