import { useContext, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { TracehouseStyleContext } from './TracehouseStyleContext';
import { useThemeDetection } from '../hooks/useThemeDetection';

function PortalScope({ children }: { children: ReactNode }) {
  const className = useContext(TracehouseStyleContext);
  const theme = useThemeDetection();
  if (!className) return <>{children}</>;
  // display:contents preserves flex layout and viewport positioning. Do not add
  // transform, filter, contain, position, or z-index: the children own stacking.
  return (
    <div className={className} data-theme={theme} data-tracehouse-portal="" style={{ display: 'contents' }}>
      {children}
    </div>
  );
}

export function createTracehousePortal(
  children: ReactNode,
  container: Element | DocumentFragment,
  key?: string | null,
) {
  return createPortal(<PortalScope>{children}</PortalScope>, container, key);
}
