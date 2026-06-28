import React, { Suspense, lazy } from 'react';
import type { XRayTabProps } from '../../../frontend/src/components/query/modal/tabs/XRayTab';

export type { XRayTabProps } from '../../../frontend/src/components/query/modal/tabs/XRayTab';

const LazyXRayTab = lazy(async () => {
  const mod = await import('../../../frontend/src/components/query/modal/tabs/XRayTab.tsx?tracehouse-original');
  return { default: mod.XRayTab };
});

export const XRayTab: React.FC<XRayTabProps> = (props) => (
  <Suspense
    fallback={
      <div style={{
        minHeight: 400,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: 'var(--text-secondary)',
        fontSize: 12,
      }}>
        Loading X-Ray...
      </div>
    }
  >
    <LazyXRayTab {...props} />
  </Suspense>
);
