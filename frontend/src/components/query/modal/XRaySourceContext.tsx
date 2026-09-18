/**
 * Makes the active X-Ray source selection available to the chart components
 * without threading it through four levels of props.
 *
 * The selection itself is computed by selectQueryXRaySource() in core; this only
 * carries it. Components read `missing` to drop series their source cannot
 * populate. Per-sample rate flags disclose when a rate has no thread ceiling.
 *
 * The default is the full-fidelity sampler reading, so any component rendered
 * outside the provider behaves exactly as it did before the second source
 * existed.
 */

import React, { createContext, useContext } from 'react';
import type { QueryXRaySourceSelection } from '@tracehouse/core';

const FULL_FIDELITY: QueryXRaySourceSelection = {
  source: 'processes_history',
  reason: 'only_available',
  missing: [],
  lagMs: 0,
};

const XRaySourceContext = createContext<QueryXRaySourceSelection>(FULL_FIDELITY);

export const XRaySourceProvider: React.FC<{
  meta: QueryXRaySourceSelection;
  children: React.ReactNode;
}> = ({ meta, children }) => (
  <XRaySourceContext.Provider value={meta}>{children}</XRaySourceContext.Provider>
);

/** The active source selection for the surrounding X-Ray. */
export function useXRaySourceMeta(): QueryXRaySourceSelection {
  return useContext(XRaySourceContext);
}

/** True when the active source cannot populate this ProcessSample field. */
export function useFieldUnavailable(field: string): boolean {
  return useContext(XRaySourceContext).missing.includes(field);
}
