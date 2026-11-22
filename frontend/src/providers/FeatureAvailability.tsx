import { createContext, useContext } from 'react';

/** Shell-owned deployment availability. No feature runtime is imported here. */
export const FeatureAvailabilityContext = createContext({ objectBrowser: false });
export function useObjectBrowserEnabled(): boolean {
  return useContext(FeatureAvailabilityContext).objectBrowser === true;
}
