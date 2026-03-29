import React, { createContext, useContext, useMemo } from 'react';
import {
  type AppPluginSettings,
  type ResolvedPluginConfig,
  resolvePluginConfig,
} from './types';
import {
  RefreshConfigContext,
  ALL_REFRESH_RATE_OPTIONS as SHARED_ALL_OPTIONS,
  type RefreshConfig,
} from '@tracehouse/ui-shared';

const PluginConfigContext = createContext<ResolvedPluginConfig | null>(null);

interface PluginConfigProviderProps {
  jsonData?: AppPluginSettings;
  children: React.ReactNode;
}

/**
 * Resolves the app plugin's jsonData (from plugin.meta.jsonData)
 * and provides the resolved settings to the component tree.
 *
 * Also bridges into the shared RefreshConfigContext so that
 * frontend pages (which don't know about Grafana) can consume
 * the admin-configured refresh rates.
 */
export function PluginConfigProvider({ jsonData, children }: PluginConfigProviderProps) {
  const resolved = useMemo(() => {
    return resolvePluginConfig(jsonData);
  }, [jsonData]);

  // Bridge to the shared RefreshConfigContext used by frontend pages
  const sharedConfig = useMemo<RefreshConfig>(() => ({
    allowedRefreshRates: resolved.allowedRefreshRates,
    defaultRefreshRate: resolved.defaultRefreshRate,
    refreshRateOptions: SHARED_ALL_OPTIONS.filter(o =>
      resolved.allowedRefreshRates.includes(o.seconds)
    ),
  }), [resolved]);

  return (
    <PluginConfigContext.Provider value={resolved}>
      <RefreshConfigContext.Provider value={sharedConfig}>
        {children}
      </RefreshConfigContext.Provider>
    </PluginConfigContext.Provider>
  );
}

/**
 * Hook to access admin-configured plugin settings (Grafana-specific).
 * Falls back to sensible defaults if no admin config exists.
 */
export function usePluginConfig(): ResolvedPluginConfig {
  const ctx = useContext(PluginConfigContext);
  if (!ctx) {
    return resolvePluginConfig();
  }
  return ctx;
}
