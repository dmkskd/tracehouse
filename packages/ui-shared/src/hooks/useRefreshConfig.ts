/**
 * Refresh Config Context — framework-agnostic context for admin-configured
 * refresh rate settings. Works in both standalone and Grafana modes.
 *
 * In Grafana mode, the provider reads from plugin jsonData.
 * In standalone mode, the provider supplies sensible defaults.
 */
import { createContext, useContext } from 'react';

/** A single refresh rate option */
export interface RefreshRateOption {
  label: string;
  seconds: number;
}

/** All possible refresh rate presets (superset) */
export const ALL_REFRESH_RATE_OPTIONS: RefreshRateOption[] = [
  { label: 'Off', seconds: 0 },
  { label: '0.5s', seconds: 0.5 },
  { label: '1s', seconds: 1 },
  { label: '2s', seconds: 2 },
  { label: '5s', seconds: 5 },
  { label: '10s', seconds: 10 },
  { label: '30s', seconds: 30 },
  { label: '1m', seconds: 60 },
];

/** Resolved refresh configuration */
export interface RefreshConfig {
  /** Which refresh rates are allowed (seconds values) */
  allowedRefreshRates: number[];
  /** Default refresh rate in seconds */
  defaultRefreshRate: number;
  /** Convenience: allowed options as label/seconds pairs */
  refreshRateOptions: RefreshRateOption[];
}

/** Default config — everything enabled, 5s default */
export const DEFAULT_REFRESH_CONFIG: RefreshConfig = {
  allowedRefreshRates: ALL_REFRESH_RATE_OPTIONS.map(o => o.seconds),
  defaultRefreshRate: 5,
  refreshRateOptions: [...ALL_REFRESH_RATE_OPTIONS],
};

export const RefreshConfigContext = createContext<RefreshConfig>(DEFAULT_REFRESH_CONFIG);

/**
 * Hook to access admin-configured refresh settings.
 * Returns defaults if no provider is present.
 */
export function useRefreshConfig(): RefreshConfig {
  return useContext(RefreshConfigContext);
}

/**
 * Filter a set of options to only those allowed by the config.
 * Useful for pages that have their own subset of options.
 */
export function filterAllowedOptions(
  options: RefreshRateOption[],
  config: RefreshConfig
): RefreshRateOption[] {
  return options.filter(o => config.allowedRefreshRates.includes(o.seconds));
}

/**
 * Get the effective default rate: the configured default if it's allowed,
 * otherwise the first allowed rate, otherwise 5.
 */
export function getEffectiveDefault(config: RefreshConfig): number {
  if (config.allowedRefreshRates.includes(config.defaultRefreshRate)) {
    return config.defaultRefreshRate;
  }
  return config.allowedRefreshRates[0] ?? 5;
}

/**
 * Clamp a rate to the nearest allowed value.
 * Returns the closest allowed rate that is >= the requested rate.
 * If none is >=, returns the largest allowed rate.
 */
export function clampToAllowed(rateSeconds: number, config: RefreshConfig): number {
  const sorted = [...config.allowedRefreshRates].filter(r => r > 0).sort((a, b) => a - b);
  if (sorted.length === 0) return 0;
  const gte = sorted.find(r => r >= rateSeconds);
  return gte ?? sorted[sorted.length - 1];
}
