/**
 * Admin-configurable settings for the TraceHouse app plugin.
 * Stored in Grafana's plugin jsonData (persisted by Grafana API).
 */

/** A single refresh rate option */
export interface RefreshRateOption {
  /** Display label, e.g. "5s" */
  label: string;
  /** Interval in seconds (0 = off / manual only) */
  seconds: number;
}

/** All available refresh rate presets */
export const ALL_REFRESH_RATE_OPTIONS: RefreshRateOption[] = [
  { label: 'Off', seconds: 0 },
  { label: '1s', seconds: 1 },
  { label: '2s', seconds: 2 },
  { label: '5s', seconds: 5 },
  { label: '10s', seconds: 10 },
  { label: '30s', seconds: 30 },
  { label: '1m', seconds: 60 },
];

/** The jsonData shape persisted by Grafana for this app plugin */
export interface AppPluginSettings {
  /**
   * Allowed refresh rate values (in seconds).
   * Admin unchecks rates they want to disallow (e.g. 1s is too aggressive).
   * Default: all options enabled.
   */
  allowedRefreshRates?: number[];

  /**
   * Default refresh rate in seconds for new sessions.
   * Must be one of the allowed rates. Default: 5.
   */
  defaultRefreshRate?: number;

  /**
   * Allow killing queries from the Active Queries view.
   * Plugin-level (admin) setting. Default: false.
   */
  killQueriesEnabled?: boolean;
}

/** Resolved config after applying defaults */
export interface ResolvedPluginConfig {
  allowedRefreshRates: number[];
  defaultRefreshRate: number;
  /** Convenience: the allowed options as label/seconds pairs */
  refreshRateOptions: RefreshRateOption[];
  /** Whether kill query is allowed (admin-level) */
  killQueriesEnabled: boolean;
}

/** Apply defaults to raw jsonData */
export function resolvePluginConfig(jsonData?: AppPluginSettings): ResolvedPluginConfig {
  const allSeconds = ALL_REFRESH_RATE_OPTIONS.map(o => o.seconds);
  const allowed = jsonData?.allowedRefreshRates ?? allSeconds;
  const defaultRate = jsonData?.defaultRefreshRate ?? 5;

  // Ensure the default is in the allowed set; fall back to first allowed
  const effectiveDefault = allowed.includes(defaultRate)
    ? defaultRate
    : (allowed[0] ?? 5);

  const refreshRateOptions = ALL_REFRESH_RATE_OPTIONS.filter(o =>
    allowed.includes(o.seconds)
  );

  return {
    allowedRefreshRates: allowed,
    defaultRefreshRate: effectiveDefault,
    refreshRateOptions,
    killQueriesEnabled: jsonData?.killQueriesEnabled ?? false,
  };
}
