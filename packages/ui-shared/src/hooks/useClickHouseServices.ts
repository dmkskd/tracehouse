/**
 * Shared hook for accessing ClickHouse services.
 * 
 * This hook is implemented by both:
 * - frontend/src/providers/ClickHouseProvider.tsx (standalone app)
 * - grafana-app-plugin/src/ServiceProvider.tsx (Grafana plugin)
 * 
 * Pages import from this shared location, and each app provides
 * the context value through their respective providers.
 */

import { createContext, useContext } from 'react';
import type { 
  IClickHouseAdapter,
  DatabaseExplorer,
  QueryAnalyzer,
  MetricsCollector,
  MergeTracker,
  TimelineService,
  TraceService,
  AnalyticsService,
  EnvironmentDetector,
} from '@tracehouse/core';

export interface ClickHouseServices {
  adapter: IClickHouseAdapter;
  databaseExplorer: DatabaseExplorer;
  queryAnalyzer: QueryAnalyzer;
  metricsCollector: MetricsCollector;
  mergeTracker: MergeTracker;
  timelineService: TimelineService;
  traceService: TraceService;
  analyticsService: AnalyticsService;
  environmentDetector: EnvironmentDetector;
}

// Shared context - both apps provide this
export const ClickHouseContext = createContext<ClickHouseServices | null>(null);

/**
 * Returns the current ClickHouse services or `null` when no connection is active.
 */
export function useClickHouseServices(): ClickHouseServices | null {
  return useContext(ClickHouseContext);
}

/**
 * Throws if called outside a connected provider.
 */
export function useRequiredClickHouseServices(): ClickHouseServices {
  const services = useContext(ClickHouseContext);
  if (!services) {
    throw new Error(
      'useRequiredClickHouseServices must be used within a provider with an active connection',
    );
  }
  return services;
}
