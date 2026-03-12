/**
 * Capability Consumer Registry
 *
 * Static manifest that maps each monitoring capability to the screens
 * and components that depend on it. Used by the MonitoringCapabilitiesCard
 * to show which features are enabled/degraded/unavailable per connection.
 */

/** A screen or component that consumes a capability */
export interface CapabilityConsumer {
  /** Page or component name (e.g. 'Query Monitor', 'CPU Sampling') */
  screen: string;
  /** Parent navigation tab (e.g. 'Queries', 'Engine Internals'). Used for "By Screen" display. */
  tab?: string;
  /** Route path if it's a page, or parent page if it's a sub-component */
  route?: string;
  /** What this capability enables in that screen */
  enables: string;
  /** 'required' = screen won't render without it, 'optional' = degrades gracefully */
  importance: 'required' | 'optional';
}

/** Full registry entry for a capability */
export interface CapabilityRegistryEntry {
  /** Capability ID (matches MonitoringCapability.id) */
  capabilityId: string;
  /** Components/screens that depend on this capability */
  consumers: CapabilityConsumer[];
}

/**
 * Static registry of all capability → screen mappings.
 * Maintained manually — when you add a useCapabilityCheck() or
 * <RequiresCapability> to a component, add an entry here too.
 */
export const CAPABILITY_REGISTRY: CapabilityRegistryEntry[] = [
  {
    capabilityId: 'query_log',
    consumers: [
      { screen: 'Query Monitor', tab: 'Queries', route: '/query-monitor', enables: 'Query history, timing charts, and query analysis', importance: 'required' },
      { screen: 'Analytics', tab: 'Analytics', route: '/analytics', enables: 'Query explorer and preset analytics queries', importance: 'required' },
      { screen: 'Time Travel', tab: 'Time Travel', route: '/time-travel', enables: 'Query overlay on timeline', importance: 'required' },
      { screen: 'Query Detail Modal', tab: 'Queries', enables: 'Query comparison and history lookup', importance: 'optional' },
      { screen: 'Overview', tab: 'Overview', route: '/overview', enables: 'Slow queries widget, rejected queries count', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'text_log',
    consumers: [
      { screen: 'Query Tracer', tab: 'Queries', route: '/query-tracer', enables: 'Server log viewer for traced queries', importance: 'optional' },
      { screen: 'Query Detail Modal', tab: 'Queries', enables: 'Log tab in query detail', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'query_thread_log',
    consumers: [
      { screen: 'Query Detail', tab: 'Queries', enables: 'Per-thread CPU breakdown and thread-level analysis', importance: 'optional' },
      { screen: 'Query Detail Modal', tab: 'Queries', enables: 'Threads tab in query detail', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'trace_log',
    consumers: [
      { screen: 'CPU Sampling', tab: 'Engine Internals', route: '/engine-internals', enables: 'CPU attribution by thread pool from stack trace sampling', importance: 'required' },
      { screen: 'Query Detail Modal', tab: 'Queries', enables: 'Flamegraph tab for query stack traces', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'opentelemetry_span_log',
    consumers: [
      { screen: 'Query Tracer', tab: 'Queries', route: '/query-tracer', enables: 'OpenTelemetry span viewer and distributed trace correlation', importance: 'optional' },
      { screen: 'Query Detail Modal', tab: 'Queries', enables: 'Spans tab in query detail', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'metric_log',
    consumers: [
      { screen: 'Time Travel', tab: 'Time Travel', route: '/time-travel', enables: 'Memory and metric timeline visualization', importance: 'required' },
      { screen: 'Overview', tab: 'Overview', route: '/overview', enables: 'Historical metrics trend charts, QPS history', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'asynchronous_metric_log',
    consumers: [
      { screen: 'Metrics Dashboard', tab: 'Overview', route: '/metrics', enables: 'Async metric time-series (CPU, memory, jemalloc)', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'part_log',
    consumers: [
      { screen: 'Merge Tracker', tab: 'Merges', route: '/merge-tracker', enables: 'Merge history and part lifecycle events', importance: 'optional' },
      { screen: 'Database Explorer', tab: 'Explorer', route: '/database', enables: 'Part lineage and merge history in table detail', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'processors_profile_log',
    consumers: [
      { screen: 'Pipeline Profile', tab: 'Queries', route: '/query-tracer', enables: 'Per-processor pipeline profiling with timing data', importance: 'required' },
    ],
  },
  {
    capabilityId: 'introspection_functions',
    consumers: [
      { screen: 'CPU Sampling', tab: 'Engine Internals', route: '/engine-internals', enables: 'Stack trace symbolization (demangle, addressToSymbol)', importance: 'required' },
      { screen: 'Query Detail', tab: 'Queries', enables: 'Flamegraph symbolization in thread breakdown', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'cpu_profiler_active',
    consumers: [
      { screen: 'CPU Sampling', tab: 'Engine Internals', route: '/engine-internals', enables: 'Verifies CPU profiler is actually capturing samples (SYS_PTRACE present)', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'query_log_profile_events',
    consumers: [
      { screen: 'Query Monitor', tab: 'Queries', route: '/query-monitor', enables: 'Resource attribution columns (CPU time, read bytes, cache hits)', importance: 'optional' },
      { screen: 'Query Detail Modal', tab: 'Queries', enables: 'ProfileEvents breakdown in query overview', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'zookeeper',
    consumers: [
      { screen: 'Overview', tab: 'Overview', route: '/overview', enables: 'Replication status and replica health monitoring', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'crash_log',
    consumers: [
      { screen: 'Engine Internals', tab: 'Engine Internals', route: '/engine-internals', enables: 'Server crash/fatal error history', importance: 'optional' },
    ],
  },
  {
    capabilityId: 'clickstack',
    consumers: [
      { screen: 'Query Detail Modal', tab: 'Queries', enables: 'Deep-link trace logs to ClickStack embedded viewer', importance: 'optional' },
    ],
  },
];

/** Lookup consumers for a given capability ID */
export function getConsumersForCapability(capabilityId: string): CapabilityConsumer[] {
  return CAPABILITY_REGISTRY.find(e => e.capabilityId === capabilityId)?.consumers ?? [];
}

export interface ScreenCapabilitySummary {
  screen: string;
  tab?: string;
  route?: string;
  required: string[];
  optional: string[];
}

export function getScreenSummaries(): ScreenCapabilitySummary[] {
  const screenMap = new Map<string, ScreenCapabilitySummary>();

  for (const entry of CAPABILITY_REGISTRY) {
    for (const consumer of entry.consumers) {
      const key = consumer.screen;
      if (!screenMap.has(key)) {
        screenMap.set(key, { screen: consumer.screen, tab: consumer.tab, route: consumer.route, required: [], optional: [] });
      }
      const summary = screenMap.get(key)!;
      if (!summary.route && consumer.route) summary.route = consumer.route;
      if (!summary.tab && consumer.tab) summary.tab = consumer.tab;
      if (consumer.importance === 'required') {
        summary.required.push(entry.capabilityId);
      } else {
        summary.optional.push(entry.capabilityId);
      }
    }
  }

  return Array.from(screenMap.values());
}
