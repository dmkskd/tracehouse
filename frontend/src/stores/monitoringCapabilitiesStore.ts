/**
 * Monitoring Capabilities Store
 * 
 * Global Zustand store that holds the probed monitoring capabilities
 * for the active connection. Probed once when a connection is established,
 * then available to all pages/components for conditional rendering.
 */

import { create } from 'zustand';
import type {
  IClickHouseAdapter,
  MonitoringCapabilities,
  MonitoringCapability,
  MonitoringFlags,
} from '@tracehouse/core';
import {
  deriveMonitoringFlags,
  MonitoringCapabilitiesService,
} from '@tracehouse/core';

export type ProbeStatus = 'idle' | 'probing' | 'done' | 'error';

/** Longest the app waits for the first probe before opening without it. */
export const CAPABILITY_GATE_TIMEOUT_MS = 30_000;

interface MonitoringCapabilitiesState {
  /** Probed capabilities (null until probed) */
  capabilities: MonitoringCapabilities | null;
  /** Quick-access boolean flags */
  flags: MonitoringFlags;
  /** Probe status */
  probeStatus: ProbeStatus;
  /** Error message if probe failed */
  probeError: string | null;

  // Actions
  setCapabilities: (caps: MonitoringCapabilities) => void;
  setProbeStatus: (status: ProbeStatus) => void;
  setProbeError: (error: string | null) => void;
  refresh: (adapter: IClickHouseAdapter) => Promise<MonitoringCapabilities>;
  /** Fail the first probe if it is still running; a late result still applies. */
  expireFirstProbe: () => void;
  reset: () => void;

  // Selectors
  isAvailable: (capabilityId: string) => boolean;
  getCapability: (capabilityId: string) => MonitoringCapability | undefined;
  getByCategory: (category: MonitoringCapability['category']) => MonitoringCapability[];
}

const EMPTY_FLAGS: MonitoringFlags = {
  hasTextLog: false,
  hasQueryLog: false,
  hasQueryThreadLog: false,
  hasPartLog: false,
  hasTraceLog: false,
  hasOpenTelemetry: false,
  hasQueryProfileEvents: false,
  hasProcessorProfileLog: false,
  hasMetricLog: false,
  hasQueryMetricLog: false,
  hasQueryMetricLogXRay: false,
  hasAsyncMetricLog: false,
  hasZookeeper: false,
  hasCrashLog: false,
  hasBackupLog: false,
  hasS3QueueLog: false,
  hasBlobStorageLog: false,
  hasIntrospectionFunctions: false,
  hasCPUProfilerActive: false,
  hasClickStack: false,
  hasExplainAnalyze: false,
  isCloudService: false,
  hasProcessesHistory: false,
  hasMergesHistory: false,
  hasSystemMerges: false,
  hasSystemMutations: false,
  hasSystemClusters: false,
  hasSystemReplicas: false,
  hasSystemParts: false,
  hasSystemDatabases: false,
  hasSystemProcesses: false,
};

export const useMonitoringCapabilitiesStore = create<MonitoringCapabilitiesState>((set, get) => ({
  capabilities: null,
  flags: EMPTY_FLAGS,
  probeStatus: 'idle',
  probeError: null,

  setCapabilities: (caps: MonitoringCapabilities) => {
    set({
      capabilities: caps,
      flags: deriveMonitoringFlags(caps.capabilities, caps.serverVersion),
      probeStatus: 'done',
      probeError: null,
    });
  },

  setProbeStatus: (status: ProbeStatus) => {
    set({ probeStatus: status });
  },

  setProbeError: (error: string | null) => {
    set({
      probeError: error,
      probeStatus: error ? 'error' : get().probeStatus,
    });
  },

  refresh: async (adapter: IClickHouseAdapter) => {
    set({ probeStatus: 'probing', probeError: null });
    try {
      const capabilities = await new MonitoringCapabilitiesService(adapter).probe();
      get().setCapabilities(capabilities);
      return capabilities;
    } catch (cause) {
      const message = cause instanceof Error
        ? cause.message
        : 'Failed to probe monitoring capabilities';
      get().setProbeError(message);
      throw cause;
    }
  },

  expireFirstProbe: () => {
    const { capabilities, probeStatus } = get();
    if (capabilities !== null || probeStatus !== 'probing') return;
    get().setProbeError(
      `Capability probe did not finish within ${CAPABILITY_GATE_TIMEOUT_MS / 1000}s`,
    );
  },

  reset: () => {
    set({
      capabilities: null,
      flags: EMPTY_FLAGS,
      probeStatus: 'idle',
      probeError: null,
    });
  },

  isAvailable: (capabilityId: string) => {
    const { capabilities } = get();
    return capabilities?.capabilities.find(c => c.id === capabilityId)?.available ?? false;
  },

  getCapability: (capabilityId: string) => {
    const { capabilities } = get();
    return capabilities?.capabilities.find(c => c.id === capabilityId);
  },

  getByCategory: (category: MonitoringCapability['category']) => {
    const { capabilities } = get();
    return capabilities?.capabilities.filter(c => c.category === category) ?? [];
  },
}));

/**
 * Pages receive services only after the first probe for a connection has
 * settled, so they never run against unknown capabilities. Later refreshes
 * keep the previous result and leave the gate open.
 */
export function isCapabilityGateOpen(
  state: Pick<MonitoringCapabilitiesState, 'capabilities' | 'probeStatus'>,
): boolean {
  return state.capabilities !== null || state.probeStatus === 'error';
}

export default useMonitoringCapabilitiesStore;
