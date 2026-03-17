/**
 * Monitoring Capabilities Store
 * 
 * Global Zustand store that holds the probed monitoring capabilities
 * for the active connection. Probed once when a connection is established,
 * then available to all pages/components for conditional rendering.
 */

import { create } from 'zustand';
import type {
  MonitoringCapabilities,
  MonitoringCapability,
  MonitoringFlags,
} from '@tracehouse/core';
import { deriveMonitoringFlags } from '@tracehouse/core';

export type ProbeStatus = 'idle' | 'probing' | 'done' | 'error';

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
  hasAsyncMetricLog: false,
  hasZookeeper: false,
  hasCrashLog: false,
  hasBackupLog: false,
  hasS3QueueLog: false,
  hasBlobStorageLog: false,
  hasIntrospectionFunctions: false,
  hasCPUProfilerActive: false,
  hasClickStack: false,
  isCloudService: false,
  hasProcessesHistory: false,
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

export default useMonitoringCapabilitiesStore;
