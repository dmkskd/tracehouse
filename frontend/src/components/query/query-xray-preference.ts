import { useMemo } from 'react';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';
import type { XRayQueryState } from '@tracehouse/core';
import { createContext, useContext } from 'react';
import type { QueryXRaySourcePreference } from '@tracehouse/core';
import { useConnectionStore } from '../../stores/connectionStore';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';

/** Admin defaults are runtime configuration, never copied into user storage. */
export const QueryXRayDefaultContext = createContext<QueryXRaySourcePreference>('auto');

export function useQueryXRayPreference() {
  const connectionId = useConnectionStore(s => s.activeProfileId);
  const defaultSource = useContext(QueryXRayDefaultContext);
  const override = useUserPreferenceStore(s => connectionId ? s.queryXraySource[connectionId] : undefined);
  const setSource = useUserPreferenceStore(s => s.setQueryXraySource);
  return {
    preference: override ?? defaultSource,
    override,
    defaultSource,
    connectionId,
    setPreference: (source: QueryXRaySourcePreference | undefined) => {
      if (connectionId) setSource(connectionId, source);
    },
  };
}


/** Shared capability policy for time-window consumers. */
export function useQueryXRaySelection(queryState: XRayQueryState = 'finished') {
  const { preference } = useQueryXRayPreference();
  const processesHistory = useMonitoringCapabilitiesStore(s => s.flags.hasProcessesHistory);
  const queryMetricLog = useMonitoringCapabilitiesStore(s => s.flags.hasQueryMetricLogXRay);
  const probed = useMonitoringCapabilitiesStore(s => s.probeStatus === 'done');
  return useMemo(() => ({
    availability: { processesHistory: probed ? processesHistory : true, queryMetricLog: probed ? queryMetricLog : false },
    preference, queryState,
  }), [probed, processesHistory, queryMetricLog, preference, queryState]);
}
