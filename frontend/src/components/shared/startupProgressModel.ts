import type { CapabilityProbeProgress, CapabilityProbeStage } from '@tracehouse/core';
import type { ProbeStageTiming } from '../../stores/monitoringCapabilitiesStore';

export type StartupStepState = 'pending' | 'running' | 'done';

export interface StartupStep {
  id: 'cluster' | CapabilityProbeStage;
  label: string;
  detail: string;
  state: StartupStepState;
  /** Set once the step is done and its timing is known. */
  durationMs: number | null;
}

export interface StartupProgressInput {
  clusterDetected: boolean;
  clusterName: string | null;
  replicaCount: number;
  probeProgress: CapabilityProbeProgress | null;
  probeStages: Partial<Record<CapabilityProbeStage, ProbeStageTiming>>;
}

function stageStep(
  id: CapabilityProbeStage,
  label: string,
  detail: string,
  timing: ProbeStageTiming | undefined,
): StartupStep {
  if (!timing) return { id, label, detail, state: 'pending', durationMs: null };
  if (timing.finishedAt === null) return { id, label, detail, state: 'running', durationMs: null };
  return { id, label, detail, state: 'done', durationMs: timing.finishedAt - timing.startedAt };
}

/** Steps shown while the app prepares a new connection. */
export function buildStartupSteps(input: StartupProgressInput): StartupStep[] {
  const { clusterDetected, clusterName, replicaCount, probeProgress, probeStages } = input;
  const access = probeProgress?.stage === 'tableAccess' ? probeProgress : null;
  return [
    {
      id: 'cluster',
      label: 'Cluster topology',
      detail: !clusterDetected
        ? 'Finding the replicas to query'
        : clusterName
          ? `${clusterName} · ${replicaCount} ${replicaCount === 1 ? 'replica' : 'replicas'}`
          : 'Single node',
      state: clusterDetected ? 'done' : 'running',
      durationMs: null,
    },
    stageStep(
      'metadata',
      'Server metadata',
      'Version, system tables and settings',
      probeStages.metadata,
    ),
    stageStep(
      'tableAccess',
      'Table access',
      access
        ? `${access.checked} of ${access.total} monitoring tables checked`
        : 'Read permission on each monitoring table',
      probeStages.tableAccess,
    ),
  ];
}
