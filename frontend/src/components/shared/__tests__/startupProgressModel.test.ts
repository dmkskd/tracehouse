import { afterEach, describe, expect, it, vi } from 'vitest';
import { useMonitoringCapabilitiesStore } from '../../../stores/monitoringCapabilitiesStore';
import { buildStartupSteps } from '../startupProgressModel';

const store = () => useMonitoringCapabilitiesStore.getState();

function stepsFromStore(clusterDetected = true) {
  const { probeProgress, probeStages } = store();
  return buildStartupSteps({
    clusterDetected,
    clusterName: 'tracehouse',
    replicaCount: 4,
    probeProgress,
    probeStages,
  }).map(step => `${step.id}:${step.state}:${step.detail}:${step.durationMs ?? '-'}`);
}

describe('startup progress', () => {
  afterEach(() => {
    vi.useRealTimers();
    store().reset();
  });

  it('follows cluster detection and each probe stage through to their timings', () => {
    vi.useFakeTimers({ now: 0 });

    expect(stepsFromStore(false)).toEqual([
      'cluster:running:Finding the replicas to query:-',
      'metadata:pending:Version, system tables and settings:-',
      'tableAccess:pending:Read permission on each monitoring table:-',
    ]);

    store().beginProbe();
    store().reportProbeProgress({ stage: 'metadata', checked: 0, total: 0 });
    vi.setSystemTime(400);
    store().reportProbeProgress({ stage: 'tableAccess', checked: 0, total: 48 });
    vi.setSystemTime(1_000);
    store().reportProbeProgress({ stage: 'tableAccess', checked: 31, total: 48 });
    expect(stepsFromStore()).toEqual([
      'cluster:done:tracehouse · 4 replicas:-',
      'metadata:done:Version, system tables and settings:400',
      'tableAccess:running:31 of 48 monitoring tables checked:-',
    ]);

    vi.setSystemTime(2_400);
    store().reportProbeProgress({ stage: 'tableAccess', checked: 48, total: 48 });
    store().setCapabilities({ serverVersion: '25.8.1.1', probedAt: new Date(), capabilities: [] });
    expect(stepsFromStore()[2]).toBe('tableAccess:done:48 of 48 monitoring tables checked:2000');
    expect(store().lastProbeDurationMs).toBe(2_400);
  });
});
