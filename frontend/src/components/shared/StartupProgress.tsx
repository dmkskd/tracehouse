/**
 * StartupProgress — modal over the current screen while the app prepares a
 * new connection: cluster detection, then the one-off capability probe that
 * pages wait for. It closes itself when the parent stops rendering it.
 */

import React from 'react';
import { ModalWrapper } from './ModalWrapper';
import { useClusterStore } from '../../stores/clusterStore';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';
import { buildStartupSteps, type StartupStepState } from './startupProgressModel';

const noop = () => {};

const MARKER: Record<StartupStepState, { glyph: string; color: string }> = {
  done: { glyph: '✓', color: 'var(--color-success)' },
  running: { glyph: '◌', color: 'var(--color-info)' },
  pending: { glyph: '·', color: 'var(--text-muted)' },
};

export const StartupProgress: React.FC = () => {
  const clusterDetected = useClusterStore(state => state.detected);
  const clusterName = useClusterStore(state => state.clusterName);
  const replicaCount = useClusterStore(state => state.replicaCount);
  const probeProgress = useMonitoringCapabilitiesStore(state => state.probeProgress);
  const probeStages = useMonitoringCapabilitiesStore(state => state.probeStages);
  const steps = buildStartupSteps({
    clusterDetected,
    clusterName,
    replicaCount,
    probeProgress,
    probeStages,
  });

  return (
    <ModalWrapper
      isOpen
      onClose={noop}
      maxWidth={480}
      height="auto"
      backdropBackground="rgba(0, 0, 0, 0.45)"
      backdropBlur={2}
      ariaLabel="Checking ClickHouse capabilities"
    >
      <div style={{ padding: '20px 24px' }}>
        <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' }}>
          Checking ClickHouse capabilities
        </div>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 4, marginBottom: 16 }}>
          TraceHouse adapts its pages to the server version, configuration and permissions.
          <div style={{ color: 'var(--text-tertiary)', marginTop: 2 }}>
            Runs on every page load.
          </div>
        </div>
        <ol style={{ listStyle: 'none', margin: 0, padding: 0 }}>
          {steps.map(step => (
            <li
              key={step.id}
              style={{
                display: 'flex',
                alignItems: 'baseline',
                gap: 10,
                padding: '6px 0',
                borderTop: '1px solid var(--border-secondary)',
                opacity: step.state === 'pending' ? 0.55 : 1,
              }}
            >
              <span style={{ width: 12, color: MARKER[step.state].color, fontFamily: 'monospace' }}>
                {MARKER[step.state].glyph}
              </span>
              <span style={{ flex: 1, minWidth: 0 }}>
                <span style={{ fontSize: 12, color: 'var(--text-primary)' }}>{step.label}</span>
                <span style={{ fontSize: 11, color: 'var(--text-tertiary)', marginLeft: 8 }}>{step.detail}</span>
              </span>
              <span style={{ fontSize: 11, color: 'var(--text-tertiary)', fontFamily: 'monospace' }}>
                {step.durationMs !== null ? `${(step.durationMs / 1000).toFixed(1)}s` : ''}
              </span>
            </li>
          ))}
        </ol>
      </div>
    </ModalWrapper>
  );
};
