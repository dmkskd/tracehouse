// @vitest-environment jsdom
import React from 'react';
import { act, cleanup, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ClusterService } from '@tracehouse/core/services/cluster-service';
import { MonitoringCapabilitiesService } from '@tracehouse/core/services/monitoring-capabilities';
import { EventsService, type ClusterInfo } from '@tracehouse/core';
import { useClusterStore } from '@frontend/stores/clusterStore';
import { useMonitoringCapabilitiesStore } from '@frontend/stores/monitoringCapabilitiesStore';
import { ServiceProvider, useServices } from './ServiceProvider';

const grafana = vi.hoisted(() => ({ search: '', partial: vi.fn() }));

vi.mock('@grafana/runtime', () => ({
  getDataSourceSrv: () => ({
    getList: () => [{ uid: 'clickhouse-test', name: 'ClickHouse', type: 'grafana-clickhouse-datasource' }],
  }),
  locationService: {
    getLocation: () => ({ search: grafana.search }),
    partial: grafana.partial,
  },
}));
vi.mock('@grafana/data', () => ({ dateTime: vi.fn() }));
vi.mock('./PluginConfigContext', () => ({ usePluginConfig: () => ({ cluster: undefined }) }));

const topology: ClusterInfo = {
  clusterName: 'default', replicaCount: 1, shardCount: 1,
  availableClusters: [
    { name: 'default', replicaCount: 1, shardCount: 1 },
    { name: 'other', replicaCount: 2, shardCount: 1 },
  ],
};

function ConnectionState() {
  const { services, isLoading, error } = useServices();
  const cluster = useClusterStore(state => state.clusterName);
  return <output>{error ?? (isLoading ? 'loading' : services ? `ready:${cluster ?? 'single'}` : 'disconnected')}</output>;
}

describe('Grafana ServiceProvider cluster lifecycle', () => {
  beforeEach(() => {
    const stored = new Map<string, string>();
    vi.stubGlobal('localStorage', {
      getItem: (key: string) => stored.get(key) ?? null,
      setItem: (key: string, value: string) => { stored.set(key, value); },
      removeItem: (key: string) => { stored.delete(key); },
      clear: () => stored.clear(),
    });
    localStorage.setItem('tracehouse-datasource', JSON.stringify({ uid: 'clickhouse-test', name: 'ClickHouse' }));
    grafana.search = '';
    grafana.partial.mockClear();
    useClusterStore.getState().reset();
    useMonitoringCapabilitiesStore.getState().reset();
    vi.spyOn(MonitoringCapabilitiesService.prototype, 'probe').mockResolvedValue({
      serverVersion: '26.9.7.9', probedAt: new Date(), capabilities: [],
    });
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    useClusterStore.getState().reset();
    useMonitoringCapabilitiesStore.getState().reset();
  });

  it('finishes cluster startup and resets event permissions through the Grafana services wrapper', async () => {
    let resolveDetection!: (info: ClusterInfo) => void;
    vi.spyOn(ClusterService, 'detect').mockImplementation(() => new Promise(resolve => { resolveDetection = resolve; }));
    const reset = vi.spyOn(EventsService.prototype, 'resetSourceAccess');
    const errors = vi.spyOn(console, 'error');
    render(<ServiceProvider><ConnectionState /></ServiceProvider>);
    expect(screen.getByText('loading')).toBeTruthy();

    await act(async () => { resolveDetection(topology); });

    await waitFor(() => expect(screen.getByText('ready:default')).toBeTruthy());
    expect(reset).toHaveBeenCalled();
    expect(errors).not.toHaveBeenCalled();

    const beforeSwitch = reset.mock.calls.length;
    act(() => useClusterStore.getState().switchCluster('other'));
    expect(screen.getByText('ready:other')).toBeTruthy();
    expect(reset.mock.calls.length).toBeGreaterThan(beforeSwitch);
    expect(grafana.partial).toHaveBeenLastCalledWith(expect.objectContaining({ th_cluster: 'other' }), true);
  });

  it('restores a shared named cluster and rechecks permissions after capability refresh', async () => {
    grafana.search = '?th_ds=clickhouse-test&th_cluster=other';
    vi.spyOn(ClusterService, 'detect').mockResolvedValue(topology);
    const reset = vi.spyOn(EventsService.prototype, 'resetSourceAccess');
    render(<ServiceProvider><ConnectionState /></ServiceProvider>);
    await waitFor(() => expect(screen.getByText('ready:other')).toBeTruthy());
    expect(grafana.partial).not.toHaveBeenCalled();

    const beforeRefresh = reset.mock.calls.length;
    act(() => useMonitoringCapabilitiesStore.getState().setCapabilities({
      serverVersion: '26.9.7.9', probedAt: new Date(), capabilities: [],
    }));
    expect(reset.mock.calls.length).toBeGreaterThan(beforeRefresh);
    expect(screen.getByText('ready:other')).toBeTruthy();
  });
});
