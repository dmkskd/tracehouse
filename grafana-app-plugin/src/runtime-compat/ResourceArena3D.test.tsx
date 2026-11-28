// @vitest-environment jsdom
import React from 'react';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { ClickHouseContext, type ClickHouseServices } from '@tracehouse/ui-shared';
import { useMonitoringCapabilitiesStore } from '@frontend/stores/monitoringCapabilitiesStore';
import { useRefreshSettingsStore } from '@frontend/stores/refreshSettingsStore';
import { ResourceArena3D } from './ResourceArena3D';
import type { ResourceArena3DProps } from '@frontend/components/overview/ResourceArena3D';

// The separately loaded scene cannot read the main bundle's contexts or stores.
vi.mock('./runtimeComponent', () => ({
  createRuntimeComponent: () => ({ eventState }: ResourceArena3DProps) => (
    <button onClick={() => eventState!.onVisibilityChange(!eventState!.visible)}>
      {eventState!.visible ? eventState!.data.events.map(event => event.title).join(',') : 'hidden'}
    </button>
  ),
}));

beforeEach(() => {
  const stored = new Map<string, string>();
  vi.stubGlobal('localStorage', {
    getItem: (key: string) => stored.get(key) ?? null,
    setItem: (key: string, value: string) => { stored.set(key, value); },
    removeItem: (key: string) => { stored.delete(key); },
  });
});

afterEach(() => {
  cleanup();
  useMonitoringCapabilitiesStore.getState().reset();
  useRefreshSettingsStore.setState({ refreshRateSeconds: 5 });
  vi.unstubAllGlobals();
});

it('collects arena events in the host bundle and forwards visibility changes', async () => {
  useRefreshSettingsStore.setState({ refreshRateSeconds: 0 });
  useMonitoringCapabilitiesStore.setState({
    capabilities: { capabilities: [{ id: 'query_log', available: true }] } as never,
  });
  const getEvents = vi.fn().mockResolvedValue({
    events: [{ id: 'oom', title: 'Query OOM' }], coverage: [],
  });
  render(
    <ClickHouseContext.Provider value={{ eventsService: { getEvents } } as unknown as ClickHouseServices}>
      <ResourceArena3D queries={[]} merges={[]} cpuUsage={0} memoryPct={0} hosts={['node-01']} />
    </ClickHouseContext.Provider>,
  );
  expect(await screen.findByText('Query OOM')).toBeTruthy();
  expect(getEvents).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({
    hostname: ['node-01'], availableCapabilities: ['query_log'], origin: 'overview',
  }));
  fireEvent.click(screen.getByRole('button'));
  expect(screen.getByText('hidden')).toBeTruthy();
  fireEvent.click(screen.getByRole('button'));
  await waitFor(() => expect(getEvents).toHaveBeenCalledTimes(2));
  expect(await screen.findByText('Query OOM')).toBeTruthy();
});
