import { QueryXRayDefaultContext } from '../../query/query-xray-preference';
import { act, cleanup, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import { useMonitoringCapabilitiesStore } from '../../../stores/monitoringCapabilitiesStore';
import { DashboardViewer } from '../DashboardViewer';

const mocks = vi.hoisted(() => ({
  run: vi.fn().mockResolvedValue([]),
}));

const storedValues = new Map<string, string>();
const localStorageMock: Storage = {
  get length() {
    return storedValues.size;
  },
  clear: () => storedValues.clear(),
  getItem: key => storedValues.get(key) ?? null,
  key: index => Array.from(storedValues.keys())[index] ?? null,
  removeItem: key => {
    storedValues.delete(key);
  },
  setItem: (key, value) => {
    storedValues.set(key, value);
  },
};
vi.stubGlobal('localStorage', localStorageMock);

vi.mock('../../../providers/ClickHouseProvider', () => {
  const services = { interactiveQueryService: { run: mocks.run } };
  return { useClickHouseServices: () => services };
});

function setServerVersion(serverVersion: string): void {
  act(() => {
    useMonitoringCapabilitiesStore.getState().setCapabilities({
      probedAt: new Date('2026-07-30T00:00:00Z'),
      serverVersion,
      capabilities: [],
    });
  });
}

describe('DashboardViewer query version compatibility', () => {
  beforeEach(() => {
    localStorage.clear();
    mocks.run.mockClear();
  });

  afterEach(() => {
    cleanup();
    useMonitoringCapabilitiesStore.getState().reset();
  });

  test('shows the compatibility reason and does not execute an unsupported panel', async () => {
    setServerVersion('24.3.18.7');

    render(
      <MemoryRouter>
        <DashboardViewer initialDashboardId="cloud-monitoring" />
      </MemoryRouter>,
    );

    expect(await screen.findByText(
      'Not run · Requires ClickHouse ≥ 24.8 · connected server 24.3.18.7',
    )).toBeInTheDocument();

    await waitFor(() => expect(mocks.run).toHaveBeenCalled());
    expect(
      mocks.run.mock.calls.some(([sql]) => (
        typeof sql === 'string' && sql.includes('ProfileEvent_MergeTotalMilliseconds')
      )),
    ).toBe(false);
  });

  test('executes the panel at its minimum supported version', async () => {
    setServerVersion('24.8.14.39');

    render(
      <MemoryRouter>
        <DashboardViewer initialDashboardId="cloud-monitoring" />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(
        mocks.run.mock.calls.some(([sql]) => (
          typeof sql === 'string' && sql.includes('ProfileEvent_MergeTotalMilliseconds')
        )),
      ).toBe(true);
    });

    expect(screen.queryByText(/Not run · Requires ClickHouse ≥ 24\.8/)).not.toBeInTheDocument();
  });
});


describe('Series dashboard source preference', () => {
  afterEach(() => { cleanup(); useMonitoringCapabilitiesStore.getState().reset(); });

  test('reruns supported panels when the runtime default changes and explains unavailable progress bytes', async () => {
    mocks.run.mockClear();
    useMonitoringCapabilitiesStore.setState(state => ({
      probeStatus: 'done',
      flags: { ...state.flags, hasProcessesHistory: true, hasQueryMetricLogXRay: true },
    }));
    const { rerender } = render(
      <MemoryRouter><QueryXRayDefaultContext.Provider value="processes_history">
        <DashboardViewer initialDashboardId="xray" />
      </QueryXRayDefaultContext.Provider></MemoryRouter>,
    );
    await waitFor(() => expect(mocks.run.mock.calls.filter(([sql]) => sql.includes('tracehouse.processes_history'))).toHaveLength(4));
    mocks.run.mockClear();
    rerender(
      <MemoryRouter><QueryXRayDefaultContext.Provider value="query_metric_log">
        <DashboardViewer initialDashboardId="xray" />
      </QueryXRayDefaultContext.Provider></MemoryRouter>,
    );
    await waitFor(() => expect(mocks.run.mock.calls.filter(([sql]) => sql.includes('system.query_metric_log'))).toHaveLength(3));
    expect(mocks.run.mock.calls.every(([sql]) => !sql.includes('{{query_xray_overlay:'))).toBe(true);
    expect(await screen.findByText(/Sampled read_bytes is unavailable/)).toBeInTheDocument();
  });
});
