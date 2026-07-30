import { act, cleanup, render, screen, waitFor } from '@testing-library/react';
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

vi.mock('../../../providers/ClickHouseProvider', () => ({
  useClickHouseServices: () => ({
    interactiveQueryService: {
      run: mocks.run,
    },
  }),
}));

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

    render(<DashboardViewer initialDashboardId="cloud-monitoring" />);

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

    render(<DashboardViewer initialDashboardId="cloud-monitoring" />);

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
