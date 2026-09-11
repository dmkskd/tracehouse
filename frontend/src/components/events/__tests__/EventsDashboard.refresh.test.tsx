import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { EventsDashboard } from '../EventsDashboard';

const mocks = vi.hoisted(() => {
  const getEvents = vi.fn();
  return {
    getEvents,
    services: {
      adapter: { executeQuery: vi.fn() },
      eventsService: {
        getEvents,
      },
    },
    monitoringCapabilities: {
      capabilities: [{ id: 'query_log', available: true }],
    },
    capabilityProbeStatus: 'done',
    refreshCapabilities: vi.fn(),
    refreshRateSeconds: 5,
    manualRefreshTick: 0,
    touch: vi.fn(),
    setStatus: vi.fn(),
  };
});

vi.mock('../../../providers/ClickHouseProvider', () => ({
  useClickHouseServices: () => mocks.services,
}));

vi.mock('../../../stores/monitoringCapabilitiesStore', () => ({
  useMonitoringCapabilitiesStore: (
    selector: (state: {
      capabilities: {
        capabilities: Array<{ id: string; available: boolean }>;
      };
      probeStatus: string;
      refresh: typeof mocks.refreshCapabilities;
    }) => unknown,
  ) => selector({
    capabilities: mocks.monitoringCapabilities,
    probeStatus: mocks.capabilityProbeStatus,
    refresh: mocks.refreshCapabilities,
  }),
}));

vi.mock('../../../stores/refreshSettingsStore', () => ({
  useRefreshSettingsStore: (
    selector: (state: { refreshRateSeconds: number }) => unknown,
  ) => selector({ refreshRateSeconds: mocks.refreshRateSeconds }),
  useGlobalLastUpdatedStore: (
    selector: (state: {
      manualRefreshTick: number;
      touch: () => void;
      setStatus: (status: 'idle' | 'polling' | 'error') => void;
    }) => unknown,
  ) => selector({
    manualRefreshTick: mocks.manualRefreshTick,
    touch: mocks.touch,
    setStatus: mocks.setStatus,
  }),
}));

vi.mock('../EventDistribution', () => ({
  EventDistribution: () => <div data-testid="event-distribution" />,
}));

vi.mock('../../common/TimeRangePicker', () => ({
  TimeRangePicker: () => <div data-testid="time-range-picker" />,
}));

vi.mock('../../common/DocsLink', () => ({
  DocsLink: () => null,
}));

/**
 * The dashboard's filter and toggle state lives in the Events page, not in the
 * component, so the test owns it too. Without this the component renders with
 * `search` undefined and the UI toggles it exposes have nothing to write to.
 */
type DashboardProps = React.ComponentProps<typeof EventsDashboard>;

const Harness: React.FC<Partial<DashboardProps>> = (overrides) => {
  const [search, setSearch] = React.useState('');
  const [severity, setSeverity] = React.useState<React.ComponentProps<typeof EventsDashboard>['severity']>('all');
  const [category, setCategory] = React.useState<React.ComponentProps<typeof EventsDashboard>['category']>('all');
  const [kind, setKind] = React.useState<React.ComponentProps<typeof EventsDashboard>['kind']>('all');
  const [autoRefresh, setAutoRefresh] = React.useState(false);
  const [groupSimilarEvents, setGroupSimilarEvents] = React.useState(true);
  const [selectedPanel, setSelectedPanel] = React.useState<'details' | 'context'>('details');
  return (
    <EventsDashboard
      rangeHours={24}
      timeRangeValue="1 DAY"
      onTimeRangeChange={vi.fn()}
      onSelectEvent={vi.fn()}
      search={search}
      onSearchChange={setSearch}
      severity={severity}
      onSeverityChange={setSeverity}
      category={category}
      onCategoryChange={setCategory}
      kind={kind}
      onKindChange={setKind}
      autoRefresh={autoRefresh}
      onAutoRefreshChange={setAutoRefresh}
      groupSimilarEvents={groupSimilarEvents}
      onGroupSimilarEventsChange={setGroupSimilarEvents}
      selectedPanel={selectedPanel}
      onSelectedPanelChange={setSelectedPanel}
      {...overrides}
    />
  );
};

function dashboard(rangeCenterTime?: string) {
  return <Harness rangeCenterTime={rangeCenterTime} />;
}

describe('EventsDashboard refresh behavior', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-26T12:00:00.000Z'));
    mocks.refreshRateSeconds = 5;
    mocks.manualRefreshTick = 0;
    mocks.getEvents.mockReset().mockResolvedValue({ events: [], coverage: [] });
    mocks.capabilityProbeStatus = 'done';
    mocks.refreshCapabilities.mockReset().mockResolvedValue(
      mocks.monitoringCapabilities,
    );
    mocks.touch.mockReset();
    mocks.setStatus.mockReset();
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it('polls an opted-in live range at the Events minimum and advances its query window', async () => {
    await act(async () => {
      render(dashboard());
    });

    expect(mocks.getEvents).toHaveBeenCalledTimes(1);
    expect(mocks.getEvents.mock.calls[0][0].endTime).toBe('2026-07-26T12:00:00.000Z');
    expect(mocks.getEvents.mock.calls[0][0].limit).toBe(100);
    expect(mocks.touch).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole('button', { name: 'Auto' }));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(mocks.getEvents).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(mocks.getEvents).toHaveBeenCalledTimes(2);
    expect(mocks.getEvents.mock.calls[1][0].endTime).toBe('2026-07-26T12:00:10.000Z');
    expect(mocks.touch).toHaveBeenCalledTimes(2);
  });

  it('keeps anchored ranges manual-only and responds to the header refresh tick', async () => {
    const view = render(dashboard('2026-07-25T12:00:00.000Z'));
    await act(async () => {});

    expect(mocks.getEvents).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('button', { name: 'Auto' })).toBeDisabled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(mocks.getEvents).toHaveBeenCalledTimes(1);

    mocks.manualRefreshTick += 1;
    await act(async () => {
      view.rerender(dashboard('2026-07-25T12:00:00.000Z'));
    });

    expect(mocks.getEvents).toHaveBeenCalledTimes(2);
  });

  it('re-probes event source capabilities on manual page refresh', async () => {
    await act(async () => {
      render(dashboard());
    });

    fireEvent.click(screen.getByRole('button', { name: '↻ Refresh' }));

    expect(mocks.refreshCapabilities).toHaveBeenCalledWith(mocks.services.adapter);
  });

  it('opens source status directly from the truncation indicator', async () => {
    mocks.getEvents.mockResolvedValueOnce({
      events: [],
      coverage: [{
        source: 'system.query_log',
        capability: 'query_log',
        status: 'loaded',
        event_count: 100,
        truncated: true,
      }],
    });

    await act(async () => {
      render(dashboard());
    });

    fireEvent.click(screen.getByRole('button', {
      name: '1 source truncated. View source status.',
    }));

    expect(screen.getByRole('button', { name: 'Source status' })).toHaveStyle({
      color: '#58a6ff',
    });
    expect(screen.getByText('truncated · 100')).toBeInTheDocument();
  });

  it('offers merge details for a failed mutation part-log event', async () => {
    const mutationFailure = {
      id: 'mutation-failure',
      occurred_at: '2026-07-31T20:14:33.880Z',
      kind: 'mutation_failure' as const,
      category: 'merges' as const,
      severity: 'error' as const,
      precision: 'exact' as const,
      title: 'Mutation failed · analytics.events',
      source: 'system.part_log',
      capability: 'part_log',
      hostname: 'ch-1',
      database: 'analytics',
      table: 'events',
      part_name: 'all_1_1_0_2',
      operation: 'MutatePart',
    };
    const onOpenMergeDetails = vi.fn();
    mocks.getEvents.mockResolvedValueOnce({ events: [mutationFailure], coverage: [] });

    await act(async () => {
      render(
        <Harness
          selectedEventId={mutationFailure.id}
          onOpenMergeDetails={onOpenMergeDetails}
        />,
      );
    });

    fireEvent.click(screen.getByRole('button', { name: 'View Merge Details ↗' }));
    expect(onOpenMergeDetails).toHaveBeenCalledWith(mutationFailure);
  });

  it('collapses similar bursts and keeps the individual events expandable', async () => {
    mocks.getEvents.mockResolvedValueOnce({
      events: [
        {
          id: 'ddl-3',
          occurred_at: '2026-07-26T11:59:59.300Z',
          kind: 'ddl',
          category: 'changes',
          severity: 'info',
          precision: 'exact',
          title: 'DDL · Create',
          hostname: 'ch-1',
          source: 'system.query_log',
          capability: 'query_log',
        },
        {
          id: 'ddl-2',
          occurred_at: '2026-07-26T11:59:59.200Z',
          kind: 'ddl',
          category: 'changes',
          severity: 'info',
          precision: 'exact',
          title: 'DDL · Create',
          hostname: 'ch-1',
          source: 'system.query_log',
          capability: 'query_log',
        },
        {
          id: 'ddl-1',
          occurred_at: '2026-07-26T11:59:59.100Z',
          kind: 'ddl',
          category: 'changes',
          severity: 'info',
          precision: 'exact',
          title: 'DDL · Create',
          hostname: 'ch-1',
          source: 'system.query_log',
          capability: 'query_log',
        },
      ],
      coverage: [],
    });

    await act(async () => {
      render(dashboard());
    });

    const cluster = screen.getByRole('button', {
      name: 'Expand 3 similar events: DDL · Create',
    });
    expect(cluster).toHaveAttribute('aria-expanded', 'false');

    fireEvent.click(cluster);

    expect(screen.getByRole('button', {
      name: 'Collapse 3 similar events: DDL · Create',
    })).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getAllByRole('button', {
      name: /^DDL · Create/,
    })).toHaveLength(3);
  });
});
