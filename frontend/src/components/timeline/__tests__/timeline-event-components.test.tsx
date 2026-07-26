import React, { act } from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import type { MemoryTimeline, OperationalEvent } from '@tracehouse/core';
import { TimelineEventControls } from '../TimelineEventControls';
import { TimelineEventOverlay } from '../TimelineEventOverlay';
import { TimelineChart } from '../TimelineChart';
import { emptyTimelineEventFilter } from '../timeline-event-model';

const RANGE_START = Date.parse('2026-07-25T12:00:00.000Z');
const RANGE_END = Date.parse('2026-07-25T12:01:00.000Z');
const resizeObserverCallbacks: ResizeObserverCallback[] = [];

function event(
  id: string,
  occurredAt: string,
  title = id,
): OperationalEvent {
  return {
    id,
    occurred_at: occurredAt,
    kind: 'ddl',
    category: 'changes',
    severity: 'info',
    precision: 'exact',
    title,
    source: 'system.query_log',
    capability: 'query_log',
  };
}

beforeAll(() => {
  class ResizeObserverMock {
    constructor(callback: ResizeObserverCallback) {
      resizeObserverCallbacks.push(callback);
    }
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  vi.stubGlobal('ResizeObserver', ResizeObserverMock);
});

describe('TimelineEventControls', () => {
  it('keeps visibility separate from event filtering', () => {
    const onVisibilityChange = vi.fn();
    const onFilterChange = vi.fn();
    const events = [
      event('first', '2026-07-25T12:00:10.000Z'),
      event('second', '2026-07-25T12:00:20.000Z'),
    ];
    const view = render(
      <TimelineEventControls
        visible
        shownCount={1}
        totalCount={2}
        filterUniverse={events}
        coverage={[]}
        filter={emptyTimelineEventFilter()}
        onVisibilityChange={onVisibilityChange}
        onFilterChange={onFilterChange}
      />,
    );

    expect(screen.getByText('1/2')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Errors+' }));
    expect(onFilterChange).toHaveBeenCalledWith(expect.objectContaining({
      hiddenSeverities: new Set(['warning', 'info']),
    }));

    fireEvent.click(screen.getByRole('button', { name: 'Hide' }));
    expect(onVisibilityChange).toHaveBeenCalledWith(false);

    view.rerender(
      <TimelineEventControls
        visible={false}
        shownCount={1}
        totalCount={2}
        filterUniverse={events}
        coverage={[]}
        filter={emptyTimelineEventFilter()}
        onVisibilityChange={onVisibilityChange}
        onFilterChange={onFilterChange}
      />,
    );
    fireEvent.click(screen.getByRole('button', { name: /Events hidden/ }));
    expect(onVisibilityChange).toHaveBeenLastCalledWith(true);
  });
});

describe('TimelineEventOverlay', () => {
  it('selects a marker and presents the selected event actions', () => {
    const onSelectEvent = vi.fn();
    const onClearEventSelection = vi.fn();
    const onViewEventDetails = vi.fn();
    const sourceEvent = event(
      'ddl',
      '2026-07-25T12:00:20.000Z',
      'Table altered',
    );
    const view = render(
      <TimelineEventOverlay
        events={[sourceEvent]}
        rangeStartMs={RANGE_START}
        rangeEndMs={RANGE_END}
        onSelectEvent={onSelectEvent}
        onClearEventSelection={onClearEventSelection}
        onViewEventDetails={onViewEventDetails}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Table altered' }));
    expect(onSelectEvent).toHaveBeenCalledWith(sourceEvent);

    view.rerender(
      <TimelineEventOverlay
        events={[sourceEvent]}
        rangeStartMs={RANGE_START}
        rangeEndMs={RANGE_END}
        selectedEventId={sourceEvent.id}
        onSelectEvent={onSelectEvent}
        onClearEventSelection={onClearEventSelection}
        onViewEventDetails={onViewEventDetails}
      />,
    );
    expect(screen.getByText('Activity pinned at this time')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'View details →' }));
    expect(onViewEventDetails).toHaveBeenCalledWith(sourceEvent);
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(onClearEventSelection).toHaveBeenCalledOnce();
  });

  it('expands colliding markers before choosing an event', () => {
    const onSelectEvent = vi.fn();
    const events = [
      event('first', '2026-07-25T12:00:20.000Z', 'First DDL'),
      event('second', '2026-07-25T12:00:20.100Z', 'Second DDL'),
    ];
    render(
      <TimelineEventOverlay
        events={events}
        rangeStartMs={RANGE_START}
        rangeEndMs={RANGE_END}
        onSelectEvent={onSelectEvent}
      />,
    );

    fireEvent.click(screen.getByRole('button', { name: '2 events' }));
    expect(screen.getByText('2 events at this time')).toBeInTheDocument();
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(screen.queryByText('2 events at this time')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: '2 events' }));
    fireEvent.click(screen.getByRole('button', { name: /Second DDL/ }));
    expect(onSelectEvent).toHaveBeenCalledWith(events[1]);
  });
});

describe('TimelineChart event annotations', () => {
  it('shows an event inside the requested window before the first metric sample', () => {
    resizeObserverCallbacks.length = 0;
    const restart = event(
      'restart',
      '2026-07-25T12:00:05.000Z',
      'Server restart',
    );
    const data: MemoryTimeline = {
      window_start: '2026-07-25T12:00:00.000Z',
      window_end: '2026-07-25T12:01:00.000Z',
      target: '2026-07-25T12:00:30.000Z',
      // Simulate the metric_log gap caused by a restart.
      server_memory: [],
      server_cpu: [
        { t: '2026-07-25 12:00:10', v: 500_000 },
        { t: '2026-07-25 12:00:55', v: 500_000 },
      ],
      server_network_send: [],
      server_network_recv: [],
      server_disk_read: [],
      server_disk_write: [],
      server_total_ram: 0,
      cpu_cores: 1,
      host_count: 1,
      queries: [],
      merges: [],
      mutations: [],
      query_count: 0,
      merge_count: 0,
      merge_peak_total: 0,
      mutation_count: 0,
    };

    const view = render(
      <TimelineChart
        data={data}
        metricMode="cpu"
        hoverMs={null}
        pinnedMs={null}
        onHover={vi.fn()}
        onPin={vi.fn()}
        zoomRange={null}
        onZoom={vi.fn()}
        highlightedItem={null}
        eventAnnotations={[restart]}
        onEventSelect={vi.fn()}
      />,
    );

    expect(screen.getByRole('button', { name: 'Server restart' })).toBeInTheDocument();

    const chartContainer = view.container.firstElementChild as HTMLDivElement;
    Object.defineProperty(chartContainer, 'clientWidth', {
      configurable: true,
      value: 2000,
    });
    act(() => {
      resizeObserverCallbacks.forEach(callback =>
        callback([], {} as ResizeObserver));
    });

    const svg = view.container.querySelector('svg');
    const firstGridLine = svg?.querySelector('g line');
    expect(svg).toHaveAttribute('viewBox', '0 0 2000 380');
    expect(firstGridLine).toHaveAttribute('x1', '52');
    expect(firstGridLine).toHaveAttribute('x2', '1910');
  });
});
