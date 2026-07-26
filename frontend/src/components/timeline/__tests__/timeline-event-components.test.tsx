import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import type { OperationalEvent } from '@tracehouse/core';
import { TimelineEventControls } from '../TimelineEventControls';
import { TimelineEventOverlay } from '../TimelineEventOverlay';
import { emptyTimelineEventFilter } from '../timeline-event-model';

const RANGE_START = Date.parse('2026-07-25T12:00:00.000Z');
const RANGE_END = Date.parse('2026-07-25T12:01:00.000Z');

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
