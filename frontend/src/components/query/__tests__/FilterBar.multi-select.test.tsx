import React, { useState } from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { QueryFilterBar, type QueryFilterState } from '../QueryFilterBar';
import { MergeFilterBar } from '../../merge/MergeFilterBar';
import type { MergeHistoryFilter } from '../../../stores/mergeStore';

vi.mock('../../common/TimeRangePicker', () => ({
  TimeRangePicker: () => <div data-testid="time-range-picker" />,
}));

afterEach(cleanup);

describe('filter bar multi-value autocomplete', () => {
  it('distinguishes prepared quick filters from field filters', () => {
    const changes = vi.fn();

    function Harness() {
      const [filter, setFilter] = useState<QueryFilterState>({ timeRange: '1 HOUR' });
      return (
        <QueryFilterBar
          filter={filter}
          onFilterChange={patch => {
            changes(patch);
            setFilter(current => ({ ...current, ...patch }));
          }}
        />
      );
    }

    render(<Harness />);

    fireEvent.focus(screen.getByPlaceholderText('Type to filter (user, server, query…)'));

    expect(screen.getByText('Quick filters')).toBeInTheDocument();
    expect(screen.getByText('Add a filter')).toBeInTheDocument();
    expect(screen.getByText('Currently executing')).toBeInTheDocument();
    expect(screen.getByText('Completed in the selected time range')).toBeInTheDocument();
    expect(screen.getByText('Errors in the selected time range')).toBeInTheDocument();
    expect(screen.getByText('Duration ≥ 1 second')).toBeInTheDocument();

    fireEvent.mouseDown(screen.getByText('Running now'));

    expect(changes).toHaveBeenLastCalledWith({
      quickFilter: 'running',
      status: ['running'],
      minDurationMs: undefined,
    });
    expect(screen.getByText('Running now')).toBeInTheDocument();
    expect(screen.queryByText('Status:')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Remove quick filter Running now' }));

    expect(changes).toHaveBeenLastCalledWith({
      quickFilter: undefined,
      status: undefined,
      minDurationMs: undefined,
    });
  });

  it.each([
    ['Recent', { quickFilter: 'recent', status: ['success', 'error'], minDurationMs: undefined }],
    ['Failed queries', { quickFilter: 'failed', status: ['error'], minDurationMs: undefined }],
    ['Slow queries', { quickFilter: 'slow', status: undefined, minDurationMs: 1_000 }],
  ])('applies the %s prepared filter through existing query criteria', (label, expectedPatch) => {
    const changes = vi.fn();

    render(
      <QueryFilterBar
        filter={{ timeRange: '1 HOUR' }}
        onFilterChange={changes}
      />,
    );

    fireEvent.focus(screen.getByPlaceholderText('Type to filter (user, server, query…)'));
    fireEvent.mouseDown(screen.getByText(label));

    expect(changes).toHaveBeenLastCalledWith(expectedPatch);
  });

  it('keeps Query Status active and suggests only the remaining values', () => {
    const changes = vi.fn();

    function Harness() {
      const [filter, setFilter] = useState<QueryFilterState>({ timeRange: '1 HOUR' });
      return (
        <QueryFilterBar
          filter={filter}
          onFilterChange={patch => {
            changes(patch);
            setFilter(current => ({ ...current, ...patch }));
          }}
        />
      );
    }

    render(<Harness />);

    fireEvent.focus(screen.getByPlaceholderText('Type to filter (user, server, query…)'));
    fireEvent.mouseDown(screen.getByText('Status'));
    fireEvent.mouseDown(screen.getByText('running'));

    expect(changes).toHaveBeenLastCalledWith({ status: ['running'] });
    expect(screen.getByPlaceholderText('Add another status…')).toBeInTheDocument();
    expect(screen.getByText('success')).toBeInTheDocument();
    expect(screen.getByText('error')).toBeInTheDocument();
    expect(screen.getAllByText('running')).toHaveLength(1);

    fireEvent.mouseDown(screen.getByText('error'));

    expect(changes).toHaveBeenLastCalledWith({ status: ['running', 'error'] });
    expect(screen.getByPlaceholderText('Add another status…')).toBeInTheDocument();
    expect(screen.getByText('success')).toBeInTheDocument();
    expect(screen.getAllByText('error')).toHaveLength(1);
    expect(screen.getAllByText('Status:')).toHaveLength(1);

    fireEvent.click(screen.getByRole('button', { name: 'Remove Status running' }));

    expect(changes).toHaveBeenLastCalledWith({ status: ['error'] });
    expect(screen.queryByRole('button', { name: 'Remove Status running' })).not.toBeInTheDocument();
    expect(screen.getByText('running')).toBeInTheDocument();
  });

  it('offers error codes from the current results under Status: error', () => {
    const changes = vi.fn();

    function Harness() {
      const [filter, setFilter] = useState<QueryFilterState>({
        timeRange: '1 HOUR',
        status: ['error'],
      });
      return (
        <QueryFilterBar
          filter={filter}
          errorCodeSuggestions={[
            { code: 394, label: 'Code 394 · QUERY_WAS_CANCELLED (4)' },
            { code: 60, label: 'Code 60 · UNKNOWN_TABLE (1)' },
          ]}
          onFilterChange={patch => {
            changes(patch);
            setFilter(current => ({ ...current, ...patch }));
          }}
        />
      );
    }

    render(<Harness />);

    fireEvent.click(screen.getByText('Status:'));

    expect(screen.getByText('Error codes in results')).toBeInTheDocument();
    expect(screen.getByText('Code 394 · QUERY_WAS_CANCELLED (4)')).toBeInTheDocument();
    expect(screen.getByText('Code 60 · UNKNOWN_TABLE (1)')).toBeInTheDocument();

    fireEvent.mouseDown(screen.getByText('Code 394 · QUERY_WAS_CANCELLED (4)'));

    expect(changes).toHaveBeenLastCalledWith({ exceptionCode: [394] });
    expect(screen.getByText('Error code:')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Remove Error code 394' })).toBeInTheDocument();
  });

  it('offers error codes on demand after applying the Failed queries quick filter', () => {
    const changes = vi.fn();

    function Harness() {
      const [filter, setFilter] = useState<QueryFilterState>({ timeRange: '1 HOUR' });
      return (
        <QueryFilterBar
          filter={filter}
          errorCodeSuggestions={[
            { code: 160, label: 'Code 160 · TOO_SLOW (23)' },
            { code: 394, label: 'Code 394 · QUERY_WAS_CANCELLED (5)' },
          ]}
          onFilterChange={patch => {
            changes(patch);
            setFilter(current => ({ ...current, ...patch }));
          }}
        />
      );
    }

    render(<Harness />);

    fireEvent.focus(screen.getByPlaceholderText('Type to filter (user, server, query…)'));
    fireEvent.mouseDown(screen.getByText('Failed queries'));

    expect(changes).toHaveBeenLastCalledWith({
      quickFilter: 'failed',
      status: ['error'],
      minDurationMs: undefined,
    });
    expect(screen.queryByText('Error codes in results')).not.toBeInTheDocument();
    expect(screen.getByPlaceholderText('Add filter…')).toBeInTheDocument();

    const refineButton = screen.getByRole('button', { name: '+ Error code' });
    expect(fireEvent.mouseDown(refineButton)).toBe(false);
    fireEvent.click(refineButton);

    expect(screen.getByText('Error codes in results')).toBeInTheDocument();
    expect(screen.getByText('Code 160 · TOO_SLOW (23)')).toBeInTheDocument();

    fireEvent.mouseDown(screen.getByText('Code 160 · TOO_SLOW (23)'));

    expect(changes).toHaveBeenLastCalledWith({ exceptionCode: [160] });
    expect(screen.getByText('Failed queries')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Remove Error code 160' })).toBeInTheDocument();
  });

  it('keeps Merge Status active and suggests only the remaining values', () => {
    const changes = vi.fn();

    function Harness() {
      const [filter, setFilter] = useState<MergeHistoryFilter>({
        timeRange: '1 HOUR',
        limit: 100,
      });
      const [status, setStatus] = useState<string[]>();

      return (
        <MergeFilterBar
          tab="merges"
          filter={filter}
          onFilterChange={patch => setFilter(current => ({ ...current, ...patch }))}
          availableDatabases={[]}
          availableTables={[]}
          availableStatuses={['Running', 'OK', 'Error']}
          selectedStatus={status}
          onStatusChange={next => {
            changes(next);
            setStatus(next);
          }}
        />
      );
    }

    render(<Harness />);

    fireEvent.focus(screen.getByPlaceholderText('Type to filter (database, table…)'));
    fireEvent.mouseDown(screen.getByText('Status'));
    fireEvent.mouseDown(screen.getByText('Running'));

    expect(changes).toHaveBeenLastCalledWith(['Running']);
    expect(screen.getByPlaceholderText('Add another status…')).toBeInTheDocument();
    expect(screen.getByText('OK')).toBeInTheDocument();
    expect(screen.getByText('Error')).toBeInTheDocument();
    expect(screen.getAllByText('Running')).toHaveLength(1);

    fireEvent.mouseDown(screen.getByText('Error'));

    expect(changes).toHaveBeenLastCalledWith(['Running', 'Error']);
    expect(screen.getByPlaceholderText('Add another status…')).toBeInTheDocument();
    expect(screen.getByText('OK')).toBeInTheDocument();
    expect(screen.getAllByText('Error')).toHaveLength(1);
    expect(screen.getAllByText('Status:')).toHaveLength(1);

    fireEvent.click(screen.getByRole('button', { name: 'Remove Status Running' }));

    expect(changes).toHaveBeenLastCalledWith(['Error']);
    expect(screen.queryByRole('button', { name: 'Remove Status Running' })).not.toBeInTheDocument();
    expect(screen.getByText('Running')).toBeInTheDocument();
  });

  it('distinguishes prepared merge quick filters from field filters', () => {
    const quickChanges = vi.fn();

    function Harness() {
      const [filter, setFilter] = useState<MergeHistoryFilter>({
        timeRange: '1 HOUR',
        limit: 100,
      });
      const [status, setStatus] = useState<string[]>();
      const [quickFilter, setQuickFilter] = useState<'running' | 'recent' | 'failed' | 'slow'>();

      return (
        <MergeFilterBar
          tab="merges"
          filter={filter}
          onFilterChange={patch => setFilter(current => ({ ...current, ...patch }))}
          availableDatabases={[]}
          availableTables={[]}
          availableStatuses={['Running', 'OK', 'Error']}
          selectedStatus={status}
          onStatusChange={setStatus}
          quickFilter={quickFilter}
          onQuickFilterChange={(next, constraints) => {
            quickChanges(next, constraints);
            setQuickFilter(next);
            setStatus(constraints.status);
            setFilter(current => ({
              ...current,
              minDurationMs: constraints.minDurationMs,
            }));
          }}
        />
      );
    }

    render(<Harness />);

    fireEvent.focus(screen.getByPlaceholderText('Type to filter (database, table…)'));

    expect(screen.getByText('Quick filters')).toBeInTheDocument();
    expect(screen.getByText('Add a filter')).toBeInTheDocument();
    expect(screen.getByText('Currently executing')).toBeInTheDocument();
    expect(screen.getByText('Completed in the selected time range')).toBeInTheDocument();
    expect(screen.getByText('Errors in the selected time range')).toBeInTheDocument();
    expect(screen.getByText('Duration ≥ 10 seconds')).toBeInTheDocument();

    fireEvent.mouseDown(screen.getByText('Running now'));

    expect(quickChanges).toHaveBeenLastCalledWith('running', {
      status: ['Running'],
      minDurationMs: undefined,
    });
    expect(screen.getByText('Running now')).toBeInTheDocument();
    expect(screen.queryByText('Status:')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Remove quick filter Running now' }));

    expect(quickChanges).toHaveBeenLastCalledWith(undefined, {
      status: undefined,
      minDurationMs: undefined,
    });
  });

  it.each([
    ['Recent', 'recent', ['OK', 'Error'], undefined],
    ['Failed merges', 'failed', ['Error'], undefined],
    ['Slow merges', 'slow', undefined, 10_000],
  ])('applies the %s prepared merge filter through existing criteria', (
    label,
    quickFilter,
    status,
    minDurationMs,
  ) => {
    const quickChanges = vi.fn();

    render(
      <MergeFilterBar
        tab="merges"
        filter={{ timeRange: '1 HOUR', limit: 100 }}
        onFilterChange={() => {}}
        availableDatabases={[]}
        availableTables={[]}
        onQuickFilterChange={quickChanges}
      />,
    );

    fireEvent.focus(screen.getByPlaceholderText('Type to filter (database, table…)'));
    fireEvent.mouseDown(screen.getByText(label));

    expect(quickChanges).toHaveBeenLastCalledWith(quickFilter, {
      status,
      minDurationMs,
    });
  });

  it('turns a merge preset into ordinary filters when its constraints are edited', () => {
    const quickChanges = vi.fn();
    const statusChanges = vi.fn();

    function Harness() {
      const [status, setStatus] = useState<string[]>(['Running']);
      const [quickFilter, setQuickFilter] = useState<'running' | 'recent' | 'failed' | 'slow'>('running');

      return (
        <MergeFilterBar
          tab="merges"
          filter={{ timeRange: '1 HOUR', limit: 100 }}
          onFilterChange={() => {}}
          availableDatabases={[]}
          availableTables={[]}
          availableStatuses={['Running', 'OK', 'Error']}
          selectedStatus={status}
          onStatusChange={next => {
            statusChanges(next);
            setStatus(next);
            setQuickFilter(undefined);
          }}
          quickFilter={quickFilter}
          onQuickFilterChange={(next, constraints) => {
            quickChanges(next, constraints);
            setQuickFilter(next);
            setStatus(constraints.status);
          }}
        />
      );
    }

    render(<Harness />);

    fireEvent.focus(screen.getByPlaceholderText('Add filter…'));
    fireEvent.mouseDown(screen.getByText('Status'));
    fireEvent.mouseDown(screen.getByText('Error'));

    expect(statusChanges).toHaveBeenLastCalledWith(['Running', 'Error']);
    expect(quickChanges).not.toHaveBeenCalled();
    expect(screen.queryByText('Running now')).not.toBeInTheDocument();
    expect(screen.getByText('Status:')).toBeInTheDocument();
  });

  it('keeps the Hide replicas control visible without detected replica rows', () => {
    const onHideReplicaMergesChange = vi.fn();

    render(
      <MergeFilterBar
        tab="merges"
        filter={{ timeRange: '1 HOUR', limit: 100 }}
        onFilterChange={() => {}}
        availableDatabases={[]}
        availableTables={[]}
        hideReplicaMerges={false}
        onHideReplicaMergesChange={onHideReplicaMergesChange}
      />,
    );

    fireEvent.click(screen.getByRole('checkbox', { name: 'Hide replicas' }));

    expect(onHideReplicaMergesChange).toHaveBeenCalledWith(true);
  });
});
