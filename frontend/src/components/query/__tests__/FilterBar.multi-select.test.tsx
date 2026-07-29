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
});
