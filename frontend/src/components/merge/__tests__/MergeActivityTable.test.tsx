import React, { useState } from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { expect, it } from 'vitest';
import type { MergeHistoryRecord, MergeHistorySort } from '../../../stores/mergeStore';
import { MergeActivityTable } from '../MergeActivityTable';
import { buildMergeActivityRecords } from '../merge-activity-model';

const failed = (duration: number): MergeHistoryRecord => ({
  event_time: '2026-11-06T18:04:59.000Z',
  event_type: 'MergeParts',
  database: 'default',
  table: 'events',
  hostname: 'node-1',
  part_name: 'all_1_2_1',
  partition_id: 'all',
  rows: 0,
  size_in_bytes: 0,
  duration_ms: duration,
  merge_reason: 'Regular',
  source_part_names: ['p1', 'p2'],
  bytes_uncompressed: 0,
  read_bytes: 0,
  read_rows: 0,
  peak_memory_usage: 0,
  size_diff: 0,
  size_diff_pct: 0,
  rows_diff: 0,
  error: 241,
});

it('keeps repeated failures sorted through successive duration and time sort changes', () => {
  // Retries of the same part can finish within the same timestamp second.
  const history = [81, 5, 134, 8250, 951, 14, 5].map((duration, index) => ({
    ...failed(duration),
    part_name: `all_${index % 3}_2_1`,
  }));
  const activity = buildMergeActivityRecords([], history);
  expect(new Set(activity.map(record => record.activityKey)).size).toBe(history.length);
  function Table() {
    const [sort, setSort] = useState<MergeHistorySort>({ field: 'event_time', direction: 'desc' });
    return <MergeActivityTable activity={activity} sort={sort} onSortChange={setSort}
      isLoading={false} onSelectLive={() => {}} onSelectHistory={() => {}} />;
  }
  render(<Table />);
  const durations = () => screen.getAllByRole('row').slice(1)
    .map(row => within(row).getAllByRole('cell')[7].textContent);

  for (let repeat = 0; repeat < 3; repeat++) {
    fireEvent.click(screen.getByRole('columnheader', { name: /Duration/ }));
    expect(durations()).toEqual(['8.25s', '951ms', '134ms', '81ms', '14ms', '5ms', '5ms']);
    fireEvent.click(screen.getByRole('columnheader', { name: /Duration/ }));
    expect(durations()).toEqual(['5ms', '5ms', '14ms', '81ms', '134ms', '951ms', '8.25s']);
    fireEvent.click(screen.getByRole('columnheader', { name: /Started/ }));
    expect(durations()).toEqual(['5ms', '5ms', '14ms', '81ms', '134ms', '951ms', '8.25s']);
    fireEvent.click(screen.getByRole('columnheader', { name: /Started/ }));
    expect(durations()).toEqual(['8.25s', '951ms', '134ms', '81ms', '14ms', '5ms', '5ms']);
  }
});
