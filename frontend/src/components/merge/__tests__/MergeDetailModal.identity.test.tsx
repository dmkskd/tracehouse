import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { MergeSeries } from '@tracehouse/core';

const mocks = vi.hoisted(() => ({ getMergeHistoryByPartName: vi.fn() }));
vi.mock('../../../providers/ClickHouseProvider', () => ({
  useClickHouseServices: () => services,
}));
const services = { mergeTracker: mocks };
vi.mock('../../shared/ModalWrapper', () => ({
  ModalWrapper: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}));

import { MergeDetailModal, MutationDetailModal } from '../MergeDetailModal';

const failed: MergeSeries = {
  part_name: '1794492000_202_207_1', table: 'web_analytics.pageviews_local',
  hostname: '38a1be6f8d5f', error: 241,
  event_time_microseconds: '2026-11-12 14:48:06.684123Z',
  // Visible bounds may be clipped by the timeline window.
  start_time: '2026-11-12T14:48:02Z', end_time: '2026-11-12T14:48:04Z',
  duration_ms: 7150, peak_memory: 0, cpu_us: 0,
  net_send: 0, net_recv: 0, disk_read: 0, disk_write: 0,
};

beforeEach(() => { mocks.getMergeHistoryByPartName.mockReset().mockResolvedValue(null); });

describe('Time Travel merge detail identity', () => {
  it('selects the failed attempt with its original completion time and host', async () => {
    render(<MergeDetailModal merge={failed} onClose={() => {}} />);
    await waitFor(() => expect(mocks.getMergeHistoryByPartName).toHaveBeenCalledWith(
      'web_analytics', 'pageviews_local', failed.part_name,
      {
        hostname: failed.hostname, event_type: 'MergeParts',
        event_time: failed.event_time_microseconds,
        event_time_microseconds: failed.event_time_microseconds,
      },
    ));
  });

  it('reloads when another attempt or replica has the same part name', async () => {
    const { rerender } = render(<MergeDetailModal merge={failed} onClose={() => {}} />);
    await waitFor(() => expect(mocks.getMergeHistoryByPartName).toHaveBeenCalledTimes(1));
    rerender(<MergeDetailModal merge={{ ...failed, error: 0, event_time_microseconds: '2026-11-12 14:48:14.983000Z' }} onClose={() => {}} />);
    await waitFor(() => expect(mocks.getMergeHistoryByPartName).toHaveBeenCalledTimes(2));
    rerender(<MergeDetailModal merge={{ ...failed, hostname: 'other-replica' }} onClose={() => {}} />);
    await waitFor(() => expect(mocks.getMergeHistoryByPartName).toHaveBeenCalledTimes(3));
  });

  it('uses the mutation event type for mutation attempts', async () => {
    render(<MutationDetailModal mutation={failed} onClose={() => {}} />);
    await waitFor(() => expect(mocks.getMergeHistoryByPartName).toHaveBeenCalledWith(
      'web_analytics', 'pageviews_local', failed.part_name,
      expect.objectContaining({ event_type: 'MutatePart', hostname: failed.hostname }),
    ));
  });
});
