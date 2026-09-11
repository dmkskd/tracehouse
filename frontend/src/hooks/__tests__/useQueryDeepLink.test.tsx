/**
 * Regression cover for the deep-link fetch race against cluster detection.
 *
 * ClusterAwareAdapter starts with clusterName === null, so a qd_id fetch that
 * fires before ClusterService.detect() resolves queries the local node only and
 * finds nothing for a query that ran on another replica. The hook must treat
 * that miss as retryable and re-issue the fetch once the cluster name lands.
 */

import type { PropsWithChildren } from 'react';
import { act, renderHook, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  ClickHouseContext,
  type ClickHouseServices,
} from '@tracehouse/ui-shared';
import { useQueryDeepLink } from '../useQueryDeepLink';
import { useClusterStore } from '../../stores/clusterStore';

const QD_ID = '780e3ee5-6b78-405b-af02-58f50cb4db08';

const detail = {
  query_id: QD_ID,
  query: 'SELECT 1',
  user: 'default',
  query_duration_ms: 120,
  query_start_time: '2026-09-11T12:00:00Z',
  memory_usage: 1024,
  read_bytes: 2048,
  ProfileEvents: {},
};

function harness(getQueryDetail: ReturnType<typeof vi.fn>) {
  const services = {
    queryAnalyzer: { getQueryDetail },
  } as unknown as ClickHouseServices;

  const wrapper = ({ children }: PropsWithChildren) => (
    <MemoryRouter initialEntries={[`/queries?qd_id=${QD_ID}`]}>
      <ClickHouseContext.Provider value={services}>
        {children}
      </ClickHouseContext.Provider>
    </MemoryRouter>
  );

  return renderHook(() => useQueryDeepLink(null, () => {}), { wrapper });
}

afterEach(() => {
  useClusterStore.getState().reset();
});

describe('useQueryDeepLink', () => {
  it('retries the qd_id fetch once the cluster name resolves', async () => {
    // First attempt runs with clusterName === null (local node only) and misses.
    const getQueryDetail = vi.fn()
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce(detail);

    const hook = harness(getQueryDetail);

    await waitFor(() => expect(getQueryDetail).toHaveBeenCalledTimes(1));
    expect(hook.result.current.query).toBeNull();

    act(() => {
      useClusterStore.getState().setCluster({
        clusterName: 'dev',
        replicaCount: 4,
        shardCount: 2,
        availableClusters: [{ name: 'dev', replicaCount: 4, shardCount: 2 }],
      });
    });

    await waitFor(() => expect(hook.result.current.query?.query_id).toBe(QD_ID));
    expect(getQueryDetail).toHaveBeenCalledTimes(2);
  });

  it('retries after a failed fetch rather than poisoning the guard', async () => {
    const getQueryDetail = vi.fn()
      .mockRejectedValueOnce(new Error('adapter not connected'))
      .mockResolvedValueOnce(detail);
    vi.spyOn(console, 'error').mockImplementation(() => {});

    const hook = harness(getQueryDetail);

    await waitFor(() => expect(getQueryDetail).toHaveBeenCalledTimes(1));
    expect(hook.result.current.query).toBeNull();

    act(() => {
      useClusterStore.getState().setCluster({
        clusterName: 'dev',
        replicaCount: 4,
        shardCount: 2,
        availableClusters: [],
      });
    });

    await waitFor(() => expect(hook.result.current.query?.query_id).toBe(QD_ID));
  });

  it('does not re-fetch while the cluster name is unchanged', async () => {
    const getQueryDetail = vi.fn().mockResolvedValue(null);

    const hook = harness(getQueryDetail);

    await waitFor(() => expect(getQueryDetail).toHaveBeenCalledTimes(1));
    hook.rerender();
    hook.rerender();

    expect(getQueryDetail).toHaveBeenCalledTimes(1);
  });

  it('stops fetching once a detail has been resolved', async () => {
    const getQueryDetail = vi.fn().mockResolvedValue(detail);

    const hook = harness(getQueryDetail);

    await waitFor(() => expect(hook.result.current.query?.query_id).toBe(QD_ID));

    act(() => {
      useClusterStore.getState().setCluster({
        clusterName: 'dev',
        replicaCount: 4,
        shardCount: 2,
        availableClusters: [],
      });
    });

    // A resolved query_id is held by fetchedRef, so a later cluster switch
    // must not re-issue the request.
    expect(getQueryDetail).toHaveBeenCalledTimes(1);
  });
});
