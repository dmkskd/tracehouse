import { describe, expect, it } from 'vitest';
import {
  timeTravelHostnameFilter,
  timeTravelRowHosts,
  updateTimeTravelHostSelection,
} from '../time-travel-host-selection';

describe('Time Travel host selection', () => {
  it('replaces the selection on a plain click', () => {
    expect(updateTimeTravelHostSelection(['host-a', 'host-b'], 'host-c', false))
      .toEqual(['host-c']);
  });

  it('adds and removes hosts on a modifier click', () => {
    expect(updateTimeTravelHostSelection(['host-a'], 'host-b', true))
      .toEqual(['host-a', 'host-b']);
    expect(updateTimeTravelHostSelection(['host-a', 'host-b'], 'host-a', true))
      .toEqual(['host-b']);
  });

  it('maps an empty selection to the cluster-wide filter', () => {
    expect(timeTravelHostnameFilter([])).toBeNull();
    expect(timeTravelHostnameFilter(['host-a', 'host-b']))
      .toEqual(['host-a', 'host-b']);
  });

  it('shows the selected scope as rows only in Per-server view', () => {
    const clusterHosts = ['host-a', 'host-b', 'host-c', 'host-d'];
    expect(timeTravelRowHosts(clusterHosts, ['host-b'], false)).toEqual([]);
    expect(timeTravelRowHosts(clusterHosts, ['host-d', 'host-b'], true))
      .toEqual(['host-b', 'host-d']);
    expect(timeTravelRowHosts(clusterHosts, [], true)).toEqual(clusterHosts);
  });
});
