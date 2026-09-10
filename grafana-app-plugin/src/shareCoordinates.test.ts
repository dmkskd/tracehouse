import { describe, expect, it } from 'vitest';
import {
  CONNECTED_NODE_SCOPE,
  readShareCoordinates,
  shareCoordinateUpdate,
} from './shareCoordinates';

describe('Grafana share coordinates', () => {
  it('reads datasource and explicit cluster coordinates', () => {
    expect(readShareCoordinates('?orgId=2&th_v=1&th_ds=clickhouse-a&th_cluster=production')).toEqual({
      version: 1,
      datasourceUid: 'clickhouse-a',
      clusterName: 'production',
    });
  });

  it('distinguishes connected-node scope from a missing cluster coordinate', () => {
    expect(readShareCoordinates(`?th_ds=clickhouse-a&th_cluster=${CONNECTED_NODE_SCOPE}`)?.clusterName).toBeNull();
    expect(readShareCoordinates('?th_ds=clickhouse-a')?.clusterName).toBeUndefined();
    expect(shareCoordinateUpdate('clickhouse-a', undefined).th_cluster).toBeNull();
  });
});
