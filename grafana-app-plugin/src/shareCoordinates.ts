export const SHARE_VERSION_PARAM = 'th_v';
export const SHARE_DATASOURCE_PARAM = 'th_ds';
export const SHARE_CLUSTER_PARAM = 'th_cluster';
export const CONNECTED_NODE_SCOPE = '__connected__';

export interface GrafanaShareCoordinates {
  version: 1;
  datasourceUid: string;
  clusterName: string | null | undefined;
}

export function readShareCoordinates(search: string): GrafanaShareCoordinates | null {
  const params = new URLSearchParams(search);
  const datasourceUid = params.get(SHARE_DATASOURCE_PARAM)?.trim();
  if (!datasourceUid) return null;

  const hasCluster = params.has(SHARE_CLUSTER_PARAM);
  const cluster = params.get(SHARE_CLUSTER_PARAM);
  return {
    version: 1,
    datasourceUid,
    clusterName: !hasCluster ? undefined : cluster === CONNECTED_NODE_SCOPE ? null : cluster,
  };
}

export function shareCoordinateUpdate(
  datasourceUid: string,
  clusterName: string | null | undefined,
): Record<string, string | null> {
  return {
    [SHARE_VERSION_PARAM]: '1',
    [SHARE_DATASOURCE_PARAM]: datasourceUid,
    [SHARE_CLUSTER_PARAM]: clusterName === undefined
      ? null
      : clusterName ?? CONNECTED_NODE_SCOPE,
  };
}
