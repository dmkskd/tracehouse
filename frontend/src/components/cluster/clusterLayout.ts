/**
 * clusterLayout — pure spatial layout logic for the cluster topology 3D scene.
 *
 * Extracted from ClusterTopology.tsx so the component file stays focused
 * on rendering and the layout math is independently testable.
 */

interface NodeRow {
  shard_num: number;
  replica_num: number;
  host_name: string;
  host_address: string;
  port: number;
  is_local: number;
  errors_count: number;
  slowdowns_count: number;
  estimated_recovery_time: number;
}

interface KeeperNode {
  host: string;
  port: number;
  index: number;
  connected_time: string;
  is_expired: number;
  keeper_api_version: number;
}

export interface LayoutResult {
  shardGroups: {
    shardNum: number;
    center: [number, number, number];
    enclosureSize: [number, number, number];
    replicas: {
      node: NodeRow;
      position: [number, number, number];
    }[];
  }[];
  keeperNodes: {
    node: KeeperNode;
    position: [number, number, number];
  }[];
  keeperEnclosure: {
    center: [number, number, number];
    size: [number, number, number];
  } | null;
  totalWidth: number;
}

export function computeLayout(nodes: NodeRow[], keepers: KeeperNode[]): LayoutResult {
  const nodeSize: [number, number, number] = [0.7, 0.4, 0.4];
  const replicaSpacing = 0.6;
  const shardSpacing = 1.0;
  const shardPadding = 1.0;

  // Group by shard
  const shardMap = new Map<number, NodeRow[]>();
  for (const n of nodes) {
    const s = Number(n.shard_num);
    if (!shardMap.has(s)) shardMap.set(s, []);
    shardMap.get(s)!.push(n);
  }
  const sortedShards = [...shardMap.entries()].sort(([a], [b]) => a - b);

  // Layout shards along X axis
  let xCursor = 0;
  const shardGroups: LayoutResult['shardGroups'] = [];

  for (const [shardNum, replicas] of sortedShards) {
    const sorted = replicas.sort((a, b) => Number(a.replica_num) - Number(b.replica_num));
    const replicaSpan = sorted.length * replicaSpacing;
    const stripeH = 0.6;
    const enclosureW = nodeSize[0] + shardPadding * 2;
    const enclosureH = replicaSpan + shardPadding;
    const enclosureD = Math.max(nodeSize[2], replicaSpan) + shardPadding * 2 + stripeH;

    const centerX = xCursor + enclosureW / 2;
    const nodeZOffset = -stripeH / 2;
    const replicaPositions = sorted.map((node, i) => ({
      node,
      position: [
        centerX,
        nodeSize[1] / 2 + 0.1,
        (i - (sorted.length - 1) / 2) * replicaSpacing + nodeZOffset,
      ] as [number, number, number],
    }));

    shardGroups.push({
      shardNum,
      center: [centerX, enclosureH / 2, 0],
      enclosureSize: [enclosureW, enclosureH, enclosureD],
      replicas: replicaPositions,
    });

    xCursor += enclosureW + shardSpacing;
  }

  const totalWidth = xCursor - shardSpacing;
  const offsetX = -totalWidth / 2;

  // Shift everything to center
  for (const sg of shardGroups) {
    sg.center[0] += offsetX;
    for (const r of sg.replicas) {
      r.position[0] += offsetX;
    }
  }

  // Keeper nodes below (positive Z, away from camera)
  const keeperY = nodeSize[1] / 2 + 0.1;
  const keeperZ = (sortedShards.length > 0
    ? Math.max(...shardGroups.map(sg => sg.enclosureSize[2])) / 2 + 2.0
    : 2.0);
  const keeperSpacing = 2.0;
  const keeperPadding = 1.0;
  const keeperNodeW = 0.6;
  const keeperNodeD = 0.3;
  const keeperTotalW = keepers.length * keeperSpacing;
  const keeperOffsetX = -keeperTotalW / 2 + 1.0;

  const keeperLayout = keepers.map((node, i) => ({
    node,
    position: [keeperOffsetX + i * keeperSpacing, keeperY, keeperZ] as [number, number, number],
  }));

  // Compute keeper enclosure
  let keeperEnclosure: LayoutResult['keeperEnclosure'] = null;
  if (keepers.length > 0) {
    const stripeH = 0.6;
    const innerW = keepers.length === 1 ? keeperNodeW : (keepers.length - 1) * keeperSpacing + keeperNodeW;
    const encW = innerW + keeperPadding * 2;
    const encD = keeperNodeD + keeperPadding * 2 + stripeH;
    keeperEnclosure = {
      center: [0, encD / 2, keeperZ],
      size: [encW, encD, encD],
    };
  }

  return { shardGroups, keeperNodes: keeperLayout, keeperEnclosure, totalWidth: Math.max(totalWidth, keeperTotalW) };
}
