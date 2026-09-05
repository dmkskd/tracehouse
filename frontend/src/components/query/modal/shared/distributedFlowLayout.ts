/**
 * distributed-flow-diagram — turns an inferred DistributedTopology into a pure
 * geometry model for the isometric "flow" view of a distributed query.
 *
 * The timeline view answers "when, and for how long". This model answers
 * "who asked whom, and where did the rows come from": nodes are laid out as a
 * tidy tree (coordinator -> shard coordinators -> readers) with one edge per
 * parent/child relationship, and time is carried as a metric on the node
 * rather than as an X position.
 *
 * Geometry only. No colors, no formatting, no React: the renderer maps roles to
 * colors and metrics to text, so this file stays unit-testable. It sits beside
 * its component rather than in @tracehouse/core because it is presentation
 * layout in CSS pixels, not shared business logic — the same split as
 * components/cluster/clusterLayout.ts.
 */

import type {
  DistributedTopology,
  DistributedTopologyNode,
  TopologyNodeRole,
} from '@tracehouse/core';

/** Horizontal position of the first column's node anchor. */
const COLUMN_X0 = 90;
/**
 * Width one node occupies: the cube plus the label block drawn to its right.
 * Labels sit beside the cube rather than under it so a row costs ~130px of
 * height instead of ~200px, which is what lets a wide fan-out stay on screen.
 */
const NODE_SLOT_WIDTH = 250;
/** Room reserved between two ranks for the edge and its rows/bytes label. */
const EDGE_LANE_WIDTH = 200;
/** Inset of the cluster box from the panel edges. */
const BOX_MARGIN_X = 20;
/** How far a column caption sits above the first cube's anchor. */
const COLUMN_LABEL_RISE = 88;
/** Vertical distance between sibling rows: a 68px cube, its shadow, and a gap. */
const ROW_PITCH = 132;
/** Vertical position of the first row's anchor (the cube's base). */
const ROW_Y0 = 168;
/**
 * Siblings past this many rows wrap into another sub-column of the same rank.
 * A 16-way fan-out is 4 x 4 rather than a 2000px-tall single file.
 */
const MAX_ROWS_PER_COLUMN = 5;
/** Past this many distinct query shapes, one column each stops being readable. */
const MAX_SHAPE_COLUMNS = 6;
/** Padding from the outermost node anchors to the boundary box edges. */
const BOX_PAD_LEFT = 70;
/** Wider on the right: the anchor is the cube, and its labels extend rightward. */
const BOX_PAD_RIGHT = 215;
const BOX_TOP = 86;
const BOX_PAD_BOTTOM = 78;
/** Gap between a node anchor and where its edges start/end, so paths clear the cube. */
const EDGE_PADDING = 40;
/**
 * Edges enter a node at the middle of its left face, which is clear of labels.
 * They leave from lower down on the right face, because that is the side the
 * label block occupies and a fan-out would otherwise run straight through it.
 */
const EDGE_IN_Y_OFFSET = -20;
const EDGE_OUT_Y_OFFSET = 8;

export type DistributedFlowNodeRole = TopologyNodeRole | 'local_reader';

export type DistributedFlowEdgeKind = 'dispatch' | 'insert_flush' | 'folded_local_read';

export interface DistributedFlowNodeMetrics {
  durationMs: number;
  readRows: number;
  readBytes: number;
  /**
   * What this node handed back to its parent, which is what actually crossed
   * the link the edge draws. A shard reading a hundred million rows to answer
   * a GROUP BY returns a few hundred; read volume on an edge would say the
   * whole scan travelled.
   */
  resultRows: number;
  resultBytes: number;
  memoryUsage: number;
  /** Share of all rows read by any participant, 0..1. Undefined when unknown. */
  rowShare?: number;
  selectedParts?: number;
}

export interface DistributedFlowNode {
  id: string;
  queryId: string;
  hostname: string;
  /** Short display name for the host, e.g. `s2r1`. */
  hostLabel: string;
  role: DistributedFlowNodeRole;
  shardNum?: number;
  replicaNum?: number;
  /** Identity of the query this node ran, for grouping siblings by shape. */
  shapeKey: string;
  /** Human-readable form of that shape, usually the table it reads. */
  shapeLabel: string;
  /** Rank in the request graph: 0 is the coordinator. */
  column: number;
  /** Position within a wrapped rank. 0 unless the siblings needed more than one file. */
  subColumn: number;
  /** Anchor point: the horizontal centre and the base of the cube. */
  x: number;
  y: number;
  isCoordinator: boolean;
  /** A participant that never produced its own query_log row (folded into the coordinator). */
  isFolded: boolean;
  hasError: boolean;
  metrics: DistributedFlowNodeMetrics;
}

export interface DistributedFlowEdge {
  id: string;
  sourceId: string;
  targetId: string;
  kind: DistributedFlowEdgeKind;
  /** SVG path data for a cubic bezier between the two node anchors. */
  path: string;
  labelX: number;
  labelY: number;
  /**
   * What the target returned to its parent, which is what actually crossed
   * this link. Named for that rather than "rows"/"bytes": the generic names
   * once held the target's read counters, which sat here reading as though the
   * whole scan had travelled. Undefined when query_log carried no result
   * counters — an edge with nothing to say draws bare, and "0 rows returned"
   * is a claim this data cannot support.
   */
  returnedRows?: number;
  returnedBytes?: number;
}

export interface DistributedFlowGroup {
  id: string;
  label: string;
  x: number;
  y: number;
  width: number;
  height: number;
}

/** Caption over a sub-column, naming the query shape the column ran. */
export interface DistributedFlowColumnLabel {
  id: string;
  label: string;
  x: number;
  y: number;
}

export interface DistributedFlowDiagram {
  width: number;
  height: number;
  nodes: DistributedFlowNode[];
  edges: DistributedFlowEdge[];
  groups: DistributedFlowGroup[];
  columnLabels: DistributedFlowColumnLabel[];
}

export interface DistributedFlowDiagramOptions {
  /** Maps a hostname to its short display label. Defaults to the first dotted segment. */
  hostLabel?: (hostname: string) => string;
  /** Query ids that failed, so the renderer can flag them. */
  failedQueryIds?: Iterable<string>;
  /**
   * Width of the panel the diagram will be drawn into, in the same units as the
   * geometry. Surplus space widens the gaps between ranks and then the cluster
   * box, so a small query fills the panel instead of huddling on the left.
   */
  availableWidth?: number;
}

function defaultHostLabel(hostname: string): string {
  return hostname.split('.')[0] || hostname;
}

/** Readers sort under their shard coordinator; unattributed children sort last. */
function roleRank(role: DistributedFlowNodeRole): number {
  switch (role) {
    case 'coordinator': return 0;
    case 'shard_leader': return 1;
    case 'nested_coordinator': return 1;
    case 'insert_forwarder': return 1;
    case 'replica_reader': return 2;
    case 'local_reader': return 2;
    case 'async_insert_flush': return 3;
    default: return 4;
  }
}

function compareNodes(a: DistributedFlowNode, b: DistributedFlowNode): number {
  return (a.shardNum ?? 9999) - (b.shardNum ?? 9999)
    || roleRank(a.role) - roleRank(b.role)
    || (a.replicaNum ?? 9999) - (b.replicaNum ?? 9999)
    || a.hostLabel.localeCompare(b.hostLabel)
    || a.id.localeCompare(b.id);
}

function hostIdentity(hostname: string): string {
  return hostname.split('.')[0] || hostname;
}

/**
 * What query a participant ran. The normalized hash is the precise answer; the
 * tables it touched are the readable one, and either alone is enough to tell
 * one leg of a multi-query fan-out from another.
 */
function shapeKeyOf(node: DistributedTopologyNode): string {
  return node.normalizedQueryHash ?? node.tables.join(',');
}

function shapeLabelOf(node: DistributedTopologyNode): string {
  if (node.tables.length === 1) return node.tables[0];
  if (node.tables.length > 1) return `${node.tables[0]} +${node.tables.length - 1}`;
  return node.normalizedQueryHash ? `shape ${node.normalizedQueryHash.slice(0, 6)}` : '';
}

/** Where along the curve an edge label sits. Past the midpoint, so labels on a
 * fan-out spread with their targets instead of stacking over the parent. */
const LABEL_T = 0.62;

function cubicAt(t: number, p0: number, p1: number, p2: number, p3: number): number {
  const u = 1 - t;
  return u * u * u * p0 + 3 * u * u * t * p1 + 3 * u * t * t * p2 + t * t * t * p3;
}

function edgePath(source: DistributedFlowNode, target: DistributedFlowNode): {
  path: string;
  labelX: number;
  labelY: number;
} {
  const x1 = source.x + EDGE_PADDING;
  const x2 = target.x - EDGE_PADDING;
  const y1 = source.y + EDGE_OUT_Y_OFFSET;
  const y2 = target.y + EDGE_IN_Y_OFFSET;
  const bend = Math.max(48, Math.abs(x2 - x1) * 0.42);
  const c1x = x1 + bend;
  const c2x = x2 - bend;
  return {
    path: `M ${x1},${y1} C ${c1x},${y1} ${c2x},${y2} ${x2},${y2}`,
    labelX: cubicAt(LABEL_T, x1, c1x, c2x, x2),
    labelY: cubicAt(LABEL_T, y1, y1, y2, y2) - 10,
  };
}

/**
 * Build the flow model. Nodes with no resolvable parent are attached to the
 * coordinator, so a topology whose child roles were never identified still
 * draws as a one-level fan-out rather than as disconnected cubes.
 */
export function buildDistributedFlowDiagram(
  topology: DistributedTopology,
  options: DistributedFlowDiagramOptions = {},
): DistributedFlowDiagram {
  const hostLabel = options.hostLabel ?? defaultHostLabel;
  const failedQueryIds = new Set(options.failedQueryIds ?? []);

  const rowShareByParticipant = new Map<string, number>();
  const partsByParticipant = new Map<string, number>();
  for (const entry of topology.readDistribution.entries) {
    const key = `${entry.queryId}:${hostIdentity(entry.hostname)}`;
    rowShareByParticipant.set(key, entry.rowShare);
    partsByParticipant.set(key, entry.selectedParts);
  }

  const toNode = (source: DistributedTopologyNode): DistributedFlowNode => {
    const key = `${source.queryId}:${hostIdentity(source.hostname)}`;
    return {
      id: source.id,
      queryId: source.queryId,
      hostname: source.hostname,
      hostLabel: hostLabel(source.hostname),
      role: source.role,
      shardNum: source.shardNum,
      replicaNum: source.replicaNum,
      shapeKey: shapeKeyOf(source),
      shapeLabel: shapeLabelOf(source),
      column: 0,
      subColumn: 0,
      x: 0,
      y: 0,
      isCoordinator: source.role === 'coordinator',
      isFolded: false,
      hasError: failedQueryIds.has(source.queryId),
      metrics: {
        durationMs: source.queryDurationMs,
        readRows: source.readRows,
        readBytes: source.readBytes,
        resultRows: source.resultRows,
        resultBytes: source.resultBytes,
        memoryUsage: source.memoryUsage,
        rowShare: rowShareByParticipant.get(key),
        selectedParts: partsByParticipant.get(key),
      },
    };
  };

  // insert_client rows describe the client that issued the INSERT, not a
  // ClickHouse participant, so they are not part of the cluster picture.
  const sourceNodes = topology.nodes.filter(node => node.role !== 'insert_client');
  const nodes: DistributedFlowNode[] = sourceNodes.map(toNode);
  const nodeById = new Map(nodes.map(node => [node.id, node]));

  const coordinator = nodes.find(node => node.isCoordinator)
    ?? nodes.find(node => node.queryId === topology.rootQueryId);

  // A participant that read locally on the initiator never gets its own
  // query_log row; the timeline folds it into the coordinator label, and here
  // it becomes a ghost child so the fan-out width matches the participant count.
  const foldedNode: DistributedFlowNode | undefined = topology.localRead && coordinator
    ? {
      id: `folded:${topology.localRead.queryId}:${topology.localRead.hostname}`,
      queryId: topology.localRead.queryId,
      hostname: topology.localRead.hostname,
      hostLabel: hostLabel(topology.localRead.hostname),
      role: 'local_reader',
      shardNum: topology.localRead.shardNum,
      replicaNum: topology.localRead.replicaNum,
      // Folded reads have no query_log row of their own, so no shape to group by.
      shapeKey: '',
      shapeLabel: '',
      column: 0,
      subColumn: 0,
      x: 0,
      y: 0,
      isCoordinator: false,
      isFolded: true,
      hasError: false,
      metrics: {
        durationMs: 0,
        readRows: topology.localRead.readRows ?? 0,
        readBytes: topology.localRead.readBytes ?? 0,
        // A folded read never left the initiator, so nothing travelled.
        resultRows: 0,
        resultBytes: 0,
        memoryUsage: 0,
        selectedParts: topology.localRead.selectedParts,
      },
    }
    : undefined;
  if (foldedNode) {
    nodes.push(foldedNode);
    nodeById.set(foldedNode.id, foldedNode);
  }

  // Parenting. Readers hang off their shard's coordinator when one was
  // identified; an async flush hangs off the INSERT that produced it.
  const leaderByShard = new Map<number, DistributedFlowNode>();
  for (const shard of topology.shards) {
    if (!shard.leader) continue;
    const leader = nodeById.get(shard.leader.id);
    if (leader) leaderByShard.set(shard.shardNum, leader);
  }
  const insertNodeByFlushQueryId = new Map<string, DistributedFlowNode>();
  for (const link of topology.asyncInsertLinks) {
    const insertNode = nodes.find(node => node.queryId === link.queryId);
    if (insertNode) insertNodeByFlushQueryId.set(link.flushQueryId, insertNode);
  }

  const parentOf = new Map<string, DistributedFlowNode | undefined>();
  for (const node of nodes) {
    if (node === coordinator) {
      parentOf.set(node.id, undefined);
      continue;
    }
    if (node.role === 'async_insert_flush') {
      parentOf.set(node.id, insertNodeByFlushQueryId.get(node.queryId) ?? coordinator);
      continue;
    }
    if (node.role === 'replica_reader' && node.shardNum != null) {
      const leader = leaderByShard.get(node.shardNum);
      parentOf.set(node.id, leader && leader.id !== node.id ? leader : coordinator);
      continue;
    }
    parentOf.set(node.id, coordinator);
  }

  const childrenOf = new Map<string, DistributedFlowNode[]>();
  const roots: DistributedFlowNode[] = [];
  for (const node of nodes) {
    const parent = parentOf.get(node.id);
    if (!parent) {
      roots.push(node);
      continue;
    }
    const siblings = childrenOf.get(parent.id);
    if (siblings) siblings.push(node);
    else childrenOf.set(parent.id, [node]);
  }
  roots.sort(compareNodes);
  for (const siblings of childrenOf.values()) siblings.sort(compareNodes);

  // Pass 1: rows and ranks. Leaves take the next free row and a parent centres
  // on its children, except that a wide group of leaf siblings wraps into
  // several sub-columns so the diagram grows sideways instead of downwards.
  // Depth is bounded by the role graph, but guard anyway so a malformed
  // topology cannot recurse forever.
  let nextRow = 0;
  const visited = new Set<string>();
  const placeInRow = (node: DistributedFlowNode, column: number, subColumn: number, rowIndex: number) => {
    visited.add(node.id);
    node.column = column;
    node.subColumn = subColumn;
    node.y = ROW_Y0 + rowIndex * ROW_PITCH;
  };
  const assign = (node: DistributedFlowNode, column: number): number => {
    if (visited.has(node.id)) return node.y;
    visited.add(node.id);
    node.column = column;
    const children = childrenOf.get(node.id) ?? [];
    if (children.length === 0) {
      node.y = ROW_Y0 + nextRow * ROW_PITCH;
      nextRow += 1;
      return node.y;
    }
    const allLeaves = children.every(child => (childrenOf.get(child.id) ?? []).length === 0);
    if (allLeaves && children.length > MAX_ROWS_PER_COLUMN) {
      // A fan-out this wide is usually one coordinator issuing several
      // different queries to every node. Giving each query shape its own
      // sub-column answers "which query went where" by position, instead of
      // leaving sixteen identically named cubes to be told apart by hovering.
      const byShape = new Map<string, DistributedFlowNode[]>();
      for (const child of children) {
        const group = byShape.get(child.shapeKey);
        if (group) group.push(child);
        else byShape.set(child.shapeKey, [child]);
      }
      const shapeGroups = [...byShape.values()];
      const grouped = shapeGroups.length > 1 && shapeGroups.length <= MAX_SHAPE_COLUMNS;
      const blocks = grouped
        ? shapeGroups
        : [children];
      const firstRow = nextRow;
      let subColumn = 0;
      let rowsUsed = 0;
      for (const block of blocks) {
        const subColumns = Math.ceil(block.length / MAX_ROWS_PER_COLUMN);
        const rows = Math.ceil(block.length / subColumns);
        block.forEach((child, index) => {
          placeInRow(child, column + 1, subColumn + Math.floor(index / rows), firstRow + (index % rows));
        });
        subColumn += subColumns;
        rowsUsed = Math.max(rowsUsed, rows);
      }
      nextRow += rowsUsed;
      const wrappedYs = children.map(child => child.y);
      node.y = (Math.min(...wrappedYs) + Math.max(...wrappedYs)) / 2;
      return node.y;
    }
    const childYs = children.map(child => assign(child, column + 1));
    node.y = (Math.min(...childYs) + Math.max(...childYs)) / 2;
    return node.y;
  };
  for (const root of roots) assign(root, 0);
  // Anything unreachable (a cycle in the parent map) still needs a position.
  for (const node of nodes) {
    if (visited.has(node.id)) continue;
    visited.add(node.id);
    node.y = ROW_Y0 + nextRow * ROW_PITCH;
    nextRow += 1;
  }

  // Pass 2: x. A rank is as wide as the widest sub-column count in it, so a
  // wrapped rank pushes the ranks after it further right instead of overlapping.
  const subColumnsInRank = new Map<number, number>();
  for (const node of nodes) {
    subColumnsInRank.set(node.column, Math.max(subColumnsInRank.get(node.column) ?? 1, node.subColumn + 1));
  }
  const deepestRank = Math.max(0, ...nodes.map(node => node.column));
  /** Places every node and returns the x of the rightmost one. */
  const layoutRanks = (): number => {
    const rankX = new Map<number, number>();
    let cursor = COLUMN_X0;
    for (let rank = 0; rank <= deepestRank; rank += 1) {
      rankX.set(rank, cursor);
      cursor += (subColumnsInRank.get(rank) ?? 1) * NODE_SLOT_WIDTH + EDGE_LANE_WIDTH;
    }
    for (const node of nodes) {
      node.x = (rankX.get(node.column) ?? COLUMN_X0) + node.subColumn * NODE_SLOT_WIDTH;
    }
    return nodes.length > 0 ? Math.max(...nodes.map(node => node.x)) : COLUMN_X0;
  };

  let rightmost = layoutRanks();
  const naturalWidth = rightmost + BOX_PAD_RIGHT + BOX_PAD_LEFT;
  const available = options.availableWidth ?? 0;
  if (available > naturalWidth) {
    // Surplus space centres the participants rather than spreading them: a
    // two-node query stretched across a wide panel reads as distance between
    // the nodes, which is not something this diagram means.
    const shift = (available - naturalWidth) / 2;
    for (const node of nodes) node.x += shift;
    rightmost += shift;
  }

  const edges: DistributedFlowEdge[] = [];
  for (const node of nodes) {
    const parent = parentOf.get(node.id);
    if (!parent) continue;
    const { path, labelX, labelY } = edgePath(parent, node);
    edges.push({
      id: `${parent.id}->${node.id}`,
      sourceId: parent.id,
      targetId: node.id,
      kind: node.isFolded
        ? 'folded_local_read'
        : node.role === 'async_insert_flush' ? 'insert_flush' : 'dispatch',
      path,
      labelX,
      labelY,
      returnedRows: node.metrics.resultRows > 0 ? node.metrics.resultRows : undefined,
      returnedBytes: node.metrics.resultBytes > 0 ? node.metrics.resultBytes : undefined,
    });
  }

  const xs = nodes.map(node => node.x);
  const ys = nodes.map(node => node.y);
  // The box spans the panel when there is one: a cluster boundary that stops
  // short of the panel edge reads as an empty column rather than as a boundary.
  const boxLeft = nodes.length > 0
    ? Math.min(Math.min(...xs) - BOX_PAD_LEFT, BOX_MARGIN_X)
    : 0;
  const boxRight = nodes.length > 0
    ? Math.max(rightmost + BOX_PAD_RIGHT, available - BOX_MARGIN_X)
    : 0;
  const boxBottom = nodes.length > 0 ? Math.max(...ys) + BOX_PAD_BOTTOM : BOX_TOP;
  const clusterName = topology.clusterAllReplicas?.cluster;
  const groups: DistributedFlowGroup[] = nodes.length > 0
    ? [{
      id: 'clickhouse',
      label: clusterName ? `CLICKHOUSE · ${clusterName.toUpperCase()}` : 'CLICKHOUSE',
      x: boxLeft,
      y: BOX_TOP,
      width: boxRight - boxLeft,
      height: boxBottom - BOX_TOP,
    }]
    : [];

  // One caption per sub-column, but only where the columns actually differ:
  // repeating the same table over every column would be noise.
  const columnLabels: DistributedFlowColumnLabel[] = [];
  const bySubColumn = new Map<string, DistributedFlowNode[]>();
  for (const node of nodes) {
    if (node.column === 0) continue;
    const key = `${node.column}:${node.subColumn}`;
    const group = bySubColumn.get(key);
    if (group) group.push(node);
    else bySubColumn.set(key, [node]);
  }
  const distinctShapes = new Set(
    [...bySubColumn.values()].map(group => group[0].shapeKey),
  );
  if (bySubColumn.size > 1 && distinctShapes.size > 1) {
    for (const [key, group] of bySubColumn) {
      const label = group[0].shapeLabel;
      if (!label || group.some(node => node.shapeKey !== group[0].shapeKey)) continue;
      columnLabels.push({
        id: key,
        label,
        x: group[0].x,
        y: Math.min(...group.map(node => node.y)) - COLUMN_LABEL_RISE,
      });
    }
  }

  return {
    width: Math.max(boxRight + BOX_MARGIN_X, available),
    height: boxBottom + 24,
    nodes,
    edges,
    groups,
    columnLabels,
  };
}

/**
 * Gauges painted on each cube: how big this participant is next to the biggest
 * one in the same diagram. The numbers beside a cube say "6ms · 2 rows", which
 * only answers "how big" once you have read every other cube and done the
 * division; a share of the maximum answers it at a glance.
 *
 * Normalised against the whole diagram, coordinator included: a coordinator
 * that holds 8 MB while its children hold 200 KB each is the point of the
 * picture, so flattening it out of the scale would hide the finding.
 */
export type FlowGaugeKey = 'duration' | 'memory' | 'rows';

export interface FlowNodeGauge {
  key: FlowGaugeKey;
  /** Short caption for the legend and the accessible description. */
  label: string;
  value: number;
  /** Value as a fraction of the largest node's value for this metric, 0..1. */
  share: number;
}

const GAUGE_SPECS: { key: FlowGaugeKey; label: string; read: (node: DistributedFlowNode) => number }[] = [
  { key: 'duration', label: 'duration', read: node => node.metrics.durationMs },
  { key: 'memory', label: 'memory', read: node => node.metrics.memoryUsage },
  { key: 'rows', label: 'rows read', read: node => node.metrics.readRows },
];

export function computeFlowGauges(nodes: DistributedFlowNode[]): Map<string, FlowNodeGauge[]> {
  const maxima = GAUGE_SPECS.map(spec => Math.max(0, ...nodes.map(spec.read)));
  const byNodeId = new Map<string, FlowNodeGauge[]>();
  for (const node of nodes) {
    byNodeId.set(node.id, GAUGE_SPECS.map((spec, index) => {
      const value = spec.read(node);
      const max = maxima[index];
      return {
        key: spec.key,
        label: spec.label,
        value,
        // A metric no node reported draws as an empty track rather than a full
        // one: "nobody has this" must not read as "this node has all of it".
        share: max > 0 && value > 0 ? Math.min(1, value / max) : 0,
      };
    }));
  }
  return byNodeId;
}
