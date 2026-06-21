export type DistributedQueryKind =
  | 'local'
  | 'plain_distributed_select'
  | 'parallel_replicas_select'
  | 'cluster_all_replicas'
  | 'object_storage_swarm_select'
  | 'hybrid_storage_select'
  | 'distributed_insert'
  | 'unknown_distributed';

export type TopologyNodeRole =
  | 'coordinator'
  | 'shard_leader'
  | 'replica_reader'
  | 'remote_child'
  | 'independent_child'
  | 'object_storage_worker'
  | 'hybrid_segment'
  | 'insert_client'
  | 'insert_forwarder'
  | 'async_insert_flush'
  | 'unknown';

export type TopologyConfidence = 'high' | 'medium' | 'low';

export type TopologyDecisionLevel = 'info' | 'warning' | 'degraded';

export type TopologyCapabilityId =
  | 'query_log'
  | 'profile_events'
  | 'processors_profile_log'
  | 'system_clusters'
  | 'text_log'
  | 'shared_merge_tree_metadata'
  | 'object_storage_cluster_metadata'
  | 'iceberg_metadata'
  | 'parquet_metadata'
  | 'hybrid_table_metadata'
  | 'cloud_metadata';

export interface ProfileEventsMap {
  [key: string]: number | string | undefined;
}

export interface DistributedQueryExecutionInput {
  queryId: string;
  initialQueryId: string;
  isInitialQuery: boolean;
  hostname: string;
  queryKind?: string;
  queryStartTimeMicroseconds?: string;
  queryDurationMs?: number;
  readRows?: number;
  readBytes?: number;
  writtenRows?: number;
  writtenBytes?: number;
  resultRows?: number;
  resultBytes?: number;
  memoryUsage?: number;
  tables?: string[];
  tableEngines?: string[];
  usedStorages?: string[];
  settings?: Record<string, string | number | boolean | undefined>;
  queryPreview?: string;
  profileEvents?: ProfileEventsMap;
}

export interface ClusterHostInput {
  hostName: string;
  shardNum: number;
  replicaNum: number;
  cluster?: string;
}

export interface ProcessorProfileInput {
  queryId: string;
  initialQueryId: string;
  hostname: string;
  planStepName?: string;
  planStepDescription?: string;
  processorName?: string;
}

export interface DistributedTextLogInput {
  queryId: string;
  eventTimeMicroseconds: string;
  level?: string;
  source?: string;
  message: string;
  threadName?: string;
}

export type DistributedExecutionPhaseKind =
  | 'remote_scalar_exchange'
  | 'merge_partial_aggregation'
  | 'aggregation_output'
  | 'final_read'
  | 'memory_peak'
  | 'profile_events';

export interface DistributedExecutionPhase {
  kind: DistributedExecutionPhaseKind;
  source: 'text_log';
  queryId: string;
  eventTimeMicroseconds: string;
  offsetMs?: number;
  hostname?: string;
  title: string;
  detail: string;
  rows?: number;
  blocks?: number;
  scalars?: number;
  bytesText?: string;
  durationMs?: number;
}

export type DistributedExecutionFlowEventKind =
  | 'coordinator_started'
  | 'remote_started'
  | 'remote_setup'
  | 'remote_read_completed'
  | 'coordinator_merge'
  | 'coordinator_output'
  | 'coordinator_read_completed';

export interface DistributedExecutionFlowEvent {
  kind: DistributedExecutionFlowEventKind;
  source: 'query_log' | 'text_log';
  offsetMs: number;
  actor: string;
  actorType: 'coordinator' | 'remote';
  title: string;
  detail: string;
  queryId?: string;
  hostname?: string;
  rows?: number;
  bytesText?: string;
  durationMs?: number;
}

export interface DistributedTopologyCapabilities {
  queryLog: boolean;
  profileEvents: boolean;
  processorsProfileLog: boolean;
  systemClusters: boolean;
  textLog: boolean;
  sharedMergeTreeMetadata: boolean;
  objectStorageClusterMetadata: boolean;
  icebergMetadata: boolean;
  parquetMetadata: boolean;
  hybridTableMetadata: boolean;
  cloudMetadata: boolean;
}

export interface DistributedTopologyInput {
  rootQueryId: string;
  executions: DistributedQueryExecutionInput[];
  clusterHosts?: ClusterHostInput[];
  processorProfiles?: ProcessorProfileInput[];
  textLogs?: DistributedTextLogInput[];
  capabilities?: Partial<DistributedTopologyCapabilities>;
}

export interface TopologyEvidence {
  source: 'query_log' | 'profile_events' | 'processors_profile_log' | 'system_clusters' | 'text_log' | 'capability';
  message: string;
}

export interface TopologyDecision {
  level: TopologyDecisionLevel;
  code: string;
  message: string;
  source: TopologyEvidence['source'];
}

export interface TopologyDetectorPlugin {
  id: string;
  label: string;
  requiredCapabilities: TopologyCapabilityId[];
  applies(input: DistributedTopologyInput, capabilities: DistributedTopologyCapabilities): boolean;
}

export interface DistributedTopologyNode {
  id: string;
  queryId: string;
  hostname: string;
  role: TopologyNodeRole;
  shardNum?: number;
  replicaNum?: number;
  queryKind?: string;
  queryStartTimeMicroseconds?: string;
  queryDurationMs: number;
  readRows: number;
  readBytes: number;
  writtenRows: number;
  writtenBytes: number;
  queryPreview?: string;
  profileEvents: ProfileEventsMap;
  tables: string[];
  evidence: TopologyEvidence[];
}

export interface DistributedTopologyShard {
  shardNum: number;
  leader?: DistributedTopologyNode;
  readers: DistributedTopologyNode[];
  children: DistributedTopologyNode[];
}

export interface DistributedShardCoverage {
  observedShards: number;
  expectedShardSends?: number;
  allExpectedShardSendsObserved?: boolean;
}

export interface DistributedTopology {
  kind: DistributedQueryKind;
  rootQueryId: string;
  capabilities: DistributedTopologyCapabilities;
  coordinator?: DistributedTopologyNode;
  nodes: DistributedTopologyNode[];
  shards: DistributedTopologyShard[];
  shardCoverage: DistributedShardCoverage;
  confidence: TopologyConfidence;
  evidence: TopologyEvidence[];
  decisions: TopologyDecision[];
  executionPhases: DistributedExecutionPhase[];
  executionFlow: DistributedExecutionFlowEvent[];
  detectorPlugins: string[];
  warnings: string[];
}

export interface DistributedExecutionFlowStep {
  event: DistributedExecutionFlowEvent;
  node?: DistributedTopologyNode;
  showQueryPreview: boolean;
  groupId?: string;
  parentNodeId?: string;
  depth: number;
}

const DEFAULT_CAPABILITIES: DistributedTopologyCapabilities = {
  queryLog: true,
  profileEvents: true,
  processorsProfileLog: false,
  systemClusters: false,
  textLog: false,
  sharedMergeTreeMetadata: false,
  objectStorageClusterMetadata: false,
  icebergMetadata: false,
  parquetMetadata: false,
  hybridTableMetadata: false,
  cloudMetadata: false,
};

export const BUILT_IN_TOPOLOGY_DETECTORS: TopologyDetectorPlugin[] = [
  {
    id: 'query-log-read-fanout',
    label: 'query_log read fan-out detector',
    requiredCapabilities: ['query_log'],
    applies: (input, capabilities) =>
      capabilities.queryLog && input.executions.some((execution) => (execution.queryKind ?? 'Select') === 'Select'),
  },
  {
    id: 'profile-events-parallel-replicas',
    label: 'ProfileEvents parallel replicas detector',
    requiredCapabilities: ['profile_events'],
    applies: (input, capabilities) =>
      capabilities.profileEvents && input.executions.some((execution) => isParallelLeader(execution) || hasAnyEvent(execution.profileEvents, ['ParallelReplicas'])),
  },
  {
    id: 'processor-phase-classifier',
    label: 'processors_profile_log phase classifier',
    requiredCapabilities: ['processors_profile_log'],
    applies: (input, capabilities) => capabilities.processorsProfileLog && (input.processorProfiles?.length ?? 0) > 0,
  },
  {
    id: 'cluster-host-mapper',
    label: 'system.clusters host to shard mapper',
    requiredCapabilities: ['system_clusters'],
    applies: (input, capabilities) => capabilities.systemClusters && (input.clusterHosts?.length ?? 0) > 0,
  },
  {
    id: 'query-log-write-cascade',
    label: 'query_log write cascade detector',
    requiredCapabilities: ['query_log'],
    applies: (input, capabilities) =>
      capabilities.queryLog && input.executions.some((execution) => ['Insert', 'AsyncInsertFlush'].includes(execution.queryKind ?? '')),
  },
  {
    id: 'object-storage-swarm',
    label: 'object-storage swarm execution detector',
    requiredCapabilities: ['object_storage_cluster_metadata'],
    applies: (input, capabilities) =>
      capabilities.objectStorageClusterMetadata && input.executions.some((execution) =>
        Boolean(execution.settings?.object_storage_cluster) || hasObjectStorageSignal(execution),
      ),
  },
  {
    id: 'iceberg-parquet-storage',
    label: 'Iceberg/Parquet storage detector',
    requiredCapabilities: ['iceberg_metadata', 'parquet_metadata'],
    applies: (input, capabilities) =>
      (capabilities.icebergMetadata || capabilities.parquetMetadata) && input.executions.some(hasObjectStorageSignal),
  },
  {
    id: 'hybrid-storage',
    label: 'Hybrid storage segment detector',
    requiredCapabilities: ['hybrid_table_metadata'],
    applies: (input, capabilities) =>
      capabilities.hybridTableMetadata && input.executions.some((execution) => hasHybridSignal(execution)),
  },
];

function resolveCapabilities(input: DistributedTopologyInput): DistributedTopologyCapabilities {
  const supplied = input.capabilities ?? {};
  return {
    ...DEFAULT_CAPABILITIES,
    processorsProfileLog: (input.processorProfiles?.length ?? 0) > 0,
    systemClusters: (input.clusterHosts?.length ?? 0) > 0,
    textLog: (input.textLogs?.length ?? 0) > 0,
    ...supplied,
  };
}

function activeDetectors(input: DistributedTopologyInput, capabilities: DistributedTopologyCapabilities): TopologyDetectorPlugin[] {
  return BUILT_IN_TOPOLOGY_DETECTORS.filter((plugin) => plugin.applies(input, capabilities));
}

function addDecision(
  decisions: TopologyDecision[],
  level: TopologyDecisionLevel,
  code: string,
  source: TopologyEvidence['source'],
  message: string,
): void {
  decisions.push({ level, code, source, message });
}

function detectorSource(detector: TopologyDetectorPlugin): TopologyEvidence['source'] {
  if (detector.requiredCapabilities.includes('system_clusters')) return 'system_clusters';
  if (detector.requiredCapabilities.includes('processors_profile_log')) return 'processors_profile_log';
  if (detector.requiredCapabilities.includes('profile_events')) return 'profile_events';
  return 'query_log';
}

function num(value: number | string | undefined): number {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseCount(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const parsed = Number(value.replace(/,/g, ''));
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseDurationSeconds(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed * 1000 : undefined;
}

function parseTimestampMs(timestamp: string | undefined): number | undefined {
  if (!timestamp) return undefined;
  const dotIdx = timestamp.lastIndexOf('.');
  const base = dotIdx >= 0 ? timestamp.slice(0, dotIdx) : timestamp;
  const parsed = Date.parse(`${base.replace(' ', 'T')}Z`);
  if (!Number.isFinite(parsed)) return undefined;
  if (dotIdx < 0) return parsed;
  const fraction = timestamp.slice(dotIdx + 1).padEnd(6, '0').slice(0, 6);
  const milliseconds = Number(fraction.slice(0, 3));
  return parsed + (Number.isFinite(milliseconds) ? milliseconds : 0);
}

function hasEvent(events: ProfileEventsMap | undefined, name: string): boolean {
  return num(events?.[name]) > 0;
}

function hasAnyEvent(events: ProfileEventsMap | undefined, prefixes: string[]): boolean {
  if (!events) return false;
  return Object.keys(events).some((key) => prefixes.some((prefix) => key.startsWith(prefix)) && num(events[key]) > 0);
}

function normalizeHost(hostname: string): string {
  return hostname.split('.')[0];
}

function hostKey(hostname: string): string {
  return normalizeHost(hostname).toLowerCase();
}

function isUnsignedIntegerToken(token: string | undefined): boolean {
  if (!token) return false;
  const parsed = Number(token);
  return Number.isInteger(parsed) && parsed >= 0 && String(parsed) === token;
}

function clusterServiceHostKeyFromPodHost(key: string): string | undefined {
  const parts = key.split('-');
  if (parts.length < 4) return undefined;
  const last = parts[parts.length - 1];
  const replica = parts[parts.length - 2];
  const shard = parts[parts.length - 3];
  if (!isUnsignedIntegerToken(shard) || !isUnsignedIntegerToken(replica) || !isUnsignedIntegerToken(last)) {
    return undefined;
  }
  return parts.slice(0, -1).join('-');
}

function hostKeys(hostname: string): string[] {
  const key = hostKey(hostname);
  const keys = [key, hostname.toLowerCase()];
  const serviceKey = clusterServiceHostKeyFromPodHost(key);
  if (serviceKey) keys.push(serviceKey);
  return [...new Set(keys)];
}

function extractConnectionHost(source: string, message: string): string | undefined {
  const text = `${source} ${message}`;
  const match = text.match(/Connection \(([^:)]+)(?::\d+)?\)/);
  return match?.[1];
}

function tableLooksLocal(table: string): boolean {
  return /(^|[._])local$/i.test(table) || /_local$/i.test(table);
}

function hasObjectStorageSignal(execution: DistributedQueryExecutionInput): boolean {
  const values = [
    ...(execution.tableEngines ?? []),
    ...(execution.usedStorages ?? []),
    ...(execution.tables ?? []),
    execution.queryPreview ?? '',
  ].join(' ');
  return /\b(Iceberg|IcebergS3|S3|URL|Parquet|Hive)\b/i.test(values);
}

function hasHybridSignal(execution: DistributedQueryExecutionInput): boolean {
  const values = [
    ...(execution.tableEngines ?? []),
    ...(execution.usedStorages ?? []),
    ...(execution.tables ?? []),
    execution.queryPreview ?? '',
  ].join(' ');
  return /\bHybrid\b/i.test(values);
}

function makeNodeId(execution: DistributedQueryExecutionInput, index: number): string {
  return `${execution.queryId}:${hostKey(execution.hostname)}:${execution.queryStartTimeMicroseconds ?? index}`;
}

function clusterHostMap(clusterHosts: ClusterHostInput[] | undefined): Map<string, ClusterHostInput> {
  const map = new Map<string, ClusterHostInput>();
  for (const host of clusterHosts ?? []) {
    for (const key of hostKeys(host.hostName)) {
      map.set(key, host);
    }
  }
  return map;
}

function getClusterHostInfo(hostMap: Map<string, ClusterHostInput>, hostname: string): ClusterHostInput | undefined {
  for (const key of hostKeys(hostname)) {
    const host = hostMap.get(key);
    if (host) return host;
  }
  return undefined;
}

function processorsByQuery(processors: ProcessorProfileInput[] | undefined): Map<string, ProcessorProfileInput[]> {
  const map = new Map<string, ProcessorProfileInput[]>();
  for (const processor of processors ?? []) {
    const list = map.get(processor.queryId);
    if (list) list.push(processor);
    else map.set(processor.queryId, [processor]);
  }
  return map;
}

function hasProcessorPool(processors: ProcessorProfileInput[], poolName: string): boolean {
  return processors.some((processor) => (processor.planStepDescription ?? '').includes(poolName));
}

function hasProcessorName(processors: ProcessorProfileInput[], name: string): boolean {
  return processors.some((processor) =>
    processor.processorName === name ||
    processor.planStepName === name ||
    (processor.planStepDescription ?? '').includes(name),
  );
}

function hasObjectStorageProcessor(processors: ProcessorProfileInput[]): boolean {
  return processors.some((processor) => {
    const values = [
      processor.processorName ?? '',
      processor.planStepName ?? '',
      processor.planStepDescription ?? '',
    ].join(' ');
    return /\b(ObjectStorage|Iceberg|Parquet|S3|ReadFromStorage|ReadFromFile)\b/i.test(values);
  });
}

function hasHybridProcessor(processors: ProcessorProfileInput[]): boolean {
  return processors.some((processor) => {
    const values = [
      processor.processorName ?? '',
      processor.planStepName ?? '',
      processor.planStepDescription ?? '',
    ].join(' ');
    return /\bHybrid\b/i.test(values);
  });
}

function isParallelLeader(execution: DistributedQueryExecutionInput): boolean {
  const events = execution.profileEvents;
  return hasAnyEvent(events, [
    'ParallelReplicasHandle',
    'ParallelReplicasReadAssigned',
    'ParallelReplicasNumRequests',
    'ParallelReplicasUsedCount',
    'ParallelReplicasAvailableCount',
    'ParallelReplicasQueryCount',
  ]);
}

function isParallelReader(execution: DistributedQueryExecutionInput, processors: ProcessorProfileInput[]): boolean {
  const events = execution.profileEvents;
  return hasEvent(events, 'ParallelReplicasReadMarks') ||
    hasEvent(events, 'ParallelReplicasReadRequestMicroseconds') ||
    hasProcessorPool(processors, 'ReadPoolParallelReplicas');
}

function classifyReadRole(
  execution: DistributedQueryExecutionInput,
  processors: ProcessorProfileInput[],
  hasParallelReplicas: boolean,
  capabilities: DistributedTopologyCapabilities,
): { role: TopologyNodeRole; evidence: TopologyEvidence[] } {
  const evidence: TopologyEvidence[] = [];

  if (execution.isInitialQuery) {
    evidence.push({ source: 'query_log', message: 'is_initial_query = 1' });
    return { role: 'coordinator', evidence };
  }

  if (
    capabilities.hybridTableMetadata &&
    (hasHybridSignal(execution) || (capabilities.processorsProfileLog && hasHybridProcessor(processors)))
  ) {
    evidence.push({
      source: capabilities.processorsProfileLog && hasHybridProcessor(processors) ? 'processors_profile_log' : 'query_log',
      message: 'Hybrid table segment signal present',
    });
    return { role: 'hybrid_segment', evidence };
  }

  if (
    (capabilities.objectStorageClusterMetadata || capabilities.icebergMetadata || capabilities.parquetMetadata) &&
    (hasObjectStorageSignal(execution) || (capabilities.processorsProfileLog && hasObjectStorageProcessor(processors)))
  ) {
    evidence.push({
      source: capabilities.processorsProfileLog && hasObjectStorageProcessor(processors) ? 'processors_profile_log' : 'query_log',
      message: 'object-storage table/processor signal present',
    });
    return { role: 'object_storage_worker', evidence };
  }

  if (capabilities.profileEvents && isParallelLeader(execution)) {
    evidence.push({ source: 'profile_events', message: 'ParallelReplicas leader/coordination counters present' });
    return { role: 'shard_leader', evidence };
  }

  if (hasParallelReplicas && isParallelReader(
    capabilities.profileEvents ? execution : { ...execution, profileEvents: {} },
    capabilities.processorsProfileLog ? processors : [],
  )) {
    evidence.push({
      source: hasEvent(execution.profileEvents, 'ParallelReplicasReadMarks') || hasEvent(execution.profileEvents, 'ParallelReplicasReadRequestMicroseconds')
        ? 'profile_events'
        : 'processors_profile_log',
      message: 'ParallelReplicas reader counters or ReadPoolParallelReplicas processor present',
    });
    return { role: 'replica_reader', evidence };
  }

  if (capabilities.processorsProfileLog && hasProcessorName(processors, 'Remote') && !hasProcessorPool(processors, 'ReadPool')) {
    evidence.push({ source: 'processors_profile_log', message: 'Remote processor without local read pool' });
    return { role: 'shard_leader', evidence };
  }

  evidence.push({ source: 'query_log', message: 'child query without parallel-replica counters' });
  return { role: 'remote_child', evidence };
}

function classifyWriteRole(
  execution: DistributedQueryExecutionInput,
  capabilities: DistributedTopologyCapabilities,
): { role: TopologyNodeRole; evidence: TopologyEvidence[] } {
  const evidence: TopologyEvidence[] = [];
  const kind = execution.queryKind ?? '';

  if (kind === 'AsyncInsertFlush') {
    evidence.push({ source: 'query_log', message: 'query_kind = AsyncInsertFlush' });
    return { role: 'async_insert_flush', evidence };
  }

  if (kind === 'Insert' && execution.isInitialQuery) {
    evidence.push({ source: 'query_log', message: 'client-visible Insert with is_initial_query = 1' });
    return { role: 'insert_client', evidence };
  }

  if (kind === 'Insert' && ((capabilities.profileEvents && hasEvent(execution.profileEvents, 'AsyncInsertQuery')) || execution.tables?.some(tableLooksLocal))) {
    evidence.push({
      source: capabilities.profileEvents && hasEvent(execution.profileEvents, 'AsyncInsertQuery') ? 'profile_events' : 'query_log',
      message: 'local Insert forwarding / async insert counters present',
    });
    return { role: 'insert_forwarder', evidence };
  }

  return { role: 'unknown', evidence: [{ source: 'query_log', message: `unclassified write query kind: ${kind || 'unknown'}` }] };
}

function buildNode(
  execution: DistributedQueryExecutionInput,
  index: number,
  role: TopologyNodeRole,
  roleEvidence: TopologyEvidence[],
  hostInfo: ClusterHostInput | undefined,
): DistributedTopologyNode {
  const evidence = [...roleEvidence];
  if (hostInfo) {
    evidence.push({
      source: 'system_clusters',
      message: `host maps to shard ${hostInfo.shardNum}, replica ${hostInfo.replicaNum}`,
    });
  }

  return {
    id: makeNodeId(execution, index),
    queryId: execution.queryId,
    hostname: execution.hostname,
    role,
    shardNum: hostInfo?.shardNum,
    replicaNum: hostInfo?.replicaNum,
    queryKind: execution.queryKind,
    queryStartTimeMicroseconds: execution.queryStartTimeMicroseconds,
    queryDurationMs: num(execution.queryDurationMs),
    readRows: num(execution.readRows),
    readBytes: num(execution.readBytes),
    writtenRows: num(execution.writtenRows),
    writtenBytes: num(execution.writtenBytes),
    queryPreview: execution.queryPreview,
    profileEvents: execution.profileEvents ?? {},
    tables: execution.tables ?? [],
    evidence,
  };
}

function buildShards(nodes: DistributedTopologyNode[]): DistributedTopologyShard[] {
  const byShard = new Map<number, DistributedTopologyShard>();
  for (const node of nodes) {
    if (node.shardNum == null) continue;
    let shard = byShard.get(node.shardNum);
    if (!shard) {
      shard = { shardNum: node.shardNum, readers: [], children: [] };
      byShard.set(node.shardNum, shard);
    }

    if (node.role === 'shard_leader') shard.leader = node;
    else if (node.role === 'replica_reader') shard.readers.push(node);
    else if (node.role !== 'coordinator') shard.children.push(node);
  }

  return [...byShard.values()].sort((a, b) => a.shardNum - b.shardNum);
}

function buildShardCoverage(nodes: DistributedTopologyNode[], shards: DistributedTopologyShard[]): DistributedShardCoverage {
  const coordinator = nodes.find((node) => node.role === 'coordinator' || node.role === 'insert_client');
  const expectedShardSends = num(coordinator?.profileEvents?.Shards);
  return {
    observedShards: shards.length,
    ...(expectedShardSends > 0 ? {
      expectedShardSends,
      allExpectedShardSendsObserved: shards.length >= expectedShardSends,
    } : {}),
  };
}

function inferKind(nodes: DistributedTopologyNode[]): DistributedQueryKind {
  if (nodes.length <= 1) return 'local';
  if (nodes.some((node) => node.role === 'insert_client' || node.role === 'insert_forwarder' || node.role === 'async_insert_flush')) {
    return 'distributed_insert';
  }
  if (nodes.some((node) => node.role === 'hybrid_segment')) {
    return 'hybrid_storage_select';
  }
  if (nodes.some((node) => node.role === 'object_storage_worker')) {
    return 'object_storage_swarm_select';
  }
  if (nodes.some((node) => node.role === 'shard_leader' || node.role === 'replica_reader')) {
    return 'parallel_replicas_select';
  }
  const childNodes = nodes.filter((node) => node.role !== 'coordinator');
  const allChildrenMapped = childNodes.length > 0 && childNodes.every((node) => node.shardNum != null);
  const uniqueShardCount = new Set(childNodes.map((node) => node.shardNum).filter((value) => value != null)).size;
  const hasMultipleChildrenPerShard = uniqueShardCount > 0 && childNodes.length > uniqueShardCount;
  if (allChildrenMapped && hasMultipleChildrenPerShard) return 'cluster_all_replicas';
  if (childNodes.length > 0) return 'plain_distributed_select';
  return 'unknown_distributed';
}

function confidenceFor(nodes: DistributedTopologyNode[], clusterHosts: ClusterHostInput[] | undefined, warnings: string[]): TopologyConfidence {
  if (nodes.length <= 1) return 'high';
  if (warnings.length > 0) return 'low';
  const hasRoleEvidence = nodes.some((node) => node.evidence.some((item) => item.source === 'profile_events' || item.source === 'processors_profile_log'));
  const hasClusterMapping = (clusterHosts?.length ?? 0) > 0 && nodes.some((node) => node.shardNum != null);
  if (hasRoleEvidence && hasClusterMapping) return 'high';
  if (hasRoleEvidence || hasClusterMapping) return 'medium';
  return 'low';
}

export function parseDistributedTextLogPhases(textLogs: DistributedTextLogInput[] = []): DistributedExecutionPhase[] {
  const phases: DistributedExecutionPhase[] = [];

  for (const log of textLogs) {
    const source = log.source ?? '';
    const message = log.message ?? '';
    const eventTimeMicroseconds = log.eventTimeMicroseconds || '';
    const queryId = log.queryId || '';
    const hostname = extractConnectionHost(source, message);

    const scalarMatch = message.match(/Sent data for ([\d,]+) scalars?, total ([\d,]+) rows? .*?(?:compressed|no compression)/);
    if (hostname && scalarMatch) {
      const scalars = parseCount(scalarMatch[1]);
      const rows = parseCount(scalarMatch[2]);
      phases.push({
        kind: 'remote_scalar_exchange',
        source: 'text_log',
        queryId,
        eventTimeMicroseconds,
        hostname,
        title: 'Remote scalar exchange',
        detail: `${hostname} sent ${scalars ?? scalarMatch[1]} scalar${scalars === 1 ? '' : 's'} / ${rows ?? scalarMatch[2]} row${rows === 1 ? '' : 's'}`,
        scalars,
        rows,
      });
      continue;
    }

    const mergeMatch = message.match(/Read ([\d,]+) blocks of partially aggregated data, total ([\d,]+) rows/i);
    if (mergeMatch) {
      const blocks = parseCount(mergeMatch[1]);
      const rows = parseCount(mergeMatch[2]);
      phases.push({
        kind: 'merge_partial_aggregation',
        source: 'text_log',
        queryId,
        eventTimeMicroseconds,
        title: 'Merge partial aggregation',
        detail: `Read ${blocks ?? mergeMatch[1]} partial block${blocks === 1 ? '' : 's'} / ${rows ?? mergeMatch[2]} row${rows === 1 ? '' : 's'}`,
        blocks,
        rows,
      });
      continue;
    }

    const outputMatch = message.match(/Converted aggregated data to chunks\. ([\d,]+) rows, ([\d.]+ (?:B|KiB|MiB|GiB|TiB|KB|MB|GB|TB)) in ([\d.]+) sec/i);
    if (outputMatch) {
      const rows = parseCount(outputMatch[1]);
      phases.push({
        kind: 'aggregation_output',
        source: 'text_log',
        queryId,
        eventTimeMicroseconds,
        title: 'Aggregation output',
        detail: `Converted ${rows ?? outputMatch[1]} row${rows === 1 ? '' : 's'} to chunks`,
        rows,
        bytesText: outputMatch[2],
        durationMs: parseDurationSeconds(outputMatch[3]),
      });
      continue;
    }

    const finalReadMatch = message.match(/^Read ([\d,]+) rows, ([\d.]+ (?:B|KiB|MiB|GiB|TiB|KB|MB|GB|TB)) in ([\d.]+) sec\.,/i);
    if (finalReadMatch) {
      const rows = parseCount(finalReadMatch[1]);
      phases.push({
        kind: 'final_read',
        source: 'text_log',
        queryId,
        eventTimeMicroseconds,
        title: 'Final read summary',
        detail: `Read ${rows ?? finalReadMatch[1]} row${rows === 1 ? '' : 's'}`,
        rows,
        bytesText: finalReadMatch[2],
        durationMs: parseDurationSeconds(finalReadMatch[3]),
      });
      continue;
    }

    const memoryMatch = message.match(/Query peak memory usage: ([\d.]+ (?:B|KiB|MiB|GiB|TiB|KB|MB|GB|TB))\./i);
    if (memoryMatch) {
      phases.push({
        kind: 'memory_peak',
        source: 'text_log',
        queryId,
        eventTimeMicroseconds,
        title: 'Peak memory',
        detail: `Peak memory ${memoryMatch[1]}`,
        bytesText: memoryMatch[1],
      });
      continue;
    }

    const profileEventsMatch = message.match(/Sending profile events block with ([\d,]+) rows, ([\d,]+) bytes took ([\d,]+) milliseconds/i);
    if (profileEventsMatch) {
      const rows = parseCount(profileEventsMatch[1]);
      phases.push({
        kind: 'profile_events',
        source: 'text_log',
        queryId,
        eventTimeMicroseconds,
        title: 'Profile events sent',
        detail: `Sent ${rows ?? profileEventsMatch[1]} profile event row${rows === 1 ? '' : 's'}`,
        rows,
        bytesText: `${profileEventsMatch[2]} bytes`,
        durationMs: parseCount(profileEventsMatch[3]),
      });
    }
  }

  return phases;
}

function attachPhaseOffsets(
  phases: DistributedExecutionPhase[],
  executions: DistributedQueryExecutionInput[],
  rootQueryId: string,
): DistributedExecutionPhase[] {
  const rootExecution =
    executions.find(execution => execution.queryId === rootQueryId && execution.isInitialQuery) ??
    executions.find(execution => execution.isInitialQuery) ??
    executions[0];
  const rootStartMs = parseTimestampMs(rootExecution?.queryStartTimeMicroseconds);
  if (rootStartMs == null) return phases;

  return phases.map((phase) => {
    const phaseMs = parseTimestampMs(phase.eventTimeMicroseconds);
    if (phaseMs == null) return phase;
    return {
      ...phase,
      offsetMs: Math.max(0, phaseMs - rootStartMs),
    };
  });
}

function actorLabelForNode(node: DistributedTopologyNode): string {
  if (node.role === 'coordinator' || node.role === 'insert_client') return 'Coordinator';
  if (node.shardNum != null && node.replicaNum != null) return `s${node.shardNum}r${node.replicaNum}`;
  return normalizeHost(node.hostname);
}

function eventDetailMetrics(rows: number, bytes: number): string {
  const parts: string[] = [];
  if (rows > 0) parts.push(`${rows.toLocaleString()} rows`);
  if (bytes > 0) parts.push(`${bytes.toLocaleString()} bytes`);
  return parts.join(' / ');
}

function buildExecutionFlow(
  nodes: DistributedTopologyNode[],
  phases: DistributedExecutionPhase[],
  rootQueryId: string,
): DistributedExecutionFlowEvent[] {
  const rootNode =
    nodes.find(node => node.queryId === rootQueryId && (node.role === 'coordinator' || node.role === 'insert_client')) ??
    nodes.find(node => node.role === 'coordinator' || node.role === 'insert_client') ??
    nodes[0];
  const rootStartMs = parseTimestampMs(rootNode?.queryStartTimeMicroseconds);
  if (!rootNode || rootStartMs == null) return [];

  const events: DistributedExecutionFlowEvent[] = [{
    kind: 'coordinator_started',
    source: 'query_log',
    offsetMs: 0,
    actor: actorLabelForNode(rootNode),
    actorType: 'coordinator',
    title: 'Coordinator started',
    detail: 'Top-level query accepted by the coordinator.',
    queryId: rootNode.queryId,
    hostname: rootNode.hostname,
  }];

  for (const node of nodes) {
    if (node.id === rootNode.id || node.role === 'coordinator' || node.role === 'insert_client') continue;
    const startMs = parseTimestampMs(node.queryStartTimeMicroseconds);
    if (startMs == null) continue;
    const startOffsetMs = Math.max(0, startMs - rootStartMs);
    const actor = actorLabelForNode(node);

    events.push({
      kind: 'remote_started',
      source: 'query_log',
      offsetMs: startOffsetMs,
      actor,
      actorType: 'remote',
      title: 'Remote read started',
      detail: `${actor} started ${node.queryKind || 'query'} on ${normalizeHost(node.hostname)}.`,
      queryId: node.queryId,
      hostname: node.hostname,
    });

    events.push({
      kind: 'remote_read_completed',
      source: 'query_log',
      offsetMs: startOffsetMs + node.queryDurationMs,
      actor,
      actorType: 'remote',
      title: 'Remote read completed',
      detail: eventDetailMetrics(node.readRows, node.readBytes) || 'Remote child query completed.',
      queryId: node.queryId,
      hostname: node.hostname,
      rows: node.readRows,
      durationMs: node.queryDurationMs,
    });
  }

  const nodeQueryIds = new Set(nodes.filter(node => node.id !== rootNode.id).map(node => node.queryId));
  for (const phase of phases) {
    if (phase.offsetMs == null) continue;
    if (phase.kind === 'final_read' && nodeQueryIds.has(phase.queryId)) continue;

    if (phase.kind === 'remote_scalar_exchange') {
      events.push({
        kind: 'remote_setup',
        source: 'text_log',
        offsetMs: phase.offsetMs,
        actor: normalizeHost(phase.hostname ?? 'remote'),
        actorType: 'remote',
        title: 'Remote setup',
        detail: phase.detail,
        queryId: phase.queryId,
        hostname: phase.hostname,
        rows: phase.rows,
      });
    } else if (phase.kind === 'merge_partial_aggregation') {
      events.push({
        kind: 'coordinator_merge',
        source: 'text_log',
        offsetMs: phase.offsetMs,
        actor: 'Coordinator',
        actorType: 'coordinator',
        title: 'Coordinator merge',
        detail: phase.detail,
        queryId: phase.queryId,
        rows: phase.rows,
      });
    } else if (phase.kind === 'aggregation_output') {
      events.push({
        kind: 'coordinator_output',
        source: 'text_log',
        offsetMs: phase.offsetMs,
        actor: 'Coordinator',
        actorType: 'coordinator',
        title: 'Result output',
        detail: phase.detail,
        queryId: phase.queryId,
        rows: phase.rows,
        bytesText: phase.bytesText,
        durationMs: phase.durationMs,
      });
    } else if (phase.kind === 'final_read' && phase.queryId === rootNode.queryId) {
      events.push({
        kind: 'coordinator_read_completed',
        source: 'text_log',
        offsetMs: phase.offsetMs,
        actor: 'Coordinator',
        actorType: 'coordinator',
        title: 'Coordinator read completed',
        detail: phase.detail,
        queryId: phase.queryId,
        rows: phase.rows,
        bytesText: phase.bytesText,
        durationMs: phase.durationMs,
      });
    }
  }

  return events
    .sort((a, b) => a.offsetMs - b.offsetMs || a.actor.localeCompare(b.actor) || a.title.localeCompare(b.title))
    .slice(0, 200);
}

function nodeForExecutionFlowEvent(
  event: DistributedExecutionFlowEvent,
  nodes: DistributedTopologyNode[],
): DistributedTopologyNode | undefined {
  if (event.actorType === 'coordinator') {
    return nodes.find((node) => node.role === 'coordinator' || node.role === 'insert_client');
  }

  const remoteNodes = nodes.filter((node) => node.role !== 'coordinator' && node.role !== 'insert_client');
  return remoteNodes.find((node) =>
    event.queryId &&
    node.queryId === event.queryId &&
    event.hostname &&
    hostKey(node.hostname) === hostKey(event.hostname)
  ) ?? remoteNodes.find((node) =>
    event.hostname && hostKey(node.hostname) === hostKey(event.hostname)
  ) ?? remoteNodes.find((node) =>
    event.queryId && node.queryId === event.queryId
  );
}

function groupIdForNode(node: DistributedTopologyNode | undefined): string | undefined {
  if (!node || node.shardNum == null) return undefined;
  return `shard:${node.shardNum}`;
}

function parentNodeForExecutionFlowNode(
  node: DistributedTopologyNode | undefined,
  nodes: DistributedTopologyNode[],
): DistributedTopologyNode | undefined {
  if (!node || node.role !== 'replica_reader' || node.shardNum == null) return undefined;
  return nodes.find((candidate) =>
    candidate.role === 'shard_leader' &&
    candidate.shardNum === node.shardNum
  );
}

function depthForExecutionFlowNode(
  event: DistributedExecutionFlowEvent,
  node: DistributedTopologyNode | undefined,
  parentNode: DistributedTopologyNode | undefined,
): number {
  if (event.actorType === 'coordinator') return 0;
  if (parentNode) return 2;
  return node?.role === 'replica_reader' ? 1 : 1;
}

export function buildDistributedExecutionFlowSteps(topology: DistributedTopology): DistributedExecutionFlowStep[] {
  const previewShown = new Set<string>();

  return topology.executionFlow
    .filter((event) => event.kind !== 'remote_setup')
    .map((event) => {
      const node = nodeForExecutionFlowEvent(event, topology.nodes);
      const parentNode = parentNodeForExecutionFlowNode(node, topology.nodes);
      const previewKey = node?.id ?? `${event.queryId ?? ''}:${hostKey(event.hostname ?? '')}:${event.offsetMs}`;
      const showQueryPreview = (event.kind === 'coordinator_started' || event.kind === 'remote_started') && Boolean(node?.queryPreview) && !previewShown.has(previewKey);
      if (showQueryPreview) previewShown.add(previewKey);

      return {
        event,
        node,
        showQueryPreview,
        groupId: groupIdForNode(node),
        parentNodeId: parentNode?.id,
        depth: depthForExecutionFlowNode(event, node, parentNode),
      };
    });
}

export function inferDistributedTopology(input: DistributedTopologyInput): DistributedTopology {
  const capabilities = resolveCapabilities(input);
  const detectors = activeDetectors(input, capabilities);
  const hostMap = capabilities.systemClusters ? clusterHostMap(input.clusterHosts) : new Map<string, ClusterHostInput>();
  const processorMap = capabilities.processorsProfileLog ? processorsByQuery(input.processorProfiles) : new Map<string, ProcessorProfileInput[]>();
  const executions = [...input.executions].sort((a, b) =>
    String(a.queryStartTimeMicroseconds ?? '').localeCompare(String(b.queryStartTimeMicroseconds ?? '')),
  );
  const hasParallelReplicas = executions.some((execution) =>
    (capabilities.profileEvents && isParallelLeader(execution)) ||
    isParallelReader(
      capabilities.profileEvents ? execution : { ...execution, profileEvents: {} },
      capabilities.processorsProfileLog ? (processorMap.get(execution.queryId) ?? []) : [],
    ),
  );
  const hasWritePath = executions.some((execution) => ['Insert', 'AsyncInsertFlush'].includes(execution.queryKind ?? ''));
  const executionPhases = capabilities.textLog
    ? attachPhaseOffsets(parseDistributedTextLogPhases(input.textLogs), executions, input.rootQueryId)
    : [];
  const warnings: string[] = [];
  const decisions: TopologyDecision[] = [];

  if (executions.length === 0) {
    warnings.push('No query executions supplied');
    addDecision(decisions, 'warning', 'no-executions', 'query_log', 'No query executions were supplied for topology inference.');
  }

  if (!capabilities.queryLog) {
    warnings.push('system.query_log is unavailable; distributed topology cannot be inferred reliably');
    addDecision(decisions, 'degraded', 'missing-query-log', 'capability', 'system.query_log is unavailable, so only caller-supplied rows can be used.');
  }
  if (!capabilities.profileEvents) {
    addDecision(decisions, 'degraded', 'missing-profile-events', 'capability', 'ProfileEvents are unavailable; role detection falls back to query shape and processor data.');
  }
  if (!capabilities.systemClusters) {
    addDecision(decisions, 'degraded', 'missing-system-clusters', 'capability', 'system.clusters mapping is unavailable; shard and replica labels may be unknown.');
  }
  if (!capabilities.processorsProfileLog) {
    addDecision(decisions, 'info', 'missing-processors-profile-log', 'capability', 'processors_profile_log is unavailable; phase labels use query_log/ProfileEvents only.');
  }
  if (!capabilities.textLog) {
    addDecision(decisions, 'info', 'missing-text-log', 'capability', 'system.text_log was not used for execution phase enrichment.');
  }
  if (!capabilities.objectStorageClusterMetadata) {
    addDecision(decisions, 'info', 'missing-object-storage-cluster-metadata', 'capability', 'object-storage cluster metadata was not used; swarm execution can only be inferred from query rows.');
  }
  if (!capabilities.icebergMetadata && !capabilities.parquetMetadata) {
    addDecision(decisions, 'info', 'missing-object-storage-file-metadata', 'capability', 'Iceberg/Parquet metadata was not used; file, partition, manifest, and row-group work units are omitted.');
  }
  if (!capabilities.hybridTableMetadata) {
    addDecision(decisions, 'info', 'missing-hybrid-table-metadata', 'capability', 'Hybrid table metadata was not used; hot/cold storage segment pruning cannot be shown.');
  }

  for (const detector of detectors) {
    addDecision(decisions, 'info', `detector:${detector.id}`, detectorSource(detector), `${detector.label} is active.`);
  }

  const nodes = executions.map((execution, index) => {
    const hostInfo = getClusterHostInfo(hostMap, execution.hostname);
    const processors = processorMap.get(execution.queryId) ?? [];
    const classified = hasWritePath
      ? classifyWriteRole(execution, capabilities)
      : classifyReadRole(execution, processors, hasParallelReplicas, capabilities);
    return buildNode(execution, index, classified.role, classified.evidence, hostInfo);
  });

  const coordinator = nodes.find((node) => node.role === 'coordinator' || node.role === 'insert_client');
  const shards = buildShards(nodes);
  const shardCoverage = buildShardCoverage(nodes, shards);
  const kind = inferKind(nodes);
  const executionFlow = buildExecutionFlow(nodes, executionPhases, input.rootQueryId);
  const evidence: TopologyEvidence[] = [];

  if ((input.clusterHosts?.length ?? 0) > 0) {
    evidence.push({ source: 'system_clusters', message: `${input.clusterHosts?.length ?? 0} host-to-shard row(s) loaded` });
    addDecision(decisions, 'info', 'cluster-hosts-loaded', 'system_clusters', `${input.clusterHosts?.length ?? 0} host-to-shard row(s) loaded from system.clusters.`);
  }
  if (hasParallelReplicas) {
    evidence.push({ source: 'profile_events', message: 'parallel-replica counters or processors detected' });
    addDecision(decisions, 'info', 'parallel-replicas-detected', 'profile_events', 'Parallel-replica counters or processor phases were detected.');
  }
  if (hasWritePath) {
    evidence.push({ source: 'query_log', message: 'write-side query kinds detected' });
    addDecision(decisions, 'info', 'write-path-detected', 'query_log', 'Insert or AsyncInsertFlush query kinds were detected.');
  }
  if (capabilities.textLog && executionPhases.length > 0) {
    evidence.push({ source: 'text_log', message: `${executionPhases.length} execution event(s) parsed` });
    addDecision(decisions, 'info', 'text-log-phases-detected', 'text_log', `${executionPhases.length} execution event(s) were parsed from system.text_log.`);
  } else if (capabilities.textLog) {
    addDecision(decisions, 'info', 'text-log-no-phases', 'text_log', 'system.text_log was available, but no execution events matched known distributed-query patterns.');
  }
  if (nodes.some((node) => node.role === 'object_storage_worker')) {
    evidence.push({ source: 'query_log', message: 'object-storage worker rows detected' });
    addDecision(decisions, 'info', 'object-storage-execution-detected', 'query_log', 'Object-storage execution was detected from table engines, storages, settings, or processor phases.');
  }
  if (nodes.some((node) => node.role === 'hybrid_segment')) {
    evidence.push({ source: 'query_log', message: 'Hybrid storage segment rows detected' });
    addDecision(decisions, 'info', 'hybrid-storage-detected', 'query_log', 'Hybrid storage segments were detected from table engines, storages, or processor phases.');
  }

  if (nodes.length > 1 && !coordinator) {
    warnings.push('No coordinator/client root row found');
    addDecision(decisions, 'warning', 'missing-coordinator', 'query_log', 'No coordinator/client root row was found among the supplied executions.');
  }

  return {
    kind,
    rootQueryId: input.rootQueryId,
    capabilities,
    coordinator,
    nodes,
    shards,
    shardCoverage,
    confidence: confidenceFor(nodes, capabilities.systemClusters ? input.clusterHosts : undefined, warnings),
    evidence,
    decisions,
    executionPhases,
    executionFlow,
    detectorPlugins: detectors.map((detector) => detector.id),
    warnings,
  };
}

export function formatDistributedTopologyReport(topology: DistributedTopology): string {
  const lines: string[] = [
    `Distributed topology report for ${topology.rootQueryId}`,
    `Kind: ${topology.kind}`,
    `Confidence: ${topology.confidence}`,
    `Nodes: ${topology.nodes.length}`,
  ];

  if (topology.shards.length > 0) {
    lines.push(`Shards: ${topology.shards.map((shard) => {
      const parts = [`shard ${shard.shardNum}`];
      if (shard.leader) parts.push(`leader ${shard.leader.hostname}`);
      if (shard.readers.length > 0) parts.push(`${shard.readers.length} reader(s)`);
      if (shard.children.length > 0) parts.push(`${shard.children.length} child query(s)`);
      return parts.join(' - ');
    }).join('; ')}`);
  }

  const activeCapabilities = Object.entries(topology.capabilities)
    .filter(([, enabled]) => enabled)
    .map(([name]) => name);
  lines.push(`Capabilities used: ${activeCapabilities.length > 0 ? activeCapabilities.join(', ') : 'none'}`);

  if (topology.detectorPlugins.length > 0) {
    lines.push(`Detectors: ${topology.detectorPlugins.join(', ')}`);
  }

  if (topology.executionPhases.length > 0) {
    lines.push(`Execution phases: ${topology.executionPhases.map((phase) => phase.title).join(', ')}`);
  }

  if (topology.warnings.length > 0) {
    lines.push('Warnings:');
    for (const warning of topology.warnings) {
      lines.push(`- ${warning}`);
    }
  }

  if (topology.decisions.length > 0) {
    lines.push('Decision trace:');
    for (const decision of topology.decisions) {
      lines.push(`- [${decision.level}] ${decision.code}: ${decision.message}`);
    }
  }

  return lines.join('\n');
}
