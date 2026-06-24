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
  | 'nested_coordinator'
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

export type DistributedFanoutMode =
  | 'none'
  | 'one_replica_per_shard'
  | 'all_replicas'
  | 'parallel_replicas'
  | 'unknown';

export type TopologyCapabilityId =
  | 'query_log'
  | 'profile_events'
  | 'processors_profile_log'
  | 'system_clusters'
  | 'text_log'
  | 'asynchronous_insert_log'
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
  normalizedQueryHash?: string;
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

export interface AsyncInsertLogInput {
  queryId: string;
  flushQueryId: string;
  hostname?: string;
  database?: string;
  table?: string;
  status?: string;
  exception?: string;
  rows?: number;
  bytes?: number;
  eventTimeMicroseconds?: string;
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
  | 'async_insert_buffered'
  | 'local_read_started'
  | 'local_read_completed'
  | 'remote_started'
  | 'remote_setup'
  | 'remote_read_completed'
  | 'coordinator_merge'
  | 'coordinator_output'
  | 'coordinator_read_completed';

export interface DistributedExecutionFlowEvent {
  kind: DistributedExecutionFlowEventKind;
  source: 'query_log' | 'text_log' | 'asynchronous_insert_log';
  offsetMs: number;
  actor: string;
  actorType: 'coordinator' | 'local' | 'remote';
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
  asynchronousInsertLog: boolean;
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
  asyncInsertLogs?: AsyncInsertLogInput[];
  capabilities?: Partial<DistributedTopologyCapabilities>;
}

export interface TopologyEvidence {
  source: 'query_log' | 'profile_events' | 'processors_profile_log' | 'system_clusters' | 'text_log' | 'asynchronous_insert_log' | 'capability';
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
  normalizedQueryHash?: string;
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
  memoryUsage: number;
  queryPreview?: string;
  settings: Record<string, string | number | boolean | undefined>;
  profileEvents: ProfileEventsMap;
  tables: string[];
  evidence: TopologyEvidence[];
}

export interface DistributedAsyncInsertLink {
  source: 'asynchronous_insert_log';
  queryId: string;
  flushQueryId: string;
  hostname?: string;
  database?: string;
  table?: string;
  status?: string;
  exception?: string;
  rows?: number;
  bytes?: number;
  eventTimeMicroseconds?: string;
  confidence: TopologyConfidence;
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

export interface ClusterAllReplicasMarker {
  fanoutMode: 'all_replicas';
  cluster?: string;
  expectedParticipants?: number;
  observedRemoteChildren: number;
  localParticipantsOnInitiator: number;
  allExpectedParticipantsAccounted?: boolean;
  evidence: TopologyEvidence[];
}

export interface FoldedLocalReadParticipant {
  kind: 'folded_into_coordinator';
  hostname: string;
  shardNum?: number;
  replicaNum?: number;
  queryId: string;
  readRows?: number;
  readBytes?: number;
  selectedParts?: number;
  selectedMarks?: number;
  confidence: TopologyConfidence;
  evidence: TopologyEvidence[];
}

export interface DistributedReadDistributionEntry {
  participantId: string;
  queryId: string;
  normalizedQueryHash?: string;
  queryPreview?: string;
  hostname: string;
  role: TopologyNodeRole | 'local_reader';
  shardNum?: number;
  replicaNum?: number;
  readRows: number;
  readBytes: number;
  durationMs: number;
  memoryUsage: number;
  selectedParts: number;
  selectedMarks: number;
  rowShare: number;
  byteShare: number;
  foldedIntoCoordinator: boolean;
}

export type DistributedSkewMetric =
  | 'read_rows'
  | 'read_bytes'
  | 'duration_ms'
  | 'memory_usage'
  | 'selected_parts'
  | 'selected_marks';

export interface DistributedSkewMetricSummary {
  metric: DistributedSkewMetric;
  total: number;
  min: number;
  max: number;
  average: number;
  maxShare: number;
  skewRatio: number;
  maxParticipantId?: string;
  maxHostname?: string;
  maxShardNum?: number;
  maxReplicaNum?: number;
  severity: 'none' | 'low' | 'medium' | 'high';
}

export interface DistributedResourceSkew {
  participantCount: number;
  metrics: DistributedSkewMetricSummary[];
}

export interface DistributedReadDistributionGroup {
  key: string;
  label: string;
  queryPreview?: string;
  shardCount: number;
  shardCoordinatorCount: number;
  nestedCoordinatorCount: number;
  readerCount: number;
  remoteChildCount: number;
  entries: DistributedReadDistributionEntry[];
  skew: DistributedResourceSkew;
}

export interface DistributedReadDistributionShard {
  shardNum: number;
  readRows: number;
  readBytes: number;
  rowShare: number;
  byteShare: number;
  replicas: DistributedReadDistributionEntry[];
  hasPerReplicaReading: boolean;
}

export interface DistributedReadDistribution {
  totalReadRows: number;
  totalReadBytes: number;
  entries: DistributedReadDistributionEntry[];
  groups: DistributedReadDistributionGroup[];
  shards: DistributedReadDistributionShard[];
  hasPerReplicaReading: boolean;
  skew: DistributedResourceSkew;
}

export interface DistributedTopology {
  kind: DistributedQueryKind;
  fanoutMode: DistributedFanoutMode;
  rootQueryId: string;
  capabilities: DistributedTopologyCapabilities;
  coordinator?: DistributedTopologyNode;
  nodes: DistributedTopologyNode[];
  shards: DistributedTopologyShard[];
  shardCoverage: DistributedShardCoverage;
  clusterAllReplicas?: ClusterAllReplicasMarker;
  localRead?: FoldedLocalReadParticipant;
  asyncInsertLinks: DistributedAsyncInsertLink[];
  readDistribution: DistributedReadDistribution;
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

export function distributedQueryKindLabel(kind?: DistributedQueryKind): string {
  switch (kind) {
    case 'local': return 'Local';
    case 'plain_distributed_select': return 'Distributed SELECT';
    case 'parallel_replicas_select': return 'Parallel replicas';
    case 'cluster_all_replicas': return 'All replicas fan-out';
    case 'object_storage_swarm_select': return 'Object storage swarm';
    case 'hybrid_storage_select': return 'Hybrid storage';
    case 'distributed_insert': return 'Distributed INSERT';
    case 'unknown_distributed': return 'Distributed';
    default: return 'Unknown';
  }
}

export function topologyNodeRoleLabel(role: TopologyNodeRole | 'local_reader'): string {
  switch (role) {
    case 'coordinator': return 'Coordinator';
    case 'shard_leader': return 'Shard coordinator';
    case 'nested_coordinator': return 'Nested coordinator';
    case 'replica_reader': return 'Reader';
    case 'remote_child': return 'Remote child';
    case 'independent_child': return 'Independent child';
    case 'object_storage_worker': return 'Object worker';
    case 'hybrid_segment': return 'Hybrid segment';
    case 'insert_client': return 'Insert client';
    case 'insert_forwarder': return 'Remote table INSERT';
    case 'async_insert_flush': return 'Async insert flush';
    case 'local_reader': return 'Local reader';
    default: return 'Unknown';
  }
}

export function topologyNodeRoleText(role: TopologyNodeRole | 'local_reader'): string {
  switch (role) {
    case 'shard_leader': return 'shard coordinator';
    case 'nested_coordinator': return 'nested coordinator';
    case 'replica_reader': return 'reader';
    case 'local_reader': return 'local reader';
    default: return role.replace(/_/g, ' ');
  }
}

const DEFAULT_CAPABILITIES: DistributedTopologyCapabilities = {
  queryLog: true,
  profileEvents: true,
  processorsProfileLog: false,
  systemClusters: false,
  textLog: false,
  asynchronousInsertLog: false,
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
    id: 'async-insert-log-linker',
    label: 'asynchronous_insert_log query to flush linker',
    requiredCapabilities: ['asynchronous_insert_log'],
    applies: (input, capabilities) =>
      capabilities.asynchronousInsertLog && (input.asyncInsertLogs?.length ?? 0) > 0,
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
    asynchronousInsertLog: (input.asyncInsertLogs?.length ?? 0) > 0,
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
  if (detector.requiredCapabilities.includes('asynchronous_insert_log')) return 'asynchronous_insert_log';
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

function clusterAllReplicasClusterName(query: string | undefined): string | undefined {
  if (!query) return undefined;
  const match = query.match(/\bclusterAllReplicas\s*\(\s*'([^']+)'/i);
  return match?.[1];
}

function usesClusterAllReplicas(execution: DistributedQueryExecutionInput | undefined): boolean {
  return Boolean(clusterAllReplicasClusterName(execution?.queryPreview) || /\bclusterAllReplicas\s*\(/i.test(execution?.queryPreview ?? ''));
}

function usesClusterTableFunction(query: string | undefined): boolean {
  return /\bcluster(?:AllReplicas)?\s*\(/i.test(query ?? '');
}

function clusterHostParticipantKey(host: Pick<ClusterHostInput, 'hostName' | 'shardNum' | 'replicaNum'>): string {
  return `${host.shardNum}:${host.replicaNum}`;
}

function executionParticipantKey(node: DistributedTopologyNode): string | undefined {
  if (node.shardNum == null || node.replicaNum == null) return undefined;
  return `${node.shardNum}:${node.replicaNum}`;
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

  if (usesClusterTableFunction(execution.queryPreview)) {
    evidence.push({
      source: 'query_log',
      message: 'non-initial child query contains nested cluster()/clusterAllReplicas() fan-out',
    });
    return { role: 'nested_coordinator', evidence };
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
    normalizedQueryHash: execution.normalizedQueryHash,
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
    memoryUsage: num(execution.memoryUsage),
    queryPreview: execution.queryPreview,
    settings: execution.settings ?? {},
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

function inferFanoutMode(kind: DistributedQueryKind, clusterAllReplicas?: ClusterAllReplicasMarker): DistributedFanoutMode {
  if (clusterAllReplicas) return 'all_replicas';
  if (kind === 'parallel_replicas_select') return 'parallel_replicas';
  if (kind === 'plain_distributed_select') return 'one_replica_per_shard';
  if (kind === 'local') return 'none';
  return 'unknown';
}

function buildClusterAllReplicasMarker(
  rootExecution: DistributedQueryExecutionInput | undefined,
  coordinator: DistributedTopologyNode | undefined,
  nodes: DistributedTopologyNode[],
  clusterHosts: ClusterHostInput[] | undefined,
  capabilities: DistributedTopologyCapabilities,
): { marker?: ClusterAllReplicasMarker; localRead?: FoldedLocalReadParticipant } {
  if (!rootExecution || !usesClusterAllReplicas(rootExecution)) return {};

  const cluster = clusterAllReplicasClusterName(rootExecution.queryPreview);
  const eligibleHosts = (clusterHosts ?? [])
    .filter(host => host.hostName && host.shardNum > 0 && host.replicaNum > 0);
  const namedClusterHosts = cluster
    ? eligibleHosts.filter(host => host.cluster === cluster)
    : eligibleHosts;
  const targetHosts = namedClusterHosts.length > 0 ? namedClusterHosts : eligibleHosts;

  const expectedParticipantKeys = new Set(targetHosts.map(clusterHostParticipantKey));
  const remoteChildren = nodes.filter(node => node.role !== 'coordinator' && node.role !== 'insert_client');
  const observedRemoteParticipantKeys = new Set(remoteChildren.map(executionParticipantKey).filter((value): value is string => Boolean(value)));
  const coordinatorParticipantKey = coordinator ? executionParticipantKey(coordinator) : undefined;
  const coordinatorIsExpectedParticipant = Boolean(coordinatorParticipantKey && expectedParticipantKeys.has(coordinatorParticipantKey));
  const coordinatorAlsoHasRemoteChild = Boolean(coordinatorParticipantKey && observedRemoteParticipantKeys.has(coordinatorParticipantKey));
  const localParticipantsOnInitiator = coordinatorIsExpectedParticipant && !coordinatorAlsoHasRemoteChild ? 1 : 0;
  const expectedParticipants = expectedParticipantKeys.size > 0 ? expectedParticipantKeys.size : undefined;
  const accountedParticipants = observedRemoteParticipantKeys.size + localParticipantsOnInitiator;
  const allExpectedParticipantsAccounted = expectedParticipants != null
    ? accountedParticipants >= expectedParticipants
    : undefined;

  const evidence: TopologyEvidence[] = [
    {
      source: 'query_log',
      message: cluster
        ? `root query uses clusterAllReplicas('${cluster}', ...)`
        : 'root query uses clusterAllReplicas(...)',
    },
  ];

  if (capabilities.systemClusters && expectedParticipants != null) {
    evidence.push({
      source: 'system_clusters',
      message: `clusterAllReplicas target has ${expectedParticipants} configured participant(s)`,
    });
  }

  const marker: ClusterAllReplicasMarker = {
    fanoutMode: 'all_replicas',
    cluster,
    expectedParticipants,
    observedRemoteChildren: remoteChildren.length,
    localParticipantsOnInitiator,
    allExpectedParticipantsAccounted,
    evidence,
  };

  const selectedParts = num(coordinator?.profileEvents?.SelectedParts);
  const selectedMarks = num(coordinator?.profileEvents?.SelectedMarks);
  const localRead = coordinator && localParticipantsOnInitiator > 0 ? {
    kind: 'folded_into_coordinator' as const,
    hostname: coordinator.hostname,
    shardNum: coordinator.shardNum,
    replicaNum: coordinator.replicaNum,
    queryId: coordinator.queryId,
    readRows: coordinator.readRows,
    readBytes: coordinator.readBytes,
    ...(selectedParts > 0 ? { selectedParts } : {}),
    ...(selectedMarks > 0 ? { selectedMarks } : {}),
    confidence: capabilities.systemClusters ? 'high' as const : 'medium' as const,
    evidence: [
      ...evidence,
      {
        source: 'query_log' as const,
        message: 'initiator host is an addressed cluster replica; local read is folded into the initial query row',
      },
    ],
  } : undefined;

  return { marker, localRead };
}

function buildReadDistribution(
  nodes: DistributedTopologyNode[],
  localRead?: FoldedLocalReadParticipant,
): DistributedReadDistribution {
  const entriesWithoutShares: Omit<DistributedReadDistributionEntry, 'rowShare' | 'byteShare'>[] = [];
  const shardsWithReplicaReaders = new Set(
    nodes
      .filter(node => node.role === 'replica_reader' && node.shardNum != null)
      .map(node => `${node.normalizedQueryHash || 'unknown'}:${node.shardNum as number}`),
  );

  for (const node of nodes) {
    if (node.role === 'coordinator' || node.role === 'insert_client') continue;
    if (node.role === 'shard_leader' && node.shardNum != null && shardsWithReplicaReaders.has(`${node.normalizedQueryHash || 'unknown'}:${node.shardNum}`)) continue;
    if (
      node.readRows <= 0 &&
      node.readBytes <= 0 &&
      node.queryDurationMs <= 0 &&
      node.memoryUsage <= 0 &&
      num(node.profileEvents.SelectedParts) <= 0 &&
      num(node.profileEvents.SelectedMarks) <= 0
    ) continue;
    entriesWithoutShares.push({
      participantId: node.id,
      queryId: node.queryId,
      normalizedQueryHash: node.normalizedQueryHash,
      queryPreview: node.queryPreview,
      hostname: node.hostname,
      role: node.role,
      shardNum: node.shardNum,
      replicaNum: node.replicaNum,
      readRows: node.readRows,
      readBytes: node.readBytes,
      durationMs: node.queryDurationMs,
      memoryUsage: node.memoryUsage,
      selectedParts: num(node.profileEvents.SelectedParts),
      selectedMarks: num(node.profileEvents.SelectedMarks),
      foldedIntoCoordinator: false,
    });
  }

  if (localRead && ((localRead.readRows ?? 0) > 0 || (localRead.readBytes ?? 0) > 0)) {
    entriesWithoutShares.push({
      participantId: `local:${localRead.queryId}:${hostKey(localRead.hostname)}`,
      queryId: localRead.queryId,
      queryPreview: undefined,
      hostname: localRead.hostname,
      role: 'local_reader',
      shardNum: localRead.shardNum,
      replicaNum: localRead.replicaNum,
      readRows: localRead.readRows ?? 0,
      readBytes: localRead.readBytes ?? 0,
      durationMs: 0,
      memoryUsage: 0,
      selectedParts: localRead.selectedParts ?? 0,
      selectedMarks: localRead.selectedMarks ?? 0,
      foldedIntoCoordinator: true,
    });
  }

  const totalReadRows = entriesWithoutShares.reduce((sum, entry) => sum + entry.readRows, 0);
  const totalReadBytes = entriesWithoutShares.reduce((sum, entry) => sum + entry.readBytes, 0);

  const entries = entriesWithoutShares
    .map((entry): DistributedReadDistributionEntry => ({
      ...entry,
      rowShare: totalReadRows > 0 ? entry.readRows / totalReadRows : 0,
      byteShare: totalReadBytes > 0 ? entry.readBytes / totalReadBytes : 0,
    }))
    .sort((a, b) => b.readBytes - a.readBytes || b.readRows - a.readRows || a.hostname.localeCompare(b.hostname));

  const entriesByShard = new Map<number, DistributedReadDistributionEntry[]>();
  for (const entry of entries) {
    if (entry.shardNum == null) continue;
    const list = entriesByShard.get(entry.shardNum);
    if (list) list.push(entry);
    else entriesByShard.set(entry.shardNum, [entry]);
  }

  const shards = [...entriesByShard.entries()]
    .map(([shardNum, replicas]): DistributedReadDistributionShard => {
      const readRows = replicas.reduce((sum, entry) => sum + entry.readRows, 0);
      const readBytes = replicas.reduce((sum, entry) => sum + entry.readBytes, 0);
      const replicaReaderCount = replicas.filter(entry => entry.role === 'replica_reader' || entry.role === 'local_reader').length;
      const uniqueReplicas = new Set(replicas.map(entry => entry.replicaNum).filter((value) => value != null));
      return {
        shardNum,
        readRows,
        readBytes,
        rowShare: totalReadRows > 0 ? readRows / totalReadRows : 0,
        byteShare: totalReadBytes > 0 ? readBytes / totalReadBytes : 0,
        replicas,
        hasPerReplicaReading: replicaReaderCount > 1 || uniqueReplicas.size > 1,
      };
    })
    .sort((a, b) => a.shardNum - b.shardNum);

  return {
    totalReadRows,
    totalReadBytes,
    entries,
    groups: buildReadDistributionGroups(entries, nodes),
    shards,
    hasPerReplicaReading: shards.some(shard => shard.hasPerReplicaReading),
    skew: buildResourceSkew(entries),
  };
}

function distributionGroupKey(entry: DistributedReadDistributionEntry): string {
  if (entry.foldedIntoCoordinator) return 'local_reader';
  return entry.normalizedQueryHash || 'unknown';
}

function compactSqlLabel(sql: string | undefined): string {
  const normalized = (sql ?? '').replace(/\s+/g, ' ').trim();
  const tableFunctionMatch = normalized.match(/\b(?:cluster|clusterAllReplicas)\s*\(\s*'[^']+'\s*,\s*`?([\w.-]+)`?\.`?([\w.-]+)`?/i)
    ?? normalized.match(/\b(?:cluster|clusterAllReplicas)\s*\(\s*'[^']+'\s*,\s*`?([\w.-]+)`?/i);
  if (tableFunctionMatch) {
    return tableFunctionMatch[2] ? `${tableFunctionMatch[1]}.${tableFunctionMatch[2]}` : tableFunctionMatch[1];
  }
  const tableMatch = normalized.match(/\bFROM\s+`?([\w.-]+)`?\.`?([\w.-]+)`?/i)
    ?? normalized.match(/\bFROM\s+`?([\w.-]+)`?/i);
  if (tableMatch) {
    return tableMatch[2] ? `${tableMatch[1]}.${tableMatch[2]}` : tableMatch[1];
  }
  const selectMatch = normalized.match(/\bSELECT\s+(.+?)(?:\s+FROM\b|\s+WHERE\b|\s+UNION\b|$)/i);
  if (selectMatch) {
    const projection = selectMatch[1].replace(/[`'"]/g, '').trim();
    return projection.length > 34 ? `${projection.slice(0, 31)}...` : projection;
  }
  return normalized.length > 34 ? `${normalized.slice(0, 31)}...` : normalized || 'Ungrouped';
}

interface ReadDistributionGroupRoleStats {
  shardCount: number;
  shardCoordinatorCount: number;
  nestedCoordinatorCount: number;
  readerCount: number;
  remoteChildCount: number;
}

function nodeDistributionGroupKey(node: DistributedTopologyNode): string {
  return node.normalizedQueryHash || 'unknown';
}

function buildReadDistributionGroupRoleStats(nodes: DistributedTopologyNode[]): Map<string, ReadDistributionGroupRoleStats> {
  const stats = new Map<string, {
    shards: Set<number>;
    shardCoordinatorCount: number;
    nestedCoordinatorCount: number;
    readerCount: number;
    remoteChildCount: number;
  }>();

  for (const node of nodes) {
    if (node.role === 'coordinator' || node.role === 'insert_client') continue;
    const key = nodeDistributionGroupKey(node);
    let item = stats.get(key);
    if (!item) {
      item = { shards: new Set(), shardCoordinatorCount: 0, nestedCoordinatorCount: 0, readerCount: 0, remoteChildCount: 0 };
      stats.set(key, item);
    }
    if (node.shardNum != null) item.shards.add(node.shardNum);
    if (node.role === 'shard_leader') item.shardCoordinatorCount += 1;
    else if (node.role === 'nested_coordinator') item.nestedCoordinatorCount += 1;
    else if (node.role === 'replica_reader') item.readerCount += 1;
    else if (node.role === 'remote_child' || node.role === 'independent_child') item.remoteChildCount += 1;
  }

  return new Map([...stats.entries()].map(([key, item]) => [key, {
    shardCount: item.shards.size,
    shardCoordinatorCount: item.shardCoordinatorCount,
    nestedCoordinatorCount: item.nestedCoordinatorCount,
    readerCount: item.readerCount,
    remoteChildCount: item.remoteChildCount,
  }]));
}

function buildReadDistributionGroups(
  entries: DistributedReadDistributionEntry[],
  nodes: DistributedTopologyNode[],
): DistributedReadDistributionGroup[] {
  const groups = new Map<string, DistributedReadDistributionEntry[]>();
  for (const entry of entries) {
    const key = distributionGroupKey(entry);
    const group = groups.get(key);
    if (group) group.push(entry);
    else groups.set(key, [entry]);
  }

  const roleStats = buildReadDistributionGroupRoleStats(nodes);

  return [...groups.entries()]
    .sort((a, b) => b[1].length - a[1].length || a[0].localeCompare(b[0]))
    .map(([key, groupEntries], index): DistributedReadDistributionGroup => {
      const queryPreview = groupEntries.find(entry => entry.queryPreview)?.queryPreview;
      const label = compactSqlLabel(queryPreview);
      const hasFoldedLocalReader = groupEntries.some(entry => entry.foldedIntoCoordinator);
      const stats = roleStats.get(key);
      return {
        key,
        label: hasFoldedLocalReader ? 'Coordinator local read' : label !== 'Ungrouped' ? label : `Query shape ${index + 1}`,
        queryPreview,
        shardCount: stats?.shardCount ?? new Set(groupEntries.map(entry => entry.shardNum).filter(value => value != null)).size,
        shardCoordinatorCount: stats?.shardCoordinatorCount ?? groupEntries.filter(entry => entry.role === 'shard_leader').length,
        nestedCoordinatorCount: stats?.nestedCoordinatorCount ?? groupEntries.filter(entry => entry.role === 'nested_coordinator').length,
        readerCount: stats?.readerCount ?? groupEntries.filter(entry => entry.role === 'replica_reader' || entry.role === 'local_reader').length,
        remoteChildCount: stats?.remoteChildCount ?? groupEntries.filter(entry => entry.role === 'remote_child' || entry.role === 'independent_child').length,
        entries: groupEntries,
        skew: buildResourceSkew(groupEntries),
      };
    });
}

function skewSeverity(maxShare: number, skewRatio: number, participantCount: number): DistributedSkewMetricSummary['severity'] {
  if (participantCount <= 1 || maxShare <= 0) return 'none';
  if (maxShare >= 0.75 || skewRatio >= 3) return 'high';
  if (maxShare >= 0.55 || skewRatio >= 2) return 'medium';
  if (maxShare >= 0.4 || skewRatio >= 1.5) return 'low';
  return 'none';
}

export function distributedReadMetricValue(entry: DistributedReadDistributionEntry, metric: DistributedSkewMetric): number {
  switch (metric) {
    case 'read_rows': return entry.readRows;
    case 'read_bytes': return entry.readBytes;
    case 'duration_ms': return entry.durationMs;
    case 'memory_usage': return entry.memoryUsage;
    case 'selected_parts': return entry.selectedParts;
    case 'selected_marks': return entry.selectedMarks;
  }
}

function buildSkewMetricSummary(
  entries: DistributedReadDistributionEntry[],
  metric: DistributedSkewMetric,
): DistributedSkewMetricSummary {
  const values = entries.map(entry => ({ entry, value: distributedReadMetricValue(entry, metric) }));
  const nonZeroValues = values.filter(item => item.value > 0);
  const total = values.reduce((sum, item) => sum + item.value, 0);
  const participantCount = values.length;
  const average = participantCount > 0 ? total / participantCount : 0;
  const min = nonZeroValues.length > 0 ? Math.min(...nonZeroValues.map(item => item.value)) : 0;
  const maxItem = values.reduce<typeof values[number] | undefined>((best, item) => {
    if (!best || item.value > best.value) return item;
    return best;
  }, undefined);
  const max = maxItem?.value ?? 0;
  const maxShare = total > 0 ? max / total : 0;
  const skewRatio = average > 0 ? max / average : 0;

  return {
    metric,
    total,
    min,
    max,
    average,
    maxShare,
    skewRatio,
    maxParticipantId: maxItem?.entry.participantId,
    maxHostname: maxItem?.entry.hostname,
    maxShardNum: maxItem?.entry.shardNum,
    maxReplicaNum: maxItem?.entry.replicaNum,
    severity: skewSeverity(maxShare, skewRatio, participantCount),
  };
}

function buildResourceSkew(entries: DistributedReadDistributionEntry[]): DistributedResourceSkew {
  const metrics: DistributedSkewMetric[] = [
    'read_rows',
    'read_bytes',
    'duration_ms',
    'memory_usage',
    'selected_parts',
    'selected_marks',
  ];

  return {
    participantCount: entries.length,
    metrics: metrics.map(metric => buildSkewMetricSummary(entries, metric)),
  };
}

function buildAsyncInsertLinks(asyncInsertLogs: AsyncInsertLogInput[] = []): DistributedAsyncInsertLink[] {
  const seen = new Set<string>();
  return asyncInsertLogs
    .filter(log => log.queryId && log.flushQueryId)
    .map((log): DistributedAsyncInsertLink => {
      const tableName = [log.database, log.table].filter(Boolean).join('.');
      return {
        source: 'asynchronous_insert_log',
        queryId: log.queryId,
        flushQueryId: log.flushQueryId,
        hostname: log.hostname,
        database: log.database,
        table: log.table,
        status: log.status,
        exception: log.exception,
        rows: log.rows ?? 0,
        bytes: log.bytes ?? 0,
        eventTimeMicroseconds: log.eventTimeMicroseconds,
        confidence: 'high',
        evidence: [{
          source: 'asynchronous_insert_log',
          message: `async insert ${log.queryId} flushed by ${log.flushQueryId}${tableName ? ` for ${tableName}` : ''}`,
        }],
      };
    })
    .filter((link) => {
      const key = `${link.queryId}:${link.flushQueryId}:${hostKey(link.hostname ?? '')}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
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
  localRead?: FoldedLocalReadParticipant,
  asyncInsertLinks: DistributedAsyncInsertLink[] = [],
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

  for (const link of asyncInsertLinks) {
    if (link.queryId !== rootNode.queryId) continue;
    const linkMs = parseTimestampMs(link.eventTimeMicroseconds);
    const offsetMs = linkMs == null ? 0 : Math.max(0, linkMs - rootStartMs);
    const tableName = [link.database, link.table].filter(Boolean).join('.');
    events.push({
      kind: 'async_insert_buffered',
      source: 'asynchronous_insert_log',
      offsetMs,
      actor: 'Async insert',
      actorType: 'remote',
      title: 'Async insert buffered',
      detail: [
        tableName ? `Buffered for ${tableName}.` : 'Buffered for async insert flush.',
        link.flushQueryId ? `Flush query ${link.flushQueryId}.` : '',
        eventDetailMetrics(link.rows ?? 0, link.bytes ?? 0),
      ].filter(Boolean).join(' '),
      queryId: link.flushQueryId,
      hostname: link.hostname,
      rows: link.rows,
      bytesText: link.bytes ? `${link.bytes.toLocaleString()} bytes` : undefined,
    });
  }

  if (localRead) {
    events.push({
      kind: 'local_read_started',
      source: 'query_log',
      offsetMs: 0,
      actor: 'Local reader',
      actorType: 'local',
      title: 'Local read folded into coordinator',
      detail: localRead.shardNum != null && localRead.replicaNum != null
        ? `Coordinator also reads local replica s${localRead.shardNum}r${localRead.replicaNum}; this work is included in the initial query row.`
        : 'Coordinator also reads its local replica; this work is included in the initial query row.',
      queryId: localRead.queryId,
      hostname: localRead.hostname,
      rows: localRead.readRows,
    });

    events.push({
      kind: 'local_read_completed',
      source: 'query_log',
      offsetMs: rootNode.queryDurationMs,
      actor: 'Local reader',
      actorType: 'local',
      title: 'Local read accounted in coordinator row',
      detail: eventDetailMetrics(localRead.readRows ?? 0, localRead.readBytes ?? 0) || 'Local participant work is folded into coordinator metrics.',
      queryId: localRead.queryId,
      hostname: localRead.hostname,
      rows: localRead.readRows,
      durationMs: rootNode.queryDurationMs,
    });
  }

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
  if (event.actorType === 'local') {
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
  if (event.actorType === 'local') return 1;
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
  const rootExecution =
    executions.find(execution => execution.queryId === input.rootQueryId && execution.isInitialQuery) ??
    executions.find(execution => execution.isInitialQuery);
  const { marker: clusterAllReplicas, localRead } = buildClusterAllReplicasMarker(
    rootExecution,
    coordinator,
    nodes,
    capabilities.systemClusters ? input.clusterHosts : undefined,
    capabilities,
  );
  const fanoutMode = inferFanoutMode(kind, clusterAllReplicas);
  const asyncInsertLinks = buildAsyncInsertLinks(input.asyncInsertLogs);
  const readDistribution = buildReadDistribution(nodes, localRead);
  const executionFlow = buildExecutionFlow(nodes, executionPhases, input.rootQueryId, localRead, asyncInsertLinks);
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
    if (!capabilities.asynchronousInsertLog) {
      addDecision(decisions, 'degraded', 'missing-async-insert-log', 'capability', 'system.asynchronous_insert_log was not available; async INSERT query/flush links may be incomplete.');
    }
  }
  if (asyncInsertLinks.length > 0) {
    evidence.push({ source: 'asynchronous_insert_log', message: `${asyncInsertLinks.length} async insert flush link(s) detected` });
    addDecision(decisions, 'info', 'async-insert-links-detected', 'asynchronous_insert_log', `${asyncInsertLinks.length} async insert query/flush link(s) were loaded from system.asynchronous_insert_log.`);
  }
  if (clusterAllReplicas) {
    evidence.push(...clusterAllReplicas.evidence);
    addDecision(
      decisions,
      'info',
      'cluster-all-replicas-fanout',
      'query_log',
      clusterAllReplicas.expectedParticipants != null
        ? `clusterAllReplicas forced all-replicas fan-out: ${clusterAllReplicas.observedRemoteChildren} remote child row(s), ${clusterAllReplicas.localParticipantsOnInitiator} local participant(s) on the initiator, ${clusterAllReplicas.expectedParticipants} expected participant(s).`
        : `clusterAllReplicas forced all-replicas fan-out: ${clusterAllReplicas.observedRemoteChildren} remote child row(s), ${clusterAllReplicas.localParticipantsOnInitiator} local participant(s) on the initiator.`,
    );
  }
  if (localRead) {
    evidence.push({ source: 'query_log', message: 'local read participant is folded into the coordinator query row' });
    addDecision(
      decisions,
      'info',
      'coordinator-local-read-folded',
      'query_log',
      'The initiator is also an addressed replica; local participant work is included in the coordinator row.',
    );
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
    fanoutMode,
    rootQueryId: input.rootQueryId,
    capabilities,
    coordinator,
    nodes,
    shards,
    shardCoverage,
    clusterAllReplicas,
    localRead,
    asyncInsertLinks,
    readDistribution,
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
