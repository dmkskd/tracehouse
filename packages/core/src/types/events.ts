/**
 * Operational event domain.
 *
 * Events are independent observations that may be rendered by Time Travel,
 * the Events page, or future consumers. Timeline models must not own them.
 */

export const EVENT_SEVERITIES = [
  'critical',
  'error',
  'warning',
  'info',
] as const;

export type EventSeverity = typeof EVENT_SEVERITIES[number];

export const EVENT_CATEGORY_DEFINITIONS = {
  lifecycle: { label: 'Lifecycle' },
  queries: { label: 'Queries' },
  merges: { label: 'Merges' },
  replication: { label: 'Replication' },
  coordination: { label: 'Coordination' },
  storage: { label: 'Storage' },
  changes: { label: 'Changes' },
  maintenance: { label: 'Maintenance' },
} as const;

export type EventCategory = keyof typeof EVENT_CATEGORY_DEFINITIONS;

export interface EventKindDefinition {
  label: string;
  shortLabel: string;
  description: string;
  detailLabel: string;
  categories: readonly EventCategory[];
  severities: readonly EventSeverity[];
}

/**
 * Product-level event taxonomy and wording.
 *
 * A kind may be reserved before a detector is implemented. Use
 * EVENT_SOURCE_DEFINITIONS to determine which kinds are currently supported.
 * Runtime category/severity remain on each event because some detectors
 * classify them dynamically (notably error_burst).
 */
export const EVENT_KIND_DEFINITIONS = {
  server_restart: {
    label: 'Server restart',
    shortLabel: 'Restart',
    description: 'Process startup inferred from a persisted Uptime reset.',
    detailLabel: 'Detection method',
    categories: ['lifecycle'],
    severities: ['warning'],
  },
  server_crash: {
    label: 'Server crash',
    shortLabel: 'Crash',
    description: 'Fatal ClickHouse crash with signal, version, and query ID when recorded.',
    detailLabel: 'Crash details',
    categories: ['lifecycle'],
    severities: ['critical'],
  },
  query_oom: {
    label: 'Query OOM',
    shortLabel: 'OOM',
    description: 'Query-scoped memory limit or allocation failure.',
    detailLabel: 'ClickHouse error',
    categories: ['queries'],
    severities: ['error'],
  },
  query_rejected: {
    label: 'Query rejected',
    shortLabel: 'Rejected',
    description: 'Admission, quota, simultaneous-query, parts, or mutation limit rejection.',
    detailLabel: 'ClickHouse error',
    categories: ['queries'],
    severities: ['warning'],
  },
  query_timeout: {
    label: 'Query timeout',
    shortLabel: 'Timeout',
    description: 'Query terminated with TIMEOUT_EXCEEDED.',
    detailLabel: 'ClickHouse error',
    categories: ['queries'],
    severities: ['warning'],
  },
  query_resource_limit: {
    label: 'Query resource failure',
    shortLabel: 'Resource limit',
    description: 'Query failed because a required resource was unavailable.',
    detailLabel: 'ClickHouse error',
    categories: ['queries'],
    severities: ['error'],
  },
  query_failure: {
    label: 'Query failure',
    shortLabel: 'Query failure',
    description: 'A query failed with a ClickHouse exception.',
    detailLabel: 'ClickHouse error',
    categories: ['queries'],
    severities: ['error'],
  },
  replica_readonly: {
    label: 'Replica read-only episode',
    shortLabel: 'Read-only',
    description: 'One or more replicated tables entered a persisted read-only state.',
    detailLabel: 'Detection method',
    categories: ['replication'],
    severities: ['error'],
  },
  replica_unavailable: {
    label: 'Replica unavailable',
    shortLabel: 'Unavailable',
    description: 'A replica became unavailable.',
    detailLabel: 'Detection method',
    categories: ['replication'],
    severities: ['error'],
  },
  replication_data_loss: {
    label: 'Replication data loss',
    shortLabel: 'Data loss',
    description: 'A persisted ReplicatedDataLoss counter increase.',
    detailLabel: 'Replication details',
    categories: ['replication'],
    severities: ['critical'],
  },
  replication_task_failure: {
    label: 'Replication task failure',
    shortLabel: 'Task failure',
    description: 'Failed replicated fetch, part check, or scheduled replication task.',
    detailLabel: 'Replication details',
    categories: ['replication'],
    severities: ['error'],
  },
  merge_failure: {
    label: 'Merge failure',
    shortLabel: 'Merge failed',
    description: 'A MergeTree parts merge completed with a non-zero error.',
    detailLabel: 'ClickHouse error',
    categories: ['merges'],
    severities: ['error'],
  },
  mutation_failure: {
    label: 'Mutation failure',
    shortLabel: 'Mutation failed',
    description: 'A MergeTree data mutation failed while rewriting a part.',
    detailLabel: 'ClickHouse error',
    categories: ['merges'],
    severities: ['error'],
  },
  part_move_failure: {
    label: 'Part move failure',
    shortLabel: 'Move failed',
    description: 'A data part failed to move between disks or volumes.',
    detailLabel: 'ClickHouse error',
    categories: ['storage'],
    severities: ['error'],
  },
  part_failure: {
    label: 'Part operation failure',
    shortLabel: 'Part failure',
    description: 'A MergeTree part-processing operation completed with a non-zero error.',
    detailLabel: 'ClickHouse error',
    categories: ['merges'],
    severities: ['error'],
  },
  background_task_failure: {
    label: 'Background task failure',
    shortLabel: 'Background failure',
    description: 'A scheduled ClickHouse background task failed.',
    detailLabel: 'Recorded details',
    categories: ['maintenance'],
    severities: ['error'],
  },
  error_burst: {
    label: 'Operational error burst',
    shortLabel: 'Operational error',
    description: 'Persisted replication, Keeper, storage, or maintenance error count.',
    detailLabel: 'Recorded details',
    categories: ['replication', 'coordination', 'storage', 'maintenance'],
    severities: ['error', 'critical'],
  },
  ddl: {
    label: 'DDL change',
    shortLabel: 'DDL',
    description: 'Successful CREATE, ALTER, DROP, RENAME, TRUNCATE, OPTIMIZE, or UNDROP.',
    detailLabel: 'Statement details',
    categories: ['changes'],
    severities: ['info'],
  },
  keeper_connection: {
    label: 'Keeper connection',
    shortLabel: 'Keeper',
    description: 'A ClickHouse Keeper or ZooKeeper connection state change.',
    detailLabel: 'Keeper details',
    categories: ['coordination'],
    severities: ['info', 'error'],
  },
  backup: {
    label: 'Backup / restore',
    shortLabel: 'Backup',
    description: 'A backup or restore operation completed, failed, or was cancelled.',
    detailLabel: 'Recorded details',
    categories: ['maintenance'],
    severities: ['error', 'warning', 'info'],
  },
  async_insert_failure: {
    label: 'Async insert failure',
    shortLabel: 'Async insert',
    description: 'An asynchronous insert failed.',
    detailLabel: 'ClickHouse error',
    categories: ['queries'],
    severities: ['error'],
  },
  server_log: {
    label: 'Server log',
    shortLabel: 'Server log',
    description: 'A warning-or-higher server log record.',
    detailLabel: 'Recorded details',
    categories: ['maintenance'],
    severities: ['warning', 'error', 'critical'],
  },
} as const satisfies Record<string, EventKindDefinition>;

export type EventKind = keyof typeof EVENT_KIND_DEFINITIONS;
export type EventPrecision = 'exact' | 'sampled' | 'inferred';

/**
 * Single source of truth for event detectors, source coverage, and supported
 * event kinds. The service binds each id to its query/row mapper.
 */
export const EVENT_SOURCE_DEFINITIONS = [
  {
    id: 'query_log',
    source: 'system.query_log',
    coverageLabel: undefined,
    capability: 'query_log',
    description: 'Query OOMs, timeouts, resource/admission failures, and successful DDL.',
    kinds: [
      'query_oom',
      'query_timeout',
      'query_rejected',
      'query_resource_limit',
      'ddl',
    ],
  },
  {
    id: 'async_insert_failures',
    source: 'system.asynchronous_insert_log',
    coverageLabel: undefined,
    capability: 'asynchronous_insert_log',
    description: 'Asynchronous insert parsing and flush failures.',
    kinds: ['async_insert_failure'],
  },
  {
    id: 'backups',
    source: 'system.backup_log',
    coverageLabel: undefined,
    capability: 'backup_log',
    description: 'Terminal backup and restore outcomes.',
    kinds: ['backup'],
  },
  {
    id: 'keeper_connections',
    source: 'system.zookeeper_connection_log',
    coverageLabel: undefined,
    capability: 'zookeeper_connection_log',
    description: 'Keeper and ZooKeeper connection and disconnection state changes.',
    kinds: ['keeper_connection'],
  },
  {
    id: 'server_restarts',
    source: 'system.asynchronous_metric_log',
    coverageLabel: undefined,
    capability: 'asynchronous_metric_log',
    description: 'Server restarts inferred from persisted Uptime resets.',
    kinds: ['server_restart'],
  },
  {
    id: 'server_crashes',
    source: 'system.crash_log',
    coverageLabel: undefined,
    capability: 'crash_log',
    description: 'Fatal ClickHouse process crashes, signals, versions, and related query IDs.',
    kinds: ['server_crash'],
  },
  {
    id: 'part_failures',
    source: 'system.part_log',
    coverageLabel: undefined,
    capability: 'part_log',
    description: 'Failed merges, replication transfers, and other data-part operations with table, part, disk, and error context.',
    kinds: [
      'merge_failure',
      'mutation_failure',
      'part_move_failure',
      'part_failure',
      'replication_task_failure',
    ],
  },
  {
    id: 'background_task_failures',
    source: 'system.background_schedule_pool_log',
    coverageLabel: undefined,
    capability: 'background_schedule_pool_log',
    description: 'Failures from scheduled ClickHouse background work.',
    kinds: ['background_task_failure', 'replication_task_failure'],
  },
  {
    id: 'operational_errors',
    source: 'system.error_log',
    coverageLabel: undefined,
    capability: 'error_log',
    description: 'Operational error bursts classified into replication, Keeper, storage, or maintenance.',
    kinds: ['error_burst'],
  },
  {
    id: 'replica_readonly',
    source: 'system.metric_log',
    coverageLabel: 'system.metric_log (replica state)',
    capability: 'metric_log_replication_state',
    description: 'Host-level episodes where one or more replicated tables were read-only.',
    kinds: ['replica_readonly'],
  },
  {
    id: 'replication_failures',
    source: 'system.metric_log',
    coverageLabel: 'system.metric_log (replication failures)',
    capability: 'metric_log_replication_failures',
    description: 'Persisted data-loss, failed-fetch, and failed-part-check counters.',
    kinds: ['replication_data_loss', 'replication_task_failure'],
  },
] as const satisfies readonly {
  id: string;
  source: string;
  coverageLabel?: string;
  capability: string;
  description: string;
  kinds: readonly EventKind[];
}[];

export type EventSourceCapability =
  typeof EVENT_SOURCE_DEFINITIONS[number]['capability'];

export interface EventSourceDefinition {
  id: string;
  source: string;
  coverageLabel?: string;
  capability: EventSourceCapability;
  description: string;
  kinds: readonly EventKind[];
}

/** An individual operational event occurrence or state episode. */
export interface OperationalEvent {
  id: string;
  occurred_at: string;
  ended_at?: string;
  observed_at?: string;
  hostname?: string;
  kind: EventKind;
  category: EventCategory;
  severity: EventSeverity;
  precision: EventPrecision;
  title: string;
  detail?: string;
  source: string;
  capability: string;
  query_id?: string;
  initial_query_id?: string;
  normalized_query_hash?: string;
  user?: string;
  query_kind?: string;
  query?: string;
  databases?: string[];
  tables?: string[];
  database?: string;
  table?: string;
  part_name?: string;
  partition_id?: string;
  operation?: string;
  task_name?: string;
  disk_name?: string;
  exception_code?: number;
  exception_name?: string;
  count?: number;
  metric_name?: string;
  metric_value?: number;
  previous_metric_value?: number;
  metric_unit?: string;
  remote?: boolean;
  duration_ms?: number;
  memory_usage?: number;
  signal?: number;
  version?: string;
  status?: string;
  operation_id?: string;
  storage_name?: string;
  rows?: number;
  bytes?: number;
  flush_query_id?: string;
  format?: string;
  started_at?: string;
  num_files?: number;
  total_size?: number;
  connection_state?: string;
  keeper_name?: string;
  keeper_host?: string;
  keeper_port?: number;
  keeper_client_id?: string;
  reason?: string;
}

export type EventSourceStatus =
  | 'loaded'
  | 'unavailable'
  | 'failed'
  | 'not_requested';

export interface EventSourceCoverage {
  source: string;
  capability: string;
  status: EventSourceStatus;
  event_count: number;
  truncated?: boolean;
  detail?: string;
}
