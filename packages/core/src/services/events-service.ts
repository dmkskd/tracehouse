import type { IClickHouseAdapter } from '../adapters/types.js';
import { buildQuery, tagQuery, utcDateTime } from '../queries/builder.js';
import type { QueryParameter } from '../queries/builder.js';
import { TAB_EVENTS, TAB_TIME_TRAVEL, sourceTag } from '../queries/source-tags.js';
import {
  ASYNC_INSERT_FAILURE_EVENTS,
  BACKUP_EVENTS,
  BACKGROUND_TASK_FAILURE_EVENTS,
  KEEPER_CONNECTION_EVENTS,
  OPERATIONAL_ERROR_EVENTS,
  PART_FAILURE_EVENTS,
  QUERY_EVENTS,
  REPLICA_READONLY_EPISODES,
  REPLICATION_FAILURE_EVENTS,
  SERVER_CRASH_EVENTS,
  SERVER_RESTART_EVENTS,
} from '../queries/event-queries.js';
import {
  EVENT_KIND_DEFINITIONS,
  EVENT_SOURCE_DEFINITIONS,
  type EventCategory,
  type EventKind,
  type EventSeverity,
  type EventSourceDefinition,
  type EventSourceCoverage,
  type OperationalEvent,
} from '../types/events.js';

export interface EventsOptions {
  startTime: string;
  endTime: string;
  hostname?: string | null;
  availableCapabilities: readonly string[];
  limit?: number;
  /** Consumer identity used only for query-source attribution. */
  origin?: 'events' | 'timeTravel';
}

export interface EventsResult {
  events: OperationalEvent[];
  coverage: EventSourceCoverage[];
}

interface BoundEventSourceDefinition extends EventSourceDefinition {
  fetch: (params: Record<string, QueryParameter>) => Promise<OperationalEvent[]>;
}

type EventSourceId = typeof EVENT_SOURCE_DEFINITIONS[number]['id'];

const EVENT_SOURCES_BY_ID = EVENT_SOURCE_DEFINITIONS.reduce(
  (byId, source) => {
    byId[source.id] = source;
    return byId;
  },
  {} as Record<EventSourceId, EventSourceDefinition>,
);

function eventSourceFields(
  id: EventSourceId,
): Pick<OperationalEvent, 'source' | 'capability'> {
  const source = EVENT_SOURCES_BY_ID[id];
  return {
    source: source.source,
    capability: source.capability,
  };
}

const QUERY_EXCEPTION_NAMES: Record<number, string> = {
  159: 'TIMEOUT_EXCEEDED',
  173: 'CANNOT_ALLOCATE_MEMORY',
  201: 'QUOTA_EXCEEDED',
  202: 'TOO_MANY_SIMULTANEOUS_QUERIES',
  241: 'MEMORY_LIMIT_EXCEEDED',
  243: 'NOT_ENOUGH_SPACE',
  252: 'TOO_MANY_PARTS',
  692: 'TOO_MANY_MUTATIONS',
};

const CRITICAL_OPERATIONAL_ERRORS = new Set([
  'CORRUPTED_DATA',
  'CHECKSUM_DOESNT_MATCH',
  'TOO_MANY_UNEXPECTED_DATA_PARTS',
]);

function eventDefaults(kind: EventKind): {
  category: EventCategory;
  severity: EventSeverity;
} {
  const definition = EVENT_KIND_DEFINITIONS[kind];
  return {
    category: definition.categories[0],
    severity: definition.severities[0],
  };
}

function classifyOperationalError(errorName: string): {
  category: EventCategory;
  severity: EventSeverity;
} {
  const category: EventCategory =
    /REPLICA|REPLICATED/.test(errorName)
      ? 'replication'
      : /KEEPER|ZOOKEEPER/.test(errorName)
      ? 'coordination'
      : /SPACE|CORRUPT|CHECKSUM|PART|FILE|FSYNC/.test(errorName)
        ? 'storage'
        : 'maintenance';
  return {
    category,
    severity: CRITICAL_OPERATIONAL_ERRORS.has(errorName) ? 'critical' : 'error',
  };
}

function parseChTime(value: unknown): string {
  const raw = String(value ?? '').trim();
  const normalized = raw.replace(' ', 'T');
  const withTimezone = normalized.includes('Z') || /[+-]\d\d:\d\d$/.test(normalized)
    ? normalized
    : `${normalized}Z`;
  const parsed = new Date(withTimezone);
  return Number.isNaN(parsed.getTime()) ? raw : parsed.toISOString();
}

function stableEventId(parts: Array<string | number | undefined>): string {
  return parts.map(part => String(part ?? '')).join(':');
}

function parseChBool(value: unknown): boolean {
  return value === true || value === 1 || value === '1' || value === 'true';
}

function classifyQueryFailure(code: number): {
  kind: EventKind;
  category: EventCategory;
  severity: EventSeverity;
  title: string;
} {
  const name = QUERY_EXCEPTION_NAMES[code] ?? `ClickHouse error ${code}`;
  if (code === 241 || code === 173) {
    return {
      kind: 'query_oom',
      ...eventDefaults('query_oom'),
      title: `Query OOM · ${name}`,
    };
  }
  if (code === 159) {
    return {
      kind: 'query_timeout',
      ...eventDefaults('query_timeout'),
      title: `Query timeout · ${name}`,
    };
  }
  if (code === 201 || code === 202 || code === 252 || code === 692) {
    return {
      kind: 'query_rejected',
      ...eventDefaults('query_rejected'),
      title: `Query rejected · ${name}`,
    };
  }
  return {
    kind: 'query_resource_limit',
    ...eventDefaults('query_resource_limit'),
    title: `Query resource failure · ${name}`,
  };
}

function failureDetail(row: Record<string, unknown>): string | undefined {
  const value = String(row.exception ?? '').trim();
  return value || undefined;
}

/**
 * Collects operational events independently per source. Capability checks are
 * authoritative; Promise.allSettled is only a race/permission/schema fallback.
 */
export class EventsService {
  constructor(private adapter: IClickHouseAdapter) {}

  static notRequested(): EventsResult {
    return {
      events: [],
      coverage: EVENT_SOURCE_DEFINITIONS.map(source => ({
        source: source.coverageLabel ?? source.source,
        capability: source.capability,
        status: 'not_requested',
        event_count: 0,
      })),
    };
  }

  async getEvents(
    options: EventsOptions,
  ): Promise<EventsResult> {
    const sourceTab = options.origin === 'timeTravel' ? TAB_TIME_TRAVEL : TAB_EVENTS;
    const limit = Math.max(1, Math.min(options.limit ?? 1000, 10_000));
    const params: Record<string, QueryParameter> = {
      start_time: utcDateTime(options.startTime),
      end_time: utcDateTime(options.endTime),
      hostname: options.hostname ?? '',
      event_limit: limit,
    };
    const available = new Set(options.availableCapabilities);
    const fetchById: Record<
      typeof EVENT_SOURCE_DEFINITIONS[number]['id'],
      BoundEventSourceDefinition['fetch']
    > = {
      query_log: sourceParams => this.fetchQueryEvents(sourceParams, sourceTab),
      async_insert_failures: sourceParams =>
        this.fetchAsyncInsertFailureEvents(sourceParams, sourceTab),
      backups: sourceParams => this.fetchBackupEvents(sourceParams, sourceTab),
      keeper_connections: sourceParams =>
        this.fetchKeeperConnectionEvents(sourceParams, sourceTab),
      server_restarts: sourceParams => this.fetchRestartEvents(sourceParams, sourceTab),
      server_crashes: sourceParams => this.fetchCrashEvents(sourceParams, sourceTab),
      part_failures: sourceParams => this.fetchPartFailureEvents(sourceParams, sourceTab),
      background_task_failures: sourceParams =>
        this.fetchBackgroundTaskFailureEvents(sourceParams, sourceTab),
      operational_errors: sourceParams =>
        this.fetchOperationalErrorEvents(sourceParams, sourceTab),
      replica_readonly: sourceParams =>
        this.fetchReplicaReadonlyEpisodes(sourceParams, sourceTab),
      replication_failures: sourceParams =>
        this.fetchReplicationFailureEvents(sourceParams, sourceTab),
    };
    const sources: BoundEventSourceDefinition[] = EVENT_SOURCE_DEFINITIONS.map(
      source => ({ ...source, fetch: fetchById[source.id] }),
    );

    const coverage: EventSourceCoverage[] = sources.map(source => ({
      source: source.coverageLabel ?? source.source,
      capability: source.capability,
      status: available.has(source.capability) ? 'not_requested' : 'unavailable',
      event_count: 0,
      detail: available.has(source.capability) ? undefined : 'Capability not available',
    }));

    const requested = sources
      .map((source, index) => ({ source, index }))
      .filter(({ source }) => available.has(source.capability));
    const settled = await Promise.allSettled(
      requested.map(({ source }) => source.fetch(params)),
    );

    const events: OperationalEvent[] = [];
    settled.forEach((result, settledIndex) => {
      const { index } = requested[settledIndex]!;
      const sourceCoverage = coverage[index]!;
      if (result.status === 'fulfilled') {
        sourceCoverage.status = 'loaded';
        sourceCoverage.event_count = result.value.length;
        sourceCoverage.truncated = result.value.length >= limit;
        events.push(...result.value);
      } else {
        sourceCoverage.status = 'failed';
        sourceCoverage.detail = result.reason instanceof Error
          ? result.reason.message
          : String(result.reason);
      }
    });

    // Cluster reads and overlapping source semantics can produce duplicates.
    // Preserve separate hosts and query executions, but remove exact repeats.
    const unique = new Map<string, OperationalEvent>();
    for (const event of events) unique.set(event.id, event);

    return {
      events: [...unique.values()].sort(
        (a, b) => new Date(a.occurred_at).getTime() - new Date(b.occurred_at).getTime(),
      ),
      coverage,
    };
  }

  private async fetchQueryEvents(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(QUERY_EVENTS, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsQueryLog')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const hostname = String(row.host ?? '');
      const queryId = String(row.query_id ?? '');
      const code = Number(row.exception_code ?? 0);
      const queryKind = String(row.query_kind ?? '');
      const isDdl = String(row.type ?? '') === 'QueryFinish';
      const classification = classifyQueryFailure(code);
      const semantics = isDdl ? eventDefaults('ddl') : classification;
      return {
        id: stableEventId([
          'query_log',
          hostname,
          occurredAt,
          queryId,
          isDdl ? queryKind : code,
        ]),
        occurred_at: occurredAt,
        hostname: hostname || undefined,
        kind: isDdl ? 'ddl' : classification.kind,
        category: semantics.category,
        severity: semantics.severity,
        precision: 'exact',
        title: isDdl ? `DDL · ${queryKind || 'Other'}` : classification.title,
        detail: isDdl ? undefined : failureDetail(row),
        ...eventSourceFields('query_log'),
        query_id: queryId || undefined,
        initial_query_id: String(row.initial_query_id ?? '') || undefined,
        normalized_query_hash: String(row.normalized_query_hash ?? '') || undefined,
        user: String(row.user ?? '') || undefined,
        query_kind: queryKind || undefined,
        query: String(row.query_short ?? '') || undefined,
        databases: Array.isArray(row.databases)
          ? row.databases.map(value => String(value))
          : undefined,
        tables: Array.isArray(row.tables)
          ? row.tables.map(value => String(value))
          : undefined,
        exception_code: isDdl ? undefined : code,
        exception_name: isDdl ? undefined : QUERY_EXCEPTION_NAMES[code],
        duration_ms: Number(row.query_duration_ms ?? 0),
        memory_usage: Number(row.memory_usage ?? 0),
      } satisfies OperationalEvent;
    });
  }

  private async fetchAsyncInsertFailureEvents(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(ASYNC_INSERT_FAILURE_EVENTS, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsAsyncInsertFailures')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const hostname = String(row.host ?? '');
      const queryId = String(row.query_id ?? '');
      const flushQueryId = String(row.flush_query_id ?? '');
      const database = String(row.database ?? '');
      const table = String(row.table ?? '');
      const status = String(row.status ?? '');
      return {
        id: stableEventId([
          'asynchronous_insert_log',
          hostname,
          occurredAt,
          queryId,
          flushQueryId,
          status,
        ]),
        occurred_at: occurredAt,
        hostname: hostname || undefined,
        kind: 'async_insert_failure',
        ...eventDefaults('async_insert_failure'),
        precision: 'exact',
        title: status
          ? `Async insert failed · ${status}`
          : 'Async insert failed',
        detail: failureDetail(row),
        ...eventSourceFields('async_insert_failures'),
        query_id: queryId || undefined,
        flush_query_id: flushQueryId || undefined,
        database: database || undefined,
        table: table || undefined,
        status: status || undefined,
        format: String(row.format ?? '') || undefined,
        rows: Number(row.rows ?? 0),
        bytes: Number(row.bytes ?? 0),
      } satisfies OperationalEvent;
    });
  }

  private async fetchBackupEvents(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(BACKUP_EVENTS, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsBackups')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const hostname = String(row.host ?? '');
      const operationId = String(row.operation_id ?? '');
      const queryId = String(row.query_id ?? '');
      const status = String(row.status ?? '');
      const storageName = String(row.storage_name ?? '');
      const startedAt = parseChTime(row.start_time);
      const isRestore = status.startsWith('RESTOR');
      const isFailed = status.endsWith('_FAILED');
      const isCancelled = status.endsWith('_CANCELLED');
      const severity: EventSeverity = isFailed
        ? 'error'
        : isCancelled ? 'warning' : 'info';
      const action = isRestore ? 'Restore' : 'Backup';
      const outcome = isFailed
        ? 'failed'
        : isCancelled ? 'cancelled' : 'completed';
      const startMs = new Date(startedAt).getTime();
      const endMs = new Date(occurredAt).getTime();
      const durationMs = Number.isNaN(startMs) || Number.isNaN(endMs)
        ? undefined
        : Math.max(0, endMs - startMs);
      const error = String(row.error ?? '').trim();
      return {
        id: stableEventId([
          'backup_log',
          hostname,
          occurredAt,
          operationId,
          status,
        ]),
        occurred_at: occurredAt,
        hostname: hostname || undefined,
        kind: 'backup',
        category: 'maintenance',
        severity,
        precision: 'exact',
        title: `${action} ${outcome}`,
        detail: error || storageName || undefined,
        ...eventSourceFields('backups'),
        query_id: queryId || undefined,
        operation: action,
        operation_id: operationId || undefined,
        storage_name: storageName || undefined,
        status: status || undefined,
        started_at: startedAt || undefined,
        duration_ms: durationMs,
        num_files: Number(row.num_files ?? 0),
        total_size: Number(row.total_size ?? 0),
      } satisfies OperationalEvent;
    });
  }

  private async fetchKeeperConnectionEvents(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(KEEPER_CONNECTION_EVENTS, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsKeeperConnections')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const hostname = String(row.host ?? '');
      const state = String(row.connection_state ?? '');
      const keeperName = String(row.keeper_name ?? '');
      const keeperHost = String(row.keeper_host ?? '');
      const keeperPort = Number(row.keeper_port ?? 0);
      const keeperClientId = String(row.keeper_client_id ?? '');
      const reason = String(row.reason ?? '').trim();
      const connected = state === 'Connected';
      return {
        id: stableEventId([
          'zookeeper_connection_log',
          hostname,
          occurredAt,
          keeperName,
          keeperHost,
          keeperPort,
          keeperClientId,
          state,
        ]),
        occurred_at: occurredAt,
        hostname: hostname || undefined,
        kind: 'keeper_connection',
        category: 'coordination',
        severity: connected ? 'info' : 'error',
        precision: 'exact',
        title: `Keeper ${connected ? 'connected' : 'disconnected'}${
          keeperName ? ` · ${keeperName}` : ''
        }`,
        detail: reason || undefined,
        ...eventSourceFields('keeper_connections'),
        connection_state: state || undefined,
        keeper_name: keeperName || undefined,
        keeper_host: keeperHost || undefined,
        keeper_port: keeperPort || undefined,
        keeper_client_id: keeperClientId || undefined,
        reason: reason || undefined,
      } satisfies OperationalEvent;
    });
  }

  private async fetchRestartEvents(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(SERVER_RESTART_EVENTS, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsRestarts')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const observedAt = parseChTime(row.observed_at);
      const hostname = String(row.host ?? '');
      const uptime = Number(row.uptime ?? 0);
      const previousUptime = Number(row.previous_uptime ?? uptime);
      const detectedFromReset = previousUptime > uptime + 5;
      return {
        id: stableEventId(['asynchronous_metric_log', hostname, occurredAt, 'restart']),
        occurred_at: occurredAt,
        observed_at: observedAt,
        hostname: hostname || undefined,
        kind: 'server_restart',
        ...eventDefaults('server_restart'),
        precision: 'inferred',
        title: EVENT_KIND_DEFINITIONS.server_restart.label,
        detail: detectedFromReset
          ? 'Uptime moved backwards between consecutive persisted samples. '
            + 'Restart time is inferred as the observed sample time minus reported Uptime.'
          : 'This is the first persisted Uptime sample for the host. '
            + 'Restart time is inferred as the observed sample time minus reported Uptime.',
        ...eventSourceFields('server_restarts'),
        metric_name: 'Uptime',
        metric_value: uptime,
        previous_metric_value: previousUptime,
        metric_unit: 's',
      } satisfies OperationalEvent;
    });
  }

  private async fetchCrashEvents(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(SERVER_CRASH_EVENTS, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsCrashes')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const hostname = String(row.host ?? '');
      const signal = Number(row.signal ?? 0);
      const queryId = String(row.query_id ?? '');
      return {
        id: stableEventId(['crash_log', hostname, occurredAt, signal, queryId]),
        occurred_at: occurredAt,
        hostname: hostname || undefined,
        kind: 'server_crash',
        ...eventDefaults('server_crash'),
        precision: 'exact',
        title: signal ? `Server crashed · signal ${signal}` : 'Server crashed',
        ...eventSourceFields('server_crashes'),
        query_id: queryId || undefined,
        signal,
        version: String(row.version ?? '') || undefined,
      } satisfies OperationalEvent;
    });
  }

  private async fetchPartFailureEvents(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(PART_FAILURE_EVENTS, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsPartFailures')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const hostname = String(row.host ?? '');
      const queryId = String(row.query_id ?? '');
      const operation = String(row.event_type ?? '');
      const database = String(row.database ?? '');
      const table = String(row.table ?? '');
      const partName = String(row.part_name ?? '');
      const code = Number(row.error ?? 0);
      const isReplicationTask = /DownloadPart|FetchPart|Replicated/i.test(operation);
      const kind: EventKind = isReplicationTask
        ? 'replication_task_failure'
        : 'part_failure';
      return {
        id: stableEventId([
          'part_log',
          hostname,
          occurredAt,
          database,
          table,
          partName,
          operation,
          code,
        ]),
        occurred_at: occurredAt,
        hostname: hostname || undefined,
        kind,
        ...eventDefaults(kind),
        precision: 'exact',
        title: isReplicationTask
          ? `Replication task failed · ${operation}`
          : operation ? `Part operation failed · ${operation}` : 'Part operation failed',
        detail: failureDetail(row),
        ...eventSourceFields('part_failures'),
        query_id: queryId || undefined,
        database: database || undefined,
        table: table || undefined,
        part_name: partName || undefined,
        partition_id: String(row.partition_id ?? '') || undefined,
        operation: operation || undefined,
        disk_name: String(row.disk_name ?? '') || undefined,
        exception_code: code,
        duration_ms: Number(row.duration_ms ?? 0),
      } satisfies OperationalEvent;
    });
  }

  private async fetchBackgroundTaskFailureEvents(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(BACKGROUND_TASK_FAILURE_EVENTS, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsBackgroundFailures')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const hostname = String(row.host ?? '');
      const queryId = String(row.query_id ?? '');
      const database = String(row.database ?? '');
      const table = String(row.table ?? '');
      const taskName = String(row.log_name ?? '');
      const code = Number(row.error ?? 0);
      const isReplicationTask = /Replica|ReplicatedMergeTree|FetchPart/i.test(taskName);
      const kind: EventKind = isReplicationTask
        ? 'replication_task_failure'
        : 'background_task_failure';
      return {
        id: stableEventId([
          'background_schedule_pool_log',
          hostname,
          occurredAt,
          database,
          table,
          taskName,
          queryId,
          code,
        ]),
        occurred_at: occurredAt,
        hostname: hostname || undefined,
        kind,
        ...eventDefaults(kind),
        precision: 'exact',
        title: isReplicationTask
          ? `Replication task failed · ${taskName}`
          : taskName ? `Background task failed · ${taskName}` : 'Background task failed',
        detail: failureDetail(row),
        ...eventSourceFields('background_task_failures'),
        query_id: queryId || undefined,
        database: database || undefined,
        table: table || undefined,
        task_name: taskName || undefined,
        exception_code: code,
        duration_ms: Number(row.duration_ms ?? 0),
      } satisfies OperationalEvent;
    });
  }

  private async fetchOperationalErrorEvents(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(OPERATIONAL_ERROR_EVENTS, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsOperationalErrors')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const hostname = String(row.host ?? '');
      const errorName = String(row.error ?? '');
      const code = Number(row.code ?? 0);
      const count = Number(row.value ?? 0);
      const remote = parseChBool(row.remote);
      const classification = classifyOperationalError(errorName);
      return {
        id: stableEventId([
          'error_log',
          hostname,
          occurredAt,
          code,
          errorName,
          remote ? 1 : 0,
        ]),
        occurred_at: occurredAt,
        hostname: hostname || undefined,
        kind: 'error_burst',
        category: classification.category,
        severity: classification.severity,
        precision: 'sampled',
        title: count > 1
          ? `Operational error · ${errorName || code} ×${count}`
          : `Operational error · ${errorName || code}`,
        detail: 'Persisted error count observed during this system.error_log interval',
        ...eventSourceFields('operational_errors'),
        exception_code: code,
        exception_name: errorName || undefined,
        count,
        remote,
      } satisfies OperationalEvent;
    });
  }

  private async fetchReplicaReadonlyEpisodes(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(REPLICA_READONLY_EPISODES, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsReplicaReadonly')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const rawEndedAt = String(row.ended_at ?? '').trim();
      const endedAt = rawEndedAt ? parseChTime(rawEndedAt) : undefined;
      const hostname = String(row.host ?? '');
      const count = Number(row.max_readonly_tables ?? 0);
      return {
        id: stableEventId(['metric_log', hostname, occurredAt, 'replica_readonly']),
        occurred_at: occurredAt,
        ended_at: endedAt,
        hostname: hostname || undefined,
        kind: 'replica_readonly',
        ...eventDefaults('replica_readonly'),
        precision: 'sampled',
        title: count > 1
          ? `${count} replicated tables entered read-only state`
          : 'Replicated table entered read-only state',
        detail: endedAt
          ? 'Reconstructed from the persisted ReadonlyReplica gauge; recovery was observed'
          : 'Reconstructed from the persisted ReadonlyReplica gauge; no recovery was observed in the selected range',
        ...eventSourceFields('replica_readonly'),
        count,
      } satisfies OperationalEvent;
    });
  }

  private async fetchReplicationFailureEvents(
    params: Record<string, QueryParameter>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<OperationalEvent[]> {
    const sql = buildQuery(REPLICATION_FAILURE_EVENTS, params);
    const rows = await this.adapter.executeQuery(
      tagQuery(sql, sourceTag(sourceTab, 'eventsReplicationFailures')),
    );
    return rows.map(raw => {
      const row = raw as Record<string, unknown>;
      const occurredAt = parseChTime(row.occurred_at);
      const hostname = String(row.host ?? '');
      const failureKind = String(row.failure_kind ?? '');
      const count = Number(row.value ?? 0);
      const isDataLoss = failureKind === 'data_loss';
      const label = isDataLoss
        ? 'Replication data loss'
        : failureKind === 'failed_fetch'
          ? 'Replicated part fetch failed'
          : 'Replicated part check failed';
      const kind: EventKind = isDataLoss
        ? 'replication_data_loss'
        : 'replication_task_failure';
      return {
        id: stableEventId([
          'metric_log',
          hostname,
          occurredAt,
          failureKind,
        ]),
        occurred_at: occurredAt,
        hostname: hostname || undefined,
        kind,
        ...eventDefaults(kind),
        precision: 'sampled',
        title: count > 1 ? `${label} ×${count}` : label,
        detail: 'Persisted ProfileEvent delta observed during this metric_log interval',
        ...eventSourceFields('replication_failures'),
        count,
      } satisfies OperationalEvent;
    });
  }
}
