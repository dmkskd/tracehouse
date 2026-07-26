import type { IClickHouseAdapter } from '../adapters/types.js';
import { buildQuery, tagQuery } from '../queries/builder.js';
import { TAB_EVENTS, TAB_TIME_TRAVEL, sourceTag } from '../queries/source-tags.js';
import {
  TIMELINE_BACKGROUND_TASK_FAILURE_EVENTS,
  TIMELINE_OPERATIONAL_ERROR_EVENTS,
  TIMELINE_PART_FAILURE_EVENTS,
  TIMELINE_QUERY_EVENTS,
  TIMELINE_REPLICA_READONLY_EPISODES,
  TIMELINE_REPLICATION_FAILURE_EVENTS,
  TIMELINE_SERVER_CRASH_EVENTS,
  TIMELINE_SERVER_RESTART_EVENTS,
} from '../queries/timeline-event-queries.js';
import type {
  TimelineEvent,
  TimelineEventCategory,
  TimelineEventKind,
  TimelineEventSeverity,
  TimelineEventSourceCoverage,
} from '../types/timeline.js';

export interface TimelineEventsOptions {
  startTime: string;
  endTime: string;
  hostname?: string | null;
  availableCapabilities: readonly string[];
  limit?: number;
}

export interface TimelineEventsResult {
  events: TimelineEvent[];
  coverage: TimelineEventSourceCoverage[];
}

interface EventSourceDefinition {
  source: string;
  capability: string;
  fetch: (params: Record<string, string | number>) => Promise<TimelineEvent[]>;
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

function classifyOperationalError(errorName: string): {
  category: TimelineEventCategory;
  severity: TimelineEventSeverity;
} {
  const category: TimelineEventCategory =
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

/**
 * Event sources mix DateTime and DateTime64 columns. Keep the shared query
 * bounds at whole-second precision so a DateTime column does not reject a
 * custom range containing milliseconds (ClickHouse error 53).
 */
function toClickHouseDateTime(value: string): string {
  const clickHouseDateTime = value.match(
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/,
  );
  if (clickHouseDateTime && !/[zZ]|[+-]\d{2}:?\d{2}$/.test(value)) {
    return `${clickHouseDateTime[1]} ${clickHouseDateTime[2]}`;
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value.replace('T', ' ').replace(/\.\d+/, '').replace(/Z$/, '');
  }
  return parsed.toISOString().slice(0, 19).replace('T', ' ');
}

function stableEventId(parts: Array<string | number | undefined>): string {
  return parts.map(part => String(part ?? '')).join(':');
}

function parseChBool(value: unknown): boolean {
  return value === true || value === 1 || value === '1' || value === 'true';
}

function classifyQueryFailure(code: number): {
  kind: TimelineEventKind;
  severity: TimelineEventSeverity;
  title: string;
} {
  const name = QUERY_EXCEPTION_NAMES[code] ?? `ClickHouse error ${code}`;
  if (code === 241 || code === 173) {
    return { kind: 'query_oom', severity: 'error', title: `Query OOM · ${name}` };
  }
  if (code === 159) {
    return { kind: 'query_timeout', severity: 'warning', title: `Query timeout · ${name}` };
  }
  if (code === 201 || code === 202 || code === 252 || code === 692) {
    return { kind: 'query_rejected', severity: 'warning', title: `Query rejected · ${name}` };
  }
  return { kind: 'query_resource_limit', severity: 'error', title: `Query resource failure · ${name}` };
}

function failureDetail(row: Record<string, unknown>): string | undefined {
  const value = String(row.exception ?? '').trim();
  return value || undefined;
}

/**
 * Collects operational events independently per source. Capability checks are
 * authoritative; Promise.allSettled is only a race/permission/schema fallback.
 */
export class TimelineEventsService {
  constructor(private adapter: IClickHouseAdapter) {}

  static notRequested(): TimelineEventsResult {
    return {
      events: [],
      coverage: [
        { source: 'system.query_log', capability: 'query_log', status: 'not_requested', event_count: 0 },
        { source: 'system.asynchronous_metric_log', capability: 'asynchronous_metric_log', status: 'not_requested', event_count: 0 },
        { source: 'system.crash_log', capability: 'crash_log', status: 'not_requested', event_count: 0 },
        { source: 'system.part_log', capability: 'part_log', status: 'not_requested', event_count: 0 },
        { source: 'system.background_schedule_pool_log', capability: 'background_schedule_pool_log', status: 'not_requested', event_count: 0 },
        { source: 'system.error_log', capability: 'error_log', status: 'not_requested', event_count: 0 },
        { source: 'system.metric_log (replica state)', capability: 'metric_log_replication_state', status: 'not_requested', event_count: 0 },
        { source: 'system.metric_log (replication failures)', capability: 'metric_log_replication_failures', status: 'not_requested', event_count: 0 },
      ],
    };
  }

  async getEvents(
    options: TimelineEventsOptions,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL = TAB_TIME_TRAVEL,
  ): Promise<TimelineEventsResult> {
    const limit = Math.max(1, Math.min(options.limit ?? 1000, 10_000));
    const params: Record<string, string | number> = {
      start_time: toClickHouseDateTime(options.startTime),
      end_time: toClickHouseDateTime(options.endTime),
      hostname: options.hostname ?? '',
      event_limit: limit,
    };
    const available = new Set(options.availableCapabilities);
    const sources: EventSourceDefinition[] = [
      {
        source: 'system.query_log',
        capability: 'query_log',
        fetch: sourceParams => this.fetchQueryEvents(sourceParams, sourceTab),
      },
      {
        source: 'system.asynchronous_metric_log',
        capability: 'asynchronous_metric_log',
        fetch: sourceParams => this.fetchRestartEvents(sourceParams, sourceTab),
      },
      {
        source: 'system.crash_log',
        capability: 'crash_log',
        fetch: sourceParams => this.fetchCrashEvents(sourceParams, sourceTab),
      },
      {
        source: 'system.part_log',
        capability: 'part_log',
        fetch: sourceParams => this.fetchPartFailureEvents(sourceParams, sourceTab),
      },
      {
        source: 'system.background_schedule_pool_log',
        capability: 'background_schedule_pool_log',
        fetch: sourceParams => this.fetchBackgroundTaskFailureEvents(sourceParams, sourceTab),
      },
      {
        source: 'system.error_log',
        capability: 'error_log',
        fetch: sourceParams => this.fetchOperationalErrorEvents(sourceParams, sourceTab),
      },
      {
        source: 'system.metric_log (replica state)',
        capability: 'metric_log_replication_state',
        fetch: sourceParams => this.fetchReplicaReadonlyEpisodes(sourceParams, sourceTab),
      },
      {
        source: 'system.metric_log (replication failures)',
        capability: 'metric_log_replication_failures',
        fetch: sourceParams => this.fetchReplicationFailureEvents(sourceParams, sourceTab),
      },
    ];

    const coverage: TimelineEventSourceCoverage[] = sources.map(source => ({
      source: source.source,
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

    const events: TimelineEvent[] = [];
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
    const unique = new Map<string, TimelineEvent>();
    for (const event of events) unique.set(event.id, event);

    return {
      events: [...unique.values()].sort(
        (a, b) => new Date(a.occurred_at).getTime() - new Date(b.occurred_at).getTime(),
      ),
      coverage,
    };
  }

  private async fetchQueryEvents(
    params: Record<string, string | number>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<TimelineEvent[]> {
    const sql = buildQuery(TIMELINE_QUERY_EVENTS, params);
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
        category: isDdl ? 'changes' : 'queries',
        severity: isDdl ? 'info' : classification.severity,
        precision: 'exact',
        title: isDdl ? `DDL · ${queryKind || 'Other'}` : classification.title,
        detail: isDdl ? undefined : failureDetail(row),
        source: 'system.query_log',
        capability: 'query_log',
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
      } satisfies TimelineEvent;
    });
  }

  private async fetchRestartEvents(
    params: Record<string, string | number>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<TimelineEvent[]> {
    const sql = buildQuery(TIMELINE_SERVER_RESTART_EVENTS, params);
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
        category: 'lifecycle',
        severity: 'warning',
        precision: 'inferred',
        title: 'Server restarted',
        detail: detectedFromReset
          ? 'Uptime moved backwards between consecutive persisted samples. '
            + 'Restart time is inferred as the observed sample time minus reported Uptime.'
          : 'This is the first persisted Uptime sample for the host. '
            + 'Restart time is inferred as the observed sample time minus reported Uptime.',
        source: 'system.asynchronous_metric_log',
        capability: 'asynchronous_metric_log',
        metric_name: 'Uptime',
        metric_value: uptime,
        previous_metric_value: previousUptime,
        metric_unit: 's',
      } satisfies TimelineEvent;
    });
  }

  private async fetchCrashEvents(
    params: Record<string, string | number>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<TimelineEvent[]> {
    const sql = buildQuery(TIMELINE_SERVER_CRASH_EVENTS, params);
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
        category: 'lifecycle',
        severity: 'critical',
        precision: 'exact',
        title: signal ? `Server crashed · signal ${signal}` : 'Server crashed',
        source: 'system.crash_log',
        capability: 'crash_log',
        query_id: queryId || undefined,
        signal,
        version: String(row.version ?? '') || undefined,
      } satisfies TimelineEvent;
    });
  }

  private async fetchPartFailureEvents(
    params: Record<string, string | number>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<TimelineEvent[]> {
    const sql = buildQuery(TIMELINE_PART_FAILURE_EVENTS, params);
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
        kind: isReplicationTask ? 'replication_task_failure' : 'part_failure',
        category: isReplicationTask ? 'replication' : 'storage',
        severity: 'error',
        precision: 'exact',
        title: isReplicationTask
          ? `Replication task failed · ${operation}`
          : operation ? `Part operation failed · ${operation}` : 'Part operation failed',
        detail: failureDetail(row),
        source: 'system.part_log',
        capability: 'part_log',
        query_id: queryId || undefined,
        database: database || undefined,
        table: table || undefined,
        part_name: partName || undefined,
        partition_id: String(row.partition_id ?? '') || undefined,
        operation: operation || undefined,
        disk_name: String(row.disk_name ?? '') || undefined,
        exception_code: code,
        duration_ms: Number(row.duration_ms ?? 0),
      } satisfies TimelineEvent;
    });
  }

  private async fetchBackgroundTaskFailureEvents(
    params: Record<string, string | number>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<TimelineEvent[]> {
    const sql = buildQuery(TIMELINE_BACKGROUND_TASK_FAILURE_EVENTS, params);
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
        kind: isReplicationTask ? 'replication_task_failure' : 'background_task_failure',
        category: isReplicationTask ? 'replication' : 'maintenance',
        severity: 'error',
        precision: 'exact',
        title: isReplicationTask
          ? `Replication task failed · ${taskName}`
          : taskName ? `Background task failed · ${taskName}` : 'Background task failed',
        detail: failureDetail(row),
        source: 'system.background_schedule_pool_log',
        capability: 'background_schedule_pool_log',
        query_id: queryId || undefined,
        database: database || undefined,
        table: table || undefined,
        task_name: taskName || undefined,
        exception_code: code,
        duration_ms: Number(row.duration_ms ?? 0),
      } satisfies TimelineEvent;
    });
  }

  private async fetchOperationalErrorEvents(
    params: Record<string, string | number>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<TimelineEvent[]> {
    const sql = buildQuery(TIMELINE_OPERATIONAL_ERROR_EVENTS, params);
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
        source: 'system.error_log',
        capability: 'error_log',
        exception_code: code,
        exception_name: errorName || undefined,
        count,
        remote,
      } satisfies TimelineEvent;
    });
  }

  private async fetchReplicaReadonlyEpisodes(
    params: Record<string, string | number>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<TimelineEvent[]> {
    const sql = buildQuery(TIMELINE_REPLICA_READONLY_EPISODES, params);
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
        category: 'replication',
        severity: 'error',
        precision: 'sampled',
        title: count > 1
          ? `${count} replicated tables entered read-only state`
          : 'Replicated table entered read-only state',
        detail: endedAt
          ? 'Reconstructed from the persisted ReadonlyReplica gauge; recovery was observed'
          : 'Reconstructed from the persisted ReadonlyReplica gauge; no recovery was observed in the selected range',
        source: 'system.metric_log',
        capability: 'metric_log_replication_state',
        count,
      } satisfies TimelineEvent;
    });
  }

  private async fetchReplicationFailureEvents(
    params: Record<string, string | number>,
    sourceTab: typeof TAB_EVENTS | typeof TAB_TIME_TRAVEL,
  ): Promise<TimelineEvent[]> {
    const sql = buildQuery(TIMELINE_REPLICATION_FAILURE_EVENTS, params);
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
      return {
        id: stableEventId([
          'metric_log',
          hostname,
          occurredAt,
          failureKind,
        ]),
        occurred_at: occurredAt,
        hostname: hostname || undefined,
        kind: isDataLoss ? 'replication_data_loss' : 'replication_task_failure',
        category: 'replication',
        severity: isDataLoss ? 'critical' : 'error',
        precision: 'sampled',
        title: count > 1 ? `${label} ×${count}` : label,
        detail: 'Persisted ProfileEvent delta observed during this metric_log interval',
        source: 'system.metric_log',
        capability: 'metric_log_replication_failures',
        count,
      } satisfies TimelineEvent;
    });
  }
}
