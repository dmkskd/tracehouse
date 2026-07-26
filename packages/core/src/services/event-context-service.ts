import type { IClickHouseAdapter } from '../adapters/types.js';
import {
  EVENT_CONTEXT_HOST_METRICS,
  EVENT_CONTEXT_SERVER_LOGS,
  EVENT_CONTEXT_WORKLOAD,
} from '../queries/event-context-queries.js';
import { buildQuery, tagQuery } from '../queries/builder.js';
import { TAB_EVENTS, sourceTag } from '../queries/source-tags.js';
import type {
  EventContextLogEntry,
  EventContextMetricPoint,
  EventContextMetricSnapshot,
  EventContextOptions,
  EventContextQuery,
  EventContextResult,
  EventContextSource,
} from '../types/event-context.js';

function parseChTime(value: unknown): string {
  const raw = String(value ?? '').trim();
  const normalized = raw.replace(' ', 'T');
  const withTimezone = normalized.includes('Z') || /[+-]\d\d:\d\d$/.test(normalized)
    ? normalized
    : `${normalized}Z`;
  const parsed = new Date(withTimezone);
  return Number.isNaN(parsed.getTime()) ? raw : parsed.toISOString();
}

function toClickHouseDateTime64(value: Date): string {
  return value.toISOString().replace('T', ' ').replace('Z', '');
}

function numberValue(value: unknown): number {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function stringValue(value: unknown): string {
  return String(value ?? '').trim();
}

function booleanValue(value: unknown): boolean {
  return value === true || value === 1 || value === '1' || value === 'true';
}

function emptySource<T>(
  source: string,
  capability: string,
  available: ReadonlySet<string>,
): EventContextSource<T> {
  return {
    source,
    capability,
    status: available.has(capability) ? 'loaded' : 'unavailable',
    data: [],
    detail: available.has(capability) ? undefined : 'Capability not available',
  };
}

function failureDetail(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}

/**
 * Loads historical evidence around one event. Sources are deliberately
 * independent: an absent text_log must not hide query or metric context.
 */
export class EventContextService {
  constructor(private adapter: IClickHouseAdapter) {}

  async getContext(options: EventContextOptions): Promise<EventContextResult> {
    const eventDate = new Date(options.eventTime);
    if (Number.isNaN(eventDate.getTime())) {
      throw new Error(`Invalid event time: ${options.eventTime}`);
    }

    const windowSeconds = Math.max(10, Math.min(options.windowSeconds ?? 300, 3600));
    const limit = Math.max(1, Math.min(options.limit ?? 100, 500));
    const windowStart = new Date(eventDate.getTime() - windowSeconds * 1000);
    const windowEnd = new Date(eventDate.getTime() + windowSeconds * 1000);
    const available = new Set(options.availableCapabilities);
    const params: Record<string, string | number> = {
      event_time: toClickHouseDateTime64(eventDate),
      window_start: toClickHouseDateTime64(windowStart),
      window_end: toClickHouseDateTime64(windowEnd),
      hostname: options.hostname ?? '',
      query_id: options.queryId ?? '',
      initial_query_id: options.initialQueryId ?? options.queryId ?? '',
      context_limit: limit,
    };

    const workload = emptySource<EventContextQuery>(
      'system.query_log',
      'query_log',
      available,
    );
    const metrics = {
      ...emptySource<EventContextMetricPoint>('system.metric_log', 'metric_log', available),
      snapshots: [] as EventContextMetricSnapshot[],
    };
    const logs = emptySource<EventContextLogEntry>(
      'system.text_log',
      'text_log',
      available,
    );

    const requested: Array<{
      capability: string;
      run: () => Promise<void>;
    }> = [
      {
        capability: workload.capability,
        run: async () => {
          const sql = buildQuery(EVENT_CONTEXT_WORKLOAD, params);
          const rows = await this.adapter.executeQuery(
            tagQuery(sql, sourceTag(TAB_EVENTS, 'contextWorkload')),
          );
          workload.data = rows.map(raw => {
            const row = raw as Record<string, unknown>;
            const queryId = stringValue(row.query_id);
            return {
              hostname: stringValue(row.host) || undefined,
              query_id: queryId,
              initial_query_id: stringValue(row.initial_query_id) || undefined,
              user: stringValue(row.user) || undefined,
              query_kind: stringValue(row.query_kind) || undefined,
              start_time: parseChTime(row.start_time),
              end_time: parseChTime(row.end_time),
              duration_ms: numberValue(row.query_duration_ms),
              memory_usage: numberValue(row.memory_usage),
              cpu_us: numberValue(row.cpu_us),
              read_rows: numberValue(row.read_rows),
              read_bytes: numberValue(row.read_bytes),
              written_rows: numberValue(row.written_rows),
              written_bytes: numberValue(row.written_bytes),
              status: stringValue(row.status),
              exception_code: numberValue(row.exception_code) || undefined,
              exception: stringValue(row.exception) || undefined,
              query: stringValue(row.query),
              is_event_query: booleanValue(row.is_event_query),
            };
          });
        },
      },
      {
        capability: metrics.capability,
        run: async () => {
          const sql = buildQuery(EVENT_CONTEXT_HOST_METRICS, params);
          const rows = await this.adapter.executeQuery(
            tagQuery(sql, sourceTag(TAB_EVENTS, 'contextHostMetrics')),
          );
          metrics.data = rows.map(raw => {
            const row = raw as Record<string, unknown>;
            return {
              hostname: stringValue(row.host) || undefined,
              time: parseChTime(row.sample_time),
              memory_usage: numberValue(row.memory_usage),
              active_queries: numberValue(row.active_queries),
              active_merges: numberValue(row.active_merges),
              cpu_cores: numberValue(row.cpu_cores),
            };
          });
          metrics.snapshots = this.metricSnapshots(metrics.data, eventDate.getTime());
        },
      },
      {
        capability: logs.capability,
        run: async () => {
          const sql = buildQuery(EVENT_CONTEXT_SERVER_LOGS, params);
          const rows = await this.adapter.executeQuery(
            tagQuery(sql, sourceTag(TAB_EVENTS, 'contextServerLogs')),
          );
          logs.data = rows.map(raw => {
            const row = raw as Record<string, unknown>;
            return {
              hostname: stringValue(row.host) || undefined,
              time: parseChTime(row.occurred_at),
              level: stringValue(row.level),
              logger: stringValue(row.logger_name),
              message: stringValue(row.message),
              query_id: stringValue(row.query_id) || undefined,
              thread_name: stringValue(row.thread_name) || undefined,
              is_event_query: booleanValue(row.is_event_query),
            };
          });
        },
      },
    ];

    const selected = requested.filter(source => available.has(source.capability));
    const settled = await Promise.allSettled(selected.map(source => source.run()));
    settled.forEach((result, index) => {
      if (result.status === 'fulfilled') return;
      const capability = selected[index].capability;
      const source = capability === workload.capability
        ? workload
        : capability === metrics.capability
          ? metrics
          : logs;
      source.status = 'failed';
      source.detail = failureDetail(result.reason);
      source.data = [];
      if (source === metrics) metrics.snapshots = [];
    });

    return {
      event_time: eventDate.toISOString(),
      window_start: windowStart.toISOString(),
      window_end: windowEnd.toISOString(),
      workload,
      metrics,
      logs,
    };
  }

  private metricSnapshots(
    points: readonly EventContextMetricPoint[],
    eventTimeMs: number,
  ): EventContextMetricSnapshot[] {
    const byHost = new Map<string, EventContextMetricPoint[]>();
    for (const point of points) {
      const host = point.hostname ?? '';
      const hostPoints = byHost.get(host) ?? [];
      hostPoints.push(point);
      byHost.set(host, hostPoints);
    }

    const snapshots: EventContextMetricSnapshot[] = [];
    for (const hostPoints of byHost.values()) {
      const preceding = hostPoints
        .filter(point => Date.parse(point.time) <= eventTimeMs)
        .sort((a, b) => Date.parse(b.time) - Date.parse(a.time))[0];
      if (!preceding) continue;
      snapshots.push({
        ...preceding,
        sample_age_ms: Math.max(0, eventTimeMs - Date.parse(preceding.time)),
      });
    }
    return snapshots.sort((a, b) => (a.hostname ?? '').localeCompare(b.hostname ?? ''));
  }
}
