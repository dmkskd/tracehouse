import { describe, expect, it, vi } from 'vitest';
import type { IClickHouseAdapter, TaggedQuery } from '../../adapters/types.js';
import { EventsService } from '../events-service.js';

function adapter(
  execute: (sql: string) => Promise<Record<string, unknown>[]>,
): IClickHouseAdapter {
  const executeQuery = vi.fn((sql: TaggedQuery) => execute(sql));
  return {
    executeQuery: executeQuery as IClickHouseAdapter['executeQuery'],
  };
}

const OPTIONS = {
  startTime: '2026-07-25 12:00:00',
  endTime: '2026-07-25 13:00:00',
  availableCapabilities: [] as string[],
};

describe('EventsService', () => {
  it('normalizes millisecond range bounds for DateTime-backed event sources', async () => {
    let emittedSql = '';
    const mock = adapter(async sql => {
      emittedSql = sql;
      expect(sql).toContain("'2026-07-25 12:00:00'");
      expect(sql).toContain("'2026-07-25 13:00:00'");
      expect(sql).not.toContain('12:00:00.125');
      expect(sql).not.toContain('13:00:00.875');
      return [];
    });
    const service = new EventsService(mock);

    await service.getEvents({
      startTime: '2026-07-25T12:00:00.125Z',
      endTime: '2026-07-25T13:00:00.875Z',
      availableCapabilities: ['asynchronous_metric_log'],
    });

    expect(mock.executeQuery).toHaveBeenCalledTimes(1);
    expect(emittedSql).toContain(
      "toDateTime('2026-07-25 12:00:00', 'UTC')",
    );
    expect(emittedSql).toContain(
      "toDateTime('2026-07-25 13:00:00', 'UTC')",
    );
  });

  it('converts explicit offsets to UTC before querying every event source', async () => {
    let emittedSql = '';
    const mock = adapter(async sql => {
      emittedSql = sql;
      return [];
    });
    const service = new EventsService(mock);

    await service.getEvents({
      startTime: '2026-07-25T14:00:00+01:00',
      endTime: '2026-07-25T16:00:00+02:00',
      availableCapabilities: ['query_log'],
    });

    expect(emittedSql).toContain(
      "BETWEEN toDateTime('2026-07-25 13:00:00', 'UTC') AND toDateTime('2026-07-25 14:00:00', 'UTC')",
    );
  });

  it('queries only capability-confirmed sources and reports unavailable coverage', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('system.query_log');
      return [];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['query_log'],
    });

    expect(mock.executeQuery).toHaveBeenCalledTimes(1);
    expect(result.events).toEqual([]);
    expect(result.coverage).toEqual([
      expect.objectContaining({
        source: 'system.query_log',
        status: 'loaded',
        event_count: 0,
      }),
      expect.objectContaining({
        source: 'system.asynchronous_insert_log',
        status: 'unavailable',
      }),
      expect.objectContaining({
        source: 'system.backup_log',
        status: 'unavailable',
      }),
      expect.objectContaining({
        source: 'system.zookeeper_connection_log',
        status: 'unavailable',
      }),
      expect.objectContaining({
        source: 'system.asynchronous_metric_log',
        status: 'unavailable',
      }),
      expect.objectContaining({
        source: 'system.crash_log',
        status: 'unavailable',
      }),
      expect.objectContaining({
        source: 'system.part_log',
        status: 'unavailable',
      }),
      expect.objectContaining({
        source: 'system.background_schedule_pool_log',
        status: 'unavailable',
      }),
      expect.objectContaining({
        source: 'system.error_log',
        status: 'unavailable',
      }),
      expect.objectContaining({
        source: 'system.metric_log (replica state)',
        status: 'unavailable',
      }),
      expect.objectContaining({
        source: 'system.metric_log (replication failures)',
        status: 'unavailable',
      }),
    ]);
  });

  it('tags standalone event reads with the Events component', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('/* source:TraceHouse:Events:eventsQueryLog */');
      expect(sql).toContain('LIMIT 1000');
      expect(sql).not.toContain('LIMIT 1000 BY');
      return [];
    });
    const service = new EventsService(mock);

    await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['query_log'],
      origin: 'events',
    });

    expect(mock.executeQuery).toHaveBeenCalledTimes(1);
  });

  it('uses per-category LIMIT BY when the distributed probe succeeded', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain(
        "LIMIT 1000 BY if(type = 'QueryFinish', 'ddl', 'query_resource')",
      );
      return [];
    });
    const service = new EventsService(mock);

    await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['query_log', 'distributed_limit_by'],
    });

    expect(mock.executeQuery).toHaveBeenCalledTimes(1);
  });

  it('keeps recurring query OOM occurrences and their normalized hash', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain(
        'query_start_time_microseconds + toIntervalMillisecond(query_duration_ms)',
      );
      expect(sql).toContain('source:TraceHouse:');
      return [
        {
          host: 'ch-1',
          occurred_at: '2026-07-25 12:15:00.125000',
          query_id: 'oom-1',
          initial_query_id: 'scheduled-1',
          normalized_query_hash: '18446744073709551614',
          user: 'scheduler',
          query_kind: 'Select',
          query_short: 'SELECT scheduled_work()',
          exception_code: 241,
          exception: 'Memory limit exceeded',
          query_duration_ms: 2500,
          memory_usage: 1_000_000_000,
        },
        {
          host: 'ch-1',
          occurred_at: '2026-07-25 12:30:00.125000',
          query_id: 'oom-2',
          initial_query_id: 'scheduled-2',
          normalized_query_hash: '18446744073709551614',
          user: 'scheduler',
          query_kind: 'Select',
          query_short: 'SELECT scheduled_work()',
          exception_code: 241,
          exception: 'Memory limit exceeded',
          query_duration_ms: 2600,
          memory_usage: 1_100_000_000,
        },
      ];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['query_log'],
    });

    expect(result.events).toHaveLength(2);
    expect(result.events.map(event => event.occurred_at)).toEqual([
      '2026-07-25T12:15:00.125Z',
      '2026-07-25T12:30:00.125Z',
    ]);
    for (const event of result.events) {
      expect(event).toMatchObject({
        kind: 'query_oom',
        category: 'queries',
        severity: 'error',
        precision: 'exact',
        normalized_query_hash: '18446744073709551614',
        exception_name: 'MEMORY_LIMIT_EXCEEDED',
      });
    }
  });

  it('maps successful DDL as an independently filterable Changes event', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain("type = 'QueryFinish'");
      expect(sql).toContain("'Create', 'Alter', 'Drop'");
      expect(sql).toContain("query NOT LIKE '/* ddl_entry=query-%'");
      return [{
        host: 'ch-1',
        occurred_at: '2026-07-25 12:20:03.250000',
        type: 'QueryFinish',
        query_id: 'ddl-1',
        initial_query_id: 'ddl-1',
        normalized_query_hash: '99887766',
        user: 'deployer',
        query_kind: 'Alter',
        query_short: 'ALTER TABLE analytics.events ADD COLUMN source String',
        databases: ['analytics'],
        tables: ['analytics.events'],
        exception_code: 0,
        exception: '',
        query_duration_ms: 3250,
        memory_usage: 42_000,
      }];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['query_log'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        occurred_at: '2026-07-25T12:20:03.250Z',
        kind: 'ddl',
        category: 'changes',
        severity: 'info',
        precision: 'exact',
        title: 'DDL · Alter',
        query_id: 'ddl-1',
        databases: ['analytics'],
        tables: ['analytics.events'],
      }),
    ]);
    expect(result.events[0]?.exception_code).toBeUndefined();
  });

  it('classifies admission and timeout failures separately from query OOMs', async () => {
    const mock = adapter(async () => [
      {
        host: 'ch-1',
        occurred_at: '2026-07-25 12:10:00',
        query_id: 'timeout',
        exception_code: 159,
      },
      {
        host: 'ch-1',
        occurred_at: '2026-07-25 12:11:00',
        query_id: 'rejected',
        exception_code: 202,
      },
      {
        host: 'ch-1',
        occurred_at: '2026-07-25 12:12:00',
        query_id: 'disk',
        exception_code: 243,
      },
    ]);
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['query_log'],
    });

    expect(result.events.map(event => [event.kind, event.severity])).toEqual([
      ['query_timeout', 'warning'],
      ['query_rejected', 'warning'],
      ['query_resource_limit', 'error'],
    ]);
  });

  it('maps asynchronous insert parsing and flush failures', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('system.asynchronous_insert_log');
      expect(sql).toContain("status IN ('ParsingError', 'FlushError')");
      return [{
        host: 'ch-1',
        occurred_at: '2026-07-25 12:14:00.125000',
        query_id: 'async-insert-1',
        flush_query_id: 'async-flush-1',
        database: 'analytics',
        table: 'events',
        format: 'JSONEachRow',
        status: 'FlushError',
        rows: 125,
        bytes: 8192,
        exception: 'Not enough space',
      }];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['asynchronous_insert_log'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        occurred_at: '2026-07-25T12:14:00.125Z',
        kind: 'async_insert_failure',
        category: 'queries',
        severity: 'error',
        precision: 'exact',
        title: 'Async insert failed · FlushError',
        detail: 'Not enough space',
        query_id: 'async-insert-1',
        flush_query_id: 'async-flush-1',
        database: 'analytics',
        table: 'events',
        format: 'JSONEachRow',
        status: 'FlushError',
        rows: 125,
        bytes: 8192,
      }),
    ]);
  });

  it('maps terminal backup and restore outcomes with severity', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('system.backup_log');
      expect(sql).toContain("'BACKUP_FAILED'");
      expect(sql).not.toContain("'CREATING_BACKUP',");
      return [
        {
          host: 'ch-1',
          occurred_at: '2026-07-25 12:15:03.500000',
          operation_id: 'backup-1',
          query_id: 'query-backup-1',
          storage_name: "Disk('backups', 'daily.zip')",
          status: 'BACKUP_FAILED',
          error: 'Cannot write file',
          start_time: '2026-07-25 12:15:00.000000',
          end_time: '2026-07-25 12:15:03.500000',
          num_files: 12,
          total_size: 4096,
        },
        {
          host: 'ch-1',
          occurred_at: '2026-07-25 12:20:10.000000',
          operation_id: 'restore-1',
          query_id: 'query-restore-1',
          storage_name: "Disk('backups', 'daily.zip')",
          status: 'RESTORED',
          error: '',
          start_time: '2026-07-25 12:20:00.000000',
          end_time: '2026-07-25 12:20:10.000000',
          num_files: 12,
          total_size: 4096,
        },
      ];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['backup_log'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        kind: 'backup',
        category: 'maintenance',
        severity: 'error',
        title: 'Backup failed',
        detail: 'Cannot write file',
        operation: 'Backup',
        operation_id: 'backup-1',
        status: 'BACKUP_FAILED',
        started_at: '2026-07-25T12:15:00.000Z',
        duration_ms: 3500,
        num_files: 12,
        total_size: 4096,
      }),
      expect.objectContaining({
        kind: 'backup',
        severity: 'info',
        title: 'Restore completed',
        operation: 'Restore',
        operation_id: 'restore-1',
        status: 'RESTORED',
        duration_ms: 10_000,
      }),
    ]);
  });

  it('maps Keeper disconnects and connections as exact state changes', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('system.zookeeper_connection_log');
      expect(sql).toContain('event_time_microseconds');
      return [
        {
          host: 'ch-1',
          occurred_at: '2026-07-25 12:25:00.250000',
          connection_state: 'Disconnected',
          keeper_name: 'default',
          keeper_host: 'keeper-1',
          keeper_port: 9181,
          keeper_client_id: '9223372036854775000',
          reason: 'Session expired',
        },
        {
          host: 'ch-1',
          occurred_at: '2026-07-25 12:25:02.500000',
          connection_state: 'Connected',
          keeper_name: 'default',
          keeper_host: 'keeper-2',
          keeper_port: 9181,
          keeper_client_id: '9223372036854775001',
          reason: '',
        },
      ];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['zookeeper_connection_log'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        occurred_at: '2026-07-25T12:25:00.250Z',
        kind: 'keeper_connection',
        category: 'coordination',
        severity: 'error',
        precision: 'exact',
        title: 'Keeper disconnected · default',
        detail: 'Session expired',
        connection_state: 'Disconnected',
        keeper_name: 'default',
        keeper_host: 'keeper-1',
        keeper_port: 9181,
        keeper_client_id: '9223372036854775000',
      }),
      expect.objectContaining({
        occurred_at: '2026-07-25T12:25:02.500Z',
        kind: 'keeper_connection',
        category: 'coordination',
        severity: 'info',
        title: 'Keeper connected · default',
        connection_state: 'Connected',
        keeper_host: 'keeper-2',
      }),
    ]);
  });

  it('maps inferred restarts with both occurrence and observation times', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain("metric = 'Uptime'");
      expect(sql).toContain('INTERVAL 15 MINUTE');
      return [{
        host: 'ch-1',
        occurred_at: '2026-07-25 12:20:00',
        observed_at: '2026-07-25 12:20:42',
        uptime: 42,
        previous_uptime: 900,
      }];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['asynchronous_metric_log'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        occurred_at: '2026-07-25T12:20:00.000Z',
        observed_at: '2026-07-25T12:20:42.000Z',
        kind: 'server_restart',
        category: 'lifecycle',
        precision: 'inferred',
        detail: expect.stringContaining('Uptime moved backwards'),
        metric_name: 'Uptime',
        metric_value: 42,
        previous_metric_value: 900,
        metric_unit: 's',
      }),
    ]);
  });

  it('maps failed replicated part downloads into the Replication category', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('system.part_log');
      expect(sql).toContain('error != 0');
      return [{
        host: 'ch-2',
        occurred_at: '2026-07-25 12:22:01.500000',
        query_id: 'insert-1',
        event_type: 'DownloadPart',
        database: 'analytics',
        table: 'events',
        part_name: '202607_10_10_0',
        partition_id: '202607',
        disk_name: 's3',
        duration_ms: 850,
        error: 243,
        exception: 'Not enough space',
      }];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['part_log'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        occurred_at: '2026-07-25T12:22:01.500Z',
        kind: 'replication_task_failure',
        category: 'replication',
        severity: 'error',
        title: 'Replication task failed · DownloadPart',
        query_id: 'insert-1',
        database: 'analytics',
        table: 'events',
        part_name: '202607_10_10_0',
        partition_id: '202607',
        disk_name: 's3',
        operation: 'DownloadPart',
        exception_code: 243,
      }),
    ]);
  });

  it('maps failed MergeParts operations into the Merges category', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('system.part_log');
      expect(sql).toContain('error != 0');
      return [{
        host: 'ch-1',
        occurred_at: '2026-07-25 12:21:01.250000',
        query_id: '',
        event_type: 'MergeParts',
        database: 'analytics',
        table: 'events',
        part_name: '202607_10_12_1',
        partition_id: '202607',
        disk_name: 'default',
        duration_ms: 1250,
        error: 241,
        exception: 'Memory limit exceeded',
      }];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['part_log'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        occurred_at: '2026-07-25T12:21:01.250Z',
        kind: 'merge_failure',
        category: 'merges',
        severity: 'error',
        title: 'Merge failed · analytics.events',
        database: 'analytics',
        table: 'events',
        part_name: '202607_10_12_1',
        operation: 'MergeParts',
        exception_code: 241,
      }),
    ]);
  });

  it('maps failed MutatePart operations into the Merges category', async () => {
    const mock = adapter(async () => [{
      host: 'ch-1',
      occurred_at: '2026-07-25 12:21:02.500000',
      query_id: 'mutation-1',
      event_type: 'MutatePart',
      database: 'analytics',
      table: 'events',
      part_name: '202607_10_10_0_11',
      partition_id: '202607',
      disk_name: 'default',
      duration_ms: 2500,
      error: 395,
      exception: 'Mutation expression failed',
    }]);
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['part_log'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        kind: 'mutation_failure',
        category: 'merges',
        title: 'Mutation failed · analytics.events',
        operation: 'MutatePart',
        exception_code: 395,
      }),
    ]);
  });

  it('maps failed MovePart operations into Storage and defaults other part operations to Merges', async () => {
    const mock = adapter(async () => [
      {
        host: 'ch-1',
        occurred_at: '2026-07-25 12:21:03.000000',
        event_type: 'MovePart',
        database: 'analytics',
        table: 'events',
        part_name: '202607_10_10_0',
        error: 243,
        exception: 'Not enough space on destination disk',
      },
      {
        host: 'ch-1',
        occurred_at: '2026-07-25 12:21:04.000000',
        event_type: 'RemovePart',
        database: 'analytics',
        table: 'events',
        part_name: '202607_9_9_0',
        error: 1000,
        exception: 'Part cleanup failed',
      },
    ]);
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['part_log'],
    });

    expect(result.events).toEqual(expect.arrayContaining([
      expect.objectContaining({
        kind: 'part_move_failure',
        category: 'storage',
        title: 'Part move failed · analytics.events',
      }),
      expect.objectContaining({
        kind: 'part_failure',
        category: 'merges',
        title: 'Part operation failed · RemovePart',
      }),
    ]));
  });

  it('maps failed background work as independently filterable maintenance events', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('system.background_schedule_pool_log');
      expect(sql).toContain('error != 0');
      return [{
        host: 'ch-2',
        occurred_at: '2026-07-25 12:23:02.750000',
        query_id: 'distributed-send-1',
        database: 'analytics',
        table: 'events_distributed',
        log_name: 'analytics.events_distributed/Distributed',
        duration_ms: 1750,
        error: 209,
        exception: 'Connection refused',
      }];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['background_schedule_pool_log'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        occurred_at: '2026-07-25T12:23:02.750Z',
        kind: 'background_task_failure',
        category: 'maintenance',
        severity: 'error',
        title: 'Background task failed · analytics.events_distributed/Distributed',
        query_id: 'distributed-send-1',
        database: 'analytics',
        table: 'events_distributed',
        task_name: 'analytics.events_distributed/Distributed',
        exception_code: 209,
      }),
    ]);
  });

  it('maps allowlisted operational error deltas without duplicating query failures', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('system.error_log');
      expect(sql).toContain("'KEEPER_EXCEPTION'");
      expect(sql).toContain("'CORRUPTED_DATA'");
      expect(sql).not.toContain("'MEMORY_LIMIT_EXCEEDED'");
      expect(sql).not.toContain("'QUERY_WAS_CANCELLED'");
      return [
        {
          host: 'ch-1',
          occurred_at: '2026-07-25 12:24:00',
          code: 999,
          error: 'KEEPER_EXCEPTION',
          value: 8,
          remote: 0,
        },
        {
          host: 'ch-2',
          occurred_at: '2026-07-25 12:25:00',
          code: 246,
          error: 'CORRUPTED_DATA',
          value: 1,
          remote: 1,
        },
        {
          host: 'ch-2',
          occurred_at: '2026-07-25 12:26:00',
          code: 999,
          error: 'REPLICA_IS_ALREADY_ACTIVE',
          value: 2,
          remote: 0,
        },
      ];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['error_log'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        occurred_at: '2026-07-25T12:24:00.000Z',
        kind: 'error_burst',
        category: 'coordination',
        severity: 'error',
        precision: 'sampled',
        title: 'Operational error · KEEPER_EXCEPTION ×8',
        exception_code: 999,
        exception_name: 'KEEPER_EXCEPTION',
        count: 8,
        remote: false,
      }),
      expect.objectContaining({
        occurred_at: '2026-07-25T12:25:00.000Z',
        kind: 'error_burst',
        category: 'storage',
        severity: 'critical',
        title: 'Operational error · CORRUPTED_DATA',
        count: 1,
        remote: true,
      }),
      expect.objectContaining({
        occurred_at: '2026-07-25T12:26:00.000Z',
        kind: 'error_burst',
        category: 'replication',
        severity: 'error',
        title: 'Operational error · REPLICA_IS_ALREADY_ACTIVE ×2',
      }),
    ]);
  });

  it('maps persisted read-only replica state as a recoverable interval', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('CurrentMetric_ReadonlyReplica');
      expect(sql).toContain('leadInFrame');
      return [{
        host: 'ch-1',
        occurred_at: '2026-07-25 12:30:00',
        ended_at: '2026-07-25 12:34:12',
        max_readonly_tables: 3,
      }];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['metric_log_replication_state'],
    });

    expect(result.events).toEqual([
      expect.objectContaining({
        occurred_at: '2026-07-25T12:30:00.000Z',
        ended_at: '2026-07-25T12:34:12.000Z',
        kind: 'replica_readonly',
        category: 'replication',
        severity: 'error',
        precision: 'sampled',
        count: 3,
        title: '3 replicated tables entered read-only state',
      }),
    ]);
  });

  it('maps replication counter deltas as point events with distinct severity', async () => {
    const mock = adapter(async sql => {
      expect(sql).toContain('ProfileEvent_ReplicatedDataLoss');
      expect(sql).toContain('ProfileEvent_ReplicatedPartFailedFetches');
      return [
        {
          host: 'ch-1',
          occurred_at: '2026-07-25 12:35:00',
          failure_kind: 'data_loss',
          value: 1,
        },
        {
          host: 'ch-1',
          occurred_at: '2026-07-25 12:36:00',
          failure_kind: 'failed_fetch',
          value: 4,
        },
      ];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['metric_log_replication_failures'],
    });

    expect(result.events.map(event => [
      event.kind,
      event.category,
      event.severity,
      event.count,
    ])).toEqual([
      ['replication_data_loss', 'replication', 'critical', 1],
      ['replication_task_failure', 'replication', 'error', 4],
    ]);
  });

  it('degrades one failed source without hiding successful source results', async () => {
    const mock = adapter(async sql => {
      if (sql.includes('system.crash_log')) {
        throw new Error('Not enough privileges');
      }
      return [{
        host: 'ch-1',
        occurred_at: '2026-07-25 12:20:00',
        observed_at: '2026-07-25 12:20:42',
      }];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['asynchronous_metric_log', 'crash_log'],
    });

    expect(result.events).toHaveLength(1);
    expect(result.events[0]?.kind).toBe('server_restart');
    expect(result.coverage.find(item => item.capability === 'crash_log')).toMatchObject({
      status: 'failed',
      event_count: 0,
      detail: 'Not enough privileges',
    });
  });

  it('keeps other events when the query-log event calculation fails', async () => {
    const mock = adapter(async sql => {
      if (sql.includes('system.query_log')) {
        throw new Error('Query-log calculation failed');
      }
      return [{
        host: 'ch-1',
        occurred_at: '2026-07-25 12:20:00',
        observed_at: '2026-07-25 12:20:42',
      }];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['query_log', 'asynchronous_metric_log'],
    });

    expect(result.events).toHaveLength(1);
    expect(result.events[0]?.kind).toBe('server_restart');
    expect(result.coverage.find(item => item.capability === 'query_log')).toMatchObject({
      status: 'failed',
      event_count: 0,
      detail: 'Query-log calculation failed',
    });
    expect(result.coverage.find(item => item.capability === 'asynchronous_metric_log')).toMatchObject({
      status: 'loaded',
      event_count: 1,
    });
  });

  it('marks collection as truncated when a source reaches its row limit', async () => {
    const mock = adapter(async () => [{
      host: 'ch-1',
      occurred_at: '2026-07-25 12:10:00',
      query_id: 'timeout',
      exception_code: 159,
    }]);
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      availableCapabilities: ['query_log'],
      limit: 1,
    });

    expect(result.coverage[0]).toMatchObject({
      status: 'loaded',
      event_count: 1,
      truncated: true,
    });
  });

  it('collects and combines events from every selected host', async () => {
    const mock = adapter(async sql => {
      const hostname = sql.includes("'host-a'") ? 'host-a' : 'host-b';
      return [{
        host: hostname,
        occurred_at: hostname === 'host-a'
          ? '2026-07-25 12:10:00'
          : '2026-07-25 12:20:00',
        query_id: `timeout-${hostname}`,
        exception_code: 159,
      }];
    });
    const service = new EventsService(mock);

    const result = await service.getEvents({
      ...OPTIONS,
      hostname: ['host-a', 'host-b'],
      availableCapabilities: ['query_log'],
    });

    expect(mock.executeQuery).toHaveBeenCalledTimes(2);
    expect(result.events.map(event => event.hostname)).toEqual(['host-a', 'host-b']);
    expect(result.coverage[0]).toMatchObject({
      status: 'loaded',
      event_count: 2,
    });
  });
});
