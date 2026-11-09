import { fileURLToPath } from 'node:url';
import { test, expect } from '@playwright/test';
import { engineTimezoneFixture } from '../../core/src/__tests__/integration/fixtures/engine-timezone-fixture.js';
import { partTimezoneFixture } from '../../core/src/__tests__/integration/fixtures/part-timezone-fixture.js';
import { mergeTimezoneFixture } from '../../core/src/__tests__/integration/fixtures/merge-timezone-fixture.js';
import { queryTimezoneFixture } from '../../core/src/__tests__/integration/fixtures/query-timezone-fixture.js';

const frontendUrl = process.env.TIMEZONE_FRONTEND_URL ?? 'http://localhost:5173';
const grafanaUrl = process.env.GRAFANA_E2E_URL ?? 'http://localhost:3001';
const proxyUrl = process.env.TIMEZONE_PROXY_URL ?? 'http://localhost:8990/proxy';
const corePath = fileURLToPath(new URL('../../core/src/', import.meta.url));
const moduleUrl = (path: string) => `${frontendUrl}/@fs${corePath}${path}.ts`;
const epoch = Date.parse('2026-11-08T17:00:00Z') / 1000;
const fixtureMap = Object.fromEntries([
  'query_log', 'asynchronous_metric_log', 'metric_log', 'text_log',
  'asynchronous_insert_log', 'backup_log', 'zookeeper_connection_log', 'crash_log',
  'part_log', 'background_schedule_pool_log', 'error_log', 'query_thread_log', 'asynchronous_metrics',
].map(table => [table, queryTimezoneFixture(table, epoch, 'Europe/Copenhagen')]));

for (const transport of ['standalone', 'proxy', 'grafana'] as const) {
  test(`${transport}: real transport preserves migrated service clocks`, async ({ page }, testInfo) => {
    await page.goto(transport === 'grafana' ? `${grafanaUrl}/a/dmkskd-tracehouse-app/events` : frontendUrl, { waitUntil: 'domcontentloaded' });
    if (transport === 'grafana') {
      await page.waitForFunction(() => typeof (window as any).System?.import === 'function', undefined, { timeout: 10_000 });
    }
    const result = await page.evaluate(async args => {
      const { EventsService } = await import(args.eventsUrl);
      const { InteractiveQueryService } = await import(args.interactiveUrl);
      const { AnalyticsService } = await import(args.analyticsServiceUrl);
      const { formatAnalyticsTimestamp } = await import(args.analyticsTimestampUrl);
      const { formatTimeLabels } = await import(args.resourceLabelsUrl);
      const { QueryAnalyzer } = await import(args.analyzerUrl);
      const { MergeTracker } = await import(args.mergeUrl);
      const { DatabaseExplorer } = await import(args.databaseUrl);
      const { EngineInternalsService } = await import(args.engineUrl);
      const { OverviewService } = await import(args.overviewUrl);
      const { TraceService } = await import(args.traceUrl);
      const { EventContextService } = await import(args.contextUrl);
      const { EVENT_SOURCE_DEFINITIONS } = await import(args.eventsTypesUrl);
      let raw: any;
      let analyticsSchema: any;
      if (args.transport === 'standalone') {
        const { BrowserAdapter } = await import(args.browserUrl);
        raw = new BrowserAdapter({
          host: 'localhost', port: 8123, user: 'default', password: '',
          database: 'default', secure: false, connect_timeout: 10, send_receive_timeout: 30,
        });
      } else if (args.transport === 'proxy') {
        const { ProxyAdapter } = await import(args.proxyAdapterUrl);
        raw = new ProxyAdapter({
          host: 'localhost', port: 8123, user: 'default', password: '',
          database: 'default', secure: false, connect_timeout: 10, send_receive_timeout: 30,
        }, args.proxyUrl);
      } else {
        const [runtime, rx, data] = await Promise.all(
          ['@grafana/runtime', 'rxjs', '@grafana/data'].map(name => (window as any).System.import(name)),
        );
        const sources = await (await fetch('/api/datasources')).json();
        const config = sources.find((source: any) => source.type === 'grafana-clickhouse-datasource');
        if (!config) throw new Error('No provisioned ClickHouse datasource');
        const ds = await runtime.getDataSourceSrv().get(config.uid);
        const { GrafanaAdapter, GRAFANA_QUERY_TIMEZONE } = await import(args.grafanaAdapterUrl);
        raw = new GrafanaAdapter(async (sql: string, refId: string) => {
          // Same datasource API, format and browser-timezone request as ServiceProvider.
          const response = await rx.lastValueFrom(ds.query({
            targets: [{ refId, rawSql: sql, format: 1, datasource: { uid: config.uid, type: config.type } }],
            range: { from: data.dateTime(), to: data.dateTime(), raw: { from: 'now', to: 'now' } },
            requestId: `timezone-check-${crypto.randomUUID()}`,
            interval: '1s', intervalMs: 1000, scopedVars: {}, timezone: GRAFANA_QUERY_TIMEZONE, app: 'tracehouse', startTime: Date.now(),
          }));
          if (response.error) throw new Error(response.error.message ?? String(response.error));
          if (refId !== 'types' && sql.includes('AS clock')) analyticsSchema = response.data?.[0]?.fields.map((field: any) => ({ name: field.name, type: field.type, typeInfo: field.typeInfo, config: field.config }));
          return response.data ?? [];
        });
      }
      const adapter = {
        executeQuery: (sql: string) => raw.executeQuery(sql.replace(/\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
          (_: string, table: string) => {
            if (!(table in args.fixtures)) throw new Error(`Unexpected source: ${table}`);
            return args.fixtures[table];
          })),
      };
      try {
        const events = await new EventsService(adapter).getEvents({
          startTime: new Date((args.epoch - 10) * 1000).toISOString(),
          endTime: new Date((args.epoch + 10) * 1000).toISOString(),
          availableCapabilities: [...EVENT_SOURCE_DEFINITIONS.map((source: any) => source.capability), 'distributed_limit_by'],
        });
        const context = await new EventContextService(adapter).getContext({
          eventTime: new Date(args.epoch * 1000).toISOString(), queryId: 'fixture-query', windowSeconds: 10,
          availableCapabilities: ['query_log', 'metric_log', 'text_log'],
        });
        const analyzer = new QueryAnalyzer(adapter);
        const history = await analyzer.getQueryHistory({
          start_date: '2026-11-08', start_time: new Date((args.epoch - 20) * 1000).toISOString(),
          end_time: new Date((args.epoch + 20) * 1000).toISOString(), limit: 1,
        });
        const detail = await analyzer.getQueryDetail('fixture-query', '2026-11-08');
        const similar = await analyzer.getSimilarQueries('42', 1);
        const start = new Date((args.epoch - 10) * 1000).toISOString();
        const end = new Date((args.epoch + 10) * 1000).toISOString();
        const cpu = await analyzer.getServerCpuForRange(start, end);
        const memory = await analyzer.getServerMemoryForRange(start, end);
        const threads = await analyzer.getQueryThreadBreakdown('fixture-query', '2026-11-08');
        const trace = new TraceService(adapter);
        const logs = await trace.getQueryLogs('fixture-query', undefined, '2026-11-08', 1);
        const filteredLogs = await trace.getQueryLogs('fixture-query', ['Error'], '2026-11-08', 1);
        const mergeAdapter = {
          executeQuery: (sql: string) => raw.executeQuery(sql.replace(/\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
            (_: string, table: string) => args.mergeFixtures[table])),
        };
        const tracker = new MergeTracker(mergeAdapter);
        const merges = await tracker.getMergeHistory({ limit: 2 });
        const mergeDetails = await Promise.all(merges.map((row: any) => tracker.getMergeHistoryByPartName('default', 'fixture', 'part-fixture', row)));
        const mutations = await tracker.getMutationHistory({ limit: 1 });
        const partsAdapter = {
          executeQuery: (sql: string) => raw.executeQuery(sql.replace(/\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
            (_: string, table: string) => args.partFixtures[table])),
        };
        const explorer = new DatabaseExplorer(partsAdapter);
        const parts = await explorer.getTableParts('default', 'fixture');
        const partDetail = await explorer.getPartDetail('default', 'fixture', 'fixture_1_2_1');
        const lineage = await explorer.getPartLineage('default', 'fixture', 'fixture_1_2_1');
        const patternQuery = await new AnalyticsService(adapter).getLatestQueryForPattern('42');
        const analytics = await new InteractiveQueryService(raw).run(`SELECT
          toDateTime(1794157200, 'Europe/Copenhagen') AS clock,
          fromUnixTimestamp64Micro(1794157200123456, 'Europe/Copenhagen') AS micro,
          toDate('2026-11-08') AS calendar, '2026-11-08 18:00:00' AS text,
          toUInt64(42) AS count`, 'TraceHouse:Analytics:timezoneRegression');
        const engineAdapter = {
          executeQuery: (sql: string) => raw.executeQuery(sql
            .replaceAll('now()', `toDateTime(${args.epoch + 10}, 'Europe/Copenhagen')`)
            .replaceAll('today()', `toDate(toDateTime(${args.epoch + 10}, 'Europe/Copenhagen'))`)
            .replace(/\{\{cluster_aware:system\.([a-z_]+)\}\}/g, (_: string, table: string) => args.engineFixtures[table])),
        };
        const engine = new EngineInternalsService(engineAdapter);
        const coreTimeline = await engine.getCoreTimeline();
        const dictionaries = await engine.getDictionaries();
        const qps = await new OverviewService(engineAdapter).getQpsHistory();
        return {
          coreTimeline, dictionaries, qps,
          analytics, patternQuery, analyticsSchema, analyticsLocalClock: formatAnalyticsTimestamp(String(analytics[0].clock)), surfaceClock: formatTimeLabels([String(analytics[0].clock)])[0],
          parts, partDetail, lineage,
          merges, mergeDetails, mutations,
          history, detail, logs, filteredLogs, similar, cpu, memory, threads,
          events: events.events, failed: events.coverage.filter((source: any) => source.status === 'failed'), context,
          zone: Intl.DateTimeFormat().resolvedOptions().timeZone,
        };
      } finally { await raw.close?.(); }
    }, {
      resourceLabelsUrl: moduleUrl('utils/resource-lanes-processor'),
      analyticsTimestampUrl: `${frontendUrl}/@fs${fileURLToPath(new URL('../../../frontend/src/components/analytics/timestamp.ts', import.meta.url))}`,
      engineUrl: moduleUrl('services/engine-internals'), overviewUrl: moduleUrl('services/overview-service'),
      engineFixtures: Object.fromEntries(['trace_log', 'dictionaries', 'metric_log'].map(table => [table, engineTimezoneFixture(table, epoch, 'Europe/Copenhagen')])),
      transport, proxyUrl, proxyAdapterUrl: moduleUrl('adapters/proxy-adapter'), epoch, fixtures: fixtureMap, interactiveUrl: moduleUrl('services/interactive-query-service'), analyticsServiceUrl: moduleUrl('services/analytics-service'),
      mergeFixtures: Object.fromEntries(['part_log', 'tables', 'columns', 'mutations'].map(table => [table, mergeTimezoneFixture(table, epoch, 'Europe/Copenhagen')])),
      mergeUrl: moduleUrl('services/merge-tracker'), databaseUrl: moduleUrl('services/database-explorer'),
      partFixtures: Object.fromEntries(['parts', 'parts_columns', 'tables', 'columns', 'part_log'].map(table => [table, partTimezoneFixture(table, epoch, 'Europe/Copenhagen')])),
      analyzerUrl: moduleUrl('services/query-analyzer'), traceUrl: moduleUrl('services/trace-service'),
      eventsUrl: moduleUrl('services/events-service'), contextUrl: moduleUrl('services/event-context-service'),
      browserUrl: moduleUrl('adapters/browser-adapter'), grafanaAdapterUrl: moduleUrl('adapters/grafana-adapter'),
      eventsTypesUrl: moduleUrl('types/events'),
    });
    expect(result.zone).toBe(testInfo.project.use.timezoneId);
    expect(result.failed).toEqual([]);
    await testInfo.attach('analytics-transport', { body: JSON.stringify({ analytics: result.analytics, schema: result.analyticsSchema }), contentType: 'application/json' });
    const localHour = ({ 'Europe/London': '17', 'Europe/Copenhagen': '18', 'America/New_York': '12' } as Record<string, string>)[String(testInfo.project.use.timezoneId)];
    expect(result.analyticsLocalClock).toBe(`2026-11-08 ${localHour}:00:00`);
    expect(result.surfaceClock).toBe(`${localHour}:00`);
    expect(Date.parse(result.patternQuery.start_time)).toBe((epoch - 10) * 1000 + 123);
    expect(Date.parse(result.patternQuery.end_time)).toBe(epoch * 1000 + 123);
    expect(result.coreTimeline.slots).toHaveLength(1);
    expect(result.coreTimeline.slots[0].timeMs).toBe(epoch * 1000 + 100);
    expect(Date.parse(result.dictionaries[0].lastSuccessfulUpdate)).toBe(epoch * 1000);
    expect(result.qps.length).toBeGreaterThan(0);
    expect(Date.parse(result.qps[0].time)).toBe((epoch - 15) * 1000);
    expect(result.analytics).toHaveLength(1);
    expect(Date.parse(result.analytics[0].clock)).toBe(epoch * 1000);
    expect(Date.parse(result.analytics[0].micro)).toBe(epoch * 1000 + 123);
    expect(result.analytics[0].calendar).toBe('2026-11-08');
    expect(result.analytics[0].text).toBe('2026-11-08 18:00:00');
    expect(Number(result.analytics[0].count)).toBe(42);
    expect(result.parts).toHaveLength(1);
    expect(Date.parse(result.parts[0].modification_time)).toBe(epoch * 1000);
    expect(Date.parse(result.partDetail.modification_time)).toBe(epoch * 1000);
    expect(Date.parse(result.lineage.root.event_time)).toBe(epoch * 1000);
    expect(result.lineage.root.children).toHaveLength(2);
    expect(result.merges).toHaveLength(2);
    expect(new Set(result.merges.map((row: any) => row.event_time_microseconds)).size).toBe(2);
    for (let i = 0; i < result.merges.length; i++) {
      expect(Date.parse(result.merges[i].event_time)).toBe(epoch * 1000);
      expect(result.mergeDetails[i].event_time_microseconds).toBe(result.merges[i].event_time_microseconds);
    }
    expect(result.mutations).toHaveLength(1);
    expect(Date.parse(result.mutations[0].create_time)).toBe(epoch * 1000);
    expect(Date.parse(result.mutations[0].latest_fail_time)).toBe((epoch + 2) * 1000);
    expect(result.history).toHaveLength(1);
    expect(Date.parse(result.history[0].query_start_time)).toBe((epoch - 10) * 1000);
    expect(Date.parse(result.detail.query_start_time)).toBe((epoch - 10) * 1000);
    for (const logs of [result.logs, result.filteredLogs]) {
      expect(logs).toHaveLength(1);
      expect(logs[0].event_time_microseconds).toBe('2026-11-08 17:00:00.123456');
      expect(logs[0].message).toBe('fixture message');
    }
    expect(result.detail.query_start_time_microseconds).toBe('2026-11-08 16:59:50.123456');
    expect(result.similar).toHaveLength(1);
    expect(Date.parse(result.similar[0].query_start_time)).toBe((epoch - 10) * 1000);
    expect(result.cpu.map((point: any) => Date.parse(point.t))).toEqual([-5, 0, 5].map(delta => (epoch + delta) * 1000));
    expect(result.memory.map((point: any) => point.t)).toEqual(result.cpu.map((point: any) => point.t));
    expect(result.threads).toHaveLength(1);
    expect(result.threads[0].query_start_time_us).toBe('2026-11-08 16:59:50.123456');
    expect(result.events).toHaveLength(11);
    for (const event of result.events) {
      expect(Date.parse(event.occurred_at)).toBe(epoch * 1000 + (event.kind === 'server_restart' ? -1000
        : ['server_crash', 'error_burst', 'replica_readonly', 'replication_data_loss'].includes(event.kind) ? 0 : 123));
      expect(event.id).toMatch(/^event-v2:/);
      expect(event.legacy_ids).toHaveLength(1);
    }
    for (const source of [result.context.workload, result.context.metrics, result.context.logs]) {
      expect(source.status, source.detail).toBe('loaded');
      expect(source.data.length).toBeGreaterThan(0);
    }
    expect(Date.parse(result.context.workload.data[0].start_time)).toBe((epoch - 10) * 1000 + 123);
    expect(Date.parse(result.context.logs.data[0].time)).toBe(epoch * 1000 + 123);
    expect(Date.parse(result.context.metrics.snapshots[0].time)).toBe(epoch * 1000);
  });
}
