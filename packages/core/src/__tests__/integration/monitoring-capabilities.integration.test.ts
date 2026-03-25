/**
 * Integration tests for MonitoringCapabilitiesService.
 *
 * Probes a real ClickHouse instance and verifies that capabilities are
 * correctly detected — including tracehouse.processes_history which
 * requires the tracehouse database + table to exist.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { runTracehouseSetup } from './setup/tracehouse-setup.js';
import { MonitoringCapabilitiesService } from '../../services/monitoring-capabilities.js';
import { deriveMonitoringFlags } from '../../types/monitoring-capabilities.js';

const CONTAINER_TIMEOUT = 120_000;

describe('MonitoringCapabilitiesService integration', () => {
  let ctx: TestClickHouseContext;

  beforeAll(async () => {
    ctx = await startClickHouse();
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      // Clean up tracehouse database if we created it
      if (!ctx.keepData) {
        await ctx.client.command({ query: `DROP DATABASE IF EXISTS tracehouse` }).catch(() => {});
      }
      await stopClickHouse(ctx);
    }
  }, 30_000);

  it('should probe basic capabilities on a fresh ClickHouse instance', async () => {
    const svc = new MonitoringCapabilitiesService(ctx.adapter);
    const result = await svc.probe();

    expect(result.serverVersion).toBeTruthy();
    expect(result.probedAt).toBeInstanceOf(Date);
    expect(result.capabilities.length).toBeGreaterThan(0);

    // A fresh ClickHouse instance should have query_log
    const queryLog = result.capabilities.find(c => c.id === 'query_log');
    expect(queryLog).toBeDefined();
    expect(queryLog!.available).toBe(true);

    // Server version should be parseable
    expect(result.serverVersion).toMatch(/^\d+\.\d+/);
  });

  it('should detect processes_history as unavailable when tracehouse database does not exist', async () => {
    // Make sure tracehouse doesn't exist
    await ctx.client.command({ query: `DROP DATABASE IF EXISTS tracehouse` });

    const svc = new MonitoringCapabilitiesService(ctx.adapter);
    const result = await svc.probe();

    const cap = result.capabilities.find(c => c.id === 'tracehouse_processes_history');
    expect(cap).toBeDefined();
    expect(cap!.available).toBe(false);

    const flags = deriveMonitoringFlags(result.capabilities, result.serverVersion);
    expect(flags.hasProcessesHistory).toBe(false);
  });

  it('should detect processes_history as available after creating the table', async () => {
    // Create the tracehouse database and processes_history table using the production script
    await runTracehouseSetup(ctx, { target: 'processes', tablesOnly: true });

    const svc = new MonitoringCapabilitiesService(ctx.adapter);
    const result = await svc.probe();

    const cap = result.capabilities.find(c => c.id === 'tracehouse_processes_history');
    expect(cap).toBeDefined();
    expect(cap!.available).toBe(true);
    expect(cap!.category).toBe('profiling');
    expect(cap!.detail).toContain('MergeTree');

    const flags = deriveMonitoringFlags(result.capabilities, result.serverVersion);
    expect(flags.hasProcessesHistory).toBe(true);
  });

  it('should report row count in processes_history detail', async () => {
    // Insert some synthetic data
    await ctx.client.command({
      query: `
        INSERT INTO tracehouse.processes_history
          (query_id, initial_query_id, elapsed, read_rows, read_bytes, written_rows,
           memory_usage, peak_memory_usage, thread_ids, ProfileEvents)
        VALUES
          ('q1', 'q1', 1.0, 100, 1000, 0, 1048576, 2097152, [1,2], map('OSCPUVirtualTimeMicroseconds', 500000)),
          ('q1', 'q1', 2.0, 200, 2000, 0, 2097152, 3145728, [1,2,3], map('OSCPUVirtualTimeMicroseconds', 1000000))
      `,
    });

    const svc = new MonitoringCapabilitiesService(ctx.adapter);
    const result = await svc.probe();

    const cap = result.capabilities.find(c => c.id === 'tracehouse_processes_history');
    expect(cap).toBeDefined();
    expect(cap!.available).toBe(true);
    // Should mention row count
    expect(cap!.detail).toMatch(/\d+ rows/);
  });

  it('should detect processes_history as unavailable after dropping the table', async () => {
    await ctx.client.command({ query: `DROP TABLE IF EXISTS tracehouse.processes_history` });

    const svc = new MonitoringCapabilitiesService(ctx.adapter);
    const result = await svc.probe();

    const cap = result.capabilities.find(c => c.id === 'tracehouse_processes_history');
    expect(cap).toBeDefined();
    expect(cap!.available).toBe(false);

    const flags = deriveMonitoringFlags(result.capabilities, result.serverVersion);
    expect(flags.hasProcessesHistory).toBe(false);
  });

  it('should detect introspection functions', async () => {
    const svc = new MonitoringCapabilitiesService(ctx.adapter);
    const result = await svc.probe();

    const cap = result.capabilities.find(c => c.id === 'introspection_functions');
    expect(cap).toBeDefined();
    // Introspection functions may or may not be enabled depending on container config
    expect(typeof cap!.available).toBe('boolean');
  });

  it('should detect standard system log tables', async () => {
    const svc = new MonitoringCapabilitiesService(ctx.adapter);
    const result = await svc.probe();
    const flags = deriveMonitoringFlags(result.capabilities, result.serverVersion);

    // These should all exist on a standard ClickHouse instance
    expect(flags.hasQueryLog).toBe(true);
    expect(flags.hasTraceLog).toBe(true);
    expect(flags.hasMetricLog).toBe(true);
  });
});
