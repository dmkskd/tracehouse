/**
 * ClickHouse testcontainers setup for integration tests.
 *
 * Spins up a real ClickHouse instance in Docker and provides
 * an IClickHouseAdapter backed by @clickhouse/client.
 *
 * External instance mode:
 *   Set CH_TEST_URL (e.g. "http://localhost:8123") to run tests against
 *   an existing ClickHouse instance instead of a testcontainer.
 *   Set CH_TEST_KEEP_DATA=1 to skip dropping the test database on teardown
 *   so you can inspect the data in the UI.
 */

import { ClickHouseContainer, type StartedClickHouseContainer } from '@testcontainers/clickhouse';
import { createClient, type ClickHouseClient } from '@clickhouse/client';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { AdapterError } from '../../../adapters/types.js';
import { ClusterAwareAdapter } from '../../../adapters/cluster-adapter.js';
import { CH_IMAGE } from './constants.js';

/** Thin adapter wrapping @clickhouse/client for integration tests. */
export class TestAdapter implements IClickHouseAdapter {
  constructor(private client: ClickHouseClient) {}

  async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
    try {
      const result = await this.client.query({ query: sql, format: 'JSONEachRow' });
      return await result.json<T>();
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      throw new AdapterError(msg, 'query', error instanceof Error ? error : undefined);
    }
  }

  async executeCommand(sql: string): Promise<void> {
    await this.client.command({ query: sql });
  }
}

export interface TestClickHouseContext {
  container: StartedClickHouseContainer | null;
  client: ClickHouseClient;
  /** Cluster-aware adapter (resolves placeholders). Single-node = strips to plain system.X. */
  adapter: ClusterAwareAdapter;
  /** Raw adapter without placeholder resolution, for direct SQL. */
  rawAdapter: TestAdapter;
  /** When true, teardown skips dropping the test database. */
  keepData: boolean;
}

/** True when tests target an external ClickHouse instance. */
export function isExternalInstance(): boolean {
  return !!process.env.CH_TEST_URL;
}

/**
 * Start a ClickHouse container and return a ready-to-use context.
 *
 * If CH_TEST_URL is set, connects to that instance instead of starting
 * a container. Set CH_TEST_KEEP_DATA=1 to preserve test databases on teardown.
 */
export async function startClickHouse(): Promise<TestClickHouseContext> {
  const externalUrl = process.env.CH_TEST_URL;
  const keepData = process.env.CH_TEST_KEEP_DATA === '1';

  if (externalUrl) {
    const client = createClient({ url: externalUrl });
    const rawAdapter = new TestAdapter(client);
    const adapter = new ClusterAwareAdapter(rawAdapter);
    return { container: null, client, adapter, rawAdapter, keepData };
  }

  const container = await new ClickHouseContainer(CH_IMAGE)
    .withStartupTimeout(120_000)
    .start();

  const client = createClient({
    url: container.getConnectionUrl(),
  });

  const rawAdapter = new TestAdapter(client);
  const adapter = new ClusterAwareAdapter(rawAdapter);

  return { container, client, adapter, rawAdapter, keepData };
}

/** Tear down the container and close the client. */
export async function stopClickHouse(ctx: TestClickHouseContext): Promise<void> {
  await ctx.client.close();
  if (ctx.container) {
    await ctx.container.stop();
  }
}
