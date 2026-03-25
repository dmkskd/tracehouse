/**
 * Run the production setup_sampling.sh script against a test ClickHouse instance.
 *
 * This creates the tracehouse database and sampling infrastructure (tables,
 * buffers, refreshable MVs) using the same DDL that production uses, avoiding
 * manual inline CREATE TABLE statements in tests.
 *
 * Usage:
 *   await runTracehouseSetup(ctx);                          // all targets, full infra
 *   await runTracehouseSetup(ctx, { target: 'processes' }); // processes only
 *   await runTracehouseSetup(ctx, { target: 'merges' });    // merges only
 *   await runTracehouseSetup(ctx, { tablesOnly: true });    // drop buffer+MV after setup
 */

import { execSync } from 'node:child_process';
import { resolve } from 'node:path';
import type { TestClickHouseContext } from './clickhouse-container.js';

const SETUP_SCRIPT = resolve(__dirname, '../../../../../../infra/scripts/setup_sampling.sh');

export interface TracehouseSetupOptions {
  /** Which sampling target to set up: 'processes', 'merges', or 'all' (default: 'all'). */
  target?: 'processes' | 'merges' | 'all';
  /**
   * When true, drops the Buffer tables and Refreshable MVs after setup,
   * keeping only the base MergeTree tables. Use this for tests that insert
   * synthetic data directly — the sampling infrastructure can interfere
   * with direct INSERT → SELECT cycles.
   */
  tablesOnly?: boolean;
}

/**
 * Run the production setup_sampling.sh script against a TestClickHouseContext.
 *
 * For container-based tests, connects via the native port (9000).
 * For external instances (CH_TEST_URL), assumes tracehouse is already set up.
 */
export async function runTracehouseSetup(
  ctx: TestClickHouseContext,
  opts: TracehouseSetupOptions = {},
): Promise<void> {
  if (!ctx.container) {
    // External instance — assume tracehouse infrastructure already exists.
    return;
  }

  const host = ctx.container.getHost();
  const nativePort = ctx.container.getMappedPort(9000);
  const username = ctx.container.getUsername();
  const password = ctx.container.getPassword();

  const authArgs = [
    username ? `--user ${username}` : '',
    password ? `--password ${password}` : '',
  ].filter(Boolean).join(' ');

  const targetArg = opts.target ? `--target ${opts.target}` : '';

  execSync(
    `bash "${SETUP_SCRIPT}" --host ${host} --port ${nativePort} ${authArgs} ${targetArg} --ttl 0 --yes`,
    { encoding: 'utf-8', timeout: 30_000, env: { ...process.env, PATH: process.env.PATH } },
  );

  if (opts.tablesOnly) {
    const target = opts.target ?? 'all';
    if (target === 'all' || target === 'processes') {
      await ctx.client.command({ query: `DROP VIEW IF EXISTS tracehouse.processes_sampler` });
      await ctx.client.command({ query: `DROP TABLE IF EXISTS tracehouse.processes_history_buffer` });
    }
    if (target === 'all' || target === 'merges') {
      await ctx.client.command({ query: `DROP VIEW IF EXISTS tracehouse.merges_sampler` });
      await ctx.client.command({ query: `DROP TABLE IF EXISTS tracehouse.merges_history_buffer` });
    }
  }
}
