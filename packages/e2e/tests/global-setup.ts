/**
 * Playwright global setup — starts a single ClickHouse container via Docker.
 *
 * Follows the same pattern as packages/core integration tests:
 *   - If CH_E2E_URL is set, use that existing instance (skip container).
 *   - Otherwise, `docker run` a single-node ClickHouse and wait for health.
 *
 * Connection details are written to .ch-state.json so tests and fixtures
 * can read them.
 */

import { execSync, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const CH_IMAGE = 'clickhouse/clickhouse-server:26.3-alpine';
const CONTAINER_NAME = 'tracehouse-e2e-clickhouse';
const STATE_FILE = path.join(import.meta.dirname, '.ch-state.json');

/** Wait for ClickHouse to accept HTTP queries. */
function waitForHealth(host: string, port: number, password: string, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      execSync(
        `curl -sf "http://${host}:${port}/?query=SELECT+1&user=default&password=${password}" >/dev/null 2>&1`,
        { timeout: 3_000 },
      );
      return;
    } catch {
      execSync('sleep 1');
    }
  }
  throw new Error(`ClickHouse not healthy after ${timeoutMs}ms`);
}

export default async function globalSetup() {
  const externalUrl = process.env.CH_E2E_URL;

  if (externalUrl) {
    const url = new URL(externalUrl);
    fs.writeFileSync(STATE_FILE, JSON.stringify({
      host: url.hostname,
      port: parseInt(url.port) || 8123,
      user: 'default',
      password: '',
      managed: false,
    }));
    console.log(`  ClickHouse: using external ${externalUrl}`);
    return;
  }

  // Stop any leftover container from a previous run
  try {
    execSync(`docker rm -f ${CONTAINER_NAME} 2>/dev/null`, { stdio: 'ignore' });
  } catch { /* ignore */ }

  console.log(`  ClickHouse: starting container (${CH_IMAGE})...`);

  // Start container with a random host port mapped to 8123
  // CLICKHOUSE_PASSWORD is required or the image disables network access for 'default'
  execFileSync('docker', [
    'run', '-d',
    '--name', CONTAINER_NAME,
    '-p', '0:8123',
    '-e', 'CLICKHOUSE_PASSWORD=e2e',
    '-e', 'CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1',
    CH_IMAGE,
  ], { stdio: 'inherit' });

  // Get the mapped port
  const portOutput = execSync(
    `docker port ${CONTAINER_NAME} 8123/tcp`,
    { encoding: 'utf-8' },
  ).trim();
  // Output is like "0.0.0.0:55123" or "[::]:55123"
  const port = parseInt(portOutput.split(':').pop()!);

  const password = 'e2e';
  waitForHealth('localhost', port, password);
  console.log(`  ClickHouse: ready at http://localhost:${port}`);

  // Seed: run the read_only user setup SQL (same as demo uses)
  const initSqlPath = path.resolve(
    import.meta.dirname,
    '../../../infra/demo/init/01_setup_read_only_user.sql',
  );
  if (fs.existsSync(initSqlPath)) {
    const raw = fs.readFileSync(initSqlPath, 'utf-8')
      .replace(/CHANGEME_RO_PASSWORD/g, 'e2e_readonly');

    // Strip comments first, then split into individual statements
    const cleaned = raw.replace(/--[^\n]*/g, '');
    const statements = cleaned
      .split(';')
      .map(s => s.trim())
      .filter(s => s.length > 0);

    for (const stmt of statements) {
      try {
        execSync(
          `curl -sf "http://localhost:${port}/?user=default&password=${password}" --data-binary @-`,
          { input: stmt, timeout: 10_000 },
        );
      } catch (err) {
        // Show the failing statement for debugging, but don't abort — some
        // grants may fail on single-node (e.g. REMOTE) and that's OK.
        const preview = stmt.slice(0, 80).replace(/\n/g, ' ');
        console.warn(`  ClickHouse: statement failed (continuing): ${preview}...`);
      }
    }
    console.log(`  ClickHouse: init complete (${statements.length} statements)`);
  }

  fs.writeFileSync(STATE_FILE, JSON.stringify({
    host: 'localhost',
    port,
    user: 'default',
    password,
    managed: true,
  }));
}
