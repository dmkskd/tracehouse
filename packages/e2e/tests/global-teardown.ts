/**
 * Playwright global teardown — stops the ClickHouse container if we started one.
 */

import { execSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const CONTAINER_NAME = 'tracehouse-e2e-clickhouse';
const STATE_FILE = path.join(import.meta.dirname, '.ch-state.json');

export default async function globalTeardown() {
  if (!fs.existsSync(STATE_FILE)) return;

  const state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8'));
  fs.unlinkSync(STATE_FILE);

  if (!state.managed) return; // external instance — don't touch it

  console.log('  ClickHouse: stopping container...');
  try {
    execSync(`docker rm -f ${CONTAINER_NAME}`, { stdio: 'ignore' });
  } catch { /* ignore */ }
}
