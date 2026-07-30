import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const REPO_ROOT = path.resolve(import.meta.dirname, '../../..');
const COMPOSE_FILE = path.join(
  REPO_ROOT,
  'grafana-app-plugin/testing/docker-compose.yml',
);

export default function globalTeardown() {
  if (process.env.GRAFANA_E2E_URL) return;

  const port = Number.parseInt(process.env.GRAFANA_E2E_PORT ?? '3003', 10);
  const project = `tracehouse-grafana-e2e-${port}`;
  try {
    execFileSync(
      'docker',
      [
        'compose',
        '-p',
        project,
        '-f',
        COMPOSE_FILE,
        'down',
        '--remove-orphans',
      ],
      {
        cwd: REPO_ROOT,
        env: {
          ...process.env,
          GRAFANA_TEST_PORT: String(port),
          CLICKHOUSE_TEST_HTTP_PORT: String(port + 5121),
        },
        stdio: 'inherit',
      },
    );
  } catch {
    // Preserve the Playwright result; a later run cleans this project first.
  } finally {
    fs.rmSync(path.join(os.tmpdir(), project), {
      recursive: true,
      force: true,
    });
  }
}
