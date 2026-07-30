import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const REPO_ROOT = path.resolve(import.meta.dirname, '../../..');
const COMPOSE_FILE = path.join(
  REPO_ROOT,
  'grafana-app-plugin/testing/docker-compose.yml',
);

function port(): number {
  return Number.parseInt(process.env.GRAFANA_E2E_PORT ?? '3003', 10);
}

function projectName(): string {
  return `tracehouse-grafana-e2e-${port()}`;
}

function composeEnvironment(pluginDist: string): NodeJS.ProcessEnv {
  return {
    ...process.env,
    GRAFANA_VERSION: process.env.GRAFANA_VERSION ?? '13.1.0',
    GRAFANA_TEST_PORT: String(port()),
    CLICKHOUSE_TEST_HTTP_PORT: String(port() + 5121),
    GRAFANA_PLUGIN_TEST_DIST: pluginDist,
  };
}

async function waitForGrafana(baseUrl: string, timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const healthResponse = await fetch(`${baseUrl}/api/health`, {
        signal: AbortSignal.timeout(3_000),
      });
      if (!healthResponse.ok) {
        throw new Error(`Grafana health check returned ${healthResponse.status}`);
      }

      // /api/health becomes ready before Grafana finishes installing plugins
      // and provisioning datasources. TraceHouse auto-selects its sole
      // ClickHouse datasource on mount, so opening the app during that gap
      // leaves it on the datasource picker for the rest of the test.
      const datasourcesResponse = await fetch(`${baseUrl}/api/datasources`, {
        signal: AbortSignal.timeout(3_000),
      });
      if (datasourcesResponse.ok) {
        const datasources = await datasourcesResponse.json() as Array<{ type?: string }>;
        if (datasources.some(
          datasource => datasource.type === 'grafana-clickhouse-datasource',
        )) {
          return;
        }
      }
    } catch {
      // Grafana, its plugin installer, or datasource provisioning is still starting.
    }
    await new Promise(resolve => setTimeout(resolve, 1_000));
  }
  throw new Error(
    `Grafana and its ClickHouse datasource did not become ready at ${baseUrl}`,
  );
}

export default async function globalSetup() {
  const externalUrl = process.env.GRAFANA_E2E_URL;
  if (externalUrl) {
    await waitForGrafana(externalUrl);
    return;
  }

  execFileSync('just', ['grafana-plugin-build-fast'], {
    cwd: REPO_ROOT,
    stdio: 'inherit',
  });

  const stageRoot = path.join(
    os.tmpdir(),
    projectName(),
    'plugin-dist',
  );
  const pluginMount = path.join(stageRoot, 'dmkskd-tracehouse-app');
  fs.rmSync(stageRoot, { recursive: true, force: true });
  fs.mkdirSync(pluginMount, { recursive: true });
  fs.cpSync(
    path.join(REPO_ROOT, 'grafana-app-plugin/dist'),
    pluginMount,
    { recursive: true },
  );

  const commonArgs = [
    'compose',
    '-p',
    projectName(),
    '-f',
    COMPOSE_FILE,
  ];
  const env = composeEnvironment(stageRoot);

  execFileSync('docker', [...commonArgs, 'down', '--remove-orphans'], {
    cwd: REPO_ROOT,
    env,
    stdio: 'ignore',
  });
  execFileSync('docker', [...commonArgs, 'up', '-d', '--pull', 'always'], {
    cwd: REPO_ROOT,
    env,
    stdio: 'inherit',
  });

  await waitForGrafana(`http://localhost:${port()}`);
}
