import { defineConfig, devices } from '@playwright/test';

const port = process.env.GRAFANA_E2E_PORT ?? '3003';

/**
 * Smoke tests for the compiled TraceHouse Grafana app plugin.
 *
 * The global setup builds the plugin and starts the minimal provisioned
 * Grafana + ClickHouse stack unless GRAFANA_E2E_URL points at an existing one.
 */
export default defineConfig({
  testDir: './tests-grafana',
  globalSetup: './tests-grafana/global-setup.ts',
  globalTeardown: './tests-grafana/global-teardown.ts',
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: process.env.CI
    ? 'github'
    : [['list'], ['html', { open: 'never' }]],
  timeout: 45_000,

  use: {
    baseURL: process.env.GRAFANA_E2E_URL ?? `http://localhost:${port}`,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'on-first-retry',
    launchOptions: {
      // Grafana pages render Plotly/WebGL charts. Keep those charts enabled in
      // headless CI even when the runner has no hardware GPU.
      args: ['--use-angle=swiftshader', '--enable-unsafe-swiftshader'],
    },
  },

  projects: [
    {
      name: 'grafana-chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
