import { defineConfig } from '@playwright/test';

/** Pure parser contract tests: no ClickHouse, Grafana, app server, or global
 * setup/teardown. This matrix does not claim service or datasource coverage. */
export default defineConfig({
  testDir: './tests-timezone',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  workers: 3,
  reporter: 'list',
  use: { launchOptions: { timeout: 30_000 } },
  outputDir: './test-results/timezone',
  projects: (['chromium', 'webkit', 'firefox'] as const).flatMap(browserName =>
    [
      ['utc', 'UTC'],
      ['copenhagen', 'Europe/Copenhagen'],
      ['new-york', 'America/New_York'],
    ].map(([name, timezoneId]) => ({
      name: `${browserName}-${name}`,
      use: { browserName, timezoneId },
    })),
  ),
});
