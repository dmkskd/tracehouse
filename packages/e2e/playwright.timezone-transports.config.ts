import { defineConfig } from '@playwright/test';

/** Opt-in, read-only checks against the existing dev frontend, ClickHouse and
 * Grafana. No global setup/teardown, containers, fixture tables, or user data writes. */
export default defineConfig({
  testDir: './tests-timezone-transports',
  workers: 1,
  timeout: 60_000,
  reporter: 'list',
  use: { launchOptions: { timeout: 30_000 } },
  outputDir: './test-results/timezone-transports',
  projects: (['chromium', 'webkit'] as const).flatMap(browserName => [
    ['london', 'Europe/London'], ['copenhagen', 'Europe/Copenhagen'], ['new-york', 'America/New_York'],
  ].map(([name, timezoneId]) => ({ name: `${browserName}-${name}`, use: { browserName, timezoneId } }))),
});
