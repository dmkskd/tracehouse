import { defineConfig, devices } from '@playwright/test';
import { E2E_FRONTEND_ORIGIN, E2E_FRONTEND_PORT, E2E_VITE_CACHE_DIR } from './frontend-server';

/**
 * Playwright config for TraceHouse e2e tests.
 *
 * Global setup starts a ClickHouse container (or uses CH_E2E_URL).
 * Vite dev server is always started from the frontend workspace, on its own port with its own
 * dependency cache (frontend-server.ts): a dev server already running is never reused.
 */
export default defineConfig({
  testDir: './tests',
  globalSetup: './tests/global-setup.ts',
  globalTeardown: './tests/global-teardown.ts',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: process.env.CI ? 'github' : 'html',
  timeout: 30_000,

  use: {
    baseURL: E2E_FRONTEND_ORIGIN,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    video: 'on-first-retry',
    // SLOWMO=500 npx playwright test --headed  → 500ms pause between actions
    launchOptions: {
      slowMo: parseInt(process.env.SLOWMO || '0', 10),
    },
  },

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'mobile-chrome',
      use: { ...devices['Pixel 7'] },
    },
    // Uncomment after `npx playwright install firefox webkit`:
    // {
    //   name: 'firefox',
    //   use: { ...devices['Desktop Firefox'] },
    // },
    // {
    //   name: 'webkit',
    //   use: { ...devices['Desktop Safari'] },
    // },
  ],

  webServer: [
    {
      command: `npm run dev -- --port ${E2E_FRONTEND_PORT} --strictPort`,
      cwd: '../../frontend',
      url: E2E_FRONTEND_ORIGIN,
      env: { VITE_CACHE_DIR: E2E_VITE_CACHE_DIR },
      reuseExistingServer: false,
      timeout: 30_000,
    },
    {
      // Older ClickHouse HTTP endpoints do not answer browser CORS preflight.
      // The fixture enables this proxy only when the runtime probe requires it.
      command: 'npm run dev',
      cwd: '../proxy',
      url: 'http://localhost:8990/health',
      reuseExistingServer: !process.env.CI,
      timeout: 30_000,
    },
  ],
});
