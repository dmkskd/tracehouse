/**
 * Reusable Playwright fixtures for TraceHouse e2e tests.
 *
 * Provides:
 *   - `chConfig`: ClickHouse connection details from the global setup
 *   - `connectedPage`: a Page that has already gone through the
 *     "Add Connection" flow and is connected to ClickHouse
 */

import { test as base, expect, type Page } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';

const STATE_FILE = path.join(import.meta.dirname, '.ch-state.json');

export interface ChConfig {
  host: string;
  port: number;
  user: string;
  password: string;
}

/** Read ClickHouse connection details written by global-setup. */
function readChConfig(): ChConfig {
  if (!fs.existsSync(STATE_FILE)) {
    throw new Error(
      'No .ch-state.json found — did global-setup run? Is Docker available?',
    );
  }
  return JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8'));
}

/**
 * Wait until the global refresh indicator shows real data arrived
 * (i.e. "Just now" or "Xs ago"), not "Connecting..." or "Paused".
 */
async function waitForDataLoaded(page: Page, timeout = 30_000) {
  await expect(page.getByText(/\d+s ago|Just now/)).toBeVisible({ timeout });
}

/**
 * Fill in the "Add Connection" form and connect.
 * Reusable helper — call this from any test that needs a connected app.
 *
 * Uses placeholder-based selectors since the form labels don't use htmlFor.
 */
export async function connectViaUI(page: Page, config: ChConfig) {
  // Force 2D mode — headless browsers don't support WebGL.
  // Use addInitScript so it's set before the page loads AND survives any
  // soft-navigations / React re-renders after form submission.
  await page.addInitScript(() => {
    localStorage.setItem('tracehouse-view-preference', JSON.stringify({
      state: { preferredViewMode: '2d', killQueriesEnabled: false, experimentalEnabled: true, hideReplicaMerges: false },
      version: 0,
    }));
  });

  await page.goto('/#/overview');

  // Also set it directly on the already-loaded page so it persists through
  // the in-app navigation that happens after "Save Connection" (no reload).
  await page.evaluate(() => {
    localStorage.setItem('tracehouse-view-preference', JSON.stringify({
      state: { preferredViewMode: '2d', killQueriesEnabled: false, experimentalEnabled: true, hideReplicaMerges: false },
      version: 0,
    }));
  });

  // Click "Add Connection" — appears in the empty-state
  await page.getByRole('button', { name: 'Add Connection' }).click();

  // Fill in connection form using placeholders
  const nameInput = page.getByPlaceholder('My ClickHouse Server');
  await nameInput.clear();
  await nameInput.fill('E2E Test');

  const hostInput = page.getByPlaceholder('localhost');
  await hostInput.clear();
  await hostInput.fill(config.host);

  // Port is a number input
  const portInput = page.locator('input[type="number"]');
  await portInput.clear();
  await portInput.fill(String(config.port));

  if (config.user) {
    // Username placeholder is "default" — use first match (Database also has "default")
    const userInput = page.getByPlaceholder('default').first();
    await userInput.clear();
    await userInput.fill(config.user);
  }

  if (config.password) {
    const passwordInput = page.locator('input[type="password"]');
    await passwordInput.clear();
    await passwordInput.fill(config.password);
  }

  // Use persistent storage so connection survives within the test
  await page.getByRole('button', { name: 'Remember' }).click();

  // Save & connect
  await page.getByRole('button', { name: 'Save Connection' }).click();

  // Wait for connection — the empty state should disappear
  await expect(page.getByRole('heading', { name: 'No Connection' })).not.toBeVisible({ timeout: 15_000 });

  // Reload so addInitScript fires again and the zustand preference store
  // hydrates with 2D mode (the SPA navigation after save doesn't trigger it).
  await page.reload();

  // Wait for actual data to arrive — not just "Connecting..."
  await waitForDataLoaded(page);
}

/**
 * Shortcut: inject connection via localStorage (faster, no UI interaction).
 * Useful when you don't need to test the connection form itself.
 */
export async function connectViaLocalStorage(page: Page, config: ChConfig) {
  const profile = {
    id: 'e2e-test-profile',
    name: 'E2E Test',
    config: {
      host: config.host,
      port: config.port,
      user: config.user || 'default',
      password: config.password || '',
      database: 'default',
      secure: false,
      connect_timeout: 10,
      send_receive_timeout: 30,
    },
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    last_connected_at: new Date().toISOString(),
    is_connected: true,
  };

  // Set localStorage before navigating so the app picks it up on load
  await page.addInitScript((data) => {
    localStorage.setItem('tracehouse-connections', JSON.stringify({
      state: {
        profiles: [data.profile],
        activeProfileId: data.profile.id,
      },
      version: 0,
    }));
    localStorage.setItem('tracehouse-credential-storage-mode', '"persistent"');
    // Force 2D mode — headless browsers don't support WebGL
    localStorage.setItem('tracehouse-view-preference', JSON.stringify({
      state: { preferredViewMode: '2d', killQueriesEnabled: false, experimentalEnabled: true, hideReplicaMerges: false },
      version: 0,
    }));
  }, { profile });

  await page.goto('/#/overview');

  // Wait for actual data to arrive — "Just now" or "Xs ago" means ClickHouse responded
  await waitForDataLoaded(page);
}

// ── Extended test fixture ─────────────────────────────────────────────

type Fixtures = {
  chConfig: ChConfig;
  connectedPage: Page;
};

export const test = base.extend<Fixtures>({
  chConfig: async ({}, use) => {
    use(readChConfig());
  },

  connectedPage: async ({ page, chConfig }, use) => {
    await connectViaLocalStorage(page, chConfig);
    use(page);
  },
});

export { expect } from '@playwright/test';
