import { test, expect, type Page } from '@playwright/test';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** All navigable routes with their expected heading / visible text. */
const PRIMARY_ROUTES = [
  { path: '#/overview', label: 'Overview' },
  { path: '#/timetravel', label: 'Time Travel' },
  { path: '#/queries', label: 'Queries' },
  { path: '#/merges', label: 'Merges' },
  { path: '#/analytics', label: 'Analytics' },
  { path: '#/events', label: 'Events' },
  { path: '#/databases', label: 'Explorer' },
] as const;

const OVERFLOW_ROUTES = [
  { path: '#/cluster', label: 'Cluster' },
  { path: '#/replication', label: 'Replication' },
  { path: '#/engine-internals', label: 'Engine Internals' },
] as const;

const ROUTES = [...PRIMARY_ROUTES, ...OVERFLOW_ROUTES] as const;

/** Skip test when running on a mobile project (nav items overlap the header). */
function skipOnMobile() {
  if (test.info().project.name.includes('mobile')) test.skip();
}

/** Open the settings popover, handling the case where it may have auto-closed. */
async function openSettings(page: Page) {
  const gear = page.getByTitle('Settings');
  await gear.click();
  const probe = page.getByText('Dark', { exact: true });
  if (!(await probe.isVisible({ timeout: 300 }).catch(() => false))) {
    await gear.click();
  }
  await expect(probe).toBeVisible();
}

// ---------------------------------------------------------------------------
// UI shell tests (no ClickHouse connection needed)
// ---------------------------------------------------------------------------

test.describe('App boot', () => {
  test('loads and redirects to #/overview', async ({ page }) => {
    await page.goto('/');
    await expect(page).toHaveURL(/.*#\/overview/);
    await expect(page.locator('header nav')).toBeVisible();
  });

  test('has priority navigation and grouped overflow items', async ({ page }) => {
    await page.goto('/');
    const nav = page.locator('nav');
    const overflowTrigger = nav.locator('button[aria-haspopup="menu"]');
    await expect(overflowTrigger).toBeVisible();

    if (test.info().project.name.includes('mobile')) {
      await overflowTrigger.click();
      for (const { label } of ROUTES) {
        await expect(nav.getByRole('menuitem', { name: label })).toBeVisible();
      }
    } else {
      for (const { label } of PRIMARY_ROUTES) {
        await expect(nav.getByRole('link', { name: label, exact: true })).toBeVisible();
      }
      await overflowTrigger.click();
      for (const { label } of OVERFLOW_ROUTES) {
        await expect(nav.getByRole('menuitem', { name: label })).toBeVisible();
      }
    }
  });
});

test.describe('Navigation', () => {
  test('clicking each nav link navigates correctly and transitions are fast', async ({ page }) => {
    skipOnMobile();
    await page.goto('/');
    await expect(page).toHaveURL(/.*#\/overview/);

    for (const route of ROUTES) {
      const nav = page.locator('nav');
      const overflowRoute = OVERFLOW_ROUTES.some(item => item.path === route.path);
      if (overflowRoute) {
        await nav.locator('button[aria-haspopup="menu"]').click();
      }
      const link = overflowRoute
        ? nav.getByRole('menuitem', { name: route.label })
        : nav.getByRole('link', { name: route.label, exact: true });
      await link.scrollIntoViewIfNeeded();
      const start = Date.now();
      await link.click();
      await expect(page).toHaveURL(new RegExp(`.*${route.path.replace('/', '\\/')}`));
      await expect(page.locator('main')).not.toBeEmpty();
      expect(Date.now() - start).toBeLessThan(2000);
    }
  });
});

test.describe('Settings', () => {
  test('open settings popover and toggle theme', async ({ page }) => {
    skipOnMobile();
    await page.goto('/#/overview');
    await expect(page.locator('header')).toBeVisible();

    await openSettings(page);

    // Switch to light
    await page.getByText('Light', { exact: true }).click();
    await expect(page.locator('header nav')).toBeVisible();

    // Re-open (popover may close on re-render) and switch back
    await openSettings(page);
    await page.getByText('Dark', { exact: true }).click();
    await expect(page.locator('header nav')).toBeVisible();
  });

  test('toggle view mode 3D/2D', async ({ page }) => {
    skipOnMobile();
    await page.goto('/#/overview');

    await openSettings(page);
    await page.getByRole('button', { name: '2D' }).click();
    await expect(page.locator('main')).not.toBeEmpty();

    await openSettings(page);
    await page.getByRole('button', { name: '3D' }).click();
    await expect(page.locator('main')).not.toBeEmpty();
  });
});

test.describe('Responsiveness', () => {
  test('header nav wraps gracefully at narrow width', async ({ page }) => {
    await page.setViewportSize({ width: 800, height: 600 });
    await page.goto('/#/overview');

    await expect(page.locator('header')).toBeVisible();
    const mainOverflow = await page.locator('main').evaluate(
      (el) => el.scrollWidth > el.clientWidth,
    );
    expect(mainOverflow).toBe(false);
  });
});

test.describe('Performance', () => {
  test('initial load completes under 5s', async ({ page }) => {
    const start = Date.now();
    await page.goto('/#/overview');
    await expect(page.locator('header nav')).toBeVisible();
    expect(Date.now() - start).toBeLessThan(5000);
  });
});
