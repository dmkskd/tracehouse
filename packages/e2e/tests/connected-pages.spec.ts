import { test, expect } from './fixtures';

/**
 * Tests that pages render real content when connected to ClickHouse.
 * Uses the `connectedPage` fixture which auto-connects via localStorage
 * and verifies data is actually flowing before handing the page to tests.
 */
test.describe('Connected pages', () => {
  test('Overview shows server metrics', async ({ connectedPage: page }) => {
    // The fixture already verified data arrived ("Just now" / "Xs ago").
    // Now check that the Overview page shows actual metric values, not dashes.

    // Verify actual metric values loaded — the header bar shows "CPU X.X%" when data arrives
    await expect(page.getByText(/CPU \d+(\.\d+)?%/)).toBeVisible({ timeout: 10_000 });

    // The header should show the connection name (may be hidden on mobile)
    const isMobile = test.info().project.name.includes('mobile');
    if (!isMobile) {
      await expect(page.locator('header').getByText('E2E Test')).toBeVisible();
    }
  });

  test('Engine Internals loads', async ({ connectedPage: page }) => {
    await page.goto('/#/engine-internals');
    // Wait for actual content to load — engine internals should show data tables/cards
    await expect(page.getByRole('heading', { name: 'No Connection' })).not.toBeVisible({ timeout: 10_000 });
    // Should have at least one data section (monitoring capabilities, thread pools, etc.)
    await expect(page.locator('main').locator('table, [class*="card"], [class*="Card"]').first()).toBeVisible({ timeout: 15_000 });
  });

  test('Explorer shows databases', async ({ connectedPage: page }) => {
    await page.goto('/#/databases');
    // Should show the "system" database or at least the Database Explorer heading
    await expect(
      page.getByText('system', { exact: true }).or(page.getByText('Database Explorer'))
    ).toBeVisible({ timeout: 15_000 });
  });

  test('Queries page loads', async ({ connectedPage: page }) => {
    await page.goto('/#/queries');
    // The query monitor should not show the "No Connection" placeholder
    await expect(page.getByRole('heading', { name: 'No Connection' })).not.toBeVisible({ timeout: 10_000 });
    // Should show the query monitoring UI — either running queries or "No queries" message
    await expect(
      page.getByText(/running|queries|QUERIES/i).first()
    ).toBeVisible({ timeout: 15_000 });
  });

  test('Analytics page loads', async ({ connectedPage: page }) => {
    await page.goto('/#/analytics');
    await expect(page.getByRole('heading', { name: 'No Connection' })).not.toBeVisible({ timeout: 10_000 });
    // Analytics should show editor or dashboard content
    await expect(page.locator('main')).not.toBeEmpty();
  });
});
