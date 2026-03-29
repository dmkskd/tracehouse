import { test, expect, connectViaUI } from './fixtures';

function skipOnMobile() {
  if (test.info().project.name.includes('mobile')) test.skip();
}

/**
 * Tests the "Add Connection" flow end-to-end against a real ClickHouse.
 */
test.describe('Connection form', () => {
  test('connect via Add Connection button', async ({ page, chConfig }) => {
    skipOnMobile();
    // connectViaUI now waits for actual data to arrive (not just form closing)
    await connectViaUI(page, chConfig);

    // After connecting, Overview should show real data (not the empty state)
    await expect(page.getByText('Add Connection')).not.toBeVisible({ timeout: 10_000 });

    // The connection selector in the header should show our profile name
    await expect(page.locator('header').getByText('E2E Test')).toBeVisible({ timeout: 10_000 });
  });

  test('test connection button shows success', async ({ page, chConfig }) => {
    skipOnMobile();
    await page.goto('/#/overview');

    await page.getByRole('button', { name: 'Add Connection' }).click();

    // Fill host & port using placeholders (labels don't use htmlFor)
    const hostInput = page.getByPlaceholder('localhost');
    await hostInput.clear();
    await hostInput.fill(chConfig.host);

    const portInput = page.locator('input[type="number"]');
    await portInput.clear();
    await portInput.fill(String(chConfig.port));

    // Fill password if needed
    if (chConfig.password) {
      const passwordInput = page.locator('input[type="password"]');
      await passwordInput.clear();
      await passwordInput.fill(chConfig.password);
    }

    // Click "Test Connection"
    await page.getByRole('button', { name: 'Test Connection' }).click();

    // Should show success with server version
    await expect(page.getByText('Connection successful!')).toBeVisible({ timeout: 15_000 });
    await expect(page.getByText('Server version:')).toBeVisible();
  });
});
