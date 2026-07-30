import { expect, test, type Page } from '@playwright/test';

const APP_ROOT = '/a/dmkskd-tracehouse-app';

test.beforeEach(async ({ page, request }) => {
  const response = await request.get('/api/datasources');
  expect(response.ok()).toBe(true);

  const datasources = await response.json() as Array<{
    name: string;
    type: string;
    uid: string;
  }>;
  const clickHouse = datasources.find(
    datasource => datasource.type === 'grafana-clickhouse-datasource',
  );
  expect(clickHouse).toBeDefined();

  // A real user normally has this selection persisted by TraceHouse. Seed the
  // same browser state so these tests exercise plugin pages rather than the
  // datasource picker.
  await page.addInitScript(datasource => {
    localStorage.setItem('tracehouse-datasource', JSON.stringify(datasource));
  }, {
    uid: clickHouse!.uid,
    name: clickHouse!.name,
  });
});

async function expectPluginPage(
  page: Page,
  path: string,
  readyText: string,
) {
  const pageErrors: string[] = [];
  const recordPageError = (error: Error) => pageErrors.push(error.message);
  page.on('pageerror', recordPageError);

  try {
    await page.goto(`${APP_ROOT}${path}`, { waitUntil: 'domcontentloaded' });

    await expect(page.getByText('Plugin failed to load')).toHaveCount(0);
    await expect(page.getByText(readyText, { exact: false }).first())
      .toBeVisible({ timeout: 30_000 });
    expect(pageErrors).toEqual([]);
  } finally {
    page.off('pageerror', recordPageError);
  }
}

test.describe('Grafana app plugin smoke tests', () => {
  test('Merges loads with repeated URL filters and can write a quick filter', async ({ page }) => {
    await expectPluginPage(
      page,
      '/merges?status=Running&status=OK',
      'Merge Tracker',
    );

    const filterInput = page.getByPlaceholder('Add filter…');
    await filterInput.click();
    await page.getByText('Recent', { exact: true }).click();

    await expect(page).toHaveURL(/(?:[?&])quick=recent(?:&|$)/);
    const url = new URL(page.url());
    expect(url.searchParams.getAll('status')).toEqual(['OK', 'Error']);
  });

  test('Queries loads with repeated URL filters', async ({ page }) => {
    await expectPluginPage(
      page,
      '/queries?status=Running&status=Completed',
      'Query Tracker',
    );
  });

  test('core plugin routes render against the provisioned datasource', async ({ page }) => {
    await expectPluginPage(page, '/overview', 'Overview');
    await expectPluginPage(page, '/databases', 'Databases');
    await expectPluginPage(page, '/analytics', 'Analytics');
  });

  test('analytics dashboard panels retain the subtle theme border', async ({ page }) => {
    await expectPluginPage(
      page,
      '/analytics?tab=dashboards&fromDashboard=ops-overview',
      'Operations Overview',
    );

    const panel = page.locator('[data-dashboard-panel-index]').first();
    await expect(panel).toBeVisible();
    const border = await panel.evaluate(element => {
      const style = getComputedStyle(element);
      return {
        color: style.color,
        borderColor: style.borderColor,
        borderStyle: style.borderStyle,
        borderWidth: style.borderWidth,
      };
    });

    expect(border.borderWidth).toBe('1px');
    expect(border.borderStyle).toBe('solid');
    expect(border.borderColor).not.toBe(border.color);
  });
});
