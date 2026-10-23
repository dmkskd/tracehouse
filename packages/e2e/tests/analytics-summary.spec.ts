import { test, expect } from './fixtures';

const sql = `-- @meta: title='Summary fixture' group='Custom'
-- @chart: type=bar style=2d group_by=region value=elapsed unit=s
-- @summary: value=total label='Total duration' unit=s
-- @summary: value=merge_count label='Merges'
SELECT concat('Group ', toString(number)) AS region,
       55 - number AS elapsed,
       sum(55 - number) OVER () AS total,
       count() OVER () AS merge_count
FROM numbers(55)
ORDER BY elapsed DESC`;

for (const scenario of [{ theme: 'light', width: 1280 }, { theme: 'dark', width: 680 }]) {
  test(`2D summaries retain full totals and fit the plot: ${scenario.theme}, ${scenario.width}px`, async ({ connectedPage: page }, testInfo) => {
    test.setTimeout(60_000);
    await page.setViewportSize({ width: scenario.width, height: 1000 });
    await page.evaluate(({ sql, theme }) => {
      localStorage.setItem('tracehouse-theme', theme);
      localStorage.setItem('tracehouse-dashboards-user', JSON.stringify([{
        id: 'summary-browser-fixture', title: 'Summary browser fixture', columns: 1,
        panels: [{ queryName: 'Custom#Summary fixture' }], queries: [sql],
      }]));
    }, { sql, theme: scenario.theme });
    await page.goto('/#/analytics?dashboard=summary-browser-fixture');
    // Hash navigation alone preserves the already-mounted theme provider.
    await page.reload();
    await expect(page.locator('html')).toHaveAttribute('data-theme', scenario.theme);
    const summary = page.getByLabel('Result summary');
    await expect(page.getByLabel('Total duration: 1540 s', { exact: true })).toHaveText('25.7 min');
    await expect(page.getByLabel('Merges: 55', { exact: true })).toHaveText('55');
    await expect(page.locator('.recharts-bar-rectangle')).toHaveCount(50);
    const assertLayout = async () => {
      const plot = page.locator('.recharts-wrapper:visible').first();
      await expect(plot).toBeVisible();
      // Bounding-box reads do not retry like locator assertions. A panel rerun
      // or remount can briefly hide the summary after the earlier content checks.
      // Retry the complete layout check so every constraint still has to pass.
      await expect(async () => {
        const summaryBox = await summary.boundingBox();
        const plotBox = await plot.boundingBox();
        expect(summaryBox).not.toBeNull();
        expect(plotBox).not.toBeNull();
        expect(plotBox!.width).toBeGreaterThan(100);
        expect(plotBox!.height).toBeGreaterThan(100);
        expect(summaryBox!.y + summaryBox!.height).toBeLessThanOrEqual(plotBox!.y + 1);
        expect(summaryBox!.x).toBeGreaterThanOrEqual(0);
        expect(summaryBox!.x + summaryBox!.width).toBeLessThanOrEqual(scenario.width);
      }).toPass({ timeout: 5_000 });
      // Fullscreen remounts the chart. Wait for the bars to finish animating
      // before inspecting screenshots, rather than capturing an empty first frame.
      let previous = -1;
      let stable = 0;
      await expect.poll(async () => {
        const width = await plot.locator('.recharts-bar-rectangle').first().evaluate(element => (element as SVGGraphicsElement).getBBox().width);
        stable = width > 0 && Math.abs(width - previous) < 0.1 ? stable + 1 : 0;
        previous = width;
        return stable;
      }, { intervals: [100] }).toBeGreaterThanOrEqual(2);
    };
    await assertLayout();
    await page.getByRole('button', { name: 'Focus panel', exact: true }).click();
    await expect(summary).toBeVisible();
    await assertLayout();
    await page.keyboard.press('Escape');
    await page.getByTitle('Fullscreen (F)', { exact: true }).click();
    await expect(summary).toBeVisible();
    await assertLayout();
    await page.screenshot({ path: testInfo.outputPath('dashboard-summary.png') });
    await page.keyboard.press('Escape');
    await page.goto(`/#/analytics?tab=misc&view=chart&sql=${encodeURIComponent(sql)}`);
    await expect(page.getByLabel('Total duration: 1540 s', { exact: true })).toHaveText('25.7 min');
    await expect(page.locator('.recharts-bar-rectangle')).toHaveCount(50);
    await assertLayout();
    await page.getByTitle('Fullscreen (f)', { exact: true }).click();
    await expect(summary).toBeVisible();
    await assertLayout();
    await page.screenshot({ path: testInfo.outputPath('explorer-chart-summary.png') });
    await page.keyboard.press('Escape');
    await page.getByRole('button', { name: 'Table', exact: true }).click();
    await expect(summary).toBeVisible();
    await page.screenshot({ path: testInfo.outputPath('explorer-summary.png') });
  });
}
