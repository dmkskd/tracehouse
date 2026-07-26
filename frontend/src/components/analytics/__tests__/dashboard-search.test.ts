import { describe, expect, test } from 'vitest';
import type { Dashboard } from '../dashboards';
import { dashboardMatchesSearch } from '../dashboards';

const dashboard: Dashboard = {
  id: 'merge-health',
  title: 'Merge Health',
  description: 'Track background storage work',
  group: 'ClickHouse',
  category: 'Merges',
  source: 'https://clickhouse.com/docs/merges',
  columns: 2,
  panels: [
    { queryName: 'Storage#Active merges', section: 'Throughput' },
    { queryName: 'Resources#Merge memory' },
  ],
  filters: [{ param: 'database', label: 'Database', query: 'SELECT database' }],
  builtin: true,
};

describe('dashboard search', { tags: ['analytics'] }, () => {
  test('matches dashboard metadata and panel definitions', () => {
    expect(dashboardMatchesSearch(dashboard, 'merge')).toBe(true);
    expect(dashboardMatchesSearch(dashboard, 'clickhouse')).toBe(true);
    expect(dashboardMatchesSearch(dashboard, 'throughput')).toBe(true);
    expect(dashboardMatchesSearch(dashboard, 'database')).toBe(true);
    expect(dashboardMatchesSearch(dashboard, 'built-in')).toBe(true);
  });

  test('requires every search term while allowing terms across fields', () => {
    expect(dashboardMatchesSearch(dashboard, 'storage memory')).toBe(true);
    expect(dashboardMatchesSearch(dashboard, 'storage replication')).toBe(false);
    expect(dashboardMatchesSearch(dashboard, '   ')).toBe(true);
  });
});
