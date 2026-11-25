import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const dashboard = JSON.parse(readFileSync(new URL('../dashboards/TraceHouse/panel-links.json', import.meta.url), 'utf8'));
const panels = dashboard.panels;
const links = panels.flatMap(panel => [
  ...(panel.fieldConfig?.defaults.links ?? []),
  ...(panel.fieldConfig?.overrides ?? []).flatMap(rule => rule.properties.filter(p => p.id === 'links').flatMap(p => p.value)),
]);
function interpolate(template, value, fields = {}) {
  return template.replace(/\$\{([^}]+)\}/g, (_, expression) => {
    const [variable, format] = expression.split(':');
    const field = /^__data\.fields\["(.+)"\]$/.exec(variable)?.[1];
    const raw = field ? fields[field] : variable === '__value.raw' ? value : {
      datasource: 'clickhouse/demo', cluster: 'cluster west', __from: '1000', __to: '2000', '__value.time': '1500',
    }[variable];
    assert.notEqual(raw, undefined, `Unresolved variable ${variable}`);
    return format === 'percentencode' ? encodeURIComponent(raw) : raw;
  });
}

test('provisions native Grafana panels covering the TraceHouse entry points', () => {
  assert.equal(dashboard.uid, 'tracehouse-primitives');
  assert.equal(new Set(panels.map(panel => panel.id)).size, panels.length);
  assert.ok(panels.every(panel => ['text', 'table', 'timeseries'].includes(panel.type)));
  const routes = new Set(links.map(link => new URL(link.url, 'https://grafana.example').pathname.split('/').at(-1)));
  for (const page of ['objects', 'queries', 'timetravel', 'merges', 'events', 'databases']) assert.ok(routes.has(page), page);
  assert.ok(panels.filter(p => p.targets).every(p => p.targets.every(t => /^SELECT\b/.test(t.rawSql))));
});

test('every data link preserves datasource and cluster and keeps the source dashboard open', () => {
  for (const link of links) {
    assert.equal(link.targetBlank, true);
    const url = new URL(link.url, 'https://grafana.example');
    assert.equal(url.searchParams.get('th_ds'), '${datasource:percentencode}');
    assert.equal(url.searchParams.get('th_cluster'), '${cluster:percentencode}');
  }
});

test('user links open the exact Object Browser principal, including reserved characters', () => {
  const link = links.find(link => link.url.includes('ob_kind=user'));
  const user = 'alice + analyst&ops/#?=東京';
  const url = new URL(interpolate(link.url, user), 'https://grafana.example');
  assert.equal(url.searchParams.get('ob_kind'), 'user');
  assert.equal(url.searchParams.get('ob_user'), user);
  assert.equal(url.searchParams.get('th_ds'), 'clickhouse/demo');
  assert.equal(url.searchParams.get('th_cluster'), 'cluster west');
  assert.equal(url.hash, '');
});

test('table links preserve database and table as independent names', () => {
  const link = links.find(link => link.url.includes('ob_kind=table'));
  const database = 'sales.archive & ops', table = 'orders/2026?#';
  const url = new URL(interpolate(link.url, table, { Database: database }), 'https://grafana.example');
  assert.equal(url.searchParams.get('ob_db'), database);
  assert.equal(url.searchParams.get('ob_tbl'), table);
});

test('query and merge details identify the clicked record rather than a name-only lookup', () => {
  const query = links.find(link => link.url.includes('qd_id='));
  const queryUrl = new URL(interpolate(query.url, 'query-id', { Time: '1500' }), 'https://grafana.example');
  assert.equal(queryUrl.searchParams.get('qd_id'), 'query-id');
  assert.equal(queryUrl.searchParams.get('qd_end'), '1500');
  const merge = links.find(link => link.url.includes('md_us='));
  const fields = { Database: 'sales', Table: 'orders', Host: 'replica-2', Time: '2026-11-25 12:34:56.123456', 'Event type': 'MergeParts' };
  const url = new URL(interpolate(merge.url, 'all_1_2_1', fields), 'https://grafana.example');
  assert.equal(url.searchParams.get('md_part'), 'all_1_2_1');
  assert.equal(url.searchParams.get('md_host'), fields.Host);
  assert.equal(url.searchParams.get('md_us'), fields.Time);
  assert.equal(url.searchParams.get('md_type'), fields['Event type']);
});
