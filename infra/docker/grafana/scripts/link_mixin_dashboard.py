#!/usr/bin/env python3
"""Build the TraceHouse-linked copy of the official ClickHouse mixin dashboard.

The copy adds Grafana data links from the mixin's query, insert, merge, CPU and
memory panels to the TraceHouse app plugin, following the recipes in
docs/proposals/grafana-last-mile.md. Rerun it when the upstream mixin changes:

    python3 infra/docker/grafana/scripts/link_mixin_dashboard.py

The upstream file is never modified. A missing panel id fails the run, so an
upstream change cannot silently drop a link.
"""

import json
from pathlib import Path

DASHBOARDS = Path(__file__).resolve().parent.parent / 'dashboards'
SOURCE = DASHBOARDS / 'ClickHouse (official)' / 'clickhouse-mixin.json'
TARGET = DASHBOARDS / 'TraceHouse' / 'clickhouse-mixin-linked.json'

APP = '/a/dmkskd-tracehouse-app'
# The mixin's own ${datasource} is Prometheus; links need a ClickHouse one.
# With a single ClickHouse data source TraceHouse selects it without th_ds.
WHERE = 'th_ds=${th_datasource}'
# The clicked point; TraceHouse shows 5 minutes on each side by default.
AT = 'th_at=${__value.time}'
WINDOW = '±5 min'


def link(title, page, filters=''):
    query = '&'.join(part for part in (filters, AT, WHERE) if part)
    return {'title': title, 'url': f'{APP}/{page}?{query}', 'targetBlank': True}


def time_travel(filters):
    # Time Travel zooms in fixed steps; 5 min either side rounds up to a 15 min view.
    return link('Time Travel at this point', 'timetravel', filters)


MERGES = link(f'Merges {WINDOW}', 'merges')
SLOWEST_INSERTS = link(f'Slowest inserts {WINDOW}',
                       'queries', 'queryKind=Insert&sortField=query_duration_ms&sortDir=desc')
# Every list link is paired with the same moment in Time Travel, so the dashboard
# shows both ways in: the matching records, and everything running at that time.
TT_QUERIES = time_travel('tt_ops=query')
TT_MERGES = time_travel('tt_ops=merge')

LINKS = {
    # C: failed queries
    32: [link(f'Failed queries {WINDOW}', 'queries', 'status=error'), TT_QUERIES],
    31: [link(f'Queries that hit the memory limit {WINDOW}', 'queries', 'status=error&errorCode=241'),
         TT_QUERIES],
    33: [link(f'Failed SELECT queries {WINDOW}', 'queries', 'status=error&queryKind=Select'), TT_QUERIES],
    # D1: failed and rejected inserts
    34: [link(f'Failed inserts {WINDOW}', 'queries', 'status=error&queryKind=Insert'), MERGES, TT_QUERIES],
    42: [link(f'Rejected inserts (TOO_MANY_PARTS) {WINDOW}',
              'queries', 'status=error&queryKind=Insert&errorCode=252'), MERGES, TT_QUERIES],
    # D2: delayed inserts succeed, so they link to the slowest inserts
    41: [SLOWEST_INSERTS, MERGES, TT_QUERIES],
    43: [SLOWEST_INSERTS, MERGES, TT_QUERIES],
    # D3: async insert failures are not query_log errors
    39: [link(f'Async insert failures {WINDOW}', 'events', 'event_kind=async_insert_failure'), TT_QUERIES],
    # E: merges
    49: [MERGES, TT_MERGES],
    53: [MERGES, TT_MERGES],
    86: [MERGES, TT_MERGES],
    # F: Time Travel centred on the clicked point
    6: [TT_QUERIES],
    30: [TT_QUERIES],
    26: [time_travel('tt_metric=cpu')],
    28: [time_travel('tt_metric=memory')],
}

VARIABLES = [
    {
        'name': 'th_datasource',
        'label': 'TraceHouse ClickHouse',
        'description': 'ClickHouse data source that TraceHouse links open',
        'type': 'datasource',
        'query': 'grafana-clickhouse-datasource',
        'current': {},
        'hide': 0,
        'refresh': 1,
        'regex': '',
        'includeAll': False,
        'multi': False,
        'options': [],
    },
]


def panels(items):
    for panel in items:
        yield panel
        yield from panels(panel.get('panels', []))


def main():
    dashboard = json.loads(SOURCE.read_text())

    found = set()
    for panel in panels(dashboard['panels']):
        if panel.get('id') in LINKS:
            defaults = panel.setdefault('fieldConfig', {}).setdefault('defaults', {})
            defaults['links'] = LINKS[panel['id']]
            found.add(panel['id'])
    missing = set(LINKS) - found
    if missing:
        raise SystemExit(f'panels not found upstream: {sorted(missing)}')

    dashboard['templating']['list'].extend(VARIABLES)
    dashboard['title'] = 'ClickHouse - Prom-Exporter Instance [TraceHouse linked]'
    dashboard['uid'] = 'tracehouse-clickhouse-mixin-linked'
    dashboard['links'] = []
    dashboard.pop('id', None)

    TARGET.write_text(json.dumps(dashboard, indent=2) + '\n')
    print(f'wrote {TARGET.relative_to(DASHBOARDS.parent)}: links on panels {sorted(found)}')


if __name__ == '__main__':
    main()
