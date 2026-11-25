# TraceHouse panel links

`panel-links.json` provisions **TraceHouse · Panel links** (UID
`tracehouse-primitives`) in the TraceHouse folder. Docker and the demo already
mount this dashboard directory; their file provider discovers the JSON. It can
also be imported through Grafana's dashboard import UI.

All visualizations are native Grafana table, time-series or text panels. The
links invoke existing TraceHouse routes; they require the TraceHouse app and
ClickHouse datasource plugins, without a separate panel plugin or JavaScript.

| Panel interaction | TraceHouse view | Context |
|---|---|---|
| User cell | Object Browser, Time Travel, Queries | Exact user; activity time range |
| Table cell | Object Browser, Time Travel, Queries, Merges, Explorer | Separate database/table names; activity time range |
| Failed-query user | Object Browser or Time Travel | Current user configuration or activity around the failure |
| Query id | Query Details | Query id, finish timestamp, dashboard time range |
| Failed-query timestamp | Time Travel | Failure timestamp, including failures before query start |
| Error code or failure-chart point | Events | Query failures around the selected time |
| Failure-chart point | Queries or Time Travel | Selected time |
| Merge part | Merge Details | Database, table, part, host, exact event timestamp and event type |

Every link carries `th_ds` and `th_cluster` from the dashboard's single-select
datasource and cluster variables. Links use `targetBlank: true`. Values used in URL parameters use Grafana's `percentencode` formatter.
Object Browser shows **current configured relationships**, not a reconstructed
authorization verdict for a past failure.

Sources are `system.users`, `system.tables`, `system.query_log`, and
`system.part_log`, read through `clusterAllReplicas` for the selected cluster.
The selector reads `system.clusters`, preferring the largest non-default cluster.
The default range is one hour. Empty failure/merge panels are valid when no such
activity was recorded; the users and tables remain usable entry points.

Validate the dashboard and its link contracts with:

```sh
node --test infra/docker/grafana/scripts/panel-links.test.mjs
```

Grafana references:
[Data-link variables](https://grafana.com/docs/grafana/latest/visualizations/panels-visualizations/configure-data-links/)
and [variable formatting](https://grafana.com/docs/grafana/latest/visualizations/dashboards/variables/variable-syntax/).
