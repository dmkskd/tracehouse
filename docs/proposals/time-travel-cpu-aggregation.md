# Time Travel CPU Aggregation

Status: implemented.

## Implemented model

Time Travel Overall mode represents total selected-host CPU utilization.

- CPU samples come from `system.asynchronous_metric_log` through the cluster-aware query path.
- For each timestamp and host, CPU is computed from `CGroupUserTime + CGroupSystemTime` when available, otherwise `OSUserTime + OSSystemTime`.
- The server CPU line sums those per-host samples across the selected hosts.
- The denominator is the sum of the selected hosts' effective core counts.
- Therefore `100%` means all selected CPU capacity is saturated.
- Each per-host tooltip bar uses that host's own effective core count.

The same sum-usage/sum-capacity model is used for memory. Per-server charts
remain unchanged because both totals reduce to the selected host's values.

For example, an 8-core host at 100% and a 32-core host at 50% use 24 of
40 cores, so Overall reports 60%.
