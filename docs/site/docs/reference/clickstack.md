# ClickStack integration (removed)

:::note[Removed]
The ClickStack/HyperDX integration has been removed from TraceHouse. Native trace-log and flamegraph views remain available. The notes below describe the former integration proposal and are retained for reference.
:::

## Overview

[ClickStack](https://clickhouse.com/clickstack) is ClickHouse's observability stack for logs, traces, and metrics. TraceHouse can integrate with ClickStack to surface logs directly in the monitoring UI - for example, showing ClickHouse server logs alongside merge activity or query execution.

## Former proposal

- **Data source discovery** - Still figuring out how to create and configure a ClickStack data source from within the app. The connection model differs from the standard ClickHouse HTTP interface.
- **Log viewing** - The goal is to let users browse ClickHouse server logs (text_log, query_log, etc.) through ClickStack's log pipeline, with richer filtering and correlation than querying system tables directly.
- **Dependency on ClickStack capabilities** - What we can expose depends on what ClickStack makes available via its API. As ClickStack evolves, so will this integration.

## Questions from the former proposal

- How to programmatically register a data source in ClickStack
- Authentication and permissions model for ClickStack access
- Which log tables to route through ClickStack vs. query directly from system tables
