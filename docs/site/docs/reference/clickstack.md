# ClickStack Integration

:::caution Work in Progress
This integration is experimental and under active development.
:::

## Overview

[ClickStack](https://clickhouse.com/clickstack) is ClickHouse's observability stack for logs, traces, and metrics. TraceHouse can integrate with ClickStack to surface logs directly in the monitoring UI - for example, showing ClickHouse server logs alongside merge activity or query execution.

## Current Status

- **Data source discovery** - Still figuring out how to create and configure a ClickStack data source from within the app. The connection model differs from the standard ClickHouse HTTP interface.
- **Log viewing** - The goal is to let users browse ClickHouse server logs (text_log, query_log, etc.) through ClickStack's log pipeline, with richer filtering and correlation than querying system tables directly.
- **Dependency on ClickStack capabilities** - What we can expose depends on what ClickStack makes available via its API. As ClickStack evolves, so will this integration.

## Open Questions

- How to programmatically register a data source in ClickStack
- Authentication and permissions model for ClickStack access
- Which log tables to route through ClickStack vs. query directly from system tables
