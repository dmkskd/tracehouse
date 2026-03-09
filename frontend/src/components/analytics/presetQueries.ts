/**
 * Preset monitoring queries for ClickHouse with embedded metadata.
 *
 * Metadata format (parsed from SQL comments):
 *   -- @meta: title='...' group='...' description='...'
 *   -- @chart: type=bar labels=col values=col style=3d
 *   -- @rag: column=col_name green<2000 amber<40000
 *   -- Source: <url>
 *
 * Sources & Attribution:
 *   - ClickHouse Advanced Dashboard (system.dashboards) — Apache 2.0 License
 *     https://clickhouse.com/docs/operations/system-tables/dashboards
 *     https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
 *   - ClickHouse Blog: Monitoring & Troubleshooting INSERT Queries
 *     https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
 *   - ClickHouse Blog: Monitoring & Troubleshooting SELECT Queries
 *     https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
 */

// Re-export types, constants, and utility functions from queryUtils so existing
// imports from './presetQueries' keep working.
export {
  type QueryGroup, type ChartType, type ChartStyle, type RagRule, type PresetQuery, type CustomQuery,
  QUERY_GROUPS, CHART_TYPE_LABELS, MAX_SIDEBAR_QUERIES, TIME_RANGE_OPTIONS,
  getRagColor, resolveTimeRange, resolveDrillParams, describeTimeRange,
  parseQueryMetadata, buildCustomQuerySql, loadCustomQueries,
  deleteCustomQuery, resetCustomQueries,
} from './queryUtils';

import type { PresetQuery } from './queryUtils';
import type { CustomQuery } from './queryUtils';
import {
  parseQueryMetadata,
  isQueryNameTaken as _isQueryNameTaken,
  addCustomQuery as _addCustomQuery,
  getAllQueries as _getAllQueries,
} from './queryUtils';

import { RAW_QUERIES } from './queries';

export { RAW_QUERIES };

export const PRESET_QUERIES: PresetQuery[] = RAW_QUERIES
  .map(parseQueryMetadata)
  .filter((q): q is PresetQuery => q !== null);

// Convenience wrappers that bind PRESET_QUERIES so callers don't need to pass it.
export function isQueryNameTaken(name: string): boolean { return _isQueryNameTaken(name, PRESET_QUERIES); }
export function addCustomQuery(query: CustomQuery): PresetQuery[] { return _addCustomQuery(query, PRESET_QUERIES); }
export function getAllQueries(): PresetQuery[] { return _getAllQueries(PRESET_QUERIES); }
