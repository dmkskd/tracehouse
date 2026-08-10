/**
 * Version-Gated Capabilities
 *
 * Capabilities whose availability depends only on the ClickHouse server
 * version, not on server configuration or grants. Unlike table/column
 * capabilities these need no probe query: the version comparison is the
 * whole test.
 *
 * Each entry is transcribed from the feature availability matrix in
 * docs/development/clickhouse-compatibility.md and cites the finding that
 * established the boundary. When that document changes, change this list.
 *
 * Minimum versions here are the *tested* boundary, not necessarily the first
 * ClickHouse release that introduced support. See the cited findings.
 */

import type { MonitoringCapability } from './monitoring-capabilities.js';

export interface VersionGatedCapability {
  id: string;
  label: string;
  description: string;
  category: MonitoringCapability['category'];
  /** Minimum tested ClickHouse version, e.g. "24.8" */
  minVersion: string;
  /** Where the requirement comes from, shown as the capability source */
  source: string;
  /** CH-COMPAT-XXX finding in docs/development/clickhouse-compatibility.md */
  finding?: string;
  /**
   * Set when an old server rejects the DDL/SQL outright rather than merely
   * lacking a column. Changes the UI message from "enable this" to
   * "your version cannot run this".
   */
  reason?: 'version' | 'ddl';
}

export const VERSION_GATED_CAPABILITIES: VersionGatedCapability[] = [
  {
    id: 'distributed_limit_by',
    label: 'Distributed LIMIT BY',
    description: 'Per-category row limiting through the active distributed query path.',
    category: 'profiling',
    minVersion: '24.1',
    source: 'server version (ClickHouse #55836)',
    finding: 'CH-COMPAT-004',
  },
  {
    id: 'async_insert_log_data_kind',
    label: 'Async Insert Data Kind',
    description: 'system.asynchronous_insert_log.data_kind, used by the Async Insert Log preset and the distributed topology buffer-kind badge.',
    category: 'logging',
    minVersion: '24.3',
    source: 'system.asynchronous_insert_log.data_kind',
    finding: 'CH-COMPAT-014',
  },
  {
    id: 'metric_log_distributed_insert_failures',
    label: 'Distributed Insert Failure Counter',
    description: 'ProfileEvent_DistributedAsyncInsertionFailures in system.metric_log, used by the Distribution Insert Pressure preset.',
    category: 'metrics',
    minVersion: '24.3',
    source: 'system.metric_log.ProfileEvent_DistributedAsyncInsertionFailures',
    finding: 'CH-COMPAT-014',
  },
  {
    id: 'json_subcolumn_analysis',
    label: 'JSON Subcolumn Analysis',
    description: 'Nested subcolumn ARRAY JOIN after a LEFT JOIN, used by JSON Columns Inventory and JSON Subcolumn Pressure. Older servers expose the arrays but fail analysis of this join shape.',
    category: 'introspection',
    minVersion: '24.3',
    source: 'query analyzer (system.parts_columns subcolumns.*)',
    finding: 'CH-COMPAT-014',
  },
  {
    id: 'merge_duration_metric',
    label: 'Merge Duration Metric',
    description: 'ProfileEvent_MergeTotalMilliseconds in system.metric_log, used by the Merge Duration (avg) preset. Named MergesTimeMilliseconds on older servers.',
    category: 'metrics',
    minVersion: '24.8',
    source: 'system.metric_log.ProfileEvent_MergeTotalMilliseconds',
    finding: 'CH-COMPAT-009',
  },
  {
    id: 'merge_wait_analytics',
    label: 'Merge Wait-Time Analytics',
    description: 'Non-equality JOIN conditions used by the Part Wait Time presets. Older servers require an experimental server setting.',
    category: 'metrics',
    minVersion: '24.12',
    source: 'JOIN ON non-equality condition support',
    finding: 'CH-COMPAT-002',
  },
  {
    id: 'refreshable_mv_sampler',
    label: 'Refreshable MV Sampler DDL',
    description: 'CREATE MATERIALIZED VIEW ... REFRESH ... APPEND, required to install the TraceHouse process and merge samplers.',
    category: 'profiling',
    minVersion: '25.3',
    source: 'refreshable materialized view DDL',
    finding: 'CH-COMPAT-001',
    reason: 'ddl',
  },
];

/** Capability IDs whose installer DDL is rejected below their minimum version. */
export const SAMPLER_DDL_CAPABILITY_ID = 'refreshable_mv_sampler';
