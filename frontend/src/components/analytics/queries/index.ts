/**
 * Aggregates built-in Analytics SQL. All new/changed timestamp instants must
 * use UTC projections and absolute UTC bounds; localize only in UI formatting.
 * This applies outside Grafana too: do not rely on datasource time conversion.
 * Preserve calendar Dates and intentional local-calendar analysis separately.
 * See packages/core/src/queries/README.md for the full query-authoring contract.
 */

import overview from './overview';
import inserts from './inserts';
import selects from './selects';
import parts from './parts';
import merges from './merges';
import resources from './resources';
import advancedDashboard from './advancedDashboard';
import selfMonitoring from './selfMonitoring';
import memory from './memory';
import replication from './replication';
import mutations from './mutations';
import disks from './disks';
import mergeAnalytics from './mergeAnalytics';
import cloudMonitoring from './cloudMonitoring';
import cloudProviders from './cloudProviders';
import altinityOperator from './altinityOperator';
import altinityKb from './altinityKb';
import clickHouseKb from './clickhouseKb';
import json from './json';
import xray from './xray';
import waitBreakdown from './waitBreakdown';
import events from './events';
import workloadBreakdown from './workloadBreakdown';

export const RAW_QUERIES: string[] = [
  ...overview,
  ...inserts,
  ...selects,
  ...parts,
  ...merges,
  ...resources,
  ...advancedDashboard,
  ...selfMonitoring,
  ...memory,
  ...replication,
  ...mutations,
  ...disks,
  ...mergeAnalytics,
  ...cloudMonitoring,
  ...cloudProviders,
  ...altinityOperator,
  ...altinityKb,
  ...clickHouseKb,
  ...json,
  ...xray,
  ...waitBreakdown,
  ...events,
  ...workloadBreakdown,
];
