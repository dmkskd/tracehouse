/** Aggregates all raw query strings from per-group modules. */

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
];
