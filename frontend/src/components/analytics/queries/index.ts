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
];
