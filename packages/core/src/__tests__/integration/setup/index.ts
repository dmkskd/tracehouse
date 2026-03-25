/**
 * Integration test setup utilities.
 *
 * Usage in tests:
 *   import { startClickHouse, stopClickHouse } from './setup/index.js';
 *   import { createShadowDatabase, seedMetricLog } from './setup/index.js';
 *   import { ShadowAdapter } from './setup/index.js';
 */

export { startClickHouse, stopClickHouse, TestAdapter, type TestClickHouseContext } from './clickhouse-container.js';
export {
  createShadowDatabase,
  dropShadowDatabase,
  truncateShadowTables,
  seedMetricLog,
  seedAsyncMetricLog,
  seedAsyncMetrics,
  type MetricLogRow,
  type AsyncMetricRow,
  type AsyncMetricStaticRow,
} from './shadow-tables.js';
export { ShadowAdapter } from './shadow-adapter.js';
export {
  createTestTable,
  createTestDatabase,
  dropTestDatabase,
  type CreateTableOptions,
  type MergeTreeVariant,
} from './table-helpers.js';
export {
  startCluster,
  stopCluster,
  ClusterTestAdapter,
  type ClusterTestContext,
} from './cluster-container.js';
export {
  startAltinityCluster,
  stopAltinityCluster,
  type AltinityClusterContext,
} from './altinity-cluster-container.js';
export {
  startSamplingCluster,
  stopSamplingCluster,
  type SamplingClusterContext,
  type SamplingClusterOptions,
} from './sampling-cluster-container.js';
export {
  runTracehouseSetup,
  type TracehouseSetupOptions,
} from './tracehouse-setup.js';
