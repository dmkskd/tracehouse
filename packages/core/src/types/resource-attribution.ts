import type { AttributionSelection, AttributedOperation } from '../utils/timeline-attribution.js';
import type { OperationKind, OperationMetricMode } from '../utils/timeline-operations.js';
import type { SelectQueryXRaySourceInput } from './xray-source.js';

export interface ResourceAttributionGroupKey {
  kind: OperationKind;
  activity: string;
  tables: string[];
  tableState: 'recorded' | 'none' | 'running';
}

/** Aggregated execution contributions. No per-execution rows are transferred for totals. */
export interface ResourceAttributionGroup extends ResourceAttributionGroupKey {
  id: string;
  value: number;
  failureValue: number;
  operationCount: number;
  failedCount: number;
  sampledCount: number;
  estimatedCount: number;
  unavailableCount: number;
  zeroCount: number;
  sampledMs: number;
  estimatedMs: number;
  missingMs: number;
}

export interface ResourceAttributionSummary {
  selection: AttributionSelection;
  mode: OperationMetricMode;
  groups: ResourceAttributionGroup[];
  /** Sampling fallback warnings; result limits never affect these totals. */
  warnings: string[];
}

/** Fixed one-second snapshots prefetched together for local instant scrubbing. */
export interface ResourceAttributionWindow {
  startMs: number;
  endMs: number;
  stepMs: 1000;
  mode: OperationMetricMode;
  snapshots: Map<number, ResourceAttributionSummary>;
  warnings: string[];
}

export interface ResourceAttributionRequest {
  mode: OperationMetricMode;
  hostname?: string | readonly string[] | null;
  includeRunning?: boolean;
  normalizedQueryHash?: string | null;
  queryHashOnly?: boolean;
  sampleSelection?: SelectQueryXRaySourceInput;
}

export interface ResourceAttributionDetails {
  operations: AttributedOperation[];
  warnings: string[];
}
