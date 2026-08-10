import { describe, expect, it } from 'vitest';
import {
  VERSION_GATED_CAPABILITIES,
  SAMPLER_DDL_CAPABILITY_ID,
} from '../version-gated-capabilities.js';
import { CAPABILITY_REGISTRY } from '../capability-registry.js';
import { parseClickHouseVersion } from '../../utils/clickhouse-version.js';

describe('version-gated capabilities', () => {
  it('declares a parseable minimum version for every entry', () => {
    for (const gate of VERSION_GATED_CAPABILITIES) {
      expect(parseClickHouseVersion(gate.minVersion), gate.id).not.toBeNull();
    }
  });

  it('has no duplicate ids', () => {
    const ids = VERSION_GATED_CAPABILITIES.map(gate => gate.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it('cites a compatibility finding for every entry', () => {
    // Each floor must be traceable to docs/development/clickhouse-compatibility.md
    // so a reader can see the evidence behind the claim.
    for (const gate of VERSION_GATED_CAPABILITIES) {
      expect(gate.finding, gate.id).toMatch(/^CH-COMPAT-\d{3}$/);
    }
  });

  it('maps every version-gated capability to at least one screen', () => {
    const registered = new Set(CAPABILITY_REGISTRY.map(entry => entry.capabilityId));
    const unmapped = VERSION_GATED_CAPABILITIES
      .map(gate => gate.id)
      .filter(id => !registered.has(id));

    expect(unmapped).toEqual([]);
  });

  it('keeps the sampler DDL id in the gate list', () => {
    const ids = VERSION_GATED_CAPABILITIES.map(gate => gate.id);
    expect(ids).toContain(SAMPLER_DDL_CAPABILITY_ID);
  });

  it('preserves the tested floors from the compatibility matrix', () => {
    // Transcribed from the feature availability matrix. Changing a value here
    // without a matrix run means the app now claims something untested.
    const floors = Object.fromEntries(
      VERSION_GATED_CAPABILITIES.map(gate => [gate.id, gate.minVersion]),
    );

    expect(floors).toMatchObject({
      distributed_limit_by: '24.1',
      async_insert_log_data_kind: '24.3',
      metric_log_distributed_insert_failures: '24.3',
      json_subcolumn_analysis: '24.3',
      merge_duration_metric: '24.8',
      merge_wait_analytics: '24.12',
      refreshable_mv_sampler: '25.3',
    });
  });
});
