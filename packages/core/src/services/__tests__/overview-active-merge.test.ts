import { describe, it, expect } from 'vitest';
import { OverviewService } from '../overview-service.js';
import type { IClickHouseAdapter } from '../../adapters/types.js';

const row = (hostname: string, progress: number) => ({
  hostname, database: 'db', table: 't', part_name: 'all_1_6_1', elapsed: 10, progress,
  memory_usage: 1, bytes_read_uncompressed: 0, bytes_written_uncompressed: 0, rows_read: 0,
  num_parts: 6, is_mutation: 0, merge_type: 'Regular',
});

// Replicas merge the same part name independently; system.merges via
// clusterAllReplicas returns one row per replica.
const adapter = {
  executeQuery: async () => [row('replica-1', 0.2), row('replica-2', 0.9)],
} as unknown as IClickHouseAdapter;

describe('OverviewService.getActiveMerge', () => {
  it("returns the requested replica's merge, not the first row", async () => {
    const merge = await new OverviewService(adapter).getActiveMerge('db', 't', 'all_1_6_1', 'replica-2');
    expect(merge).toMatchObject({ hostname: 'replica-2', progress: 0.9 });
  });

  it('returns null when that replica is no longer merging the part', async () => {
    expect(await new OverviewService(adapter).getActiveMerge('db', 't', 'all_1_6_1', 'replica-3')).toBeNull();
  });
});
