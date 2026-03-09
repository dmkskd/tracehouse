import { describe, it, expect, vi } from 'vitest';
import { EngineInternalsService } from '../engine-internals.js';
import type { IClickHouseAdapter } from '../../adapters/types.js';

describe('EngineInternalsService CoreTimeline parsing', () => {
  it('parses different timestamp formats correctly', async () => {
    // We will simulate the adapter returning different formats for row.slot
    const mockAdapter: IClickHouseAdapter = {
      executeQuery: vi.fn().mockResolvedValue([
        { core: 0, slot: '2026-03-04 14:02:28', samples: 1 },
        { core: 1, slot: 'Fri, 27 Feb 2026 20:24:42', samples: 1 },
        { core: 2, slot: 1700000000000, samples: 1 },
        { core: 3, slot: new Date(1700000000000), samples: 1 },
      ]),
      executeRawQuery: vi.fn(),
    };

    const svc = new EngineInternalsService(mockAdapter);
    // @ts-ignore: private method access for forcing skip estimateCoreCount
    svc.estimateCoreCount = vi.fn().mockResolvedValue(16);

    const result = await svc.getCoreTimeline(60);

    expect(result).toBeDefined();
    expect(result?.slots).toHaveLength(4);

    // 0: std clickhouse
    expect(result?.slots[0].timeMs).toBe(new Date('2026-03-04T14:02:28Z').getTime());

    // 1: Grafana format "Fri, 27 Feb 2026 20:24:42" -> Date.parse() parses as local, so timeMs will depend on local TZ, but shouldn't be NaN
    expect(result?.slots[1].timeMs).not.toBeNaN();
    expect(result?.slots[1].timeMs).toBeGreaterThan(0);

    // 2: number/epoch
    expect(result?.slots[2].timeMs).toBe(1700000000000);

    // 3: Date object
    expect(result?.slots[3].timeMs).toBe(1700000000000);
  });
});
