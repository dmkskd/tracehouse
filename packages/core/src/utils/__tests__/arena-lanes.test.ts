import { describe, it, expect } from 'vitest';
import { packLanes, type LaneItem } from '../arena-lanes.js';

const item = (id: string, startMs: number, endMs: number, extra: Partial<LaneItem> = {}): LaneItem =>
  ({ id, group: 'select|0', laneOffset: 0, startMs, endMs, ...extra });

describe('packLanes', () => {
  it('reuses a lane for sequential items and splits overlapping ones', () => {
    const lanes = packLanes([
      item('a', 0, 10),
      item('b', 5, 15),
      item('c', 10, 20),
    ]);
    expect(lanes.get('a')).toBe(0);
    expect(lanes.get('b')).toBe(1);
    // c starts exactly when a ends, so it fits back into lane 0
    expect(lanes.get('c')).toBe(0);
  });

  it('never moves pre-assigned items and avoids their lane while they overlap', () => {
    const lanes = packLanes([
      item('running', 0, 100, { lane: 0 }),
      item('done', 20, 30),
      item('later', 100, 110),
    ]);
    expect(lanes.has('running')).toBe(false);
    expect(lanes.get('done')).toBe(1);
    expect(lanes.get('later')).toBe(0);
  });

  it('keeps groups independent and starts at the group lane offset', () => {
    const lanes = packLanes([
      item('x', 0, 10, { group: 'select|0', laneOffset: 0 }),
      item('y', 0, 10, { group: 'select|1', laneOffset: 6 }),
      item('z', 0, 10, { group: 'merge|0', laneOffset: 0 }),
    ]);
    expect(lanes.get('x')).toBe(0);
    expect(lanes.get('y')).toBe(6);
    expect(lanes.get('z')).toBe(0);
  });

  it('keeps overflowing items inside the group lane block instead of spilling into the next one', () => {
    const items = Array.from({ length: 7 }, (_, i) => item(`o${i}`, 0, 10, { laneOffset: 0, laneCount: 3 }));
    const lanes = packLanes(items);
    expect([...lanes.values()].sort()).toEqual([0, 1, 2, 2, 2, 2, 2]);
  });
});
