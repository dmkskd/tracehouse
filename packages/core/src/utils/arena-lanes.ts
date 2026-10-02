/**
 * Lane packing for the resource arena timeline.
 *
 * Backfilled (already finished) operations have a known [start, end] interval,
 * so they can share a lane when their intervals do not overlap, like a Gantt
 * chart. Operations that already hold a lane (currently running ones) are
 * passed with `lane` set and are never moved.
 */

export interface LaneItem {
  id: string;
  /** Items only share lanes within the same group (e.g. deck + host) */
  group: string;
  /** First lane index of the group (e.g. host block offset in split view) */
  laneOffset: number;
  /**
   * Number of lanes the group owns, starting at `laneOffset`. When all of them are
   * busy the item shares the last lane (overlapping in time) instead of spilling
   * into the next group's lanes. Unbounded when omitted.
   */
  laneCount?: number;
  startMs: number;
  endMs: number;
  /** Pre-assigned lane; the item keeps it and blocks it for the interval */
  lane?: number;
}

/**
 * Assign a lane to every item that does not have one yet.
 * Items are placed in start order on the first lane (from `laneOffset`) with
 * no overlapping interval. Returns lanes for the newly assigned items only.
 */
export function packLanes(items: LaneItem[]): Map<string, number> {
  const occupied = new Map<string, Map<number, Array<[number, number]>>>();
  const slots = (group: string, lane: number): Array<[number, number]> => {
    let lanes = occupied.get(group);
    if (!lanes) { lanes = new Map(); occupied.set(group, lanes); }
    let list = lanes.get(lane);
    if (!list) { list = []; lanes.set(lane, list); }
    return list;
  };
  const overlaps = (list: Array<[number, number]>, s: number, e: number) =>
    list.some(([os, oe]) => s < oe && os < e);

  for (const it of items) {
    if (it.lane !== undefined) slots(it.group, it.lane).push([it.startMs, it.endMs]);
  }

  const assigned = new Map<string, number>();
  const pending = items.filter(it => it.lane === undefined).sort((a, b) => a.startMs - b.startMs);
  for (const it of pending) {
    let lane = it.laneOffset;
    const lastLane = it.laneCount === undefined ? Infinity : it.laneOffset + it.laneCount - 1;
    while (lane < lastLane && overlaps(slots(it.group, lane), it.startMs, it.endMs)) lane++;
    slots(it.group, lane).push([it.startMs, it.endMs]);
    assigned.set(it.id, lane);
  }
  return assigned;
}
