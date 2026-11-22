/** Steps taken in focus, from the object focus opened on to the current one. */
export type FocusTrail = readonly string[];

/** Moving to `key`: a step already on the trail cuts the trail back to it; any other key is appended. */
export function stepTo(trail: FocusTrail, key: string): FocusTrail {
  const at = trail.indexOf(key);
  return at >= 0 ? trail.slice(0, at + 1) : [...trail, key];
}

export type TrailItem = { kind: 'step'; key: string } | { kind: 'gap'; hidden: string[] };

/** The trail as shown: the origin and the last `tail` steps always, with the middle
 * folded into one gap once the trail is longer than `max`. */
export function visibleTrail(trail: FocusTrail, max = 4, tail = 2): TrailItem[] {
  const steps = (keys: readonly string[]) => keys.map(key => ({ kind: 'step', key }) as const);
  if (trail.length <= max) return steps(trail);
  return [...steps(trail.slice(0, 1)), { kind: 'gap', hidden: trail.slice(1, -tail) }, ...steps(trail.slice(-tail))];
}
