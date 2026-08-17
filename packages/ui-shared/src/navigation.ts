export interface TracehouseNavigationItem {
  key: string;
  label: string;
  path: string;
}

export interface TracehouseNavigationGroup {
  label: string;
  items: readonly TracehouseNavigationItem[];
}

export const TRACEHOUSE_PRIMARY_NAVIGATION = [
  { key: 'overview', label: 'Overview', path: '/overview' },
  { key: 'timetravel', label: 'Time Travel', path: '/timetravel' },
  { key: 'queries', label: 'Queries', path: '/queries' },
  { key: 'merges', label: 'Merges', path: '/merges' },
  { key: 'analytics', label: 'Analytics', path: '/analytics' },
  { key: 'events', label: 'Events', path: '/events' },
  { key: 'databases', label: 'Explorer', path: '/databases' },
] as const satisfies readonly TracehouseNavigationItem[];

export const TRACEHOUSE_OVERFLOW_NAVIGATION = [
  {
    label: 'Infrastructure',
    items: [
      { key: 'cluster', label: 'Cluster', path: '/cluster' },
      { key: 'replication', label: 'Replication', path: '/replication' },
    ],
  },
  {
    label: 'Advanced',
    items: [
      { key: 'engine-internals', label: 'Engine Internals', path: '/engine-internals' },
    ],
  },
] as const satisfies readonly TracehouseNavigationGroup[];

export const TRACEHOUSE_OVERFLOW_ITEMS = [
  ...TRACEHOUSE_OVERFLOW_NAVIGATION[0].items,
  ...TRACEHOUSE_OVERFLOW_NAVIGATION[1].items,
] as const satisfies readonly TracehouseNavigationItem[];

export const TRACEHOUSE_NAVIGATION = [
  ...TRACEHOUSE_PRIMARY_NAVIGATION,
  ...TRACEHOUSE_OVERFLOW_ITEMS,
] as const satisfies readonly TracehouseNavigationItem[];

export interface NavigationFitOptions {
  /** Rendered width of each primary item, in source order. */
  itemWidths: readonly number[];
  /** Gap between adjacent items. */
  gap: number;
  /** Width the nav row has to work with, including the overflow trigger. */
  availableWidth: number;
  /** Width of the overflow ("More") trigger, which is always on screen. */
  overflowTriggerWidth: number;
}

/**
 * How many leading primary items fit alongside the overflow trigger.
 *
 * The trigger is always rendered — it holds the pages that are never promoted to tabs — so
 * its width is reserved up front, and every item is costed with the gap that follows it.
 * Items past the returned count belong in the overflow menu.
 */
export function countVisibleNavigationItems({
  itemWidths,
  gap,
  availableWidth,
  overflowTriggerWidth,
}: NavigationFitOptions): number {
  let remaining = availableWidth - overflowTriggerWidth;
  if (remaining <= 0) return 0;

  let count = 0;
  for (const width of itemWidths) {
    remaining -= width + gap;
    if (remaining < 0) break;
    count += 1;
  }

  return count;
}
