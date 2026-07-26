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
