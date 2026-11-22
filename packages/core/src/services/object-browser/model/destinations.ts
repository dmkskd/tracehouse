import type { ObjectType } from '../presentation/cards.js';

/** What a destination is built from: the object, and the user in focus context if any. */
export interface DestinationInput { database?: string; table?: string; user?: string; contextUser?: string }

export interface DestinationSpec {
  /** Button text, verb first ("Show queries"). Links for the user in focus context say "their". */
  label: string;
  /** What the page will show, for the tooltip. `{object}` is the table or user, `{user}` the user in focus context. */
  description: string;
  page: 'Queries' | 'Merges' | 'Time Travel';
  path: '/queries' | '/merges' | '/timetravel';
  /** Shown only with a user in focus context: the user a link named, or the user the focus chain started from. */
  contextUser?: true;
  /** URL parameters of the page, as its share schema reads them (repeated keys for arrays). */
  params: (input: DestinationInput) => [string, string][];
}

/** query_log.tables holds `database.table`; the Queries table filter matches inside it. */
const tableName = (i: DestinationInput): [string, string] => ['tableName', `${i.database}.${i.table}`];
/** Time Travel tracks a user and/or a table and opens on the matches only: every matching query
 * in the window. A tracked user's queries that failed before start (e.g. ACCESS_DENIED) are listed too. */
const timeTravel = (i: DestinationInput, user?: string): [string, string][] => [
  ...(user ? [['tt_user', user], ['tt_failed_start', '1']] as [string, string][] : []),
  ...(i.table ? [['tt_table', `${i.database}.${i.table}`]] as [string, string][] : []),
  ['tt_matched_only', '1'],
];
const ON_TABLE: DestinationSpec[] = [
  { label: 'Show queries', description: 'Queries on {object}', page: 'Queries', path: '/queries', params: i => [tableName(i)] },
  { label: 'Track in Time Travel', description: 'Time Travel tracking {object}', page: 'Time Travel', path: '/timetravel', params: i => timeTravel(i) },
  { label: 'Show their queries', description: 'Queries by {user} on {object}', page: 'Queries', path: '/queries', contextUser: true,
    params: i => [['user', i.contextUser!], tableName(i)] },
  { label: 'Show their failed queries', description: 'Failed queries by {user} on {object}', page: 'Queries', path: '/queries', contextUser: true,
    params: i => [['user', i.contextUser!], tableName(i), ['status', 'error']] },
  { label: 'Track them in Time Travel', description: 'Time Travel tracking {user} on {object}', page: 'Time Travel', path: '/timetravel', contextUser: true,
    params: i => timeTravel(i, i.contextUser) },
];
const MERGES: DestinationSpec = { label: 'Show merges', description: 'Merges of {object}', page: 'Merges', path: '/merges',
  params: i => [['database', i.database!], ['table', i.table!]] };
const OF_USER: DestinationSpec[] = [
  { label: 'Show queries', description: 'Queries by {object}', page: 'Queries', path: '/queries', params: i => [['user', i.user!]] },
  { label: 'Show failed queries', description: 'Failed queries by {object}', page: 'Queries', path: '/queries', params: i => [['user', i.user!], ['status', 'error']] },
  { label: 'Track in Time Travel', description: 'Time Travel tracking {object}', page: 'Time Travel', path: '/timetravel', params: i => timeTravel(i, i.user) },
];

/** Other TraceHouse pages each object type opens, filtered to that object. Merges exist
 * for MergeTree tables only. Roles, privileges, row policies and storage servers have no
 * page that filters by them. */
export const DESTINATIONS: Record<ObjectType, readonly DestinationSpec[]> = {
  'merge-tree': [...ON_TABLE.slice(0, 2), MERGES, ...ON_TABLE.slice(2)],
  view: ON_TABLE, 'materialized-view': ON_TABLE, distributed: ON_TABLE, dictionary: ON_TABLE, buffer: ON_TABLE,
  iceberg: ON_TABLE, lake: ON_TABLE, 'object-storage': ON_TABLE, table: ON_TABLE,
  user: OF_USER,
  role: [], 'grant-scope': [], 'row-policy': [], location: [],
};
