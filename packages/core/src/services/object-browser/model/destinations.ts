import type { ObjectType } from '../presentation/cards.js';

/** What a destination is built from: the object, and the user in focus context if any. */
export interface DestinationInput { database?: string; table?: string; user?: string; contextUser?: string }

export interface DestinationSpec {
  /** Button text: the page name. */
  label: string;
  /** What the page shows, for the tooltip: `{object}` is the table or user. */
  description: string;
  /** The same with a user chosen in the detail's user filter: `{user}` is that user. */
  descriptionForUser?: string;
  page: 'Queries' | 'Merges' | 'Time Travel';
  path: '/queries' | '/merges' | '/timetravel';
  /** URL parameters of the page, as its share schema reads them (repeated keys for arrays). `user` is the
   * user chosen in the filter, for pages that filter by user (those with `descriptionForUser`). */
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
  { label: 'Queries', description: 'Queries on {object}', descriptionForUser: 'Queries by {user} on {object}', page: 'Queries', path: '/queries',
    params: i => [...(i.contextUser ? [['user', i.contextUser]] as [string, string][] : []), tableName(i)] },
  { label: 'Time Travel', description: 'Time Travel tracking {object}', descriptionForUser: 'Time Travel tracking {user} on {object}', page: 'Time Travel', path: '/timetravel',
    params: i => timeTravel(i, i.contextUser) },
];
const MERGES: DestinationSpec = { label: 'Merges', description: 'Merges of {object} (merges have no user)', page: 'Merges', path: '/merges',
  params: i => [['database', i.database!], ['table', i.table!]] };
/** Other TraceHouse pages each object type opens, filtered to that object. Only tables and
 * views link out (with an optional user filter); merges exist for MergeTree tables only. */
export const DESTINATIONS: Record<ObjectType, readonly DestinationSpec[]> = {
  'merge-tree': [...ON_TABLE, MERGES],
  view: ON_TABLE, 'materialized-view': ON_TABLE, distributed: ON_TABLE, dictionary: ON_TABLE, buffer: ON_TABLE,
  iceberg: ON_TABLE, lake: ON_TABLE, 'object-storage': ON_TABLE, table: ON_TABLE,
  user: [], role: [], 'grant-scope': [], 'row-policy': [], location: [],
};
