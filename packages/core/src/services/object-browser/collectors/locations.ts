import { canonical } from '../model/canonical.js';
import { objectKey, type Observation, type Relationship } from '../model/types.js';

/** A ClickHouse table, the location from its definition and the storage backend its engine uses, on one host. */
export interface LocationEntry { host: string; table: string; location: string; backend: string; definition: string }

/** The storage server of a location: scheme and host of a URL (credentials dropped),
 * or the ClickHouse host's own disk for a plain path. No storage-specific rules apply. */
export function storageServer(location: string, host: string): string {
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(location) && URL.canParse(location)) {
    const url = new URL(location);
    return `${url.protocol}//${url.host}`;
  }
  return `local disk · ${host}`;
}

/** One location node per storage server, with a `points-to` link from each table stored there.
 * Tables on the same server meet at one node. The full location stays in the link evidence. */
export function locationNodes(connection: string, entries: LocationEntry[]): { observations: Observation[]; relationships: Relationship[] } {
  const servers = new Map<string, LocationEntry[]>();
  for (const entry of entries) {
    const server = storageServer(entry.location, entry.host);
    servers.set(server, [...servers.get(server) ?? [], entry]);
  }
  const observations: Observation[] = [], relationships: Relationship[] = [];
  for (const [server, stored] of servers) {
    const key = objectKey(connection, 'location', server);
    const backend = [...new Set(stored.map(e => e.backend))].sort().join(' / ');
    for (const host of new Set(stored.map(e => e.host))) {
      observations.push({ key, kind: 'location', name: server, host, source: 'tables',
        attributes: { server, backend, tables: stored.filter(e => e.host === host).length } });
    }
    for (const entry of stored) {
      const evidence = { method: 'engine-location', location: entry.location, definition: entry.definition };
      relationships.push({ id: canonical([entry.host, entry.table, key, 'points-to']), host: entry.host, from: entry.table, to: key,
        kind: 'points-to', label: 'stored on', source: 'tables', evidence });
    }
  }
  return { observations, relationships };
}
