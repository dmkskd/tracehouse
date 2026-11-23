import { canonical } from '../model/canonical.js';
import { objectKey, type Observation, type Relationship } from '../model/types.js';
import { parseTTLRules } from '../../../utils/ttl-parser.js';
import type { DiskLinkEvidence } from '../model/disk-links.js';

type Row = Record<string, unknown>;
const text = (v: unknown) => String(v ?? '');

/** Rows of ACCESS_STORAGE_POLICIES, ACCESS_DISKS and ACCESS_DISK_BUCKETS. */
export interface DiskSources { policies: Row[]; disks: Row[]; buckets: Row[] }

/** The disk holding a remote disk's data: a cache disk shares `path` with the disk it caches. */
function baseDisk(disk: Row, disks: Row[]): Row {
  if (!text(disk.cache_path)) return disk;
  return disks.find(d => d.host === disk.host && d.path === disk.path && !text(d.cache_path) && Number(d.is_remote) === 1) ?? disk;
}

/** Node name of a remote disk, e.g. `S3 disk · s3`. */
const storageName = (disk: Row) => `${text(disk.object_storage_type) && disk.object_storage_type !== 'None' ? text(disk.object_storage_type) : text(disk.type)} disk · ${text(disk.name)}`;

/**
 * One disk node per remote disk that a table's storage policy uses, with a `points-to`
 * link from each such table. The link label names the TTL rules that move parts there
 * (parsed from `engine_full`), or the volume when parts reach it otherwise. Local disks are
 * not linked. A cache disk resolves to the disk it caches, and the bucket comes from
 * system.blob_storage_log when it has rows. The endpoint is not in any system table.
 */
export function diskLocations(connection: string, tables: Row[], sources: DiskSources): { observations: Observation[]; relationships: Relationship[] } {
  const relationships: Relationship[] = [];
  const nodes = new Map<string, { host: string; name: string; base: Row; cacheDisks: Set<string>; tables: Set<string> }>();
  for (const row of tables) {
    const host = text(row.host), policy = text(row.storage_policy);
    if (!policy) continue;
    const volumes = sources.policies.filter(p => p.host === host && p.policy_name === policy)
      .sort((a, b) => Number(a.volume_priority) - Number(b.volume_priority));
    const moves = parseTTLRules(text(row.engine_full)).filter(rule => rule.action === 'move');
    const table = objectKey(connection, 'table', text(row.database), text(row.name));
    volumes.forEach((volume, index) => {
      for (const diskName of Array.isArray(volume.disks) ? volume.disks.map(text) : []) {
        const disk = sources.disks.find(d => d.host === host && d.name === diskName);
        if (!disk || Number(disk.is_remote) !== 1) continue;
        const base = baseDisk(disk, sources.disks), name = storageName(base), key = objectKey(connection, 'disk', name);
        const node = nodes.get(canonical([key, host])) ?? { host, name, base, cacheDisks: new Set<string>(), tables: new Set<string>() };
        nodes.set(canonical([key, host]), node);
        if (disk !== base) node.cacheDisks.add(diskName);
        node.tables.add(table);
        const ttl = moves.filter(rule => rule.destination!.type === 'volume' ? rule.destination!.name === text(volume.volume_name) : rule.destination!.name === diskName)
          .map(rule => rule.expression);
        const label = ttl.length ? `moves after TTL ${ttl.join(', ')}` : index === 0 ? 'stored on' : `stored on · volume ${text(volume.volume_name)}`;
        const evidence: DiskLinkEvidence = { method: 'storage-policy', policy, volume: text(volume.volume_name), disk: diskName, ...(ttl.length ? { ttl } : {}) };
        relationships.push({ id: canonical([host, table, key, 'points-to', evidence]), host, from: table, to: key, kind: 'points-to', label, source: 'tables', evidence: { ...evidence } });
      }
    });
  }
  const observations = [...nodes.values()].map(({ host, name, base, cacheDisks, tables: stored }): Observation => {
    const buckets = sources.buckets.find(b => b.host === host && b.disk_name === base.name)?.buckets;
    return { key: objectKey(connection, 'disk', name), kind: 'disk', name, host, source: 'tables', attributes: {
      server: name, backend: storageName(base).split(' · ')[0], disk: text(base.name), tables: stored.size,
      ...(cacheDisks.size ? { cache_disks: [...cacheDisks].sort() } : {}),
      ...(Array.isArray(buckets) && buckets.length ? { buckets: buckets.map(text).sort() } : {}) } };
  });
  return { observations, relationships };
}
