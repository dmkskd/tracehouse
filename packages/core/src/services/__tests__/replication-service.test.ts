import { describe, it, expect } from 'vitest';
import {
  extractShardId,
  shardIdToNum,
  assembleShards,
  parseShardingKey,
  buildEngineInfo,
  filterQueueEntries,
  classifyReplicaHealth,
} from '../replication-service.js';
import type { ReplicaInfo, ReplicaPartStats, TopologyQueueEntry } from '../../types/replication-topology.js';

// ─── extractShardId ──────────────────────────────────────────────────────────

describe('extractShardId', () => {
  it('extracts last path segment from a typical ZK path', () => {
    expect(extractShardId('/clickhouse/tables/some-uuid/0')).toBe('0');
    expect(extractShardId('/clickhouse/tables/some-uuid/1')).toBe('1');
  });

  it('handles trailing slash', () => {
    expect(extractShardId('/clickhouse/tables/some-uuid/0/')).toBe('0');
  });

  it('handles named shard identifiers', () => {
    expect(extractShardId('/clickhouse/tables/uuid/shard_01')).toBe('shard_01');
  });

  it('returns "01" for empty path', () => {
    expect(extractShardId('')).toBe('01');
    expect(extractShardId('/')).toBe('01');
  });
});

// ─── shardIdToNum ────────────────────────────────────────────────────────────

describe('shardIdToNum', () => {
  it('parses numeric strings', () => {
    expect(shardIdToNum('0')).toBe(0);
    expect(shardIdToNum('1')).toBe(1);
    expect(shardIdToNum('42')).toBe(42);
  });

  it('returns 0 for non-numeric strings', () => {
    expect(shardIdToNum('shard_01')).toBe(0);
    expect(shardIdToNum('')).toBe(0);
  });
});

// ─── assembleShards ──────────────────────────────────────────────────────────

function makeReplica(overrides: Partial<ReplicaInfo> = {}): ReplicaInfo {
  return {
    hostname: 'host-0-0',
    database: 'db',
    table: 'tbl',
    replica_name: 'r0',
    zookeeper_path: '/clickhouse/tables/uuid/0',
    replica_path: '/clickhouse/tables/uuid/0/replicas/r0',
    is_leader: 1,
    is_readonly: 0,
    is_session_expired: 0,
    absolute_delay: 0,
    queue_size: 0,
    inserts_in_queue: 0,
    merges_in_queue: 0,
    log_pointer: 100,
    log_max_index: 100,
    total_replicas: 2,
    active_replicas: 2,
    ...overrides,
  };
}

function makeParts(hostname: string, bytes: number, parts: number, rows: number): ReplicaPartStats {
  return { hostname, part_count: parts, bytes_on_disk: bytes, total_rows: rows, partition_count: 1 };
}

describe('assembleShards', () => {
  it('groups replicas by ZK path into shards', () => {
    const replicas = [
      makeReplica({ hostname: 'h0', replica_name: 'r0', zookeeper_path: '/zk/0' }),
      makeReplica({ hostname: 'h1', replica_name: 'r1', zookeeper_path: '/zk/0' }),
      makeReplica({ hostname: 'h2', replica_name: 'r2', zookeeper_path: '/zk/1' }),
    ];
    const parts = new Map([
      ['h0', makeParts('h0', 1000, 5, 500)],
      ['h1', makeParts('h1', 1000, 5, 500)],
      ['h2', makeParts('h2', 2000, 10, 1000)],
    ]);

    const result = assembleShards(replicas, parts);

    expect(result.shards).toHaveLength(2);
    expect(result.shards[0].shardNum).toBe(0);
    expect(result.shards[0].replicas).toHaveLength(2);
    expect(result.shards[0].totalBytes).toBe(1000);
    expect(result.shards[1].shardNum).toBe(1);
    expect(result.shards[1].replicas).toHaveLength(1);
    expect(result.shards[1].totalBytes).toBe(2000);
  });

  it('computes logHead as max of log_max_index across all replicas', () => {
    const replicas = [
      makeReplica({ hostname: 'h0', zookeeper_path: '/zk/0', log_max_index: 100 }),
      makeReplica({ hostname: 'h1', zookeeper_path: '/zk/0', log_max_index: 200 }),
      makeReplica({ hostname: 'h2', zookeeper_path: '/zk/1', log_max_index: 150 }),
    ];
    const result = assembleShards(replicas, new Map());
    expect(result.logHead).toBe(200);
  });

  it('handles replicas without part stats (null parts)', () => {
    const replicas = [
      makeReplica({ hostname: 'h0', zookeeper_path: '/zk/0' }),
    ];
    const result = assembleShards(replicas, new Map());
    expect(result.shards[0].totalBytes).toBe(0);
    expect(result.shards[0].totalParts).toBe(0);
    expect(result.shards[0].replicas[0].parts).toBeNull();
  });

  it('sorts shards by shard number', () => {
    const replicas = [
      makeReplica({ hostname: 'h2', zookeeper_path: '/zk/2' }),
      makeReplica({ hostname: 'h0', zookeeper_path: '/zk/0' }),
      makeReplica({ hostname: 'h1', zookeeper_path: '/zk/1' }),
    ];
    const result = assembleShards(replicas, new Map());
    expect(result.shards.map(s => s.shardNum)).toEqual([0, 1, 2]);
  });

  it('sets allLeaders=true when every replica in a shard is leader', () => {
    const replicas = [
      makeReplica({ hostname: 'h0', zookeeper_path: '/zk/0', is_leader: 1 }),
      makeReplica({ hostname: 'h1', zookeeper_path: '/zk/0', is_leader: 1 }),
    ];
    const result = assembleShards(replicas, new Map());
    expect(result.shards[0].allLeaders).toBe(true);
  });

  it('sets allLeaders=false when not all replicas are leaders', () => {
    const replicas = [
      makeReplica({ hostname: 'h0', zookeeper_path: '/zk/0', is_leader: 1 }),
      makeReplica({ hostname: 'h1', zookeeper_path: '/zk/0', is_leader: 0 }),
    ];
    const result = assembleShards(replicas, new Map());
    expect(result.shards[0].allLeaders).toBe(false);
  });

  it('sets allLeaders=false for single-replica shards (not meaningful)', () => {
    const replicas = [
      makeReplica({ hostname: 'h0', zookeeper_path: '/zk/0', is_leader: 1 }),
    ];
    const result = assembleShards(replicas, new Map());
    expect(result.shards[0].allLeaders).toBe(false);
  });

  it('uses max bytes across replicas for shard totalBytes (not sum)', () => {
    const replicas = [
      makeReplica({ hostname: 'h0', zookeeper_path: '/zk/0' }),
      makeReplica({ hostname: 'h1', zookeeper_path: '/zk/0' }),
    ];
    const parts = new Map([
      ['h0', makeParts('h0', 5000, 10, 1000)],
      ['h1', makeParts('h1', 3000, 8, 800)],
    ]);
    const result = assembleShards(replicas, parts);
    expect(result.shards[0].totalBytes).toBe(5000);
  });
});

// ─── parseShardingKey ────────────────────────────────────────────────────────

describe('parseShardingKey', () => {
  it('extracts sharding expression from Distributed engine_full', () => {
    const engineFull = "Distributed('dev', 'web_analytics', 'pageviews_local', sipHash64(domain))";
    expect(parseShardingKey(engineFull)).toBe('sipHash64(domain)');
  });

  it('handles rand() sharding', () => {
    expect(parseShardingKey("Distributed('c', 'db', 'tbl', rand())")).toBe('rand()');
  });

  it('returns null when no sharding key (3-arg Distributed)', () => {
    expect(parseShardingKey("Distributed('c', 'db', 'tbl')")).toBeNull();
  });

  it('returns null for non-Distributed engines', () => {
    expect(parseShardingKey('ReplicatedMergeTree(...)')).toBeNull();
  });

  it('handles complex sharding expressions', () => {
    expect(parseShardingKey("Distributed('c', 'db', 'tbl', cityHash64(user_id, event_date))"))
      .toBe('cityHash64(user_id, event_date)');
  });
});

// ─── buildEngineInfo ─────────────────────────────────────────────────────────

describe('buildEngineInfo', () => {
  it('returns null for empty engine rows', () => {
    expect(buildEngineInfo([], [])).toBeNull();
  });

  it('builds engine info without Distributed table', () => {
    const result = buildEngineInfo(
      [{ engine: 'ReplicatedMergeTree', partition_key: 'toYYYYMM(date)', sorting_key: 'date, id' }],
      [],
    );
    expect(result).toEqual({
      engine: 'ReplicatedMergeTree',
      partitionKey: 'toYYYYMM(date)',
      sortingKey: 'date, id',
      distributedTable: null,
      shardingKey: null,
    });
  });

  it('builds engine info with Distributed table and sharding key', () => {
    const result = buildEngineInfo(
      [{ engine: 'ReplicatedMergeTree', partition_key: 'toYYYYMM(date)', sorting_key: 'date, id' }],
      [{ dist_table: 'events_dist', engine_full: "Distributed('c', 'db', 'events', sipHash64(user_id))" }],
    );
    expect(result).toEqual({
      engine: 'ReplicatedMergeTree',
      partitionKey: 'toYYYYMM(date)',
      sortingKey: 'date, id',
      distributedTable: 'events_dist',
      shardingKey: 'sipHash64(user_id)',
    });
  });
});

// ─── filterQueueEntries ──────────────────────────────────────────────────────

describe('filterQueueEntries', () => {
  it('filters to only matching table', () => {
    const rows = [
      { table: 'events', replica_name: 'r1', type: 'GET_PART', new_part_name: 'p1', is_currently_executing: 0, num_tries: 1, num_postponed: 0, last_exception: '', create_time: '2025-01-01', hostname: 'h1' },
      { table: 'other', replica_name: 'r2', type: 'MERGE_PARTS', new_part_name: 'p2', is_currently_executing: 0, num_tries: 0, num_postponed: 0, last_exception: '', create_time: '2025-01-01', hostname: 'h2' },
      { table: 'events', replica_name: 'r3', type: 'MUTATE_PART', new_part_name: 'p3', is_currently_executing: 1, num_tries: 3, num_postponed: 0, last_exception: 'error', create_time: '2025-01-02', hostname: 'h3' },
    ];
    const result = filterQueueEntries(rows, 'events');
    expect(result).toHaveLength(2);
    expect(result[0].replica_name).toBe('r1');
    expect(result[1].replica_name).toBe('r3');
    expect(result[1].last_exception).toBe('error');
  });

  it('returns empty for no matches', () => {
    expect(filterQueueEntries([{ table: 'other' }], 'events')).toHaveLength(0);
  });
});

// ─── classifyReplicaHealth ──────────────────────────────────────────────────

function makeQueueEntry(overrides: Partial<TopologyQueueEntry> = {}): TopologyQueueEntry {
  return {
    hostname: 'h0',
    replica_name: 'r0',
    type: 'GET_PART',
    new_part_name: 'p1',
    is_currently_executing: 0,
    num_tries: 0,
    num_postponed: 0,
    last_exception: '',
    create_time: '2025-01-01',
    ...overrides,
  };
}

describe('classifyReplicaHealth', () => {
  it('returns healthy for a normal replica with no issues', () => {
    const info = makeReplica();
    const result = classifyReplicaHealth(info, []);
    expect(result.status).toBe('healthy');
    expect(result.reasons).toHaveLength(0);
  });

  it('returns healthy when queue has active items (normal operation)', () => {
    const info = makeReplica({ queue_size: 5 });
    const queue = [
      makeQueueEntry({ replica_name: 'r0', num_tries: 1, last_exception: '' }),
      makeQueueEntry({ replica_name: 'r0', num_tries: 2, last_exception: '' }),
    ];
    const result = classifyReplicaHealth(info, queue);
    expect(result.status).toBe('healthy');
  });

  it('returns healthy when replica has brief delay (normal replication lag)', () => {
    const info = makeReplica({ absolute_delay: 5 });
    const result = classifyReplicaHealth(info, []);
    expect(result.status).toBe('healthy');
  });

  it('returns error when session is expired', () => {
    const info = makeReplica({ is_session_expired: 1 });
    const result = classifyReplicaHealth(info, []);
    expect(result.status).toBe('error');
    expect(result.reasons).toContain('ZooKeeper session expired');
  });

  it('returns error when replica is readonly', () => {
    const info = makeReplica({ is_readonly: 1 });
    const result = classifyReplicaHealth(info, []);
    expect(result.status).toBe('error');
    expect(result.reasons).toContain('Replica is read-only (lost Keeper connection)');
  });

  it('returns error with both reasons when session expired AND readonly', () => {
    const info = makeReplica({ is_readonly: 1, is_session_expired: 1 });
    const result = classifyReplicaHealth(info, []);
    expect(result.status).toBe('error');
    expect(result.reasons).toHaveLength(2);
  });

  it('returns warning when queue entries are stuck (>5 retries with exceptions)', () => {
    const info = makeReplica({ replica_name: 'r0' });
    const queue = [
      makeQueueEntry({ replica_name: 'r0', num_tries: 10, last_exception: 'Code: 243. DB::Exception: ...' }),
    ];
    const result = classifyReplicaHealth(info, queue);
    expect(result.status).toBe('warning');
    expect(result.reasons[0]).toMatch(/1 stuck queue entry/);
  });

  it('returns warning with plural for multiple stuck entries', () => {
    const info = makeReplica({ replica_name: 'r0' });
    const queue = [
      makeQueueEntry({ replica_name: 'r0', num_tries: 8, last_exception: 'error1' }),
      makeQueueEntry({ replica_name: 'r0', num_tries: 12, last_exception: 'error2' }),
    ];
    const result = classifyReplicaHealth(info, queue);
    expect(result.status).toBe('warning');
    expect(result.reasons[0]).toMatch(/2 stuck queue entries/);
  });

  it('ignores queue entries with retries but no exception (transient retries)', () => {
    const info = makeReplica({ replica_name: 'r0' });
    const queue = [
      makeQueueEntry({ replica_name: 'r0', num_tries: 10, last_exception: '' }),
    ];
    const result = classifyReplicaHealth(info, queue);
    expect(result.status).toBe('healthy');
  });

  it('ignores queue entries with exceptions but few retries (just started failing)', () => {
    const info = makeReplica({ replica_name: 'r0' });
    const queue = [
      makeQueueEntry({ replica_name: 'r0', num_tries: 2, last_exception: 'error' }),
    ];
    const result = classifyReplicaHealth(info, queue);
    expect(result.status).toBe('healthy');
  });

  it('only considers queue entries for the specific replica', () => {
    const info = makeReplica({ replica_name: 'r0' });
    const queue = [
      makeQueueEntry({ replica_name: 'r1', num_tries: 20, last_exception: 'error' }),
    ];
    const result = classifyReplicaHealth(info, queue);
    expect(result.status).toBe('healthy');
  });

  it('error takes precedence over warning (readonly + stuck queue)', () => {
    const info = makeReplica({ replica_name: 'r0', is_readonly: 1 });
    const queue = [
      makeQueueEntry({ replica_name: 'r0', num_tries: 10, last_exception: 'error' }),
    ];
    const result = classifyReplicaHealth(info, queue);
    expect(result.status).toBe('error');
  });
});
