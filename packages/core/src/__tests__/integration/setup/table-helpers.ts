/**
 * Test table creation helpers.
 *
 * Provides a flexible `createTestTable()` that supports different
 * MergeTree engine variants (Replicated, Replacing, Collapsing, etc.)
 * and common table settings.
 *
 * Usage:
 *   await createTestTable(client, {
 *     database: 'test_db',
 *     table: 'events',
 *     columns: { id: 'UInt64', ts: 'DateTime DEFAULT now()', value: 'Float64' },
 *     engine: 'ReplicatedMergeTree',
 *     orderBy: '(ts, id)',
 *   });
 */

import type { ClickHouseClient } from '@clickhouse/client';

// ── Engine types ──

export type MergeTreeVariant =
  | 'MergeTree'
  | 'ReplacingMergeTree'
  | 'SummingMergeTree'
  | 'AggregatingMergeTree'
  | 'CollapsingMergeTree'
  | 'VersionedCollapsingMergeTree'
  | 'ReplicatedMergeTree'
  | 'ReplicatedReplacingMergeTree'
  | 'ReplicatedSummingMergeTree'
  | 'ReplicatedAggregatingMergeTree'
  | 'ReplicatedCollapsingMergeTree'
  | 'ReplicatedVersionedCollapsingMergeTree';

export interface CreateTableOptions {
  /** Database name (must already exist). */
  database: string;
  /** Table name. */
  table: string;
  /** Column definitions: { name: 'type [DEFAULT ...]' }. */
  columns: Record<string, string>;
  /** Engine variant. Defaults to 'MergeTree'. */
  engine?: MergeTreeVariant;
  /** Engine arguments (e.g. 'ver' for ReplacingMergeTree, 'sign' for Collapsing). */
  engineArgs?: string[];
  /** ORDER BY clause. Defaults to the first column. */
  orderBy?: string;
  /** PARTITION BY clause (optional). */
  partitionBy?: string;
  /** PRIMARY KEY clause (optional, defaults to ORDER BY). */
  primaryKey?: string;
  /** Extra SETTINGS (e.g. { min_bytes_for_wide_part: 0 }). */
  settings?: Record<string, string | number>;
  /** Use IF NOT EXISTS. Defaults to true. */
  ifNotExists?: boolean;
  /**
   * Whether the database uses ENGINE = Replicated.
   * When true, Replicated* engines use bare args (no zoo path/replica name)
   * because the database manages them automatically.
   * When false (default), explicit zoo paths are generated.
   */
  replicatedDatabase?: boolean;
}

/**
 * Build the ENGINE = ... clause.
 *
 * For Replicated* engines in a non-Replicated database, auto-generates
 * zoo_path and replica_name using macros.
 * For Replicated* engines in a Replicated database, uses bare args
 * (ClickHouse manages paths automatically and rejects explicit ones).
 */
function buildEngineClause(opts: CreateTableOptions): string {
  const engine = opts.engine ?? 'MergeTree';
  const isReplicated = engine.startsWith('Replicated');

  const args: string[] = [];

  if (isReplicated && !opts.replicatedDatabase) {
    // Non-Replicated database: explicit ZooKeeper path and replica name
    const zooPath = `/clickhouse/tables/{shard}/${opts.database}/${opts.table}`;
    args.push(`'${zooPath}'`, `'{replica}'`);
  }
  // Replicated database: no zoo path args needed (CH manages them)

  if (opts.engineArgs) {
    args.push(...opts.engineArgs);
  }

  return args.length > 0 ? `${engine}(${args.join(', ')})` : `${engine}()`;
}

/**
 * Create a test table with flexible engine and schema options.
 */
export async function createTestTable(
  client: ClickHouseClient,
  opts: CreateTableOptions,
): Promise<void> {
  const ifNotExists = opts.ifNotExists !== false ? 'IF NOT EXISTS ' : '';
  const fqn = `${opts.database}.${opts.table}`;

  const columnDefs = Object.entries(opts.columns)
    .map(([name, type]) => `  ${name} ${type}`)
    .join(',\n');

  const engineClause = buildEngineClause(opts);

  const orderBy = opts.orderBy ?? Object.keys(opts.columns)[0];

  let ddl = `CREATE TABLE ${ifNotExists}${fqn} (\n${columnDefs}\n) ENGINE = ${engineClause}\nORDER BY ${orderBy}`;

  if (opts.partitionBy) {
    ddl += `\nPARTITION BY ${opts.partitionBy}`;
  }

  if (opts.primaryKey) {
    ddl += `\nPRIMARY KEY ${opts.primaryKey}`;
  }

  if (opts.settings && Object.keys(opts.settings).length > 0) {
    const settingsPairs = Object.entries(opts.settings)
      .map(([k, v]) => `  ${k} = ${v}`)
      .join(',\n');
    ddl += `\nSETTINGS\n${settingsPairs}`;
  }

  await client.command({ query: ddl });
}

/**
 * Create a test database (IF NOT EXISTS).
 */
export async function createTestDatabase(
  client: ClickHouseClient,
  database: string,
  engine?: 'Atomic' | 'Replicated',
): Promise<void> {
  const engineClause = engine === 'Replicated'
    ? ` ENGINE = Replicated('/clickhouse/databases/${database}', '{shard}', '{replica}')`
    : '';
  await client.command({
    query: `CREATE DATABASE IF NOT EXISTS ${database}${engineClause}`,
  });
}

/**
 * Drop a test database (IF EXISTS).
 */
export async function dropTestDatabase(
  client: ClickHouseClient,
  database: string,
): Promise<void> {
  await client.command({ query: `DROP DATABASE IF EXISTS ${database}` });
}
