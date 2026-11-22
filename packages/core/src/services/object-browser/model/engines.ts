/** Table object types, one per engine family with its own presentation and links. */
export type TableType = 'merge-tree' | 'view' | 'materialized-view' | 'distributed' | 'dictionary' | 'buffer'
  | 'iceberg' | 'lake' | 'object-storage' | 'table';

/** How links missing from the native fields are read for an engine. */
export type ReferenceSource =
  /** Tables read by the stored query (system.tables.as_select), resolved by the analyzer. */
  | { reader: 'query-sources'; label: string }
  /** A destination table named by engine arguments at the given positions. */
  | { reader: 'table-arguments'; database: number; table: number; label: string }
  /** The location (URL or path) given as an engine argument, on a `backend` (S3, HDFS,
   * Local, URL). Read from the local definition only. ClickHouse is not asked to contact the storage. */
  | { reader: 'location-argument'; argument: number; backend: string; label: string };

export interface EngineSpec {
  type: TableType;
  /** system.tables dependency arrays name this object as an insert trigger of its source. */
  insertTriggered?: boolean;
  /** system.tables.target_database/target_table name its destination. */
  nativeTarget?: boolean;
  references?: ReferenceSource;
}

/** Backend names as the Iceberg table engine docs list them ("Amazon S3, Azure, HDFS and locally
 * stored tables"). Engines that pick a backend at runtime show `Storage`. */
const storedOn = (backend: string) => ({ reader: 'location-argument', argument: 0, backend, label: 'stored on' }) as const;
const S3 = storedOn('S3'), HDFS = storedOn('HDFS'), LOCAL = storedOn('Local'), URL_BACKEND = storedOn('URL'), ANY = storedOn('Storage');
const ENGINES: Record<string, EngineSpec> = {
  MaterializedView: { type: 'materialized-view', insertTriggered: true, nativeTarget: true, references: { reader: 'query-sources', label: 'read by' } },
  View: { type: 'view', references: { reader: 'query-sources', label: 'read by' } },
  Buffer: { type: 'buffer', references: { reader: 'table-arguments', database: 0, table: 1, label: 'flushes to' } },
  Distributed: { type: 'distributed', references: { reader: 'table-arguments', database: 1, table: 2, label: 'forwards to' } },
  Dictionary: { type: 'dictionary' },
  // Argument 0 is the table's URL or path. Azure variants take a connection
  // string first and have no location here yet.
  Iceberg: { type: 'iceberg', references: ANY },
  IcebergS3: { type: 'iceberg', references: S3 },
  IcebergHDFS: { type: 'iceberg', references: HDFS },
  IcebergLocal: { type: 'iceberg', references: LOCAL },
  IcebergAzure: { type: 'iceberg' },
  DeltaLake: { type: 'lake', references: ANY },
  DeltaLakeS3: { type: 'lake', references: S3 },
  DeltaLakeLocal: { type: 'lake', references: LOCAL },
  DeltaLakeAzure: { type: 'lake' },
  Hudi: { type: 'lake', references: ANY },
  S3: { type: 'object-storage', references: S3 },
  S3Queue: { type: 'object-storage', references: S3 },
  HDFS: { type: 'object-storage', references: HDFS },
  URL: { type: 'object-storage', references: URL_BACKEND },
  AzureBlobStorage: { type: 'object-storage' },
  AzureQueue: { type: 'object-storage' },
};
const MERGE_TREE: EngineSpec = { type: 'merge-tree' };
const OTHER: EngineSpec = { type: 'table' };

/** Engine behavior by name; the MergeTree family shares one entry (ReplicatedSummingMergeTree, ...). */
export function engineSpec(engine: unknown): EngineSpec {
  const name = typeof engine === 'string' ? engine : '';
  return ENGINES[name] ?? (name.endsWith('MergeTree') ? MERGE_TREE : OTHER);
}
