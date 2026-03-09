import type { DatabaseInfo, TableInfo, ColumnSchema, PartInfo, PartDetailInfo, PartColumnInfo } from '../types/database.js';
import { normalizeTimestamp } from './timestamp.js';
import { type RawRow, toInt, toStr, toFloat, toBool } from './helpers.js';

export function mapDatabaseInfo(row: RawRow): DatabaseInfo {
  return {
    name: toStr(row.name),
    engine: toStr(row.engine),
    table_count: toInt(row.table_count),
    total_bytes: toInt(row.total_bytes),
  };
}

export function mapTableInfo(row: RawRow): TableInfo {
  const engine = toStr(row.engine);
  return {
    database: toStr(row.database),
    name: toStr(row.name),
    engine,
    total_rows: toInt(row.total_rows),
    total_bytes: toInt(row.total_bytes),
    partition_key: row.partition_key != null ? toStr(row.partition_key) : null,
    sorting_key: row.sorting_key != null ? toStr(row.sorting_key) : null,
    is_merge_tree: engine.includes('MergeTree'),
  };
}

export function mapColumnSchema(row: RawRow): ColumnSchema {
  return {
    name: toStr(row.name),
    type: toStr(row.type),
    default_kind: toStr(row.default_kind),
    default_expression: toStr(row.default_expression),
    comment: toStr(row.comment),
    is_in_partition_key: toBool(row.is_in_partition_key),
    is_in_sorting_key: toBool(row.is_in_sorting_key),
    is_in_primary_key: toBool(row.is_in_primary_key),
    is_in_sampling_key: toBool(row.is_in_sampling_key),
  };
}

export function mapPartInfo(row: RawRow): PartInfo {
  return {
    partition_id: toStr(row.partition_id),
    name: toStr(row.name),
    rows: toInt(row.rows),
    bytes_on_disk: toInt(row.bytes_on_disk),
    modification_time: normalizeTimestamp(row.modification_time),
    level: toInt(row.level),
    primary_key_bytes_in_memory: toInt(row.primary_key_bytes_in_memory),
  };
}

export function mapPartColumnInfo(row: RawRow): PartColumnInfo {
  return {
    column_name: toStr(row.column_name),
    type: toStr(row.type),
    compressed_bytes: toInt(row.compressed_bytes),
    uncompressed_bytes: toInt(row.uncompressed_bytes),
    compression_ratio: toFloat(row.compression_ratio),
    codec: toStr(row.codec),
    is_in_partition_key: toBool(row.is_in_partition_key),
    is_in_sorting_key: toBool(row.is_in_sorting_key),
    is_in_primary_key: toBool(row.is_in_primary_key),
  };
}

export function mapPartDetailInfo(row: RawRow, columns: PartColumnInfo[] = []): PartDetailInfo {
  return {
    partition_id: toStr(row.partition_id),
    name: toStr(row.name),
    rows: toInt(row.rows),
    bytes_on_disk: toInt(row.bytes_on_disk),
    modification_time: normalizeTimestamp(row.modification_time),
    level: toInt(row.level),
    min_block_number: toInt(row.min_block_number),
    max_block_number: toInt(row.max_block_number),
    marks_count: toInt(row.marks_count),
    marks_bytes: toInt(row.marks_bytes),
    data_compressed_bytes: toInt(row.data_compressed_bytes),
    data_uncompressed_bytes: toInt(row.data_uncompressed_bytes),
    compression_ratio: toFloat(row.compression_ratio),
    primary_key_bytes_in_memory: toInt(row.primary_key_bytes_in_memory),
    min_date: toStr(row.min_date),
    max_date: toStr(row.max_date),
    disk_name: toStr(row.disk_name),
    path: toStr(row.path),
    part_type: toStr(row.part_type),
    partition_key: toStr(row.partition_key),
    sorting_key: toStr(row.sorting_key),
    primary_key: toStr(row.primary_key),
    columns,
  };
}
