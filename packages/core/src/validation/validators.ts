import type { DatabaseInfo, TableInfo, PartInfo, PartDetailInfo } from '../types/database.js';
import type { MergeInfo } from '../types/merge.js';
import type { ServerMetrics } from '../types/metrics.js';
import type { QueryMetrics } from '../types/query.js';
import type { ConnectionConfig } from '../types/connection.js';

function checkNonNegative(value: number, field: string, errors: string[]): void {
  if (value < 0) {
    errors.push(`${field} must be non-negative, got ${value}`);
  }
}

function checkNonEmpty(value: string, field: string, errors: string[]): void {
  if (value === '') {
    errors.push(`${field} must be non-empty`);
  }
}

function checkBounded(value: number, field: string, min: number, max: number, errors: string[]): void {
  if (value < min || value > max) {
    errors.push(`${field} must be between ${min} and ${max}, got ${value}`);
  }
}

export function validateDatabaseInfo(obj: DatabaseInfo): string[] {
  const errors: string[] = [];
  checkNonEmpty(obj.name, 'name', errors);
  checkNonEmpty(obj.engine, 'engine', errors);
  checkNonNegative(obj.table_count, 'table_count', errors);
  return errors;
}

export function validateTableInfo(obj: TableInfo): string[] {
  const errors: string[] = [];
  checkNonEmpty(obj.database, 'database', errors);
  checkNonEmpty(obj.name, 'name', errors);
  checkNonEmpty(obj.engine, 'engine', errors);
  checkNonNegative(obj.total_rows, 'total_rows', errors);
  checkNonNegative(obj.total_bytes, 'total_bytes', errors);
  return errors;
}

export function validatePartInfo(obj: PartInfo): string[] {
  const errors: string[] = [];
  checkNonEmpty(obj.partition_id, 'partition_id', errors);
  checkNonEmpty(obj.name, 'name', errors);
  checkNonNegative(obj.rows, 'rows', errors);
  checkNonNegative(obj.bytes_on_disk, 'bytes_on_disk', errors);
  checkNonNegative(obj.level, 'level', errors);
  checkNonNegative(obj.primary_key_bytes_in_memory, 'primary_key_bytes_in_memory', errors);
  return errors;
}

export function validateMergeInfo(obj: MergeInfo): string[] {
  const errors: string[] = [];
  checkNonEmpty(obj.database, 'database', errors);
  checkNonEmpty(obj.table, 'table', errors);
  checkNonNegative(obj.elapsed, 'elapsed', errors);
  checkBounded(obj.progress, 'progress', 0, 1, errors);
  checkNonNegative(obj.num_parts, 'num_parts', errors);
  if (obj.source_part_names.length === 0) {
    errors.push('source_part_names must be non-empty');
  }
  checkNonEmpty(obj.result_part_name, 'result_part_name', errors);
  checkNonNegative(obj.total_size_bytes_compressed, 'total_size_bytes_compressed', errors);
  checkNonNegative(obj.rows_read, 'rows_read', errors);
  checkNonNegative(obj.rows_written, 'rows_written', errors);
  checkNonNegative(obj.memory_usage, 'memory_usage', errors);
  checkNonEmpty(obj.merge_type, 'merge_type', errors);
  checkNonEmpty(obj.merge_algorithm, 'merge_algorithm', errors);
  checkNonNegative(obj.bytes_read_uncompressed, 'bytes_read_uncompressed', errors);
  checkNonNegative(obj.bytes_written_uncompressed, 'bytes_written_uncompressed', errors);
  checkNonNegative(obj.columns_written, 'columns_written', errors);
  checkNonNegative(obj.thread_id, 'thread_id', errors);
  return errors;
}

export function validateServerMetrics(obj: ServerMetrics): string[] {
  const errors: string[] = [];
  checkBounded(obj.cpu_usage, 'cpu_usage', 0, 100, errors);
  checkNonNegative(obj.memory_used, 'memory_used', errors);
  checkNonNegative(obj.memory_total, 'memory_total', errors);
  checkNonNegative(obj.disk_read_bytes, 'disk_read_bytes', errors);
  checkNonNegative(obj.disk_write_bytes, 'disk_write_bytes', errors);
  checkNonNegative(obj.uptime_seconds, 'uptime_seconds', errors);
  return errors;
}

export function validateQueryMetrics(obj: QueryMetrics): string[] {
  const errors: string[] = [];
  checkNonEmpty(obj.query_id, 'query_id', errors);
  checkNonEmpty(obj.user, 'user', errors);
  checkNonEmpty(obj.query, 'query', errors);
  checkNonNegative(obj.elapsed_seconds, 'elapsed_seconds', errors);
  checkNonNegative(obj.memory_usage, 'memory_usage', errors);
  checkNonNegative(obj.read_rows, 'read_rows', errors);
  checkNonNegative(obj.read_bytes, 'read_bytes', errors);
  checkNonNegative(obj.total_rows_approx, 'total_rows_approx', errors);
  checkBounded(obj.progress, 'progress', 0, 1, errors);
  return errors;
}

export function validateConnectionConfig(obj: ConnectionConfig): string[] {
  const errors: string[] = [];
  checkNonEmpty(obj.host, 'host', errors);
  checkNonNegative(obj.port, 'port', errors);
  checkNonEmpty(obj.user, 'user', errors);
  checkNonEmpty(obj.database, 'database', errors);
  checkNonNegative(obj.connect_timeout, 'connect_timeout', errors);
  checkNonNegative(obj.send_receive_timeout, 'send_receive_timeout', errors);
  return errors;
}
