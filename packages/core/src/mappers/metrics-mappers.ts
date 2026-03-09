import type { ServerMetrics } from '../types/metrics.js';
import { normalizeTimestamp } from './timestamp.js';
import { type RawRow, toInt, toFloat } from './helpers.js';

export function mapServerMetrics(row: RawRow): ServerMetrics {
  return {
    timestamp: normalizeTimestamp(row.timestamp),
    cpu_usage: toFloat(row.cpu_usage),
    memory_used: toInt(row.memory_used),
    memory_total: toInt(row.memory_total),
    disk_read_bytes: toInt(row.disk_read_bytes),
    disk_write_bytes: toInt(row.disk_write_bytes),
    uptime_seconds: toInt(row.uptime_seconds),
  };
}
