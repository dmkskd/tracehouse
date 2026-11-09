import type { ServerMetrics } from '../types/metrics.js';
import { normalizeTimestamp } from './timestamp.js';
import { type RawRow, toInt, toFloat } from './helpers.js';

/** Caller-supplied timestamp must be an epoch, Date or UTC/explicitly zoned string.
 * No production SQL caller exists in this repository; do not pass local wall time. */
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
