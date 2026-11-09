import { queryTimezoneFixture } from './query-timezone-fixture.js';

export function engineTimezoneFixture(table: string, epoch: number, zone: string): string {
  if (table === 'dictionaries') return `(SELECT 'fixture' AS name, 'Hashed' AS type,
    toUInt64(10) AS bytes_allocated, toUInt64(2) AS element_count, toFloat64(0.5) AS load_factor,
    'fixture' AS source, 'LOADED' AS status, toDateTime(${epoch}, '${zone}') AS last_successful_update_time)`;
  const base = queryTimezoneFixture(table, epoch, zone);
  return table === 'trace_log'
    ? `(SELECT *, toUInt32(1) AS cpu_id, 'CPU' AS trace_type FROM ${base})`
    : `(SELECT *, toUInt64(7) AS ProfileEvent_Query FROM ${base})`;
}
