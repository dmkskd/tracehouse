import { queryTimezoneFixture } from './query-timezone-fixture.js';

export function mergeTimezoneFixture(table: string, epoch: number, zone: string): string {
  if (table === 'columns') return `(SELECT 'system' AS database, 'mutations' AS table, 'is_killed' AS name)`;
  if (table === 'mutations') return `(SELECT 'default' AS database, 'fixture' AS table, 'mutation_1' AS mutation_id,
    'UPDATE fixture' AS command, toDateTime(${epoch}, '${zone}') AS create_time,
    toDateTime(${epoch} + 2, '${zone}') AS latest_fail_time, '' AS latest_failed_part, '' AS latest_fail_reason,
    toUInt8(1) AS is_done, toUInt8(0) AS is_killed, toUInt64(0) AS parts_to_do, [] AS parts_to_do_names,
    ['partition'] AS \`block_numbers.partition_id\`, [toInt64(1)] AS \`block_numbers.number\`)`;
  if (table === 'tables') return `(SELECT 'default' AS database, 'fixture' AS name, 'MergeTree' AS engine)`;
  const base = queryTimezoneFixture(table, epoch, zone)
    .replace('FROM numbers(1)', 'FROM numbers(2)')
    .replace('+ 123456,', '+ 123456 + number,');
  return `(SELECT *,
    toUInt64(400) AS size_in_bytes, 'RegularMerge' AS merge_reason,
    ['source_1', 'source_2'] AS merged_from, toUInt64(600) AS bytes_uncompressed,
    'Horizontal' AS merge_algorithm, '/fixture/part' AS path_on_disk
  FROM ${base})`;
}
