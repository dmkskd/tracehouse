import { mergeTimezoneFixture } from './merge-timezone-fixture.js';

export function partTimezoneFixture(table: string, epoch: number, zone: string): string {
  if (table === 'parts') return `(SELECT 'default' AS database, 'fixture' AS table,
    'fixture_1_2_1' AS name, 'fixture' AS partition_id, toUInt8(1) AS active,
    toUInt64(7) AS rows, toUInt64(400) AS bytes_on_disk, toUInt32(1) AS level,
    toDateTime(${epoch}, '${zone}') AS modification_time,
    toUInt64(1) AS min_block_number, toUInt64(2) AS max_block_number,
    toUInt64(1) AS marks, toUInt64(100) AS marks_bytes,
    toUInt64(200) AS data_compressed_bytes, toUInt64(600) AS data_uncompressed_bytes,
    toUInt64(10) AS primary_key_bytes_in_memory, toDate('2026-11-08') AS min_date,
    toDate('2026-11-08') AS max_date, 'default' AS disk_name, '/fixture' AS path, 'Wide' AS part_type)`;
  if (table === 'parts_columns') return `(SELECT 'default' AS database, 'fixture' AS table,
    'fixture_1_2_1' AS name, toUInt8(1) AS active, 'value' AS column, 'UInt64' AS type,
    toUInt64(200) AS column_data_compressed_bytes, toUInt64(600) AS column_data_uncompressed_bytes)`;
  if (table === 'columns') return `(SELECT 'default' AS database, 'fixture' AS table, 'value' AS name, 'LZ4' AS compression_codec)`;
  if (table === 'tables') return `(SELECT 'default' AS database, 'fixture' AS name,
    '' AS partition_key, 'value' AS sorting_key, 'value' AS primary_key, 'MergeTree' AS engine)`;
  return mergeTimezoneFixture(table, epoch, zone)
    .replace("'part-fixture' AS part_name", "'fixture_1_2_1' AS part_name")
    .replace("['source_1', 'source_2']", "['fixture_1_1_0', 'fixture_2_2_0']");
}
