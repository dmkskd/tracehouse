/**
 * SQL query templates for part lineage tree building.
 *
 * Used by the lineage builder to reconstruct merge history trees
 * from system.parts and system.part_log.
 */

/** Get active part sizes (compressed on-disk) from system.parts. */
export const GET_ACTIVE_PART_SIZES = `
  SELECT name, any(rows) AS rows, any(bytes_on_disk) AS bytes_on_disk, any(level) AS level
  FROM {{cluster_metadata:system.parts}}
  WHERE database = {database} AND table = {table} AND active = 1
  GROUP BY name`;

/** Get merge/mutation events for a batch of parts from system.part_log. */
export const GET_MERGE_EVENTS_BATCH = `
  SELECT
    event_time, part_name, merged_from, duration_ms, rows, size_in_bytes,
    bytes_uncompressed, read_bytes, peak_memory_usage, merge_reason, merge_algorithm,
    event_type
  FROM {{cluster_aware:system.part_log}}
  WHERE database = {database} AND table = {table}
    AND part_name IN ({partNames}) AND event_type IN ('MergeParts', 'MutatePart')
  ORDER BY event_time DESC`;

/** Get L0 (NewPart) sizes for merged-away parts from system.part_log. */
export const GET_L0_PART_SIZES = `
  SELECT part_name, rows, size_in_bytes
  FROM {{cluster_aware:system.part_log}}
  WHERE database = {database} AND table = {table}
    AND part_name IN ({partNames}) AND event_type = 'NewPart'
  ORDER BY event_time DESC`;
