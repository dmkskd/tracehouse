/**
 * Lookups run when a deep link (qd_id / md_*) finds no finished record in
 * query_log / part_log. They tell "still running" and "finished, log row not
 * flushed yet" apart from "not found". See LinkResolver.
 */

/** Is the query still running? */
export const LINK_QUERY_RUNNING = `
  SELECT hostName() AS hostname, elapsed
  FROM {{cluster_aware:system.processes}}
  WHERE query_id = {query_id}
  LIMIT 1
`;

/**
 * Did the query start? A QueryStart row without a finish row means the finish
 * row has not been flushed yet (or the query never finished).
 */
export const LINK_QUERY_STARTED = `
  SELECT hostName() AS hostname, event_time
  FROM {{cluster_aware:system.query_log}}
  WHERE event_date >= {event_date_bound}
    AND query_id = {query_id}
    AND type = 'QueryStart'
  LIMIT 1
`;

/**
 * Is the merge / mutation producing this part still running? One row per
 * replica merging it; the caller filters by host.
 */
export const LINK_MERGE_RUNNING = `
  SELECT hostName() AS hostname, elapsed, progress
  FROM {{cluster_aware:system.merges}}
  WHERE database = {database}
    AND table = {table}
    AND result_part_name = {part_name}
`;

/**
 * Was the result part written recently? part_log has no start row, so a fresh
 * result part is the sign that the merge finished but part_log is not flushed.
 * One row per replica holding it; the caller filters by host.
 */
export const LINK_MERGE_RESULT_PART_RECENT = `
  SELECT hostName() AS hostname, modification_time
  FROM {{cluster_aware:system.parts}}
  WHERE database = {database}
    AND table = {table}
    AND name = {part_name}
    AND modification_time >= now() - toIntervalSecond({window_s})
`;
