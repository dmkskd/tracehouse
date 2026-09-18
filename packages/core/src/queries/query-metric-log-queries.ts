/**
 * SQL query builders for process samples sourced from `system.query_metric_log`
 * instead of `tracehouse.processes_history`.
 *
 * These are drop-in alternatives to the builders in process-queries.ts: they
 * emit the *identical* column list, so `mapProcessSampleRow` /
 * `mapHostProcessSampleRow` are reused unchanged and nothing above the mapper
 * needs to know which table produced the rows.
 *
 * `system.query_metric_log` (CH 24.10+) is the per-query counterpart of
 * `system.metric_log`: it samples memory and every ProfileEvent every
 * `query_metric_log_interval` ms (default 1000, settable per query, sub-second
 * capable) and writes a final row when the query finishes.
 *
 * CRITICAL difference from `system.processes`: its `ProfileEvent_*` columns hold
 * the DELTA for each interval, not a running total — the same convention as
 * `system.metric_log`. Verified against a real server: summing one query's rows
 * reproduces its `system.query_log` ProfileEvents total exactly. So rates come
 * straight from the stored value, and the cumulative columns the X-Ray contract
 * expects are rebuilt with a running `sum() OVER w`. `memory_usage` and
 * `peak_memory_usage` are gauges and are read directly.
 *
 * Three things it does NOT carry, and how they are handled here:
 *
 * 1. `initial_query_id` — recovered by joining `system.query_log`, which has it.
 *    Without the join, distributed sub-queries cannot be rolled up into their
 *    coordinator.
 * 2. `peak_threads_usage` / `thread_ids` — `peak_threads_usage` is recovered
 *    from the same `query_log` join as a per-query scalar, which restores a real
 *    ceiling for the rate clamp (see process-queries.ts for why the clamp is
 *    needed). It is coarser than the sampler's per-interval bound. Thread
 *    *count* is not recoverable at all and is reported as 0; consumers must use
 *    `XRaySourceMeta.missing` rather than rendering it.
 * 3. Progress counters — `system.processes.read_rows` / `read_bytes` are
 *    progress fields with no equivalent here. `ProfileEvent_SelectedRows` /
 *    `ProfileEvent_SelectedBytes` are the closest counters but mean something
 *    different (rows/bytes selected from parts), so the two sources will not
 *    agree numerically on these fields. Also declared in `missing`.
 *
 * `query_log` and `query_metric_log` are buffered and flushed on
 * `flush_interval_milliseconds` (7500 in the stock config), so these builders
 * trail reality by up to one flush. They still cover a query that is running:
 * rows are written throughout execution, and `log_queries_min_type` defaults to
 * QUERY_START so the identity join finds it before it finishes. What is missing
 * mid-flight is `peak_threads_usage`, which lands on the QueryFinish row, so
 * rates run unclamped until then (reported as `rate_unclamped`).
 */

import { escapeValue, formatUtcDateTime } from './builder.js';

/**
 * Columns read from system.query_metric_log, aliased to the names the shared
 * delta/rate machinery below expects. Kept in one place so the capability probe
 * and the SQL cannot drift apart.
 */
export const QUERY_METRIC_LOG_PROFILE_EVENT_COLUMNS = [
  'ProfileEvent_OSCPUVirtualTimeMicroseconds',
  'ProfileEvent_OSIOWaitMicroseconds',
  'ProfileEvent_OSCPUWaitMicroseconds',
  'ProfileEvent_NetworkReceiveElapsedMicroseconds',
  'ProfileEvent_NetworkSendElapsedMicroseconds',
  'ProfileEvent_NetworkSendBytes',
  'ProfileEvent_NetworkReceiveBytes',
  'ProfileEvent_SelectedRows',
  'ProfileEvent_SelectedBytes',
  'ProfileEvent_InsertedRows',
] as const;

/**
 * Fields that `system.query_metric_log` cannot populate faithfully.
 * Consumers surface this through XRaySourceMeta.missing so the UI hides the
 * affected series instead of drawing a plausible-looking zero.
 */
export const QUERY_METRIC_LOG_MISSING_FIELDS = [
  'thread_count',
  'read_rows',
  'read_bytes',
  'written_rows',
  'd_read_rows',
  'd_read_mb',
  'd_written_rows',
] as const;

/**
 * How far back to scan system.query_log for the identity join.
 * query_log is partitioned by event_date, so a date bound is what makes this
 * cheap. Two days covers "yesterday's slow query" without a full scan.
 */
const QUERY_LOG_LOOKBACK_DAYS = 2;

/**
 * Sentinel thread bound used when query_log did not supply peak_threads_usage.
 * Large enough to never clamp a real rate, and recognisable in the output so the
 * result can report that no ceiling was applied instead of the UI guessing.
 */
const NO_THREAD_BOUND = '1000000000.';

/**
 * Normalize a start-time hint to a ClickHouse datetime, or undefined when it is
 * missing or unusable. Checked explicitly rather than caught, so a genuinely
 * invalid value is a deliberate fallback and not a swallowed error.
 */
function normalizeStartedAt(startedAt?: string): string | undefined {
  if (!startedAt) return undefined;
  const parsed = new Date(
    /(?:[zZ]|[+-]\d{2}:?\d{2})$/.test(startedAt.trim())
      ? startedAt.trim().replace(' ', 'T')
      : `${startedAt.trim().replace(' ', 'T')}Z`,
  );
  if (!Number.isFinite(parsed.getTime())) return undefined;
  return formatUtcDateTime(parsed);
}

/**
 * Identity CTE: resolve every query_id belonging to this query (the coordinator
 * plus every distributed sub-query) and carry across what query_metric_log
 * itself does not have.
 *
 * A query produces QueryStart and QueryFinish rows. peak_threads_usage is only
 * meaningful on the finish row (hence argMax over event_time), while the
 * earliest row marks when the query actually started — needed for `elapsed`,
 * which must measure from query start, not from the first sample.
 *
 * `startedAt`, when known, bounds the event_date scan to the day around the
 * query instead of a blind lookback. Without it an older query silently returns
 * no rows once it falls outside QUERY_LOG_LOOKBACK_DAYS.
 */
function identityCTE(escapedId: string, startedAt?: string): string {
  // Callers pass whatever the query row carried, typically browser ISO-8601
  // ("2026-09-17T20:51:36.000Z"), which toDate() cannot parse. Normalize to a
  // ClickHouse datetime first. An unparsable value degrades to the lookback
  // rather than breaking the X-Ray.
  const normalizedStart = normalizeStartedAt(startedAt);
  const dateBound = normalizedStart
    ? `event_date BETWEEN toDate('${escapeValue(normalizedStart)}') - 1 AND toDate('${escapeValue(normalizedStart)}') + 1`
    : `event_date >= toDate(now() - INTERVAL ${QUERY_LOG_LOOKBACK_DAYS} DAY)`;
  return `
    SELECT
        query_id,
        -- max(), not argMax(..., event_time): event_time is second-resolution,
        -- so a query that starts and finishes inside the same second has two
        -- rows with identical timestamps and argMax picks one arbitrarily. When
        -- it lands on QueryStart (peak_threads_usage = 0) the clamp silently
        -- turns off. Measured on a live server: 32k queries in one day tie this
        -- way. Only the finish row carries a non-zero value, so max is both
        -- deterministic and correct.
        toFloat64(max(peak_threads_usage)) AS peak_threads,
        min(event_time_microseconds) AS query_start
    FROM {{cluster_aware:system.query_log}}
    WHERE ${dateBound}
      AND (query_id = ${escapedId} OR initial_query_id = ${escapedId})
    GROUP BY query_id`;
}

/**
 * Inner projection shared by both builders: one row per sample, with the raw
 * deltas and the thread bound already computed.
 *
 * `partitionKeys` decides how series are separated (by host, or by query for the
 * aggregate view); `minTimePartition` decides what t=0 means.
 */
function samplesProjection(
  escapedId: string,
  partitionKeys: string,
  minTimePartition: string,
  extraSelect: string,
  startedAt?: string,
): string {
  return `
    SELECT
        ${extraSelect}
        toFloat64(dateDiff('millisecond', min_time, sample_time)) / 1000 AS t,
        -- Seconds since the query started, NOT since the first sample. Mirrors
        -- system.processes.elapsed, which XRayTab uses (samples[0].elapsed) to
        -- align trace_log windows and the flamegraph to absolute time. Measuring
        -- from the first sample would zero that offset and shift the alignment.
        -- greatest(..., 0): the query_log row and the sample can come from
        -- different nodes, so clock skew could otherwise make elapsed negative.
        greatest(if(query_start > toDateTime64(0, 6),
           toFloat64(dateDiff('millisecond', query_start, sample_time)) / 1000,
           toFloat64(dateDiff('millisecond', min_time, sample_time)) / 1000), 0) AS elapsed,
        -- Thread count is not exposed per sample by query_metric_log. Reported
        -- as 0 and declared in QUERY_METRIC_LOG_MISSING_FIELDS.
        0 AS thread_count,
        memory_usage / (1024 * 1024) AS memory_mb,
        peak_memory_usage / (1024 * 1024) AS peak_memory_mb,
        -- ProfileEvent_* in query_metric_log are PER-INTERVAL DELTAS, not
        -- cumulative counters (verified: summing a query's rows reproduces its
        -- system.query_log total exactly). The X-Ray contract wants running
        -- totals here, so accumulate rather than read directly.
        sum(pe_selected_rows) OVER w AS read_rows,
        sum(pe_inserted_rows) OVER w AS written_rows,
        sum(pe_selected_bytes) OVER w AS read_bytes,
        sum(pe_cpu) OVER w AS cpu_us,
        sum(pe_io_wait) OVER w AS io_wait_us,
        sum(pe_cpu_wait) OVER w AS cpu_wait_us,
        sum(pe_net_recv_wait) OVER w AS net_recv_wait_us,
        sum(pe_net_send_wait) OVER w AS net_send_wait_us,
        sum(pe_net_send) OVER w AS net_send_bytes,
        sum(pe_net_recv) OVER w AS net_recv_bytes,
        -- Seconds this row's delta covers. For the FIRST sample that is the
        -- time since the query started, not zero: unlike a cumulative counter,
        -- a delta column carries real work on its first row, so falling back to
        -- the dt floor would divide a full interval's work by 10ms and
        -- manufacture a spike big enough to hit the thread ceiling.
        -- lagInFrame's default is evaluated per row, so query_start fills in
        -- exactly where there is no previous sample.
        toFloat64(dateDiff('millisecond',
            lagInFrame(sample_time, 1,
                if(query_start > toDateTime64(0, 6), query_start, sample_time)) OVER w,
            sample_time
        )) / 1000 AS raw_gap,
        -- dt floors the gap so a rate stays finite. raw_gap above is kept
        -- unfloored because the roll-up bucket must be narrower than the
        -- SMALLEST real gap, which the closing row can undercut.
        greatest(raw_gap, 0.01) AS dt,
        -- Rate ceiling. peak_threads_usage comes from query_log as a per-query
        -- constant, so unlike the sampler this cannot tighten per interval.
        -- When query_log did not supply it (no finish row yet, or the column is
        -- stripped by the provider) the bound is effectively disabled rather
        -- than clamping legitimate rates to zero.
        if(peak_threads > 0, peak_threads, ${NO_THREAD_BOUND}) AS thread_bound,
        -- The interval delta is the stored value itself. Differencing it the way
        -- process-queries.ts must (its counters ARE cumulative) would subtract
        -- two independent deltas and yield noise around zero.
        pe_cpu / 1000000 AS raw_d_cpu,
        pe_io_wait / 1000000 AS raw_d_io,
        pe_cpu_wait / 1000000 AS raw_d_cpu_wait,
        pe_net_recv_wait / 1000000 AS raw_d_net_recv_wait,
        pe_net_send_wait / 1000000 AS raw_d_net_send_wait,
        pe_selected_bytes / (1024 * 1024) AS raw_d_read_mb,
        toFloat64(pe_selected_rows) AS raw_d_read_rows,
        toFloat64(pe_inserted_rows) AS raw_d_written_rows,
        pe_net_send / 1024 AS raw_d_net_send,
        pe_net_recv / 1024 AS raw_d_net_recv
    FROM (
        SELECT
            qml.hostname AS hostname,
            qml.query_id AS query_id,
            qml.event_time_microseconds AS sample_time,
            min(qml.event_time_microseconds) OVER (${minTimePartition}) AS min_time,
            ids.peak_threads AS peak_threads,
            ids.query_start AS query_start,
            qml.memory_usage AS memory_usage,
            qml.peak_memory_usage AS peak_memory_usage,
            -- Not the progress counters system.processes exposes. See the
            -- module header: different semantics, declared in missing fields.
            qml.ProfileEvent_SelectedRows AS pe_selected_rows,
            qml.ProfileEvent_SelectedBytes AS pe_selected_bytes,
            qml.ProfileEvent_InsertedRows AS pe_inserted_rows,
            qml.ProfileEvent_OSCPUVirtualTimeMicroseconds AS pe_cpu,
            qml.ProfileEvent_OSIOWaitMicroseconds AS pe_io_wait,
            qml.ProfileEvent_OSCPUWaitMicroseconds AS pe_cpu_wait,
            qml.ProfileEvent_NetworkReceiveElapsedMicroseconds AS pe_net_recv_wait,
            qml.ProfileEvent_NetworkSendElapsedMicroseconds AS pe_net_send_wait,
            qml.ProfileEvent_NetworkSendBytes AS pe_net_send,
            qml.ProfileEvent_NetworkReceiveBytes AS pe_net_recv
        FROM {{cluster_aware:system.query_metric_log}} AS qml
        INNER JOIN (${identityCTE(escapedId, startedAt)}
        ) AS ids ON qml.query_id = ids.query_id
        ORDER BY hostname, query_id, sample_time
    )
    WINDOW w AS (PARTITION BY ${partitionKeys} ORDER BY sample_time)`;
}

/**
 * Pick one `startedAt` hint that is valid for a whole set of queries.
 *
 * The scan bound covers the day around a single start time, so it can only
 * serve a set whose queries all fall inside that window. When they are spread
 * wider, no single bound is safe and the builder must fall back to its lookback
 * rather than silently dropping the queries outside the window.
 */
export function commonScanStart(startTimes: readonly (string | undefined)[]): string | undefined {
  const times = startTimes
    .filter((t): t is string => !!t)
    .map(t => ({ raw: t, ms: new Date(t.replace(' ', 'T')).getTime() }))
    .filter(t => Number.isFinite(t.ms));
  if (times.length === 0 || times.length !== startTimes.length) return undefined;

  let earliest = times[0];
  let spanMs = 0;
  for (const t of times) {
    if (t.ms < earliest.ms) earliest = t;
  }
  for (const t of times) {
    spanMs = Math.max(spanMs, t.ms - earliest.ms);
  }
  const ONE_DAY_MS = 24 * 60 * 60 * 1000;
  return spanMs < ONE_DAY_MS ? earliest.raw : undefined;
}

export interface QueryMetricLogSampleOptions {
  /**
   * When the query started, as a ClickHouse-parsable datetime. Bounds the
   * system.query_log scan to the day around it. Without it the scan falls back
   * to a fixed lookback and a query older than that returns nothing.
   */
  startedAt?: string;
}

/**
 * Substitute the projection into an aggregation template.
 *
 * A plain String.replace() would interpret `$&`, `$'` and friends in the
 * REPLACEMENT as patterns, and the projection embeds the query id — which
 * clients can set to an arbitrary string. The function form takes the
 * replacement literally.
 */
function injectProjection(template: string, projection: string): string {
  return template.replace('{{PROJECTION}}', () => projection);
}

/**
 * Aggregation shared by both builders. Identical to the processes_history
 * output contract, column for column.
 */
function aggregation(groupBy: string, leadingSelect: string, orderBy: string): string {
  return `
SELECT
    ${leadingSelect}
    max(elapsed) AS elapsed,
    sum(thread_count) AS thread_count,
    sum(memory_mb) AS memory_mb,
    max(peak_memory_mb) AS peak_memory_mb,
    sum(read_rows) AS read_rows,
    sum(written_rows) AS written_rows,
    sum(read_bytes) AS read_bytes,
    sum(cpu_us) AS cpu_us,
    sum(io_wait_us) AS io_wait_us,
    sum(cpu_wait_us) AS cpu_wait_us,
    sum(net_recv_wait_us) AS net_recv_wait_us,
    sum(net_send_wait_us) AS net_send_wait_us,
    sum(net_send_bytes) AS net_send_bytes,
    sum(net_recv_bytes) AS net_recv_bytes,
    sum(least(greatest(raw_d_cpu / dt, 0), thread_bound)) AS d_cpu_cores,
    sum(least(greatest(raw_d_io / dt, 0), thread_bound)) AS d_io_wait_s,
    sum(least(greatest(raw_d_cpu_wait / dt, 0), thread_bound)) AS d_cpu_wait_s,
    sum(least(greatest(raw_d_net_recv_wait / dt, 0), thread_bound)) AS d_net_recv_wait_s,
    sum(least(greatest(raw_d_net_send_wait / dt, 0), thread_bound)) AS d_net_send_wait_s,
    max(greatest(raw_d_cpu, raw_d_io, raw_d_cpu_wait, raw_d_net_recv_wait, raw_d_net_send_wait) / dt
        > thread_bound) AS rate_clamped,
    -- Whether this interval had no ceiling at all, i.e. query_log gave us no
    -- peak_threads_usage. Reported rather than inferred: the UI cannot know
    -- from query state alone whether the join actually found a bound.
    max(thread_bound >= ${NO_THREAD_BOUND}) AS rate_unclamped,
    sum(greatest(raw_d_read_mb / dt, 0)) AS d_read_mb,
    sum(greatest(raw_d_read_rows / dt, 0)) AS d_read_rows,
    sum(greatest(raw_d_written_rows / dt, 0)) AS d_written_rows,
    sum(greatest(raw_d_net_send / dt, 0)) AS d_net_send_kb,
    sum(greatest(raw_d_net_recv / dt, 0)) AS d_net_recv_kb
FROM (
    SELECT *, round(t / bucket_width) * bucket_width AS t_bucket
    FROM (
        -- Roll-up alignment. Unlike the 1s sampler, every query here samples on
        -- its own schedule, so a coordinator and its shard sub-queries never
        -- share a timestamp and a plain GROUP BY t would interleave their rows
        -- instead of summing them.
        --
        -- The width tracks the actual sampling interval and never exceeds it.
        -- A fixed 0.5s would be wider than the interval whenever
        -- query_metric_log_interval is set sub-second, collapsing several
        -- samples OF THE SAME query into one bucket — which this SELECT then
        -- sums, multiplying memory and every rate by the number of samples
        -- swallowed. Capped at 0.5s to match aggregateHostSamples() in the
        -- frontend; floored just above zero so identical timestamps cannot
        -- divide by zero. Uses raw_gap, not dt: dt is floored, so a closing row
        -- arriving inside that floor would make the bucket wider than the real
        -- gap and merge the pair it was meant to keep apart.
        SELECT *, greatest(least(0.5, min(raw_gap) OVER ()), 0.001) AS bucket_width
        FROM (${'{{PROJECTION}}'}
        )
    )
)
GROUP BY ${groupBy}
ORDER BY ${orderBy}
`;
}

/**
 * query_metric_log equivalent of buildProcessSamplesSQL.
 *
 * Single-query mode returns ProcessSample rows; multi-query mode returns
 * TaggedProcessSample rows carrying query_id, exactly like the sampler builder.
 */
export function buildQueryMetricLogSamplesSQL(
  queryIds: string[],
  opts: QueryMetricLogSampleOptions = {},
): string {
  if (queryIds.length === 0) {
    throw new Error('buildQueryMetricLogSamplesSQL requires at least one query id');
  }
  if (queryIds.length > 1) {
    // One subquery per id, unioned: each id needs its own identity join and its
    // own t=0, and query_metric_log has no initial_query_id to partition on.
    const arms = queryIds
      .map(id => {
        const escaped = `'${escapeValue(id)}'`;
        const projection = samplesProjection(escaped, 'query_id', '', `${escaped} AS query_id_tag,`, opts.startedAt);
        return injectProjection(
          aggregation('query_id_tag, t_bucket', 'query_id_tag AS query_id, t_bucket AS t,', 'query_id, t'),
          projection,
        ).trim();
      })
      .join('\nUNION ALL\n');
    // An ORDER BY inside a UNION ALL arm orders only that arm; the combined
    // result has no guaranteed order. Consumers build per-query series from
    // these rows, so sort the union as a whole.
    return `
SELECT *
FROM (
${arms}
)
ORDER BY query_id, t
`;
  }
  const escaped = `'${escapeValue(queryIds[0])}'`;
  const projection = samplesProjection(escaped, 'query_id', '', '', opts.startedAt);
  return injectProjection(aggregation('t_bucket', 't_bucket AS t,', 't'), projection);
}

/**
 * query_metric_log equivalent of buildHostProcessSamplesSQL: one time series
 * per host, each host's deltas computed independently.
 */
export function buildHostQueryMetricLogSamplesSQL(
  queryId: string,
  opts: QueryMetricLogSampleOptions = {},
): string {
  const escaped = `'${escapeValue(queryId)}'`;
  const projection = samplesProjection(
    escaped,
    'hostname, query_id',
    'PARTITION BY hostname',
    'hostname,',
    opts.startedAt,
  );
  return injectProjection(aggregation('hostname, t', 'hostname, t,', 'hostname, t'), projection);
}
