import { escapeValue } from './builder.js';
import { APP_SOURCE_LIKE } from './source-tags.js';
import { buildXRayWindowSamplesSQL } from './xray-window-queries.js';
import { selectQueryXRaySource } from '../types/xray-source.js';
import type { ResourceAttributionRequest, ResourceAttributionGroupKey } from '../types/resource-attribution.js';

export interface ResourceAggregationSQLRequest extends ResourceAttributionRequest {
  startMs: number;
  endMs: number;
  snapshots: boolean;
  mergeSamplesAvailable: boolean;
  details?: { groups: ResourceAttributionGroupKey[]; limit: number };
}

const literal = (value: string) => `'${escapeValue(value)}'`;
const emptySpans = `SELECT '' AS kind, '' AS hostname, '' AS local_id, toInt64(0) AS span_start_ms,
  toInt64(0) AS span_end_ms, toFloat64(0) AS sample_value WHERE 0`;

/** One aggregation statement, with no top-N restriction on the execution population. */
export function buildResourceAttributionSQL(options: ResourceAggregationSQLRequest): string {
  const { startMs, endMs, mode, snapshots } = options;
  if (!Number.isSafeInteger(startMs) || !Number.isSafeInteger(endMs) || endMs <= startMs || endMs - startMs > 86400000) throw new Error('Invalid attribution window (maximum 24 hours)');
  if (snapshots && (startMs % 1000 || endMs % 1000 || endMs - startMs > 601000)) throw new Error('Instant snapshot windows must be aligned to seconds and at most ten minutes');
  if (options.normalizedQueryHash && !/^\d+$/.test(options.normalizedQueryHash)) throw new Error('Invalid normalized query hash');
  if (options.details && (!Number.isSafeInteger(options.details.limit) || options.details.limit < 1 || options.details.limit > 10000 || !options.details.groups.length)) throw new Error('Choose at most 10,000 operation details');
  const groupPredicate = options.details?.groups.map(group => {
    if (!['query','merge','mutation'].includes(group.kind)) throw new Error('Invalid operation kind');
    return `(kind=${literal(group.kind)} AND activity=${literal(group.activity)} AND table_state=${literal(group.tableState)} AND tables=[${group.tables.map(literal).join(',')}])`;
  }).join(' OR ');
  // Narrow identities, rather than referencing the operations CTE from the sample
  // scan: distributed CTE expansion would repeat the expensive log scans.
  const identityFilter = options.details?.groups.filter(group => group.kind === 'query').map(group =>
    `(upper(toString(query_kind))=${literal(group.activity)}${group.tableState === 'running' ? '' : ` AND arraySort(arrayDistinct(tables))=[${group.tables.map(literal).join(',')}]`})`).join(' OR ');
  const hosts = typeof options.hostname === 'string' ? [options.hostname] : options.hostname ?? [];
  const hostFilter = hosts.length ? `AND hostName() IN (${hosts.map(literal).join(',')})` : '';
  const hashFilter = options.queryHashOnly && options.normalizedQueryHash ? `AND normalized_query_hash = ${options.normalizedQueryHash}` : '';
  const partsEnabled = !(options.queryHashOnly && options.normalizedQueryHash);
  const sqlRate = mode === 'cpu' ? 'cpu_us' : mode === 'network' ? 'net_bytes' : 'disk_bytes';
  const queryOps = `SELECT 'query' AS kind, upper(toString(query_kind)) AS activity, arraySort(arrayDistinct(tables)) AS tables,
    if(empty(tables),'none','recorded') AS table_state, hostName() AS hostname, query_id AS local_id,
    toUnixTimestamp64Milli(toDateTime64(query_start_time_microseconds,3)) AS op_start_ms,
    op_start_ms + greatest(toInt64(query_duration_ms),1) AS op_end_ms, greatest(toFloat64(query_duration_ms),1) AS duration_ms,
    toFloat64(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us,
    toFloat64(ProfileEvents['NetworkSendBytes'] + ProfileEvents['NetworkReceiveBytes']) AS net_bytes,
    toFloat64(ProfileEvents['OSReadBytes'] + ProfileEvents['OSWriteBytes']) AS disk_bytes,
    toFloat64(memory_usage) AS peak_memory, toUInt32(exception_code) AS failure_code,
    substring(query,1,500) AS label, user, toUInt8(0) AS is_running,
    '' AS source_event_type,'' AS source_event_time_us
    FROM {{cluster_aware:system.query_log}}
    WHERE event_date >= toDate(fromUnixTimestamp64Milli(window_start_ms),'UTC') - 1
      AND type IN ('QueryFinish','ExceptionWhileProcessing') AND query NOT LIKE ${APP_SOURCE_LIKE}
      AND op_start_ms < window_end_ms AND op_end_ms >= window_start_ms ${hostFilter} ${hashFilter}`;
  const partOps = `SELECT if(event_type='MutatePart','mutation','merge') AS kind, if(kind='mutation','MUTATION','MERGE') AS activity,
    [database || '.' || table] AS tables, 'recorded' AS table_state, hostName() AS hostname, database || '.' || table || '/' || part_name AS local_id,
    toUnixTimestamp64Milli(toDateTime64(event_time_microseconds,3)) AS op_end_ms,
    op_end_ms - greatest(toInt64(duration_ms),1) AS op_start_ms, greatest(toFloat64(duration_ms),1) AS duration_ms,
    toFloat64(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us,
    toFloat64(ProfileEvents['NetworkSendBytes'] + ProfileEvents['NetworkReceiveBytes']) AS net_bytes,
    toFloat64(ProfileEvents['OSReadBytes'] + ProfileEvents['OSWriteBytes']) AS disk_bytes,
    toFloat64(peak_memory_usage) AS peak_memory, toUInt32(error) AS failure_code,
    part_name AS label, '' AS user, toUInt8(0) AS is_running,
    toString(event_type) AS source_event_type,concat(toString(event_time_microseconds,'UTC'),'Z') AS source_event_time_us
    FROM {{cluster_aware:system.part_log}}
    WHERE event_date >= toDate(fromUnixTimestamp64Milli(window_start_ms),'UTC') - 1
      AND event_type IN ('MergeParts','MutatePart','MovePart') AND op_start_ms < window_end_ms AND op_end_ms >= window_start_ms ${hostFilter}`;
  const columns = 'kind,activity,tables,table_state,hostname,local_id,op_start_ms,op_end_ms,duration_ms,cpu_us,net_bytes,disk_bytes,peak_memory,failure_code,label,user,is_running,source_event_type,source_event_time_us';
  const runningQueries = `SELECT 'query' AS kind, upper(toString(query_kind)) AS activity, CAST([],'Array(String)') AS tables,
    'running' AS table_state, hostName() AS hostname, query_id AS local_id,
    now_ms-toInt64(elapsed*1000) AS op_start_ms, now_ms AS op_end_ms, greatest(elapsed*1000,1) AS duration_ms,
    toFloat64(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds']) AS cpu_us,
    toFloat64(ProfileEvents['NetworkSendBytes']+ProfileEvents['NetworkReceiveBytes']) AS net_bytes,
    toFloat64(ProfileEvents['OSReadBytes']+ProfileEvents['OSWriteBytes']) AS disk_bytes, toFloat64(memory_usage) AS peak_memory,
    toUInt32(0) AS failure_code, substring(query,1,500) AS label, user, toUInt8(1) AS is_running,'' AS source_event_type,'' AS source_event_time_us
    FROM {{cluster_aware:system.processes}} WHERE query NOT LIKE ${APP_SOURCE_LIKE} AND op_start_ms<window_end_ms AND op_end_ms>=window_start_ms
    ${hostFilter}
    ${hashFilter ? `AND normalizedQueryHash(query) = ${options.normalizedQueryHash}` : ''}`;
  const runningParts = `SELECT if(is_mutation,'mutation','merge') AS kind, if(is_mutation,'MUTATION','MERGE') AS activity,
    [database || '.' || table] AS tables, 'recorded' AS table_state, hostName() AS hostname, database || '.' || table || '/' || result_part_name AS local_id,
    now_ms-toInt64(elapsed*1000) AS op_start_ms, now_ms AS op_end_ms, greatest(elapsed*1000,1) AS duration_ms,
    toFloat64(elapsed*500000) AS cpu_us, toFloat64(0) AS net_bytes,
    toFloat64(bytes_read_uncompressed+bytes_written_uncompressed) AS disk_bytes, toFloat64(memory_usage) AS peak_memory,
    toUInt32(0) AS failure_code, result_part_name AS label, '' AS user, toUInt8(1) AS is_running,
    if(is_mutation,'MutatePart','MergeParts') AS source_event_type,'' AS source_event_time_us
    FROM {{cluster_aware:system.merges}} WHERE op_start_ms<window_end_ms AND op_end_ms>=window_start_ms
    ${hostFilter}`;
  const source = options.sampleSelection ? selectQueryXRaySource(options.sampleSelection).source : null;
  // Progress read_bytes/written_bytes are logical input/output, not OS disk I/O.
  // Keep disk attribution on execution OSReadBytes/OSWriteBytes averages.
  const canSampleRates = source !== null && mode !== 'disk';
  const raw = source ? buildXRayWindowSamplesSQL(options.sampleSelection!, {
    start: 'fromUnixTimestamp64Milli(window_start_ms-2000)', end: 'fromUnixTimestamp64Milli(window_end_ms+2000)',
    hostname: options.hostname ?? undefined, localExecutions: true, localIdentityFilter: identityFilter,
  }) : '';
  const gaugeQuery = source ? `SELECT 'query' AS kind, hostname, sample_query_id AS local_id,
    intDiv(ts_ms,1000)*1000 AS ms, avg(toFloat64(memory_usage)) AS memory FROM query_samples GROUP BY kind,hostname,local_id,ms` : '';
  const gaugeParts = options.mergeSamplesAvailable && partsEnabled ? `SELECT if(is_mutation,'mutation','merge') AS kind,
    hostName() AS hostname,database || '.' || table || '/' || result_part_name AS local_id,intDiv(toUnixTimestamp64Milli(sample_time),1000)*1000 AS ms,
    avg(toFloat64(memory_usage)) AS memory FROM {{cluster_aware:tracehouse.merges_history}}
    WHERE sample_time>=fromUnixTimestamp64Milli(window_start_ms-4000) AND sample_time<=fromUnixTimestamp64Milli(window_end_ms+4000)
    ${hostFilter} GROUP BY kind,hostname,local_id,ms` : '';
  const memorySpans = [gaugeQuery, gaugeParts].filter(Boolean).length ? `SELECT kind,hostname,local_id,
    greatest(ms-2000, intDiv(ms+lagInFrame(ms,1,ms-4000) OVER w,2)) AS span_start_ms,
    least(ms+2000, intDiv(ms+leadInFrame(ms,1,ms+4000) OVER w,2)) AS span_end_ms,memory AS sample_value
    FROM (${[gaugeQuery,gaugeParts].filter(Boolean).join(' UNION ALL ')}) WINDOW w AS
      (PARTITION BY kind,hostname,local_id ORDER BY ms ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)` : emptySpans;
  const sampleMetric = mode === 'cpu' ? 'r.cpu_us' : mode === 'network' ? '(r.net_send_bytes+r.net_recv_bytes)' : '(r.read_bytes+r.written_bytes)';
  // A compressed bitmap retains captured buckets without reading query_samples twice.
  // Invalid counter baselines still count as captured gauges, matching memory membership.
  const overlapSeconds = 'greatest(0,least(r.ts_ms,r.ms+1000)-greatest(r.ts_ms-toInt64(r.dt*1000),r.ms))/1000';
  const rateSpans = canSampleRates ? `SELECT 'query' AS kind,r.hostname AS hostname,r.sample_query_id AS local_id,
    r.ms AS span_start_ms,r.ms+1000 AS span_end_ms,
    sum(${sampleMetric} * (${overlapSeconds})/r.dt)/sum(${overlapSeconds}) AS sample_value
    FROM (SELECT *,toInt64(arrayJoin(range(
      toUInt64(intDiv(greatest(ts_ms-toInt64(dt*1000),window_start_ms-2000),1000)),
      toUInt64(intDiv(ts_ms+999,1000))))*1000) AS ms
      FROM (SELECT *,groupBitmapState(toUInt64(intDiv(ts_ms,1000))) OVER (PARTITION BY hostname,sample_query_id) AS captured_buckets
        FROM query_samples) WHERE interval_valid AND dt>0) AS r
    WHERE bitmapContains(r.captured_buckets,toUInt64(intDiv(r.ms,1000)))
      AND r.ms<r.ts_ms AND r.ms+1000>r.ts_ms-toInt64(r.dt*1000)
    GROUP BY kind,r.hostname,r.sample_query_id,r.ms` : emptySpans;
  const selectedOperations = groupPredicate ? `(SELECT * FROM operations WHERE (${groupPredicate}))` : 'operations';
  const opTimes = snapshots ? `SELECT *,toInt64(arrayJoin(range(
      toUInt64(intDiv(greatest(op_start_ms,window_start_ms)+999,1000)),
      toUInt64(greatest(intDiv(greatest(op_start_ms,window_start_ms)+999,1000),intDiv(least(op_end_ms,window_end_ms-1),1000)+1))
    ))*1000) AS snapshot_ms, toFloat64(0) AS active_ms FROM ${selectedOperations}` : `SELECT *,toUInt64(0) AS snapshot_ms,
      toFloat64(greatest(0,least(op_end_ms,window_end_ms)-greatest(op_start_ms,window_start_ms))) AS active_ms FROM ${selectedOperations}
      WHERE active_ms>0`;
  const overlap = 'greatest(0,least(o.op_end_ms,window_end_ms,s.span_end_ms)-greatest(o.op_start_ms,window_start_ms,s.span_start_ms))';
  const evaluations = snapshots ? `SELECT o.*,if(s.span_start_ms<=o.snapshot_ms AND s.span_end_ms>o.snapshot_ms,1,0) AS sampled,
    toFloat64(0) AS sampled_ms,toFloat64(0) AS estimated_ms,toFloat64(0) AS missing_ms,
    ${mode === 'memory' ? 'if(sampled,toNullable(s.sample_value),NULL)' : `if(sampled,s.sample_value,o.${sqlRate}/o.duration_ms*1000)`} AS attributed_value,
    toUInt8(${mode === 'memory' ? '0' : 'NOT sampled'}) AS estimated
    FROM op_times AS o ASOF LEFT JOIN (SELECT * FROM sample_spans ORDER BY kind,hostname,local_id,span_start_ms) AS s
      ON o.kind=s.kind AND o.hostname=s.hostname AND o.local_id=s.local_id AND o.snapshot_ms>=s.span_start_ms` : `SELECT *,
      sampled_ms>0 AS sampled, ${mode === 'memory' ? 'toUInt8(0)' : 'active_ms>sampled_ms'} AS estimated,
      ${mode === 'memory' ? 'toFloat64(0)' : 'greatest(0,active_ms-sampled_ms)'} AS estimated_ms,
      ${mode === 'memory' ? 'greatest(0,active_ms-sampled_ms)' : 'toFloat64(0)'} AS missing_ms,
      ${mode === 'memory' ? 'if(sampled_ms>0,toNullable(sampled_value),NULL)' : `sampled_value+${sqlRate}/duration_ms*greatest(0,active_ms-sampled_ms)`} AS attributed_value
      FROM (SELECT o.*,sum(${overlap}) AS sampled_ms,sum(s.sample_value*(${overlap})/1000) AS sampled_value
        FROM op_times AS o LEFT JOIN sample_spans AS s ON o.kind=s.kind AND o.hostname=s.hostname AND o.local_id=s.local_id GROUP BY ALL)`;
  const projection = options.details ? `SELECT * FROM evaluated WHERE (${groupPredicate}) AND attributed_value IS NOT NULL
    ORDER BY attributed_value DESC,hostname,local_id,op_start_ms LIMIT ${options.details.limit}` : `SELECT snapshot_ms,kind,activity,tables,table_state,
    sum(ifNull(attributed_value,0)) AS value,sumIf(ifNull(attributed_value,0),failure_code!=0) AS failure_value,
    count() AS operation_count,countIf(failure_code!=0) AS failed_count,countIf(sampled) AS sampled_count,countIf(estimated) AS estimated_count,
    countIf(attributed_value IS NULL) AS unavailable_count,countIf(attributed_value=0) AS zero_count,
    sum(sampled_ms) AS sampled_ms,sum(estimated_ms) AS estimated_ms,sum(missing_ms) AS missing_ms
    FROM evaluated GROUP BY snapshot_ms,kind,activity,tables,table_state ORDER BY snapshot_ms,kind,activity,tables,table_state`;
  return `WITH toInt64(${startMs}) AS window_start_ms,toInt64(${endMs}) AS window_end_ms,toUnixTimestamp64Milli(now64(3)) AS now_ms,
    logged AS (SELECT ${columns} FROM (${queryOps})${partsEnabled ? ` UNION ALL SELECT ${columns} FROM (${partOps})` : ''}),
    operations AS (SELECT * FROM (SELECT * FROM logged${options.includeRunning ? ` UNION ALL SELECT ${columns} FROM (${runningQueries})${partsEnabled ? ` UNION ALL SELECT ${columns} FROM (${runningParts})` : ''}` : ''})
      ${options.includeRunning ? 'QUALIFY is_running=0 OR countIf(is_running=0) OVER (PARTITION BY kind,hostname,local_id)=0' : ''}),
    ${source ? `query_samples AS (${raw}),` : ''}
    sample_spans AS (${mode === 'memory' ? memorySpans : rateSpans}),
    op_times AS (${opTimes}),evaluated AS (${evaluations})
    ${projection}`;
}
