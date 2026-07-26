import type {
  EventContextMetricPoint,
  OperationalEvent,
} from '@tracehouse/core';

export interface NearbyEvent {
  event: OperationalEvent;
  distanceMs: number;
  relation: string;
}

export function selectNearbyEvents(
  selected: OperationalEvent,
  events: readonly OperationalEvent[],
  windowSeconds: number,
  limit = 12,
): NearbyEvent[] {
  const selectedMs = Date.parse(selected.occurred_at);
  const maxDistanceMs = windowSeconds * 1000;

  return events
    .filter(event => event.id !== selected.id)
    .map(event => ({
      event,
      distanceMs: Date.parse(event.occurred_at) - selectedMs,
      relation: eventContextRelation(selected, event),
    }))
    .filter(item => (
      Number.isFinite(item.distanceMs)
      && Math.abs(item.distanceMs) <= maxDistanceMs
    ))
    .sort((a, b) => {
      const relationDifference = relationRank(a.relation) - relationRank(b.relation);
      return relationDifference || Math.abs(a.distanceMs) - Math.abs(b.distanceMs);
    })
    .slice(0, limit);
}

export function eventContextRelation(
  selected: OperationalEvent,
  candidate: OperationalEvent,
): string {
  if (
    selected.query_id
    && (
      candidate.query_id === selected.query_id
      || candidate.initial_query_id === selected.query_id
      || candidate.query_id === selected.initial_query_id
    )
  ) return 'same query';
  if (selected.hostname && candidate.hostname === selected.hostname) return 'same host';
  if (
    selected.table
    && (
      candidate.table === selected.table
      || candidate.tables?.includes(selected.table)
    )
  ) return 'same table';
  if (candidate.category === selected.category) return 'same category';
  return 'same window';
}

function relationRank(relation: string): number {
  switch (relation) {
    case 'same query': return 0;
    case 'same host': return 1;
    case 'same table': return 2;
    case 'same category': return 3;
    default: return 4;
  }
}

export function formatContextOffset(distanceMs: number): string {
  if (Math.abs(distanceMs) < 1000) return 'at event time';
  const seconds = Math.round(Math.abs(distanceMs) / 1000);
  const amount = seconds >= 60
    ? `${Math.floor(seconds / 60)}m ${seconds % 60}s`
    : `${seconds}s`;
  return distanceMs < 0 ? `${amount} before` : `${amount} after`;
}

export function metricSeriesForHost(
  points: readonly EventContextMetricPoint[],
  hostname?: string,
): EventContextMetricPoint[] {
  return points
    .filter(point => point.hostname === hostname)
    .sort((a, b) => Date.parse(a.time) - Date.parse(b.time));
}

export function sparklinePoints(
  values: readonly number[],
  width: number,
  height: number,
): string {
  if (values.length === 0) return '';
  const minimum = Math.min(...values);
  const maximum = Math.max(...values);
  const spread = Math.max(1, maximum - minimum);
  return values.map((value, index) => {
    const x = values.length === 1 ? width / 2 : index / (values.length - 1) * width;
    const y = height - ((value - minimum) / spread * (height - 4) + 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
}

export interface MetricChartAxis {
  minimum: number;
  midpoint: number;
  maximum: number;
}

export interface MetricChartGeometry {
  memoryPoints: string;
  cpuPoints: string;
  memoryAxis: MetricChartAxis;
  cpuAxis: MetricChartAxis;
  eventX: number;
  pointXs: number[];
}

export function buildMetricChartGeometry(
  points: readonly EventContextMetricPoint[],
  eventTime: string,
  width: number,
  height: number,
): MetricChartGeometry {
  const timestamps = points.map(point => Date.parse(point.time));
  const validTimestamps = timestamps.filter(Number.isFinite);
  const firstTime = validTimestamps[0] ?? 0;
  const lastTime = validTimestamps[validTimestamps.length - 1] ?? firstTime;
  const timeSpan = Math.max(1, lastTime - firstTime);
  const memoryAxis = paddedAxis(points.map(point => point.memory_usage), false);
  const cpuAxis = paddedAxis(points.map(point => point.cpu_cores), true);
  const pointXs = timestamps.map(time => (
    clamp((time - firstTime) / timeSpan * width, 0, width)
  ));

  return {
    memoryPoints: seriesPoints(
      points.map(point => point.memory_usage),
      pointXs,
      memoryAxis,
      height,
    ),
    cpuPoints: seriesPoints(
      points.map(point => point.cpu_cores),
      pointXs,
      cpuAxis,
      height,
    ),
    memoryAxis,
    cpuAxis,
    eventX: clamp(
      (Date.parse(eventTime) - firstTime) / timeSpan * width,
      0,
      width,
    ),
    pointXs,
  };
}

export function closestMetricPointIndex(
  points: readonly EventContextMetricPoint[],
  targetX: number,
  width: number,
): number | null {
  if (points.length === 0) return null;
  const firstTime = Date.parse(points[0].time);
  const lastTime = Date.parse(points[points.length - 1].time);
  const targetTime = firstTime
    + clamp(targetX / Math.max(1, width), 0, 1) * Math.max(1, lastTime - firstTime);
  let closestIndex = 0;
  let closestDistance = Number.POSITIVE_INFINITY;
  points.forEach((point, index) => {
    const distance = Math.abs(Date.parse(point.time) - targetTime);
    if (distance < closestDistance) {
      closestIndex = index;
      closestDistance = distance;
    }
  });
  return closestIndex;
}

function paddedAxis(values: readonly number[], zeroFloor: boolean): MetricChartAxis {
  const finiteValues = values.filter(Number.isFinite);
  if (finiteValues.length === 0) {
    return { minimum: 0, midpoint: 0.5, maximum: 1 };
  }
  const rawMinimum = Math.min(...finiteValues);
  const rawMaximum = Math.max(...finiteValues);
  let minimum: number;
  let maximum: number;
  if (zeroFloor) {
    minimum = 0;
    maximum = rawMaximum > 0 ? rawMaximum * 1.1 : 1;
  } else {
    const spread = rawMaximum - rawMinimum;
    const padding = spread > 0
      ? spread * 0.08
      : Math.max(Math.abs(rawMaximum) * 0.02, 1);
    minimum = Math.max(0, rawMinimum - padding);
    maximum = rawMaximum + padding;
  }
  if (maximum <= minimum) maximum = minimum + 1;
  return {
    minimum,
    midpoint: minimum + (maximum - minimum) / 2,
    maximum,
  };
}

function seriesPoints(
  values: readonly number[],
  pointXs: readonly number[],
  axis: MetricChartAxis,
  height: number,
): string {
  const spread = Math.max(Number.EPSILON, axis.maximum - axis.minimum);
  return values.map((value, index) => {
    const y = height - clamp((value - axis.minimum) / spread, 0, 1) * height;
    return `${(pointXs[index] ?? 0).toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
}

function clamp(value: number, minimum: number, maximum: number): number {
  return Math.min(maximum, Math.max(minimum, value));
}
