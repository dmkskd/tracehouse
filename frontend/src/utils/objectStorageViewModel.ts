import type { ObjectStorageEvent, ObjectStorageProfileSummary } from '@tracehouse/core';
import { formatBytes } from '../stores/databaseStore';
import { formatMicroseconds } from './formatters';

export type ProfileEventDescriptions = Record<string, string>;

export interface ObjectStorageMetric {
  label: string;
  value: string;
  detail?: string;
}

export interface ObjectStorageCostLine {
  label: string;
  countOrBytes: string;
  cost: string;
  detail: string;
}

export interface ObjectStorageCostView {
  total: string;
  pricingProfile: string;
  pricingTooltip: string;
  requestBarWidth: string;
  transferBarWidth: string;
  requestLegend: string;
  transferLegend: string;
  note: string;
  lines: ObjectStorageCostLine[];
}

export interface ObjectStorageKpi {
  label: string;
  value: string;
  formula: string;
  status: 'ok' | 'watch' | 'high' | 'neutral';
  hint: string;
}

export interface ObjectStorageTimeSegment {
  label: string;
  detail?: string;
  value: string;
  microseconds: number;
  widthPercent: number;
  color: string;
}

export interface ObjectStorageTimeView {
  title: string;
  subtitle: string;
  total: ObjectStorageMetric;
  totalColor: string;
  scaleMicroseconds: number;
  segments: ObjectStorageTimeSegment[];
}

export interface ObjectStorageEventGroup {
  title: string;
  events: ObjectStorageMetric[];
}

export interface ObjectStorageViewModel {
  title: string;
  subtitle: string;
  cost: ObjectStorageCostView;
  efficiency: ObjectStorageKpi[];
  time: ObjectStorageTimeView[];
  reliability: ObjectStorageMetric[];
  context: ObjectStorageMetric[];
  rawEventGroups: ObjectStorageEventGroup[];
}

function detectorLabel(detector: ObjectStorageProfileSummary['detector']): string {
  if (detector === 's3_compatible') return 'S3-compatible';
  return 'Object storage';
}

function profileEventDescription(descriptions: ProfileEventDescriptions, eventName: string): string | undefined {
  return descriptions[eventName];
}

function profileEventDetail(descriptions: ProfileEventDescriptions, eventName: string): string | undefined {
  const official = profileEventDescription(descriptions, eventName);
  return official;
}

function fmtCount(value: number): string {
  return value.toLocaleString();
}

function fmtMoney(value: number): string {
  return value === 0 ? '$0.00' : `$${value.toFixed(value < 0.01 ? 4 : 2)}`;
}

function fmtRate(value: number): string {
  return `$${value.toFixed(4)}`;
}

function pricingTooltipLines(profile: ObjectStorageProfileSummary['estimatedCost']['pricingProfile']): string[] {
  const lines = [
    'Rates used for this estimate:',
    `GET: ${fmtRate(profile.requestPricing.getPer1000)} / 1,000`,
    `HEAD: ${fmtRate(profile.requestPricing.headPer1000)} / 1,000`,
    `PUT: ${fmtRate(profile.requestPricing.putPer1000)} / 1,000`,
    `POST: ${fmtRate(profile.requestPricing.postPer1000)} / 1,000`,
    `LIST: ${fmtRate(profile.requestPricing.listPer1000)} / 1,000`,
    `Egress used: ${fmtRate(profile.transferPricing.egressPerGb)} / GB`,
  ];

  if (profile.transferPricing.sameRegionPerGb !== profile.transferPricing.egressPerGb) {
    lines.push(`Same-region: ${fmtRate(profile.transferPricing.sameRegionPerGb)} / GB`);
  }
  if (profile.transferPricing.crossRegionPerGb !== undefined) {
    lines.push(`Cross-region: ${fmtRate(profile.transferPricing.crossRegionPerGb)} / GB`);
  }
  if (profile.transferPricing.internetPerGb !== undefined) {
    lines.push(`Internet egress reference: ${fmtRate(profile.transferPricing.internetPerGb)} / GB`);
  }

  lines.push(`Currency: ${profile.currency}`);
  return lines;
}

function fmtPct(value: number | null): string {
  return value == null ? '—' : `${(value * 100).toFixed(1)}%`;
}

function fmtBytes(value: number | null): string {
  return value == null ? '—' : formatBytes(value);
}

function fmtThroughput(value: number | null): string {
  if (value == null) return '—';
  return `${formatBytes(value)}/s`;
}

function fmtKpiDurationUs(value: number | null): string {
  if (value == null) return '—';
  if (value < 1000) return `${Number(value.toFixed(1))}µs`;
  if (value < 1_000_000) return `${Number((value / 1000).toFixed(1))}ms`;
  return formatMicroseconds(value);
}

function eventValue(name: string, value: number): string {
  if (name.includes('Bytes') || name.includes('Weight')) return formatBytes(value);
  if (name.includes('Microseconds')) return formatMicroseconds(value);
  return value.toLocaleString();
}

function groupEvents(events: ObjectStorageEvent[]): ObjectStorageEventGroup[] {
  const grouped = events.reduce<Record<string, ObjectStorageMetric[]>>((acc, event) => {
    const key = event.group;
    if (!acc[key]) acc[key] = [];
    acc[key].push({ label: event.name, value: eventValue(event.name, event.value) });
    return acc;
  }, {});
  const labels: Record<string, string> = {
    read_buffer: 'Read buffer',
    disk_s3: 'Disk S3',
    s3: 'S3 API',
    iceberg: 'Iceberg',
    other: 'Other',
  };
  return Object.entries(grouped).map(([group, eventsForGroup]) => ({
    title: labels[group] ?? group,
    events: eventsForGroup,
  }));
}

function buildCostView(summary: ObjectStorageProfileSummary): ObjectStorageCostView {
  const hasCost = summary.estimatedCost.totalCostUsd > 0;
  const requestPercent = hasCost ? Math.round(summary.estimatedCost.requestShare * 100) : 100;
  const transferPercent = hasCost ? Math.round(summary.estimatedCost.transferShare * 100) : 0;
  const profile = summary.estimatedCost.pricingProfile;
  const egressRate = fmtRate(profile.transferPricing.egressPerGb);
  return {
    total: fmtMoney(summary.estimatedCost.totalCostUsd),
    pricingProfile: profile.label,
    pricingTooltip: pricingTooltipLines(profile).join('\n'),
    requestBarWidth: hasCost ? `${requestPercent}%` : '100%',
    transferBarWidth: hasCost ? `${transferPercent}%` : '0%',
    requestLegend: `API requests ${requestPercent}%`,
    transferLegend: `Egress ${transferPercent}%`,
    note: 'Estimate only. Real cost depends on provider, region, network path, and discounts; self-hosted object storage may cost $0 here.',
    lines: [
      { label: 'S3GetObject', countOrBytes: fmtCount(summary.getRequests), cost: fmtMoney(summary.estimatedCost.getCostUsd), detail: 'GET requests × request rate' },
      { label: 'S3HeadObject', countOrBytes: fmtCount(summary.headRequests), cost: fmtMoney(summary.estimatedCost.headCostUsd), detail: 'HEAD requests × request rate' },
      ...(summary.putRequests > 0 ? [{ label: 'S3PutObject', countOrBytes: fmtCount(summary.putRequests), cost: fmtMoney(summary.estimatedCost.putCostUsd), detail: 'PUT requests × request rate' }] : []),
      ...(summary.postRequests > 0 ? [{ label: 'S3PostObject', countOrBytes: fmtCount(summary.postRequests), cost: fmtMoney(summary.estimatedCost.postCostUsd), detail: 'POST requests × request rate' }] : []),
      { label: 'S3ListObjects', countOrBytes: fmtCount(summary.listRequests), cost: fmtMoney(summary.estimatedCost.listCostUsd), detail: 'Object listing calls × LIST request rate' },
      { label: 'ReadBufferFromS3Bytes', countOrBytes: formatBytes(summary.bytesRead), cost: fmtMoney(summary.estimatedCost.transferCostUsd), detail: `Bytes read × ${egressRate}/GB selected egress rate` },
      ...(summary.bytesWritten > 0 ? [{ label: 'WriteBufferFromS3Bytes', countOrBytes: formatBytes(summary.bytesWritten), cost: '$0.00', detail: 'Bytes written; cloud providers usually bill write requests, not inbound bytes' }] : []),
    ],
  };
}

function kpiStatus(value: number | null, watch: number, high: number, direction: 'higherIsBetter' | 'lowerIsBetter'): ObjectStorageKpi['status'] {
  if (value == null) return 'neutral';
  if (direction === 'higherIsBetter') {
    if (value >= high) return 'ok';
    if (value >= watch) return 'watch';
    return 'high';
  }
  if (value <= watch) return 'ok';
  if (value <= high) return 'watch';
  return 'high';
}

function buildEfficiency(summary: ObjectStorageProfileSummary, descriptions: ProfileEventDescriptions): ObjectStorageKpi[] {
  const readKpis: ObjectStorageKpi[] = [
    {
      label: 'Bytes / S3GetObject',
      value: fmtBytes(summary.avgBytesPerGet),
      formula: 'ReadBufferFromS3Bytes / S3GetObject',
      status: kpiStatus(summary.avgBytesPerGet, 1 * 1024 * 1024, 8 * 1024 * 1024, 'higherIsBetter'),
      hint: 'Higher usually means fewer small object-range reads.',
    },
    {
      label: 'Avg S3 GET open time',
      value: fmtKpiDurationUs(summary.avgS3GetOpenMicroseconds),
      formula: 'ReadBufferFromS3InitMicroseconds / S3GetObject',
      status: 'neutral',
      hint: profileEventDetail(descriptions, 'ReadBufferFromS3InitMicroseconds') ?? 'ReadBufferFromS3 initialization time.',
    },
    {
      label: 'Avg S3 GET/HEAD request time',
      value: fmtKpiDurationUs(summary.avgS3ReadRequestMicroseconds),
      formula: 'S3ReadMicroseconds / S3ReadRequestsCount',
      status: 'neutral',
      hint: profileEventDetail(descriptions, 'S3ReadMicroseconds') ?? 'S3 GET/HEAD request time.',
    },
    {
      label: 'Body streaming throughput',
      value: fmtThroughput(summary.effectiveBodyStreamingThroughputBytesPerSecond),
      formula: 'ReadBufferFromS3Bytes / (ReadBufferFromS3Microseconds - ReadBufferFromS3InitMicroseconds)',
      status: 'neutral',
      hint: 'Derived from ClickHouse ProfileEvents. The denominator is approximate body-streaming time and may include retry sleeps or read-buffer work.',
    },
  ];
  const writeKpis: ObjectStorageKpi[] = summary.bytesWritten > 0 || summary.writeRequests > 0 ? [
    {
      label: 'Bytes / S3WriteRequestsCount',
      value: fmtBytes(summary.avgBytesPerWriteRequest),
      formula: 'WriteBufferFromS3Bytes / S3WriteRequestsCount',
      status: 'neutral',
      hint: profileEventDetail(descriptions, 'S3WriteRequestsCount') ?? 'Number of S3 write requests.',
    },
    {
      label: 'Avg S3 write request time',
      value: fmtKpiDurationUs(summary.avgS3WriteRequestMicroseconds),
      formula: 'S3WriteMicroseconds / S3WriteRequestsCount',
      status: 'neutral',
      hint: profileEventDetail(descriptions, 'S3WriteMicroseconds') ?? 'S3 write request time.',
    },
  ] : [];

  return summary.bytesWritten > 0 || summary.writeRequests > 0 ? [...writeKpis, ...readKpis] : readKpis;
}

function segment(label: string, value: number, total: number, color: string, detail?: string): ObjectStorageTimeSegment {
  return {
    label,
    detail,
    value: formatMicroseconds(value),
    microseconds: value,
    widthPercent: total > 0 ? Math.min(100, Math.max(0, (value / total) * 100)) : 0,
    color,
  };
}

function buildReadTimeView(summary: ObjectStorageProfileSummary, descriptions: ProfileEventDescriptions): ObjectStorageTimeView | null {
  const total = summary.bufferReadMicroseconds;
  if (total <= 0 && summary.s3ReadMicroseconds <= 0 && summary.initMicroseconds <= 0) return null;
  const scaleMicroseconds = Math.max(total, summary.s3ReadMicroseconds, summary.initMicroseconds, summary.approxBodyStreamingMicroseconds);
  return {
    title: 'Read Time',
    subtitle: 'Read-buffer elapsed time and summed HTTP request time are separate ClickHouse ProfileEvents.',
    total: { label: 'Read buffer elapsed', value: formatMicroseconds(total) },
    totalColor: '#dbeafe',
    scaleMicroseconds,
    segments: [
      segment('S3 open/init time', summary.initMicroseconds, scaleMicroseconds, '#2f81f7', profileEventDetail(descriptions, 'ReadBufferFromS3InitMicroseconds')),
      segment(
        'S3 request time, summed',
        summary.s3ReadMicroseconds,
        scaleMicroseconds,
        '#79b8ff',
        profileEventDetail(descriptions, 'S3ReadMicroseconds') ?? 'Summed GET/HEAD request time; can exceed read-buffer elapsed time when requests overlap or multiple requests are accumulated.',
      ),
      segment(
        'Body streaming estimate',
        summary.approxBodyStreamingMicroseconds,
        scaleMicroseconds,
        '#dbeafe',
        'Formula: ReadBufferFromS3Microseconds - ReadBufferFromS3InitMicroseconds. Derived value, not a ClickHouse ProfileEvent. May include retry sleeps and read-buffer work.',
      ),
    ],
  };
}

function buildWriteTimeView(summary: ObjectStorageProfileSummary, descriptions: ProfileEventDescriptions): ObjectStorageTimeView | null {
  const total = summary.bufferWriteMicroseconds || summary.s3WriteMicroseconds;
  if (total <= 0) return null;
  const writeBufferRemainder = Number.isFinite(summary.writeBufferMicrosecondsOutsideS3Write)
    ? summary.writeBufferMicrosecondsOutsideS3Write
    : Math.max(0, summary.bufferWriteMicroseconds - summary.s3WriteMicroseconds);
  const scaleMicroseconds = Math.max(total, summary.s3WriteMicroseconds, writeBufferRemainder);

  return {
    title: 'Write Time',
    subtitle: 'Write-buffer and HTTP-layer timers are separate ClickHouse ProfileEvents; they are not additive.',
    total: {
      label: summary.bufferWriteMicroseconds > 0 ? 'WriteBufferFromS3Microseconds' : 'S3WriteMicroseconds',
      value: formatMicroseconds(total),
    },
    totalColor: '#ffedd5',
    scaleMicroseconds,
    segments: [
      segment('S3WriteMicroseconds', summary.s3WriteMicroseconds, scaleMicroseconds, '#f59e0b', profileEventDetail(descriptions, 'S3WriteMicroseconds')),
      segment('Write buffer overhead', writeBufferRemainder, scaleMicroseconds, '#fde68a', 'Formula: WriteBufferFromS3Microseconds - S3WriteMicroseconds. Derived value, not a ClickHouse ProfileEvent.'),
    ],
  };
}

function buildTimeViews(summary: ObjectStorageProfileSummary, descriptions: ProfileEventDescriptions): ObjectStorageTimeView[] {
  const read = buildReadTimeView(summary, descriptions);
  const write = buildWriteTimeView(summary, descriptions);
  return summary.bytesWritten > 0 || summary.writeRequests > 0
    ? [write, read].filter((view): view is ObjectStorageTimeView => view !== null)
    : [read, write].filter((view): view is ObjectStorageTimeView => view !== null);
}

function buildContext(summary: ObjectStorageProfileSummary): ObjectStorageMetric[] {
  if (!summary.iceberg) return [];
  return [
    { label: 'IcebergMetadataReadWaitTimeMicroseconds', value: formatMicroseconds(summary.iceberg.readWaitMicroseconds) },
    { label: 'Iceberg metadata cache hit rate', value: fmtPct(summary.iceberg.cacheHitRate), detail: `${fmtCount(summary.iceberg.cacheHits)} hits / ${fmtCount(summary.iceberg.cacheMisses)} misses` },
    { label: 'IcebergMetadataReturnedObjectInfos', value: fmtCount(summary.iceberg.returnedObjectInfos) },
  ];
}

function buildReliability(summary: ObjectStorageProfileSummary): ObjectStorageMetric[] {
  const metrics: ObjectStorageMetric[] = [];
  if (summary.readRequestAttempts > 0) {
    metrics.push({ label: 'S3ReadRequestAttempts', value: fmtCount(summary.readRequestAttempts) });
  }
  if (summary.readRequestErrors > 0) {
    metrics.push({ label: 'S3ReadRequestsErrors', value: fmtCount(summary.readRequestErrors) });
  }
  if (summary.writeRequestAttempts > 0) {
    metrics.push({ label: 'S3WriteRequestAttempts', value: fmtCount(summary.writeRequestAttempts) });
  }
  if (summary.writeRequestErrors > 0) {
    metrics.push({ label: 'S3WriteRequestsErrors', value: fmtCount(summary.writeRequestErrors) });
  }
  return metrics;
}

export function buildObjectStorageViewModel(summary: ObjectStorageProfileSummary, descriptions: ProfileEventDescriptions = {}): ObjectStorageViewModel {
  const storageLabel = detectorLabel(summary.detector);
  return {
    title: 'Object Storage',
    subtitle: summary.bytesWritten > 0 || summary.writeRequests > 0 ? `${storageLabel} reads or writes detected in this query.` : `${storageLabel} reads detected in this query.`,
    cost: buildCostView(summary),
    efficiency: buildEfficiency(summary, descriptions),
    time: buildTimeViews(summary, descriptions),
    reliability: buildReliability(summary),
    context: buildContext(summary),
    rawEventGroups: groupEvents(summary.rawEvents),
  };
}
