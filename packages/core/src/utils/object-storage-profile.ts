export type ProfileEventsMap = Record<string, number | string | undefined>;

export interface ObjectStoragePricingProfile {
  id: string;
  label: string;
  provider: 'aws' | 'gcp' | 'azure' | 'minio' | 'custom';
  region: string;
  storageClass?: string;
  currency: 'USD';
  requestPricing: {
    getPer1000: number;
    headPer1000: number;
    putPer1000: number;
    postPer1000: number;
    listPer1000: number;
  };
  transferPricing: {
    egressPerGb: number;
    sameRegionPerGb: number;
    crossRegionPerGb?: number;
    internetPerGb?: number;
  };
}

export interface ObjectStorageEvent {
  name: string;
  value: number;
  group: 's3' | 'read_buffer' | 'disk_s3' | 'iceberg' | 'other';
}

export interface ObjectStorageCostEstimate {
  getCostUsd: number;
  headCostUsd: number;
  putCostUsd: number;
  postCostUsd: number;
  listCostUsd: number;
  requestCostUsd: number;
  transferCostUsd: number;
  totalCostUsd: number;
  requestShare: number;
  transferShare: number;
  pricingProfile: ObjectStoragePricingProfile;
}

export interface ObjectStorageProfileSummary {
  hasObjectStorageIO: boolean;
  detector: 's3_compatible';
  bytesRead: number;
  readRequests: number;
  getRequests: number;
  headRequests: number;
  putRequests: number;
  postRequests: number;
  listRequests: number;
  bytesWritten: number;
  writeRequests: number;
  readRequestAttempts: number;
  readRequestErrors: number;
  writeRequestAttempts: number;
  writeRequestErrors: number;
  bufferReadMicroseconds: number;
  bufferWriteMicroseconds: number;
  s3ReadMicroseconds: number;
  s3WriteMicroseconds: number;
  initMicroseconds: number;
  avgBytesPerRequest: number | null;
  avgBytesPerGet: number | null;
  overheadMicroseconds: number;
  approxBodyStreamingMicroseconds: number;
  writeBufferMicrosecondsOutsideS3Write: number;
  avgS3GetOpenMicroseconds: number | null;
  avgS3ReadRequestMicroseconds: number | null;
  avgS3WriteRequestMicroseconds: number | null;
  avgBytesPerWriteRequest: number | null;
  effectiveThroughputBytesPerSecond: number | null;
  effectiveBodyStreamingThroughputBytesPerSecond: number | null;
  initShare: number | null;
  patterns: string[];
  rawEvents: ObjectStorageEvent[];
  iceberg?: {
    readWaitMicroseconds: number;
    cacheHits: number;
    cacheMisses: number;
    cacheHitRate: number | null;
    returnedObjectInfos: number;
    cacheWeightLost: number;
    events: ObjectStorageEvent[];
  };
  estimatedCost: ObjectStorageCostEstimate;
}

const S3_COMPATIBLE_PROFILE_EVENTS = {
  bytesRead: 'ReadBufferFromS3Bytes',
  bytesWritten: 'WriteBufferFromS3Bytes',
  readRequests: 'S3ReadRequestsCount',
  writeRequests: 'S3WriteRequestsCount',
  readRequestAttempts: 'S3ReadRequestAttempts',
  readRequestErrors: 'S3ReadRequestsErrors',
  writeRequestAttempts: 'S3WriteRequestAttempts',
  writeRequestErrors: 'S3WriteRequestsErrors',
  getRequests: 'S3GetObject',
  headRequests: 'S3HeadObject',
  putRequests: 'S3PutObject',
  postRequests: 'S3PostObject',
  listRequests: 'S3ListObjects',
  diskListRequests: 'DiskS3ListObjects',
  bufferReadMicroseconds: 'ReadBufferFromS3Microseconds',
  bufferWriteMicroseconds: 'WriteBufferFromS3Microseconds',
  s3ReadMicroseconds: 'S3ReadMicroseconds',
  s3WriteMicroseconds: 'S3WriteMicroseconds',
  initMicroseconds: 'ReadBufferFromS3InitMicroseconds',
} as const;

export const DEFAULT_OBJECT_STORAGE_PRICING_PROFILE: ObjectStoragePricingProfile = {
  id: 'aws-s3-us-east-1-standard-same-region',
  label: 'AWS S3 Standard · us-east-1 · same-region',
  provider: 'aws',
  region: 'us-east-1',
  storageClass: 'standard',
  currency: 'USD',
  requestPricing: {
    getPer1000: 0.0004,
    headPer1000: 0.0004,
    putPer1000: 0.005,
    postPer1000: 0.005,
    listPer1000: 0.005,
  },
  transferPricing: {
    egressPerGb: 0,
    sameRegionPerGb: 0,
    internetPerGb: 0.09,
  },
};

function toNumber(value: number | string | undefined): number {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function eventValue(events: ProfileEventsMap, name: string): number {
  return toNumber(events[name]);
}

function classifyEvent(name: string): ObjectStorageEvent['group'] {
  if (name.startsWith('ReadBufferFromS3') || name.startsWith('WriteBufferFromS3')) return 'read_buffer';
  if (name.startsWith('DiskS3')) return 'disk_s3';
  if (name.startsWith('Iceberg')) return 'iceberg';
  if (name.startsWith('S3')) return 's3';
  return 'other';
}

function estimateObjectStorageCost(
  bytesRead: number,
  getRequests: number,
  headRequests: number,
  putRequests: number,
  postRequests: number,
  listRequests: number,
  pricingProfile: ObjectStoragePricingProfile,
): ObjectStorageCostEstimate {
  const getCostUsd = (getRequests / 1000) * pricingProfile.requestPricing.getPer1000;
  const headCostUsd = (headRequests / 1000) * pricingProfile.requestPricing.headPer1000;
  const putCostUsd = (putRequests / 1000) * pricingProfile.requestPricing.putPer1000;
  const postCostUsd = (postRequests / 1000) * pricingProfile.requestPricing.postPer1000;
  const listCostUsd = (listRequests / 1000) * pricingProfile.requestPricing.listPer1000;
  const requestCostUsd = getCostUsd + headCostUsd + putCostUsd + postCostUsd + listCostUsd;
  const transferCostUsd = (bytesRead / 1024 / 1024 / 1024) * pricingProfile.transferPricing.egressPerGb;
  const totalCostUsd = requestCostUsd + transferCostUsd;
  return {
    getCostUsd,
    headCostUsd,
    putCostUsd,
    postCostUsd,
    listCostUsd,
    requestCostUsd,
    transferCostUsd,
    totalCostUsd,
    requestShare: totalCostUsd > 0 ? requestCostUsd / totalCostUsd : 0,
    transferShare: totalCostUsd > 0 ? transferCostUsd / totalCostUsd : 0,
    pricingProfile,
  };
}

export function summarizeObjectStorageProfile(
  events: ProfileEventsMap | undefined,
  pricingProfile: ObjectStoragePricingProfile = DEFAULT_OBJECT_STORAGE_PRICING_PROFILE,
): ObjectStorageProfileSummary {
  const profileEvents = events ?? {};
  const rawEvents = Object.entries(profileEvents)
    .map(([name, value]) => ({ name, value: toNumber(value), group: classifyEvent(name) }))
    .filter((event) => event.value > 0 && event.group !== 'other')
    .sort((a, b) => a.group.localeCompare(b.group) || a.name.localeCompare(b.name));

  const s3Events = S3_COMPATIBLE_PROFILE_EVENTS;
  const bytesRead = eventValue(profileEvents, s3Events.bytesRead);
  const readRequests = eventValue(profileEvents, s3Events.readRequests);
  const getRequests = eventValue(profileEvents, s3Events.getRequests);
  const headRequests = eventValue(profileEvents, s3Events.headRequests);
  const putRequests = eventValue(profileEvents, s3Events.putRequests);
  const postRequests = eventValue(profileEvents, s3Events.postRequests);
  const listRequests = Math.max(eventValue(profileEvents, s3Events.listRequests), eventValue(profileEvents, s3Events.diskListRequests));
  const bytesWritten = eventValue(profileEvents, s3Events.bytesWritten);
  const writeRequests = eventValue(profileEvents, s3Events.writeRequests);
  const readRequestAttempts = eventValue(profileEvents, s3Events.readRequestAttempts);
  const readRequestErrors = eventValue(profileEvents, s3Events.readRequestErrors);
  const writeRequestAttempts = eventValue(profileEvents, s3Events.writeRequestAttempts);
  const writeRequestErrors = eventValue(profileEvents, s3Events.writeRequestErrors);
  const bufferReadMicroseconds = eventValue(profileEvents, s3Events.bufferReadMicroseconds);
  const bufferWriteMicroseconds = eventValue(profileEvents, s3Events.bufferWriteMicroseconds);
  const s3ReadMicroseconds = eventValue(profileEvents, s3Events.s3ReadMicroseconds);
  const s3WriteMicroseconds = eventValue(profileEvents, s3Events.s3WriteMicroseconds);
  const initMicroseconds = eventValue(profileEvents, s3Events.initMicroseconds);
  const avgBytesPerRequest = readRequests > 0 ? bytesRead / readRequests : null;
  const avgBytesPerGet = getRequests > 0 ? bytesRead / getRequests : null;
  const overheadMicroseconds = Math.max(0, bufferReadMicroseconds - s3ReadMicroseconds);
  const approxBodyStreamingMicroseconds = Math.max(0, bufferReadMicroseconds - initMicroseconds);
  const writeBufferMicrosecondsOutsideS3Write = Math.max(0, bufferWriteMicroseconds - s3WriteMicroseconds);
  const avgS3GetOpenMicroseconds = getRequests > 0 ? initMicroseconds / getRequests : null;
  const avgS3ReadRequestMicroseconds = readRequests > 0 ? s3ReadMicroseconds / readRequests : null;
  const avgS3WriteRequestMicroseconds = writeRequests > 0 ? s3WriteMicroseconds / writeRequests : null;
  const avgBytesPerWriteRequest = writeRequests > 0 ? bytesWritten / writeRequests : null;
  const effectiveThroughputBytesPerSecond = s3ReadMicroseconds > 0 ? bytesRead / (s3ReadMicroseconds / 1_000_000) : null;
  const effectiveBodyStreamingThroughputBytesPerSecond = approxBodyStreamingMicroseconds > 0
    ? bytesRead / (approxBodyStreamingMicroseconds / 1_000_000)
    : null;
  const initShare = bufferReadMicroseconds > 0 ? initMicroseconds / bufferReadMicroseconds : null;

  const hasObjectStorageIO = bytesRead > 0 || bytesWritten > 0 || readRequests > 0 || writeRequests > 0 ||
    getRequests > 0 || headRequests > 0 || putRequests > 0 || postRequests > 0 || listRequests > 0 ||
    rawEvents.some((event) => event.group === 'disk_s3' || event.group === 's3' || event.group === 'read_buffer');

  const patterns: string[] = [];
  if (avgBytesPerRequest !== null && avgBytesPerRequest < 1024 * 1024) patterns.push('Request-amplified');
  if (headRequests > getRequests * 0.5 && headRequests > 100) patterns.push('Metadata checks');
  if (putRequests > 0 || writeRequests > 0 || bytesWritten > 0) patterns.push('Object writes');
  if (readRequestErrors > 0 || writeRequestErrors > 0) patterns.push('S3 request errors');
  if (listRequests > 0) patterns.push('Object listing');
  if (initShare !== null && initShare > 0.4) patterns.push('Init-bound');
  if (bytesRead >= 10 * 1024 * 1024 * 1024) patterns.push('Large object scan');

  const icebergEvents = rawEvents.filter((event) => event.group === 'iceberg');
  const icebergCacheHits = eventValue(profileEvents, 'IcebergMetadataFilesCacheHits');
  const icebergCacheMisses = eventValue(profileEvents, 'IcebergMetadataFilesCacheMisses');
  const icebergCacheTotal = icebergCacheHits + icebergCacheMisses;
  const iceberg = icebergEvents.length > 0 ? {
    readWaitMicroseconds: eventValue(profileEvents, 'IcebergMetadataReadWaitTimeMicroseconds'),
    cacheHits: icebergCacheHits,
    cacheMisses: icebergCacheMisses,
    cacheHitRate: icebergCacheTotal > 0 ? icebergCacheHits / icebergCacheTotal : null,
    returnedObjectInfos: eventValue(profileEvents, 'IcebergMetadataReturnedObjectInfos'),
    cacheWeightLost: eventValue(profileEvents, 'IcebergMetadataFilesCacheWeightLost'),
    events: icebergEvents,
  } : undefined;

  if (iceberg && iceberg.cacheHitRate !== null && iceberg.cacheHitRate < 0.5) patterns.push('Iceberg cache misses');
  if (iceberg && iceberg.returnedObjectInfos > 1000) patterns.push('Many Iceberg objects');

  return {
    hasObjectStorageIO,
    detector: 's3_compatible',
    bytesRead,
    readRequests,
    getRequests,
    headRequests,
    putRequests,
    postRequests,
    listRequests,
    bytesWritten,
    writeRequests,
    readRequestAttempts,
    readRequestErrors,
    writeRequestAttempts,
    writeRequestErrors,
    bufferReadMicroseconds,
    bufferWriteMicroseconds,
    s3ReadMicroseconds,
    s3WriteMicroseconds,
    initMicroseconds,
    avgBytesPerRequest,
    avgBytesPerGet,
    overheadMicroseconds,
    approxBodyStreamingMicroseconds,
    writeBufferMicrosecondsOutsideS3Write,
    avgS3GetOpenMicroseconds,
    avgS3ReadRequestMicroseconds,
    avgS3WriteRequestMicroseconds,
    avgBytesPerWriteRequest,
    effectiveThroughputBytesPerSecond,
    effectiveBodyStreamingThroughputBytesPerSecond,
    initShare,
    patterns,
    rawEvents,
    iceberg,
    estimatedCost: estimateObjectStorageCost(bytesRead, getRequests, headRequests, putRequests, postRequests, listRequests, pricingProfile),
  };
}
