import { describe, expect, it } from 'vitest';
import type { ObjectStorageProfileSummary } from '@tracehouse/core';
import { buildObjectStorageViewModel } from '../objectStorageViewModel';

const baseSummary: ObjectStorageProfileSummary = {
  hasObjectStorageIO: true,
  detector: 's3_compatible',
  bytesRead: 1024 * 1024,
  readRequests: 4,
  getRequests: 3,
  headRequests: 1,
  putRequests: 0,
  postRequests: 0,
  listRequests: 0,
  bytesWritten: 0,
  writeRequests: 0,
  readRequestAttempts: 0,
  readRequestErrors: 0,
  writeRequestAttempts: 0,
  writeRequestErrors: 0,
  bufferReadMicroseconds: 2_000_000,
  bufferWriteMicroseconds: 0,
  s3ReadMicroseconds: 1_500_000,
  s3WriteMicroseconds: 0,
  initMicroseconds: 500_000,
  avgBytesPerRequest: 256 * 1024,
  avgBytesPerGet: (1024 * 1024) / 3,
  overheadMicroseconds: 500_000,
  approxBodyStreamingMicroseconds: 1_500_000,
  writeBufferMicrosecondsOutsideS3Write: 0,
  avgS3GetOpenMicroseconds: 500_000 / 3,
  avgS3ReadRequestMicroseconds: 375_000,
  avgS3WriteRequestMicroseconds: null,
  avgBytesPerWriteRequest: null,
  effectiveThroughputBytesPerSecond: (1024 * 1024) / 1.5,
  effectiveBodyStreamingThroughputBytesPerSecond: (1024 * 1024) / 1.5,
  initShare: 0.25,
  patterns: ['Request-amplified'],
  rawEvents: [
    { name: 'ReadBufferFromS3Bytes', value: 1024 * 1024, group: 'read_buffer' },
    { name: 'S3GetObject', value: 3, group: 's3' },
  ],
  estimatedCost: {
    getCostUsd: 0.0000012,
    headCostUsd: 0.0000004,
    putCostUsd: 0,
    postCostUsd: 0,
    listCostUsd: 0,
    requestCostUsd: 0.001,
    transferCostUsd: 0,
    totalCostUsd: 0.001,
    requestShare: 1,
    transferShare: 0,
    pricingProfile: {
      id: 'test',
      label: 'Test pricing',
      provider: 'custom',
      region: 'test',
      currency: 'USD',
      requestPricing: { getPer1000: 0.0004, headPer1000: 0.0004, putPer1000: 0.005, postPer1000: 0.005, listPer1000: 0.005 },
      transferPricing: { egressPerGb: 0, sameRegionPerGb: 0 },
    },
  },
};

describe('buildObjectStorageViewModel', () => {
  it('formats summary, latency, cost, and raw events for rendering', () => {
    const vm = buildObjectStorageViewModel(baseSummary, {
      ReadBufferFromS3Microseconds: 'Time spent on reading from S3.',
      ReadBufferFromS3InitMicroseconds: 'Time spent initializing connection to S3.',
      S3ReadMicroseconds: 'Time of GET and HEAD requests to S3 storage.',
      S3ReadRequestsCount: 'Number of GET and HEAD requests to S3 storage.',
    });

    expect(vm.cost.lines.find((line) => line.label === 'S3GetObject')?.countOrBytes).toBe('3');
    expect(vm.efficiency.find((metric) => metric.label === 'Bytes / S3GetObject')?.formula).toBe('ReadBufferFromS3Bytes / S3GetObject');
    expect(vm.efficiency.find((metric) => metric.label === 'Avg S3 GET open time')?.value).toBe('166.7ms');
    expect(vm.efficiency.find((metric) => metric.label === 'Avg S3 GET open time')?.hint).toBe('Time spent initializing connection to S3.');
    expect(vm.efficiency.find((metric) => metric.label === 'Avg S3 GET/HEAD request time')?.value).toBe('375.0ms');
    expect(vm.efficiency.find((metric) => metric.label === 'Avg S3 GET/HEAD request time')?.hint).toBe('Time of GET and HEAD requests to S3 storage.');
    expect(vm.efficiency.find((metric) => metric.label === 'Body streaming throughput')?.formula).toBe('ReadBufferFromS3Bytes / (ReadBufferFromS3Microseconds - ReadBufferFromS3InitMicroseconds)');
    expect(vm.time[0]?.segments.find((segment) => segment.label === 'ReadBufferFromS3InitMicroseconds')?.value).toBe('500.0ms');
    expect(vm.time[0]?.segments.find((segment) => segment.label === 'ReadBufferFromS3InitMicroseconds')?.detail).toBe('Time spent initializing connection to S3.');
    expect(vm.time[0]?.segments.find((segment) => segment.label === 'Approx. body streaming time')?.value).toBe('1.50s');
    expect(vm.time[0]?.segments.find((segment) => segment.label === 'Approx. body streaming time')?.detail).toContain('Formula: ReadBufferFromS3Microseconds - ReadBufferFromS3InitMicroseconds');
    expect(vm.time[0]?.subtitle).toBe('Read-buffer and HTTP-layer timers are separate ClickHouse ProfileEvents; they are not additive.');
    expect(vm.time[0]?.scaleMicroseconds).toBe(2_000_000);
    expect(vm.cost.requestLegend).toBe('API requests 100%');
    expect(vm.cost.transferLegend).toBe('Egress 0%');
    expect(vm.cost.pricingProfile).toBe('Test pricing');
    expect(vm.cost.pricingTooltip).toContain('GET: $0.0004 / 1,000');
    expect(vm.cost.pricingTooltip).toContain('Egress used: $0.0000 / GB');
    expect(vm.cost.lines.find((line) => line.label === 'ReadBufferFromS3Bytes')?.detail).toBe('Bytes read × $0.0000/GB selected egress rate');
    expect(vm.rawEventGroups.find((group) => group.title === 'S3 API')?.events).toEqual([{ label: 'S3GetObject', value: '3' }]);
  });

  it('adds Iceberg metrics only when Iceberg context exists', () => {
    expect(buildObjectStorageViewModel(baseSummary).context).toEqual([]);

    const vm = buildObjectStorageViewModel({
      ...baseSummary,
      iceberg: {
        readWaitMicroseconds: 594_030_000,
        cacheHits: 960,
        cacheMisses: 12_763,
        cacheHitRate: 960 / (960 + 12_763),
        returnedObjectInfos: 13_718,
        cacheWeightLost: 230_847_118,
        events: [],
      },
    });

    expect(vm.context.find((metric) => metric.label === 'Iceberg metadata cache hit rate')?.value).toBe('7.0%');
    expect(vm.context.find((metric) => metric.label === 'Iceberg metadata cache hit rate')?.detail).toBe('960 hits / 12,763 misses');
    expect(vm.context.find((metric) => metric.label === 'IcebergMetadataReturnedObjectInfos')?.value).toBe('13,718');
  });

  it('shows write-side S3 request costs when insert counters exist', () => {
    const vm = buildObjectStorageViewModel({
      ...baseSummary,
      putRequests: 4,
      bytesWritten: 17_920_000,
      writeRequests: 4,
      readRequestAttempts: 4,
      readRequestErrors: 2,
      bufferWriteMicroseconds: 1_800_000,
      s3WriteMicroseconds: 1_650_000,
      writeBufferMicrosecondsOutsideS3Write: 150_000,
      avgS3WriteRequestMicroseconds: 412_500,
      avgBytesPerWriteRequest: 4_480_000,
      estimatedCost: {
        ...baseSummary.estimatedCost,
        putCostUsd: 0.00002,
        requestCostUsd: 0.00102,
        totalCostUsd: 0.00102,
      },
    });

    expect(vm.subtitle).toBe('S3-compatible reads or writes detected in this query.');
    expect(vm.efficiency[0]?.label).toBe('Bytes / S3WriteRequestsCount');
    expect(vm.efficiency[0]?.value).toBe('4.27 MB');
    expect(vm.efficiency[1]?.label).toBe('Avg S3 write request time');
    expect(vm.efficiency[1]?.value).toBe('412.5ms');
    expect(vm.cost.lines.find((line) => line.label === 'S3PutObject')?.countOrBytes).toBe('4');
    expect(vm.cost.lines.find((line) => line.label === 'WriteBufferFromS3Bytes')?.countOrBytes).toBe('17.09 MB');
    expect(vm.time[0]?.title).toBe('Write Time');
    expect(vm.time[0]?.total.value).toBe('1.80s');
    expect(vm.time[0]?.scaleMicroseconds).toBe(1_800_000);
    expect(vm.time[0]?.totalColor).toBe('#ffedd5');
    expect(vm.time[0]?.segments.find((segment) => segment.label === 'S3WriteMicroseconds')?.color).toBe('#f59e0b');
    expect(vm.time[0]?.segments.find((segment) => segment.label === 'Write buffer overhead')?.value).toBe('150.0ms');
    expect(vm.time[0]?.segments.find((segment) => segment.label === 'Write buffer overhead')?.detail).toContain('Formula: WriteBufferFromS3Microseconds - S3WriteMicroseconds');
    expect(vm.reliability.find((metric) => metric.label === 'S3ReadRequestsErrors')?.value).toBe('2');
  });
});
