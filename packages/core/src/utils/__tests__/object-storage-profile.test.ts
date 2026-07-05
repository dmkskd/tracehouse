import { describe, expect, it } from 'vitest';
import { DEFAULT_OBJECT_STORAGE_PRICING_PROFILE, summarizeObjectStorageProfile, type ObjectStoragePricingProfile } from '../object-storage-profile.js';

describe('summarizeObjectStorageProfile', () => {
  it('does not trigger without object storage IO', () => {
    const summary = summarizeObjectStorageProfile({
      IcebergMetadataFilesCacheMisses: 10,
      IcebergMetadataFilesCacheHits: 5,
    });

    expect(summary.hasObjectStorageIO).toBe(false);
    expect(summary.iceberg?.cacheHitRate).toBeCloseTo(5 / 15);
  });

  it('summarizes S3-compatible reads and cost split', () => {
    const summary = summarizeObjectStorageProfile({
      ReadBufferFromS3Bytes: 35 * 1024 * 1024 * 1024,
      ReadBufferFromS3Microseconds: 148_000_000,
      ReadBufferFromS3InitMicroseconds: 94_000_000,
      S3ReadMicroseconds: 107_000_000,
      S3ReadRequestsCount: 61_833,
      S3GetObject: 35_214,
      S3HeadObject: 26_578,
      S3ListObjects: 41,
    });

    expect(summary.hasObjectStorageIO).toBe(true);
    expect(summary.avgBytesPerRequest).toBeCloseTo((35 * 1024 * 1024 * 1024) / 61_833);
    expect(summary.avgBytesPerGet).toBeCloseTo((35 * 1024 * 1024 * 1024) / 35_214);
    expect(summary.patterns).toContain('Request-amplified');
    expect(summary.patterns).toContain('Metadata checks');
    expect(summary.patterns).toContain('Init-bound');
    expect(summary.estimatedCost.requestCostUsd).toBeGreaterThan(0);
    expect(summary.estimatedCost.getCostUsd).toBeGreaterThan(0);
    expect(summary.estimatedCost.headCostUsd).toBeGreaterThan(0);
    expect(summary.estimatedCost.putCostUsd).toBe(0);
    expect(summary.estimatedCost.postCostUsd).toBe(0);
    expect(summary.estimatedCost.listCostUsd).toBeGreaterThan(0);
    expect(summary.estimatedCost.transferCostUsd).toBe(0);
    expect(summary.avgS3GetOpenMicroseconds).toBeCloseTo(94_000_000 / 35_214);
    expect(summary.avgS3ReadRequestMicroseconds).toBeCloseTo(107_000_000 / 61_833);
    expect(summary.approxBodyStreamingMicroseconds).toBe(54_000_000);
    expect(summary.effectiveThroughputBytesPerSecond).toBeGreaterThan(0);
    expect(summary.effectiveBodyStreamingThroughputBytesPerSecond).toBeGreaterThan(0);
    expect(summary.estimatedCost.pricingProfile).toEqual(DEFAULT_OBJECT_STORAGE_PRICING_PROFILE);
  });

  it('accounts for S3 writes from insert queries', () => {
    const summary = summarizeObjectStorageProfile({
      S3GetObject: 1,
      S3HeadObject: 2,
      S3ListObjects: 4,
      S3PutObject: 4,
      S3ReadMicroseconds: 1_480_000,
      S3ReadRequestAttempts: 4,
      S3ReadRequestsCount: 8,
      S3ReadRequestsErrors: 2,
      S3WriteMicroseconds: 1_650_000,
      S3WriteRequestAttempts: 4,
      S3WriteRequestsCount: 4,
      WriteBufferFromS3Bytes: 17_920_000,
      WriteBufferFromS3Microseconds: 1_800_000,
    });

    expect(summary.hasObjectStorageIO).toBe(true);
    expect(summary.putRequests).toBe(4);
    expect(summary.writeRequests).toBe(4);
    expect(summary.readRequestAttempts).toBe(4);
    expect(summary.readRequestErrors).toBe(2);
    expect(summary.writeRequestAttempts).toBe(4);
    expect(summary.bytesWritten).toBe(17_920_000);
    expect(summary.s3WriteMicroseconds).toBe(1_650_000);
    expect(summary.bufferWriteMicroseconds).toBe(1_800_000);
    expect(summary.writeBufferMicrosecondsOutsideS3Write).toBe(150_000);
    expect(summary.avgS3WriteRequestMicroseconds).toBe(1_650_000 / 4);
    expect(summary.avgBytesPerWriteRequest).toBe(17_920_000 / 4);
    expect(summary.patterns).toContain('Object writes');
    expect(summary.patterns).toContain('S3 request errors');
    expect(summary.estimatedCost.putCostUsd).toBeCloseTo((4 / 1000) * DEFAULT_OBJECT_STORAGE_PRICING_PROFILE.requestPricing.putPer1000);
  });

  it('does not double-count DiskS3ListObjects and S3ListObjects for estimated LIST cost', () => {
    const summary = summarizeObjectStorageProfile({
      S3ListObjects: 41,
      DiskS3ListObjects: 41,
    });

    expect(summary.listRequests).toBe(41);
    expect(summary.estimatedCost.listCostUsd).toBeCloseTo((41 / 1000) * DEFAULT_OBJECT_STORAGE_PRICING_PROFILE.requestPricing.listPer1000);
  });

  it('uses the supplied pricing profile for request and transfer estimates', () => {
    const pricing: ObjectStoragePricingProfile = {
      id: 'minio-local',
      label: 'MinIO local',
      provider: 'minio',
      region: 'local',
      currency: 'USD',
      requestPricing: { getPer1000: 0, headPer1000: 0, putPer1000: 0, postPer1000: 0, listPer1000: 0 },
      transferPricing: { egressPerGb: 0, sameRegionPerGb: 0 },
    };

    const summary = summarizeObjectStorageProfile({
      ReadBufferFromS3Bytes: 1024 * 1024 * 1024,
      S3GetObject: 1000,
      S3HeadObject: 1000,
      S3ListObjects: 1000,
    }, pricing);

    expect(summary.estimatedCost.totalCostUsd).toBe(0);
    expect(summary.estimatedCost.pricingProfile).toEqual(pricing);
  });

  it('adds Iceberg context when object storage IO also exists', () => {
    const summary = summarizeObjectStorageProfile({
      ReadBufferFromS3Bytes: 1024,
      S3ReadRequestsCount: 2,
      IcebergMetadataReadWaitTimeMicroseconds: 594_030_000,
      IcebergMetadataReturnedObjectInfos: 13_718,
      IcebergMetadataFilesCacheMisses: 12_763,
      IcebergMetadataFilesCacheHits: 960,
    });

    expect(summary.hasObjectStorageIO).toBe(true);
    expect(summary.iceberg).toMatchObject({
      readWaitMicroseconds: 594_030_000,
      returnedObjectInfos: 13_718,
      cacheHits: 960,
      cacheMisses: 12_763,
    });
    expect(summary.iceberg?.cacheHitRate).toBeCloseTo(960 / (960 + 12_763));
    expect(summary.patterns).toContain('Iceberg cache misses');
    expect(summary.patterns).toContain('Many Iceberg objects');
  });
});
