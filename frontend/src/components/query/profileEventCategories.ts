/**
 * ProfileEvents category definitions for grouping ClickHouse profile events
 * into logical categories (CPU, Memory, I/O, etc.) with associated colors.
 *
 * Extracted from QueryDetailModal.tsx.
 */

export const PROFILE_EVENT_CATEGORIES: Record<string, { label: string; color: string; events: string[] }> = {
  cpu: {
    label: 'CPU',
    color: 'var(--color-success)',
    events: ['OSCPUVirtualTimeMicroseconds', 'OSCPUWaitMicroseconds', 'UserTimeMicroseconds', 'SystemTimeMicroseconds', 'RealTimeMicroseconds'],
  },
  memory: {
    label: 'Memory',
    color: 'var(--color-memory)',
    events: ['MemoryTracker', 'PeakMemoryUsage', 'ArenaAllocChunks', 'ArenaAllocBytes', 'CompileExpressionsBytes', 'HashJoinBytes', 'SoftPageFaults', 'HardPageFaults'],
  },
  io: {
    label: 'Disk I/O',
    color: 'var(--color-warning)',
    events: ['ReadBufferFromFileDescriptorRead', 'ReadBufferFromFileDescriptorReadBytes', 'WriteBufferFromFileDescriptorWrite', 'WriteBufferFromFileDescriptorWriteBytes', 'DiskReadElapsedMicroseconds', 'DiskWriteElapsedMicroseconds'],
  },
  network: {
    label: 'Network',
    color: 'var(--color-info)',
    events: ['NetworkSendBytes', 'NetworkReceiveBytes', 'NetworkSendElapsedMicroseconds', 'NetworkReceiveElapsedMicroseconds'],
  },
  cache: {
    label: 'Cache',
    color: '#f0883e',
    events: ['MarkCacheHits', 'MarkCacheMisses', 'UncompressedCacheHits', 'UncompressedCacheMisses', 'QueryCacheHits', 'QueryCacheMisses'],
  },
  query: {
    label: 'Query Processing',
    color: '#58a6ff',
    events: ['SelectedRows', 'SelectedBytes', 'SelectedParts', 'SelectedRanges', 'SelectedMarks', 'MergedRows', 'MergedUncompressedBytes'],
  },
  s3: {
    label: 'S3 / Object Storage',
    color: '#e3716e',
    events: ['S3ReadMicroseconds', 'S3ReadRequestsCount', 'S3WriteMicroseconds', 'S3WriteRequestsCount', 'S3GetObject', 'S3PutObject', 'S3HeadObject', 'S3ListObjects', 'S3CopyObject', 'S3DeleteObjects', 'DiskS3ReadMicroseconds', 'DiskS3ReadRequestsCount', 'DiskS3WriteMicroseconds', 'DiskS3WriteRequestsCount', 'DiskS3GetObject', 'DiskS3PutObject', 'DiskS3HeadObject', 'DiskS3ListObjects', 'ReadBufferFromS3Microseconds', 'ReadBufferFromS3InitMicroseconds', 'ReadBufferFromS3Bytes', 'ReadBufferFromS3RequestsErrors', 'WriteBufferFromS3Microseconds', 'WriteBufferFromS3Bytes'],
  },
  insert: {
    label: 'Insert',
    color: '#a371f7',
    events: ['InsertQuery', 'InsertedRows', 'InsertedBytes', 'InsertedWideParts', 'InsertedCompactParts', 'InsertedInMemoryParts', 'MergedIntoWideParts', 'MergedIntoCompactParts', 'InsertedBlocks', 'DuplicatedInsertedBlocks', 'DelayedInserts', 'RejectedInserts', 'DelayedInsertsMilliseconds'],
  },
};
