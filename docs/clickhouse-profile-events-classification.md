# ClickHouse ProfileEvents Classification

> **See also:** [Observability tiers](clickhouse-observability-tiers.md) for how these events
> fit into the three-tier model (server-wide counters → per-table state → per-operation logs)
> and which system tables expose them.

Source: `src/Common/ProfileEvents.cpp` (master branch, current as of Feb 2026)

All events are exposed via Prometheus as `ClickHouseProfileEvents_<EventName>` counters.
CurrentMetrics (gauges) are exposed as `ClickHouseMetrics_<MetricName>`.
AsyncMetrics are exposed as `ClickHouseAsyncMetrics_<MetricName>`.

---

## 1. MERGE PROFILING / MONITORING (directly relevant to our app)

### Core Merge Events
| Event | Type | Description |
|---|---|---|
| `Merge` | Number | Background merges launched |
| `MergeSourceParts` | Number | Source parts scheduled for merges |
| `MergedRows` | Number | Rows read for merges (pre-merge count) |
| `MergedColumns` | Number | Columns merged during horizontal stage |
| `GatheredColumns` | Number | Columns gathered during vertical stage |
| `MergedUncompressedBytes` | Bytes | Uncompressed bytes read for merges |
| `MergesRejectedByMemoryLimit` | Number | Merges rejected due to memory limit |

### Merge Timing Breakdown (NEW in modern CH — very useful)
| Event | Type | Description |
|---|---|---|
| `MergeTotalMilliseconds` | ms | Total wall-clock time for merges |
| `MergeExecuteMilliseconds` | ms | Busy execution time for merges |
| `MergeHorizontalStageTotalMilliseconds` | ms | Horizontal stage total time |
| `MergeHorizontalStageExecuteMilliseconds` | ms | Horizontal stage execution time |
| `MergeVerticalStageTotalMilliseconds` | ms | Vertical stage total time |
| `MergeVerticalStageExecuteMilliseconds` | ms | Vertical stage execution time |
| `MergeTextIndexStageTotalMilliseconds` | ms | Text index stage total time |
| `MergeTextIndexStageExecuteMilliseconds` | ms | Text index stage execution time |
| `MergeProjectionStageTotalMilliseconds` | ms | Projection stage total time |
| `MergeProjectionStageExecuteMilliseconds` | ms | Projection stage execution time |
| `MergePrewarmStageTotalMilliseconds` | ms | Prewarm stage total time |
| `MergePrewarmStageExecuteMilliseconds` | ms | Prewarm stage execution time |

### Merge Engine-Specific Timing
| Event | Type | Description |
|---|---|---|
| `MergingSortedMilliseconds` | ms | Time merging sorted columns (MergeTree) |
| `AggregatingSortedMilliseconds` | ms | Time aggregating sorted columns (AggregatingMergeTree) |
| `CollapsingSortedMilliseconds` | ms | Time collapsing sorted columns (CollapsingMergeTree) |
| `ReplacingSortedMilliseconds` | ms | Time replacing sorted columns (ReplacingMergeTree) |
| `SummingSortedMilliseconds` | ms | Time summing sorted columns (SummingMergeTree) |
| `VersionedCollapsingSortedMilliseconds` | ms | Time version-collapsing (VersionedCollapsingMergeTree) |
| `CoalescingSortedMilliseconds` | ms | Time coalescing sorted columns |
| `GatheringColumnMilliseconds` | ms | Time gathering columns for vertical merge |

### Merge Output Format
| Event | Type | Description |
|---|---|---|
| `MergedIntoWideParts` | Number | Parts merged into Wide format |
| `MergedIntoCompactParts` | Number | Parts merged into Compact format |
| `InsertedWideParts` | Number | Parts inserted in Wide format |
| `InsertedCompactParts` | Number | Parts inserted in Compact format |

### Merge Background Executor Timing
| Event | Type | Description |
|---|---|---|
| `MergeMutateBackgroundExecutorTaskExecuteStepMicroseconds` | μs | executeStep() time |
| `MergeMutateBackgroundExecutorTaskCancelMicroseconds` | μs | cancel() time |
| `MergeMutateBackgroundExecutorTaskResetMicroseconds` | μs | Task reset time |
| `MergeMutateBackgroundExecutorWaitMicroseconds` | μs | Wait for completion time |

### Merge Selector / Coordinator
| Event | Type | Description |
|---|---|---|
| `MergerMutatorsGetPartsForMergeElapsedMicroseconds` | μs | Time to snapshot parts |
| `MergerMutatorPrepareRangesForMergeElapsedMicroseconds` | μs | Time to prepare merge ranges |
| `MergerMutatorSelectPartsForMergeElapsedMicroseconds` | μs | Time to select parts from ranges |
| `MergerMutatorRangesForMergeCount` | Number | Candidate ranges for merge |
| `MergerMutatorPartsInRangesForMergeCount` | Number | Candidate parts for merge |
| `MergerMutatorSelectRangePartsCount` | Number | Parts in selected range |

### Merge Throttling
| Event | Type | Description |
|---|---|---|
| `MergesThrottlerBytes` | Bytes | Bytes through `max_merges_bandwidth_for_server` |
| `MergesThrottlerSleepMicroseconds` | μs | Time sleeping for merge throttling |

### Merge Data Integrity
| Event | Type | Description |
|---|---|---|
| `DataAfterMergeDiffersFromReplica` | Number | Post-merge data divergence (9 possible causes listed in source) |
| `DataAfterMutationDiffersFromReplica` | Number | Post-mutation data divergence |

### Coordinated Merges (ClickHouse Cloud / SharedMergeTree)
| Event | Type | Description |
|---|---|---|
| `CoordinatedMergesMergeCoordinatorUpdateCount` | Number | Coordinator updates |
| `CoordinatedMergesMergeCoordinatorUpdateMicroseconds` | μs | Coordinator update time |
| `CoordinatedMergesMergeCoordinatorSelectMergesMicroseconds` | μs | Time selecting merges |
| `CoordinatedMergesMergeWorkerUpdateCount` | Number | Worker updates |
| `CoordinatedMergesMergeAssignmentRequest` | Number | Assignment requests |
| `CoordinatedMergesMergeAssignmentResponse` | Number | Assignment responses |

---

## 2. MUTATION PROFILING

| Event | Type | Description |
|---|---|---|
| `MutationTotalParts` | Number | Parts for which mutations were attempted |
| `MutationUntouchedParts` | Number | Parts skipped (predicate didn't match) |
| `MutationCreatedEmptyParts` | Number | Parts replaced with empty instead of mutating |
| `MutatedRows` | Number | Rows read for mutations (pre-mutation) |
| `MutatedUncompressedBytes` | Bytes | Uncompressed bytes read for mutations |
| `MutationAffectedRowsUpperBound` | Number | Upper bound of affected rows |
| `MutationTotalMilliseconds` | ms | Total mutation time |
| `MutationExecuteMilliseconds` | ms | Busy execution time for mutations |
| `MutationAllPartColumns` | Number | Times all columns were mutated |
| `MutationSomePartColumns` | Number | Times only some columns were mutated |
| `MutateTaskProjectionsCalculationMicroseconds` | μs | Projection calculation in mutations |
| `MutationsThrottlerBytes` | Bytes | Bytes through mutation throttler |
| `MutationsThrottlerSleepMicroseconds` | μs | Time sleeping for mutation throttling |
| `DelayedMutations` | Number | Mutations throttled (too many unfinished) |
| `RejectedMutations` | Number | Mutations rejected ("Too many mutations") |
| `DelayedMutationsMilliseconds` | ms | Time spent throttled |

---

## 3. INSERT PRESSURE (merge backlog indicators)

| Event | Type | Description |
|---|---|---|
| `InsertedRows` | Number | Rows inserted to all tables |
| `InsertedBytes` | Bytes | Bytes inserted (uncompressed) |
| `DelayedInserts` | Number | **Inserts throttled — too many parts** |
| `RejectedInserts` | Number | **Inserts rejected — "Too many parts" (CRITICAL)** |
| `DelayedInsertsMilliseconds` | ms | Time inserts were throttled |
| `DuplicatedInsertedBlocks` | Number | Deduplicated sync inserts |
| `DuplicatedAsyncInserts` | Number | Deduplicated async inserts |
| `DuplicationElapsedMicroseconds` | μs | Time checking for duplicates |

### MergeTree Data Writer (insert path)
| Event | Type | Description |
|---|---|---|
| `MergeTreeDataWriterRows` | Number | Rows inserted to MergeTree |
| `MergeTreeDataWriterUncompressedBytes` | Bytes | Uncompressed bytes inserted |
| `MergeTreeDataWriterCompressedBytes` | Bytes | Compressed bytes written to disk |
| `MergeTreeDataWriterBlocks` | Number | Blocks inserted (each = level-0 part) |
| `MergeTreeDataWriterBlocksAlreadySorted` | Number | Pre-sorted blocks (skip sort) |
| `MergeTreeDataWriterSkipIndicesCalculationMicroseconds` | μs | Skip index calculation time |
| `MergeTreeDataWriterSortingBlocksMicroseconds` | μs | Block sorting time |
| `MergeTreeDataWriterMergingBlocksMicroseconds` | μs | Block merging time (special engines) |
| `MergeTreeDataWriterProjectionsCalculationMicroseconds` | μs | Projection calculation time |

---

## 4. REPLICATION

| Event | Type | Description |
|---|---|---|
| `ReplicatedPartFetches` | Number | Parts downloaded from replicas |
| `ReplicatedPartFailedFetches` | Number | Failed part downloads |
| `ObsoleteReplicatedParts` | Number | Parts marked obsolete (covered by fetched part) |
| `ReplicatedPartMerges` | Number | Successful merges on replicated tables |
| `ReplicatedPartFetchesOfMerged` | Number | Downloaded pre-merged parts instead of merging locally |
| `ReplicatedPartMutations` | Number | Successful mutations on replicated tables |
| `ReplicatedPartChecks` / `ReplicatedPartChecksFailed` | Number | Part consistency checks |
| `ReplicatedDataLoss` | Number | **Parts permanently lost (CRITICAL alert)** |
| `CreatedLogEntryForMerge` / `NotCreatedLogEntryForMerge` | Number | Merge log entry creation success/conflict |
| `CreatedLogEntryForMutation` / `NotCreatedLogEntryForMutation` | Number | Mutation log entry creation success/conflict |
| `QuorumParts` / `QuorumFailedInserts` / `QuorumWaitMicroseconds` | Number/μs | Quorum insert metrics |

---

## 5. QUERY PERFORMANCE

| Event | Type | Description |
|---|---|---|
| `Query` / `SelectQuery` / `InsertQuery` | Number | Query counts by type |
| `SelectedParts` / `SelectedPartsTotal` | Number | Parts scanned vs total |
| `SelectedRanges` | Number | Non-adjacent ranges scanned |
| `SelectedMarks` / `SelectedMarksTotal` | Number | Marks read vs total |
| `SelectedRows` / `SelectedBytes` | Number/Bytes | Rows/bytes selected |
| `FilteringMarksWithPrimaryKeyMicroseconds` | μs | PK filtering time |
| `FilteringMarksWithSecondaryKeysMicroseconds` | μs | Skip index filtering time |
| `QueryPlanOptimizeMicroseconds` | μs | Query plan optimization time |

---

## 6. CPU / OS METRICS

| Event | Type | Description |
|---|---|---|
| `RealTimeMicroseconds` | μs | Wall clock time in processing threads |
| `UserTimeMicroseconds` | μs | User-space CPU time |
| `SystemTimeMicroseconds` | μs | Kernel CPU time |
| `OSIOWaitMicroseconds` | μs | IO wait (real IO, not page cache) |
| `OSCPUWaitMicroseconds` | μs | CPU scheduling wait |
| `OSCPUVirtualTimeMicroseconds` | μs | CPU time seen by OS |
| `OSReadBytes` / `OSWriteBytes` | Bytes | Disk IO (bypasses page cache) |
| `OSReadChars` / `OSWriteChars` | Bytes | Filesystem IO (includes page cache) |
| `SoftPageFaults` / `HardPageFaults` | Number | Page faults |

---

## 7. DISK / FILE IO

| Event | Type | Description |
|---|---|---|
| `FileOpen` | Number | Files opened |
| `ReadBufferFromFileDescriptorRead` / `ReadBytes` | Number/Bytes | FD reads |
| `WriteBufferFromFileDescriptorWrite` / `WriteBytes` | Number/Bytes | FD writes |
| `DiskReadElapsedMicroseconds` | μs | Read syscall time |
| `DiskWriteElapsedMicroseconds` | μs | Write syscall time |
| `ReadCompressedBytes` / `CompressedReadBufferBytes` | Bytes | Compression metrics |

---

## 8. S3 / OBJECT STORAGE

### S3 API Calls
| Event | Type | Description |
|---|---|---|
| `S3GetObject` / `S3PutObject` / `S3DeleteObjects` | Number | S3 API calls |
| `S3ListObjects` / `S3HeadObject` / `S3CopyObject` | Number | S3 metadata calls |
| `S3CreateMultipartUpload` / `S3UploadPart` / `S3CompleteMultipartUpload` | Number | Multipart upload |
| `S3ReadMicroseconds` / `S3WriteMicroseconds` | μs | S3 request time |
| `S3ReadRequestsErrors` / `S3WriteRequestsErrors` | Number | S3 errors |
| `S3ReadRequestsThrottling` / `S3WriteRequestsThrottling` | Number | S3 429/503 throttling |

### DiskS3 (same pattern, prefixed with `DiskS3`)
All of the above duplicated for disk-level S3 operations.

### S3 Read/Write Buffers
| Event | Type | Description |
|---|---|---|
| `ReadBufferFromS3Microseconds` / `ReadBufferFromS3Bytes` | μs/Bytes | S3 read buffer |
| `WriteBufferFromS3Microseconds` / `WriteBufferFromS3Bytes` | μs/Bytes | S3 write buffer |
| `ReadBufferFromS3RequestsErrors` | Number | S3 read exceptions |

---

## 9. ZOOKEEPER / KEEPER

| Event | Type | Description |
|---|---|---|
| `ZooKeeperInit` / `ZooKeeperClose` | Number | Connection lifecycle |
| `ZooKeeperTransactions` | Number | Total ZK operations |
| `ZooKeeperList` / `ZooKeeperCreate` / `ZooKeeperRemove` / `ZooKeeperGet` / `ZooKeeperSet` | Number | ZK operation types |
| `ZooKeeperMulti` / `ZooKeeperMultiRead` / `ZooKeeperMultiWrite` | Number | Compound transactions |
| `ZooKeeperWaitMicroseconds` | μs | ZK response wait time |
| `ZooKeeperBytesSent` / `ZooKeeperBytesReceived` | Bytes | ZK network traffic |
| `ZooKeeperUserExceptions` / `ZooKeeperHardwareExceptions` / `ZooKeeperOtherExceptions` | Number | ZK errors |

---

## 10. CACHES

| Event | Type | Description |
|---|---|---|
| `MarkCacheHits` / `MarkCacheMisses` | Number | Mark cache |
| `UncompressedCacheHits` / `UncompressedCacheMisses` | Number | Uncompressed cache |
| `QueryCacheHits` / `QueryCacheMisses` | Number | Query result cache |
| `QueryConditionCacheHits` / `QueryConditionCacheMisses` | Number | Query condition cache |
| `CachedReadBufferReadFromCacheHits` / `CacheMisses` | Number | Filesystem cache |
| `CachedReadBufferReadFromCacheBytes` / `FromSourceBytes` | Bytes | FS cache hit/miss bytes |

---

## 11. THREAD POOLS

| Event | Type | Description |
|---|---|---|
| `GlobalThreadPoolExpansions` / `GlobalThreadPoolShrinks` | Number | Pool sizing |
| `GlobalThreadPoolThreadCreationMicroseconds` | μs | Thread creation time |
| `GlobalThreadPoolJobWaitTimeMicroseconds` | μs | Job queue wait time |
| `LocalThreadPoolBusyMicroseconds` | μs | Actual work time |

---

## 12. PARTS LOCK CONTENTION

| Event | Type | Description |
|---|---|---|
| `PartsLockHoldMicroseconds` / `PartsLockWaitMicroseconds` | μs | Parts lock timing |
| `PartsLocks` | Number | Lock acquisitions |
| `RWLockAcquiredReadLocks` / `RWLockAcquiredWriteLocks` | Number | RW lock counts |
| `RWLockReadersWaitMilliseconds` / `RWLockWritersWaitMilliseconds` | ms | RW lock wait |

---

## WHAT WE SHOULD USE IN OUR APP

### Priority 1: Merge Health Dashboard (enhance MergeTracker)
From `system.events`:
- `Merge`, `MergedRows`, `MergedUncompressedBytes`
- `MergeTotalMilliseconds`, `MergeExecuteMilliseconds`
- `MergeHorizontalStageTotalMilliseconds`, `MergeVerticalStageTotalMilliseconds`
- `MergesRejectedByMemoryLimit`
- `DelayedInserts`, `RejectedInserts` (merge backlog canaries)
- `MergerMutatorRangesForMergeCount`, `MergerMutatorPartsInRangesForMergeCount`

From `system.metrics` (gauges):
- `DiskSpaceReservedForMerge`, `PartMutation`

From `system.asynchronous_metrics`:
- `TotalPartsOfMergeTreeTables`, `MaxPartCountForPartition`

### Priority 2: Mutation Monitoring
- `MutationTotalParts`, `MutatedRows`, `MutationTotalMilliseconds`
- `DelayedMutations`, `RejectedMutations`

### Priority 3: Query Performance Enhancement
- `SelectedParts`, `SelectedMarks`, `SelectedRows`
- `FilteringMarksWithPrimaryKeyMicroseconds`

### Priority 4: Grafana Dashboards via Prometheus
All of the above are available as Prometheus metrics. The ClickHouse built-in
dashboards already use these patterns:
- `rate(ClickHouseProfileEvents_Merge[5m])` — merge rate
- `rate(ClickHouseProfileEvents_MergeTotalMilliseconds[5m])` — merge time rate
- `rate(ClickHouseProfileEvents_DelayedInserts[5m])` — insert backpressure
- `ClickHouseMetrics_Merge` — concurrent merges gauge
- `ClickHouseAsyncMetrics_MaxPartCountForPartition` — partition health
