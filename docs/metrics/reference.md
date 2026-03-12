# Reference

## System Tables

| Table | What it stores | Key detail |
| --- | --- | --- |
| `system.metric_log` | Periodic snapshots (default 1s). `ProfileEvent_*` = deltas, `CurrentMetric_*` = point-in-time | Interval can vary; use `lagInFrame()` for actual interval |
| `system.asynchronous_metric_log` | Async metrics history (CPU, memory, cores) | Values are already rates (e.g. seconds/second for CPU) |
| `system.asynchronous_metrics` | Current async metric values (no history) | Fallback when log tables lack data |
| `system.query_log` | Per-query stats after completion | `memory_usage` = peak, `ProfileEvents` = final counters |
| `system.processes` | Running queries | `ProfileEvents` update live during execution |
| `system.merges` | Active merge operations | `progress` is 0.0–1.0. `is_mutation` distinguishes merges from mutations |
| `system.mutations` | Active + completed mutations | `parts_to_do` = remaining work |
| `system.parts` | All data parts | `bytes_on_disk` = compressed size |
| `system.part_log` | Part operation history (create, merge, mutate, drop) | `size_in_bytes` = compressed. `read_bytes` = UNCOMPRESSED (don't use for size!) |
| `system.trace_log` | Profiler stack trace samples | Buffered in memory, flushed ~60s. May lag under load. |

## ProfileEvents

### CPU
| Event | Unit | Description |
| --- | --- | --- |
| `OSCPUVirtualTimeMicroseconds` | µs | Total CPU time (user + system + wait) |
| `UserTimeMicroseconds` | µs | User mode CPU |
| `SystemTimeMicroseconds` | µs | Kernel mode CPU |
| `RealTimeMicroseconds` | µs | Wall clock time |
| `OSCPUWaitMicroseconds` | µs | Time waiting for CPU |

### I/O
| Event | Unit | Description |
| --- | --- | --- |
| `OSReadBytes` / `OSWriteBytes` | bytes | OS-level disk I/O |
| `ReadBufferFromFileDescriptorReadBytes` | bytes | FD-level read |
| `WriteBufferFromFileDescriptorWriteBytes` | bytes | FD-level write |
| `OSIOWaitMicroseconds` | µs | Time waiting for I/O |
| `DiskReadElapsedMicroseconds` / `DiskWriteElapsedMicroseconds` | µs | Disk operation time |

### Network
| Event | Unit | Description |
| --- | --- | --- |
| `NetworkSendBytes` / `NetworkReceiveBytes` | bytes | Network I/O |
| `NetworkSendElapsedMicroseconds` / `NetworkReceiveElapsedMicroseconds` | µs | Network time |

### Query Processing
| Event | Unit | Description |
| --- | --- | --- |
| `SelectedParts` / `SelectedPartsTotal` | count | Parts scanned vs total |
| `SelectedMarks` / `SelectedMarksTotal` | count | Marks scanned vs total |
| `SelectedRanges` / `SelectedRows` / `SelectedBytes` | count/bytes | Ranges, rows, bytes selected |

### Cache
| Event | Unit | Description |
| --- | --- | --- |
| `MarkCacheHits` / `MarkCacheMisses` | count | Mark cache |
| `UncompressedCacheHits` / `UncompressedCacheMisses` | count | Uncompressed block cache |
| `QueryCacheHits` / `QueryCacheMisses` | count | Query result cache |

## Common Gotchas

1. **metric_log interval normalization.** Collection interval is typically 1s but can vary. Calculate actual interval with `lagInFrame()`. First row in a window references data outside, producing invalid values; filter with `interval_ms > 0 AND interval_ms < 10000`.

2. **Logical vs physical cores.** Use `NumberOfCPUCores` (logical) for CPU %. In k8s, neither is correct; both report host cores. See [cpu.md](cpu.md) for cgroup detection.

3. **Timestamps.** ClickHouse returns timestamps without timezone. Always append 'Z' or handle as UTC.

4. **Server restart.** Counters reset. First few data points may be anomalous.

5. **S3/Parquet queries.** External table queries don't have MergeTree ProfileEvents (`SelectedParts`, `SelectedMarks`, etc.).

6. **ProfileEvents: running vs completed.** Running queries (`processes`) have live-updating ProfileEvents. Completed queries (`query_log`) have final values.

7. **Thread pool metrics are misleading for attribution.** `MergeTreeBackgroundExecutorThreadsActive` handles merges, moves, fetches, TTL, cleanup. Check `system.merges` for actual merge activity.

8. **trace_log flush lag.** Buffered in memory, flushed ~60s. Under heavy CPU load, flush intervals stretch to 30-60s+. Use 180s windows. Don't use `SYSTEM FLUSH LOGS` as a workaround.
