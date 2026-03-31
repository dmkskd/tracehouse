# Overview Page (Real-time Monitoring)

## CPU Resource Attribution

Breaks down total CPU by category: queries, merges, mutations, other.

**Total CPU:** `asynchronous_metrics` → `(OSUserTimeNormalized + OSSystemTimeNormalized) × 100`
(pre-normalized to [0..1] per core, no manual normalization needed)

**Query CPU:** computed per-query from `system.processes.ProfileEvents`:
```
For each running query:
  query_cores = (UserTimeMicroseconds + SystemTimeMicroseconds) / (elapsed × 1,000,000)
queryCpuPct = sum(query_cores) / total_cores × 100
```

Per-query cores are computed individually then summed. If you sum CPU time and elapsed across all queries first, 5 queries × 2 cores each gives `totalCpuTime / totalElapsed = 2` instead of the correct 10.

**Merge/Mutation CPU:** estimated heuristically since no direct CPU metrics exist for background operations:
- Merges: ~1.5 cores per active merge in `system.merges` (capped at 50% total)
- Mutations: ~1.0 cores per active mutation (capped at 30% total)
- Only attributed when entries exist in `system.merges`

**Other:** `Total - Queries - Merges - Mutations`

> **Why not thread pool metrics?** `MergeTreeBackgroundExecutorThreadsActive` handles merges, part moves, fetches, replication, TTL, cleanup. Using it attributes 50-80% to "merges" when none are running. Always check `system.merges` for actual merge activity.

> **Unused improvement path:** The service fetches actual CPU from `part_log` for recently completed merges/mutations (`recentMergeCPU`, `recentMutationCPU`) but doesn't use them yet. These could calibrate the heuristic estimates.

## Merge Monitoring

**Progress:** `system.merges.progress × 100` (0-100%)

**Throughput (active):** from `system.merges`:
```
read_rate  = bytes_read_uncompressed / elapsed
write_rate = bytes_written_uncompressed / elapsed
```

**Throughput (completed):** from `system.part_log`:
```
throughput = size_in_bytes / (duration_ms / 1000)
```

Note: active merges use uncompressed bytes; completed merges use compressed `size_in_bytes` (on-disk). These are different measures.

## Mutation Monitoring

**Progress:** `(total_parts - parts_to_do) / total_parts × 100`

**Stuck detection:** `elapsed > 3600s AND parts_to_do > 0` flags as potentially stuck.

## Alerts

| Alert | Source | Threshold |
| --- | --- | --- |
| Too many parts | `system.parts` count per table | > 150 parts |
| Low disk space | `system.disks` free/total | < 15% remaining |

## Query Progress

```
progress = min(100, max(0, read_rows / total_rows_approx × 100))
```

`total_rows_approx` is an estimate and may cause progress to jump or go backwards.

## Disk & Network I/O

From `system.metric_log` ProfileEvents, converted to rates:

```
disk_read_rate  = OSReadBytes / interval_seconds
disk_write_rate = OSWriteBytes / interval_seconds
net_send_rate   = NetworkSendBytes / interval_seconds
net_recv_rate   = NetworkReceiveBytes / interval_seconds
```

> **Tests:** `overview-metrics-collector.integration.test.ts` → "Disk I/O rates", "Network I/O rates"
