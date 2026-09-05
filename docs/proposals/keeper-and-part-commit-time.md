# Proposal: Keeper and part-commit time

**Status:** draft, not implemented. Prompted by a ClickHouse community thread on
26.7 write timeouts, 2026-09-04.

Measurements below are from the local 4-node dev cluster running 26.7.1.1315,
over a 24-hour window, unless stated otherwise. They are illustrative of the
shape of the problem, not of any production workload. The cluster ingests
continuously, so each table here is a single query taken at its own moment and
absolute counts differ between them; the ratios are the point.

Related: [wait-vs-work-breakdown.md](wait-vs-work-breakdown.md) built the
five-segment composition and its three layers of explaining "Parked", all of
which read query-scoped logs. This proposal is about time that never appears in
the query's own thread accounting at all.
[distributed-query.md](distributed-query.md) owns the Distributed tab this
would extend.

---

## 1. The failure mode we are blind to

A user reports INSERT timeouts after upgrading 25.3 to 26.7. They check query
compute time: minimal. They check Keeper wait: minimal. Then they look at
`system.part_log` and find `NewPart` events with `duration_ms` above 50 seconds
for fewer than 10 rows and a few KB.

The answer from ClickHouse was that `NewPart.duration_ms` has a wider meaning in
26.7: it additionally covers finalization, part commit and deduplication, cache
prewarming, and, on ReplicatedMergeTree, quorum waiting. The total client wait
is therefore:

```
buffering + async-worker queueing + part creation + views + replication/quorum
```

TraceHouse today shows the first and, partially, the second. It shows nothing
about the rest.

Concretely, for a Distributed INSERT the Distributed tab reports each node's
`query_duration_ms` and a coordinator overhead figure. If the cost were sitting
in part commit or quorum, that tab would look identical to a healthy one.

## 2. Where Keeper is on the critical path

Which operations actually talk to Keeper, measured by `ZooKeeperTransactions`:

| Query kind | Queries | ZK transactions | Per query |
|---|---|---|---|
| `AsyncInsertFlush` | 2,450 | 4,920 | 2.0 |
| `Insert` | 18,149 | 3,554 | 0.2 |
| `Create` (DDL) | 233 | 1,999 | 8.6 |
| `Select` | 5,056 | 36 | 0.007 |

| Part event | Events | p99 `duration_ms` | ZK transactions | Per event |
|---|---|---|---|---|
| `RemovePart` | 12,776 | 0 | 0 | 0 |
| `NewPart` | 12,662 | 2,238 | 8,474 | 0.7 |
| `DownloadPart` | 4,215 | 1,199 | 20,665 | 4.9 |
| `MergePartsStart` | 3,207 | 0 | 0 | 0 |
| `MergeParts` | 3,195 | 81,683 | 5,852 | 1.8 |

Reading this:

- **Inserts** are the headline case, and **async insert flush** is heavier per
  operation than a plain insert: the flush is where the block is deduplicated
  and the part is committed. A client using `wait_for_async_insert=1` waits for
  this, and it is a different query row from the one the client submitted.
- **Replication fetches** (`DownloadPart`) are the heaviest Keeper user per
  event by a wide margin. A replica catching up competes for the same Keeper
  that inserts are committing through.
- **Merges** touch Keeper on both assignment and commit, and have by far the
  longest tail: a p99 of 81.7 seconds against 2.2 seconds for `NewPart`.
- **DDL** is low volume and high per-operation cost, which is why one
  `ON CLUSTER` statement can stall behind a busy Keeper.
- **Selects** are effectively absent. Keeper is a write-path and metadata-path
  concern, so this work belongs in the insert, merge and replication surfaces,
  not in the SELECT analysis path.

The practical consequence: these paths share one Keeper. Diagnosing an insert
stall means being able to see the merge and fetch pressure around it, not just
the insert.

## 3. Why the existing breakdown cannot see it

`computeTimeBreakdown` has five buckets: CPU, disk wait, CPU wait, network wait,
and `unaccounted`, defined as `RealTimeMicroseconds` minus the others. Keeper
waiting, part commit, deduplication and quorum all land in `unaccounted` with no
name.

The three layers in `wait-vs-work-breakdown.md` do not reach it either. All
three are scoped to the query's own execution: `query_log` ProfileEvents,
`processors_profile_log` per-processor waits, and `trace_log` sampled stacks.
Part commit is recorded against the *part*, in a different table, with its own
duration. It is a fourth source rather than a fourth layer.

Nothing in the query path currently reads it. `system.part_log` is queried in
seven places in `packages/core/src/queries/`, but only for sizes and counts:
`lineage-queries.ts` reads `NewPart` for part sizes, `surface-queries.ts` counts
them. `duration_ms` and part-level `ProfileEvents` are never read.

## 4. The signal, and that it is already joinable

`system.part_log` carries `query_id` on `NewPart` rows, so parts join back to
the query that created them. On this cluster, `query_id` is populated on every
slow `NewPart` sampled.

The slowest local examples:

| database | table | rows | size | `duration_ms` |
|---|---|---|---|---|
| `web_analytics` | `pageviews_local` | 98,206 | 20.18 MiB | 8,952 |
| `nyc_taxi` | `trips_local` | 226,359 | 5.14 MiB | 5,233 |
| `replacing_test` | `product_prices_local` | 230,051 | 4.26 MiB | 4,656 |

For the 8,952 ms part, its own `*Microseconds` counters explain roughly half a
second:

```
MergeTreeDataWriterSortingBlocksMicroseconds   367.1 ms
DiskWriteElapsedMicroseconds                   175.6 ms
PartsLockHoldMicroseconds                        0.2 ms
```

## 5. What we cannot build on

The obvious counter is not dependable. On this cluster, over 24 hours:

- `ZooKeeperWaitMicroseconds` exists in `system.events` on 26.7, but **zero**
  `query_log` rows carry it above zero.
- **Zero of 13,445** `NewPart` rows carry it in their ProfileEvents map at all.

A panel keyed on `ZooKeeperWaitMicroseconds` would render empty here and read as
broken rather than as healthy. The same caution applies to `QuorumWait*`, which
only appears under `insert_quorum > 1`.

This matters for the 26.7 change specifically: the community answer points at
[PR #93356](https://github.com/ClickHouse/ClickHouse/pull/93356), which made
async inserts wait for replicas when `insert_quorum > 1`. That wait is inside
`NewPart.duration_ms`, and on a deployment where the detailed counters are
absent, the duration is the only evidence of it.

## 6. The durable primitive

```
unexplained_commit_ms = duration_ms - sum(part's own *Microseconds counters)
```

Measured across 13,445 `NewPart` events in 24 hours, summing each part's own
`*Microseconds` counters from its ProfileEvents map:

| Metric | Value |
|---|---|
| p50 `duration_ms` | 21 ms |
| p99 `duration_ms` | 2,342 ms |
| max `duration_ms` | 8,952 ms |
| Mean share of duration unexplained by the part's own counters | **74.8%** |

```sql
SELECT
  count(),
  round(quantile(0.99)(duration_ms), 0) AS p99_ms,
  round(avgIf(greatest(duration_ms - attributed_ms, 0) / duration_ms,
              duration_ms > 0) * 100, 1) AS avg_unattributed_pct
FROM (
  SELECT duration_ms,
         arraySum(arrayMap((k, v) -> if(endsWith(k, 'Microseconds'), v, 0),
                  mapKeys(ProfileEvents), mapValues(ProfileEvents))) / 1000 AS attributed_ms
  FROM system.part_log
  WHERE event_type = 'NewPart' AND event_date >= today() - 1
)
```

Note the method: summing the counters via `arrayMap` over the map rather than
`ARRAY JOIN`. An `ARRAY JOIN` both multiplies rows and silently drops parts
whose ProfileEvents map is empty, which is the population this measurement is
most interested in.

This degrades honestly in both directions. Where detailed counters exist, they
are attributed and the residual shrinks. Where they do not, the residual still
states that the time was spent, which is the fact the user in the thread needed
and could not get from the query.

It is the same discipline as the existing breakdown: name the residual, never
apportion it to a cause the data does not support.

## 7. Proposed surfaces

**Phase 1 - a part-commit stage in the Distributed Flow view.** The INSERT path
currently ends at "Remote table INSERT" per node. Joining `part_log` on
`query_id` adds a third rank: coordinator, node INSERT, parts committed. Commit
duration sits on the part node, and the residual from section 6 sits on the
edge. The flow layout already supports additional ranks, sub-columns and edge
labels, so this is data plumbing rather than new geometry.

**Phase 2 - name the residual in the composition.** For inserts, split
`unaccounted` into commit-and-Keeper versus the rest, fed by whichever counters
are present. The distributed topology query already selects the entire
`ProfileEvents` map for every node, so no new query is needed on the query side.

**Phase 3 - decompose coordinator overhead.** The Distributed tab prints a bare
coordinator overhead figure. With part-commit data joined, it can say what that
overhead consisted of instead of only how large it was.

**Phase 4 - the async insert path.** `AsyncInsertFlush` is a separate query row
from the client's insert, and the topology already models the link through
`asyncInsertLinks`. A client waiting on `wait_for_async_insert=1` is waiting for
the flush row's part commit, which is two hops from the query they submitted.
This is the case the thread describes and the one the current UI explains least.

## 8. Capability gating

`part_log` is not guaranteed to be enabled, and the per-part ProfileEvents map
is version dependent. This must go through the existing capability mechanism
(`DistributedTopologyCapabilities`, `monitoring-capabilities`), degrading to
today's view rather than showing an empty panel, in line with the rule already
established in `wait-vs-work-breakdown.md`.

## 9. Open questions

- Merges have a p99 of 81.7 seconds here and their own Keeper traffic. Does
  merge commit time belong in this same surface, or in the merge tracker?
- `DownloadPart` is the heaviest per-event Keeper user and has no query to hang
  off. Where does replication-fetch pressure surface, given it is often the
  cause of an insert stall rather than a symptom?
- Is there a defensible way to show Keeper contention *between* these paths, or
  does that require `system.zookeeper_log` and a separate proposal?
- How far back is `NewPart.duration_ms` comparable? The 26.7 semantics change
  means a duration from 25.3 and one from 26.7 do not mean the same thing, which
  matters for any historical comparison TraceHouse draws.

## 10. References

- ClickHouse PR [#93356](https://github.com/ClickHouse/ClickHouse/pull/93356),
  quorum with async inserts.
- Community thread, 2026-09-04: 26.7 write timeouts with low compute and high
  `NewPart` duration; guidance to array-join `part_log` ProfileEvents and
  compare `QuorumWaitMicroseconds` against `ZooKeeperWaitMicroseconds`, plus
  object-storage write latency via `DiskS3WriteMicroseconds`,
  `DiskAzureWriteMicroseconds` and `WriteBufferFromS3Microseconds`.
