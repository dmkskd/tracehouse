# ClickHouse Compatibility Matrix

This document records findings from the pinned ClickHouse compatibility matrix.
It is an evidence log, not a promise that TraceHouse supports every version in
the matrix.

The matrix can include regression checkpoints within the supported range, but
the default release gate stops at the ClickHouse 23.8 release line. Older
versions can still be run explicitly to establish a boundary without requiring
production queries or the test harness to be rewritten for an unsupported
server.

## How to update this matrix

Run the matrix from newest to oldest:

```bash
./scripts/test-clickhouse-matrix.sh
```

Or run one exact image:

```bash
./scripts/test-clickhouse-version.sh \
  clickhouse/clickhouse-server:24.8.14.39-alpine
```

For every run recorded here, include:

- the exact image tag;
- the date and final test counts;
- the smallest root-cause set, rather than every downstream failure;
- whether the result is a product incompatibility, an optional capability, or
  a test defect;
- the chosen action: fix, capability fallback, minimum-version annotation, or
  accepted unsupported version.

Before changing production code for a compatibility failure:

1. Establish what the affected field or feature means and find its consumers.
2. Inspect the actual schema or behavior on the failing version, the adjacent
   matrix checkpoint, and the current supported checkpoint. Do not infer a
   compatibility rule from one failing image.
3. Classify the difference as a rename, semantic change, optional capability,
   intentional version boundary, or test/infrastructure defect.
4. Choose removal, capability substitution, semantic fallback, or a
   minimum-version annotation. A fallback is valid only when its semantics and
   units are explicitly shown to match.
5. Record the evidence and intended action in this ledger before applying the
   production change.
6. Run focused tests on both sides of the compatibility boundary. A change
   that fixes only the originally failing image is not accepted.

Detailed Vitest reports are written to
`packages/core/test-reports/integration-results.json` and
`packages/core/test-reports/integration-html/`. These paths are overwritten by
the next run, so durable findings must be summarized here before rerunning.

## Current matrix checkpoint

Last updated: 2026-07-30.

| Image | Result | Evidence |
|---|---|---|
| `26.7.1.1315-alpine` | Passed | The 2026-07-30 fail-fast matrix advanced to the next image. |
| `26.3.17.56-alpine` | Passed | The 2026-07-30 fail-fast matrix advanced to the next image. |
| `25.8.28.1-alpine` | Passed | The 2026-07-30 fail-fast matrix advanced to the next image. The focused HTTP compression suite also passed 5/5. |
| `25.3.14.14-alpine` | Passed | The 2026-07-30 fail-fast matrix advanced to the next image. |
| `24.8.14.39-alpine` | Passed | The complete 2026-07-30 compatibility run passed and advanced to 24.3. The earlier 430 passed, 22 failed, 369 skipped run established findings CH-COMPAT-001 through CH-COMPAT-003. |
| `24.3.18.7-alpine` | Passed | Core passed with 723 passed and 98 intentional skips. Proxy passed 23/23. After CH-COMPAT-011 and CH-COMPAT-012, data-utils passed with 86 passed and 16 intentional skips. E2E passed in direct-CORS mode with 23 passed and 5 intentional mobile skips. |
| `23.8.16.40-alpine` | Passed | The complete fail-fast version runner passed and advanced to the early 23.8 checkpoint. Core passed with 718 passed and 105 intentional skips; E2E passed with 23 passed and 5 intentional mobile skips through the runtime-selected proxy fallback. |
| `23.8.2.7-alpine` | Passed | The complete fail-fast version runner passed and advanced to 23.3. This exact early 23.8 regression checkpoint also established CH-COMPAT-001 and CH-COMPAT-004. |
| `23.3.22.3-alpine` | Unsupported; removed from default matrix | The exploratory run failed before meaningful feature coverage because the expected `test` database was unavailable across shared setup hooks. See CH-COMPAT-016. |
| `22.8.21.38-alpine` | Unsupported; not run | Below the adopted 23.8 support floor and removed from the default matrix. It remains available as an explicit one-off test argument. |

## Feature availability matrix

This table is the product-facing summary. `Full` means the feature executes
with its complete semantics. `Degraded` means TraceHouse preserves the useful
subset and tells the UI what is unavailable. `UI-gated` means the query is not
sent to ClickHouse and the UI shows the required and connected versions.
`Feature-gated` / `Test-gated` means a non-dashboard feature or its dedicated
tests are intentionally not attempted. `Runtime-probed` means configuration
and permissions can matter independently of the server version.

The cells describe the exact pinned checkpoints, not every patch release in a
minor line. A `?` would mean that the checkpoint has not been measured; do not
fill such a cell from release-date assumptions.

| TraceHouse feature / server facility | `23.8.16.40` | `24.3.18.7` | `24.8.14.39` | `25.3.14.14` | TraceHouse behavior / evidence |
|---|---|---|---|---|---|
| Core query detail | Full | Full | Full | Full | Unused `used_privileges`, `missing_privileges`, and `peak_threads_usage` fields are not selected. See CH-COMPAT-007 and CH-COMPAT-014. |
| Async Insert Log preset (`data_kind`) | UI-gated | Full | Full | Full | Requires `>=24.3`; unsupported panels show the compatibility reason and do not run. |
| Distribution Insert Pressure preset | UI-gated | Full | Full | Full | Requires `>=24.3` because the 23.8 metric-log failure counter is absent. |
| JSON Columns Inventory / Subcolumn Pressure | UI-gated | Full | Full | Full | Requires `>=24.3`; 23.8 exposes nested arrays but cannot analyze these join shapes. |
| Mutation tracking | Degraded | Full | Full | Full | Runtime schema probe. On 23.8, active/completed mutation data remains available, `is_killed` falls back to false, and the UI warns that killed-state cannot be distinguished. |
| Merge Duration (avg) imported preset | UI-gated | UI-gated | Full | Full | Requires `>=24.8`; the metric was renamed from `MergesTimeMilliseconds` to `MergeTotalMilliseconds`. See CH-COMPAT-009. |
| Merge wait-time analytics | UI-gated | UI-gated | UI-gated | Full | Requires `>=24.12`; older checkpoints require an experimental non-equality JOIN setting. See CH-COMPAT-002. |
| Refreshable sampler DDL | Feature-gated | Feature-gated | Feature-gated | Full | Sampling tests are intentionally skipped below the established 25.3 checkpoint. See CH-COMPAT-001. |
| Patch-part demo settings | ? | Degraded | Full | Full | Ordinary datasets work on the measured checkpoints; hidden patch-part columns are enabled only when both MergeTree settings exist. The 23.8 data-utils run is pending. See CH-COMPAT-011. |
| Replicated database engine data-utils tests | ? | Test-gated | Full | Full | Requires the tested 24.8 boundary; 24.3 keeps the engine experimental and disabled. The 23.8 data-utils run is pending. See CH-COMPAT-012. |
| Expression-based event `LIMIT BY` | Degraded | Full | Full | Full | Early 23.8 falls back to a strict global limit; later checkpoints keep per-group limiting. See CH-COMPAT-004. |
| Optional system logs and metric columns | Runtime-probed | Runtime-probed | Runtime-probed | Runtime-probed | Availability is detected from `system.tables` / `system.columns`, not inferred only from version. See CH-COMPAT-005. |
| Read-only access to `system.user_directories` | Known upstream exposure | Blocked | Blocked | Blocked | The exact 23.8 limitation is asserted and reported; it is reproducible without TraceHouse. See CH-COMPAT-013. |
| Read-only access to own `system.quota_usage` | Denied by CH | Full | Full | Full | 23.8 requires `SHOW QUOTAS`; the matrix records the upstream authorization transition. |
| Structured HTTP query-error classification | Full | Full | Full | Full | Numeric ClickHouse error codes are classified as query errors; message matching remains a fallback. |
| Direct browser HTTP CORS (stock image) | Unavailable; proxy fallback | Full | Full | ? | Runtime `OPTIONS` probe. TraceHouse remains usable through its proxy when the server does not return CORS headers. See CH-COMPAT-015. |

For runtime-probed cluster facilities, a heterogeneous cluster is considered
capable only when every queried host exposes the required column or setting.

## Findings

### CH-COMPAT-001: refreshable materialized-view sampler DDL

**Classification:** feature minimum-version boundary.

**Observed on:** `24.8.14.39-alpine` and `23.8.2.7-alpine`.

The sampling installer generates refreshable materialized views:

```sql
CREATE MATERIALIZED VIEW ...
REFRESH EVERY 1 SECOND
APPEND
TO tracehouse.processes_history_buffer
AS ...
```

Both versions reject this syntax with code 62. On 24.8 the parser fails at
`APPEND`; on the early 23.8 build it fails at `REFRESH`.

In the 24.8 matrix run this single incompatibility caused:

- 16 direct failures across sampling setup, cluster setup, and capability tests;
- 369 downstream skips in analytics smoke, merge-sampling, process-history, and
  process-X-ray suites because their setup hooks could not create the samplers.

The same matrix passed 25.3, so the supported boundary is known to be later than
24.8 and no later than the tested 25.3 checkpoint. The exact first ClickHouse
release supporting this DDL has not yet been established by this matrix.

**Action:** do not enable unrelated server settings to force old versions
through the suite. Treat sampling as unavailable below the established minimum,
or explicitly add an older sampling implementation if supporting those versions
becomes a product requirement.

### CH-COMPAT-002: non-equality conditions in JOIN ON

**Classification:** analytics-query minimum-version boundary.

**Observed on:** `24.8.14.39-alpine`.

Four merge-wait analytics tests fail because their JOIN includes:

```sql
c.event_time <= s.merge_time
```

ClickHouse 24.8 reports that the JOIN expression contains columns from both
sides and suggests the experimental
`allow_experimental_join_condition = 1` setting.

Affected presets:

- `Part Wait Time by Table`
- `Part Wait Time by Size`
- `Part Wait Timeline`
- the table → size bucket → timeline drill chain

The matrix passed 25.3, so these queries are known to work at that checkpoint.

**Action:** prefer a minimum-version annotation for these modern analytics
queries. Do not globally enable an experimental server setting merely to make
the 24.8 matrix green.

### CH-COMPAT-003: empty-string comparison against merge_reason Enum

**Classification:** query compatibility defect with a straightforward fix.

**Observed on:** `24.8.14.39-alpine`.

Two `MergeTracker` tests fail only for the `Regular` category. The pushed
predicate is:

```sql
event_type = 'MergeParts'
AND (merge_reason = 'RegularMerge' OR merge_reason = '')
```

On 24.8, `system.part_log.merge_reason` is:

```text
Enum8(
  'NotAMerge' = 1,
  'RegularMerge' = 2,
  'TTLDeleteMerge' = 3,
  'TTLRecompressMerge' = 4
)
```

Comparing that Enum to `''` fails with code 32:

```text
ATTEMPT_TO_READ_AFTER_EOF:
Attempt to read after eof while converting '' to Enum8(...)
```

Affected tests:

- `post-filter preserves SQL-pushed Regular category rows`
- `category filter respects limit (limit applies after filter)`

**Action:** make the legacy empty-value branch type-safe, for example by
comparing `toString(merge_reason)` when the empty-string fallback is required,
or by capability/version-selecting the appropriate predicate.

### CH-COMPAT-004: LIMIT BY expression on early 23.8

**Classification:** capability fallback.

**Observed on:** `23.8.2.7-alpine`.

The events query used:

```sql
LIMIT 100 BY if(type = 'QueryFinish', 'ddl', 'query_resource')
```

ClickHouse 23.8.2.7 parses `LIMIT BY` but fails during execution with code 8,
reporting that it cannot find the derived `equals(type, 'QueryFinish')` column
in the source stream. The same query shape works on later builds.

**Action:** capability detection disables expression-based `LIMIT BY` for this
server and falls back to a global `LIMIT 100`. This preserves a strict transfer
cap without claiming all `LIMIT BY` syntax is absent.

### CH-COMPAT-005: optional system tables and columns

**Classification:** runtime capability, not a pure version boundary.

System log tables and metric columns can be absent because of server
configuration as well as ClickHouse version. Examples encountered during
matrix work include:

- `system.zookeeper_connection_log`
- `system.background_schedule_pool_log`
- `system.trace_log`
- `CurrentMetric_MarkCacheBytes`
- `CurrentMetric_UncompressedCacheBytes`

**Action:** smoke tests and application capabilities should inspect actual
`system.tables` and `system.columns`. Optional facilities should be skipped or
shown as unavailable; their absence should not be treated as proof that the
entire ClickHouse version is unsupported.

### CH-COMPAT-006: matrix test defects fixed during compatibility work

**Classification:** test defects, not ClickHouse incompatibilities.

- HTTP compression compared two 3,000-row results generated with separate
  `now()` calls. A one-second boundary changed every timestamp and produced a
  huge diff. The fixture now uses a fixed timestamp and reports only the first
  mismatched row. Verified 5/5 on `25.8.28.1-alpine`.
- Primary-key memory used a nondeterministic aggregate across replica rows. The
  query and fixture were made deterministic.
- Timeline tests assumed CPU cores must always be greater than zero instead of
  checking that filtering leaves server metrics unchanged.
- Capability tests assumed standard log tables always exist instead of
  comparing detected flags with the server's actual tables.

These fixes must not be recorded as expanded ClickHouse support: they only make
the matrix accurately report product compatibility.

### CH-COMPAT-007: unused query-log privilege metadata

**Classification:** unused optional columns removed from the product query.

**Observed on:** `24.3.18.7-alpine`.

`system.query_log` on this checkpoint does not expose:

- `used_privileges`
- `missing_privileges`

Selecting them unconditionally caused both `QueryAnalyzer.getQueryDetail`
integration tests to fail with code 47. These columns describe access checks
used by a query and privileges missing from a rejected query, so they are
potentially useful for permission auditing and authorization diagnosis.

TraceHouse does not currently consume either field beyond selecting them into
the `QueryDetail` response and declaring them on its TypeScript interface. No
service, mapper, UI component, or test uses their contents.

**Action:** remove both fields from the query and `QueryDetail` interface. This
avoids a runtime probe and compatibility branch for data with no consumer while
preserving every field used by the product.

If TraceHouse later adds a privilege-auditing or authorization-diagnostics
feature, reintroduce these fields behind a `query_log_privilege_metadata`
capability that probes `system.columns` across every targeted host. Do not gate
that future feature on a hard-coded version alone: system-log schemas can vary
with version, configuration, and partial cluster rollouts.

### CH-COMPAT-008: concurrent first pull corrupts Docker test setup

**Classification:** matrix infrastructure defect, not a ClickHouse
incompatibility.

**Observed on:** first local runs of `25.3.14.14-alpine` and
`24.3.18.7-alpine`.

Vitest starts up to ten Testcontainers workers. When an image is not cached,
those workers can ask Docker Desktop/containerd to pull and commit the same
manifest concurrently. The observed failures were:

```text
lease does not exist
failed commit on ref ... rename .../ingest/.../data ... no such file or directory
```

The affected suites failed in setup and skipped their assertions. A focused
rerun passed once the image was cached, confirming this was not a product
regression.

**Action:** the per-version runner first uses `docker image inspect` to reuse an
already cached exact-version image without touching containerd's pull leases.
When the image is absent, it performs one foreground `docker pull` before
starting any parallel test worker. If that pull reports a lease error, the
runner inspects once more and continues only when the requested image is now
provably available locally.

### CH-COMPAT-009: merge-duration ProfileEvent name

**Classification:** imported analytics-query minimum-version boundary.

**Observed on:** `24.3.18.7-alpine`, `24.8.14.39-alpine`, and
`25.3.14.14-alpine`.

The `Merge Duration (avg)` preset selected
`ProfileEvent_MergeTotalMilliseconds` from `system.metric_log`. ClickHouse 24.3
does not expose that flattened column and reports
`ProfileEvent_MergesTimeMilliseconds` as the available metric.

The exact ClickHouse sources define both names as the total time spent on
background merges:

- [24.3 defines `MergesTimeMilliseconds`](https://github.com/ClickHouse/ClickHouse/blob/v24.3.18.7-lts/src/Common/ProfileEvents.cpp);
- [24.8 defines `MergeTotalMilliseconds`](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/Common/ProfileEvents.cpp);
- [25.3 retains `MergeTotalMilliseconds`](https://github.com/ClickHouse/ClickHouse/blob/v25.3.14.14-lts/src/Common/ProfileEvents.cpp).

`ProfileEvent_Merge`, the denominator in the preset, remains the number of
launched background merges. The numerator therefore has compatible meaning
and units across the rename, but its flattened `system.metric_log` column name
does not. The first release containing the rename has not yet been established;
the tested boundary is after 24.3 and no later than 24.8.

**Action:** do not replace either name globally and do not add runtime
substitution machinery for this nonessential imported dashboard panel. Keep
the current `ProfileEvent_MergeTotalMilliseconds` query and annotate it with
`-- @requires: clickhouse>=24.8`. The compatibility smoke suite then records an
intentional skip with the required and running versions on 24.3, while 24.8
and newer continue to execute the query. Dashboard panels, previews, and Query
Explorer evaluate the same directive against the probed server version before
execution. An unsupported query is not sent to ClickHouse; the UI instead
shows the required and connected versions. If the server version cannot be
established, required queries fail closed with an explicit compatibility
message.

### CH-COMPAT-010: duplicate JSON subcolumn alias

**Classification:** cross-version SQL aliasing defect.

**Observed on:** `24.3.18.7-alpine`.

The `JSON Subcolumn Storage` preset used `subcolumn_type` as both the
`ARRAY JOIN` alias for `subcolumns.types` and the output alias for
`any(subcolumn_type)`. ClickHouse 24.3 rejects this with code 179
(`MULTIPLE_EXPRESSIONS_FOR_ALIAS`), while newer checkpoints accepted it.

**Action:** name the expanded source value `subcolumn_type_value` and retain
`subcolumn_type` only as the aggregate output alias. No data or feature is
removed.

### CH-COMPAT-011: optional patch-part MergeTree settings

**Classification:** optional table-setting capability.

**Observed on:** `24.3.18.7-alpine` and `24.8.14.39-alpine`.

The synthetic and replacing-merge demo tables unconditionally enabled:

```sql
enable_block_number_column = 1
enable_block_offset_column = 1
```

ClickHouse 24.3 exposes neither name in `system.merge_tree_settings` and rejects
the table DDL with code 115. ClickHouse 24.8 exposes both settings. These hidden
columns are not needed to create, populate, query, or remove either dataset.
They prepare the tables for the beta patch-part `UPDATE` workload, which is a
separate optional feature.

**Action:** inspect `system.merge_tree_settings` before generating the DDL.
Enable the pair only when both settings exist; otherwise omit both and preserve
the ordinary dataset behavior. Metadata-probe failures also omit the optional
settings rather than preventing table creation. Verification: data-utils
passed with 86 passed and 16 skipped on 24.3, and with 99 passed and 3 skipped
on 24.8.

### CH-COMPAT-012: Replicated database engine is experimental on 24.3

**Classification:** data-utils feature minimum-version boundary.

**Observed on:** `24.3.18.7-alpine` and `24.8.14.39-alpine`.

The exact server settings report:

```text
24.3.18.7  allow_experimental_database_replicated = 0
24.8.14.39 allow_experimental_database_replicated = 1
```

Cluster data-utils tests that use `ENGINE = Replicated(...)` therefore fail on
24.3 with code 336. Those tests specifically verify Replicated-database DDL
propagation. Substituting `Atomic` would test different behavior, and enabling
an experimental setting solely for the matrix would hide the production
boundary.

**Action:** mark only the Replicated-database-dependent tests as requiring the
tested ClickHouse 24.8 boundary. Continue running the cluster tests that use
ordinary databases on 24.3. Supporting sharded dataset creation on an older
server would require an explicit Atomic-database/`ON CLUSTER` implementation,
not a test-only setting. The 24.8 verification executed and passed all of the
Replicated-database tests.

### CH-COMPAT-013: upstream read-only system-table visibility on 23.8

**Classification:** known upstream ClickHouse authorization limitation, not a
TraceHouse authorization bypass.

**Observed on:** `23.8.16.40-alpine` and `24.3.18.7-alpine`.

The security suite executes the demo's production `read_only` user setup. On
23.8, that user can read `system.user_directories` without a grant. The same
behavior is reproducible directly with `clickhouse-client`, without TraceHouse
in the request path. The table reveals the configured access-control backends
and their filesystem paths:

```text
users_xml       {"path":"/etc/clickhouse-server/users.xml"}
local_directory {"path":"/var/lib/clickhouse/access/"}
```

Granting and then explicitly revoking `SELECT` on the table does not remove
this implicit 23.8 access. The same query on 24.3 fails with code 497 and asks
for `SELECT(name, type, params, precedence) ON system.user_directories`.

The inverse transition applies to `system.quota_usage`: 23.8 requires
`SHOW QUOTAS ON *.*`, while 24.3 allows the user to inspect its own low-risk
quota usage without that grant. These two failing assertions therefore describe
one ClickHouse authorization-behavior transition, not two missing demo grants.

TraceHouse does not grant this access and cannot revoke it on that server. The
exposure contains backend names and paths, not the contents of the referenced
files or credentials.

**Action:** keep both sides visible in the security report instead of deleting
the test:

- on the exact pinned 23.8 checkpoints, run a test named
  `KNOWN CLICKHOUSE 23.8 LIMITATION` that confirms the upstream visibility;
- skip the stronger "must be blocked" assertion there with the version encoded
  in the test condition;
- on other versions, skip the known-limitation observation and continue to
  require that `system.user_directories` is blocked;
- likewise, assert that 23.8 denies `system.quota_usage` without `SHOW QUOTAS`,
  while later versions retain the per-user accessibility assertion.

This allows the compatibility matrix to proceed without describing the
upstream behavior as a TraceHouse defect or silently erasing the security
observation. The exception is intentionally scoped to the observed 23.8 line;
older matrix checkpoints must establish their own behavior rather than inherit
an unverified blanket exemption. Focused verification passed on both sides:
64 passed and 2 intentionally skipped on `23.8.16.40-alpine`, and 64 passed
with the opposite 2 tests intentionally skipped on `24.3.18.7-alpine`.

### CH-COMPAT-014: 23.8 observability schema and analyzer differences

**Classification:** mixed optional-column capabilities, one removable unused
field, and one SQL analyzer compatibility issue.

**Observed on:** `23.8.16.40-alpine`; adjacent verification checkpoint
`24.3.18.7-alpine`.

The original 17 core failures on 23.8 reduced to the following non-security
differences:

| Difference | Current consumer | Correct compatibility treatment |
|---|---|---|
| `system.asynchronous_insert_log.data_kind` is absent | The `Async Insert Log` preset displays it; distributed-query topology uses it for a “buffer kind” badge | Preserve it on capable servers. If 23.8 remains supported, probe the actual log schema and select `'' AS data_kind` when absent; a preset-only minimum-version annotation would not protect the topology service. |
| `system.metric_log.ProfileEvent_DistributedAsyncInsertionFailures` is absent | One series in the `Distribution Insert Pressure` preset | Treat the series as optional. Either build the preset from the available event columns or mark the whole imported panel as requiring the tested 24.3 checkpoint and show the existing UI compatibility message. |
| A nested `subcolumns.names` `ARRAY JOIN` after a `LEFT JOIN` fails analysis | `JSON Columns Inventory` and `JSON Subcolumn Pressure` | This is not absence of the nested field: 23.8 exposes the `subcolumns.*` arrays. Use an expanded `parts_columns` subquery before the outer join, or apply a justified minimum-version annotation for native-JSON panels. |
| `system.mutations.is_killed` is absent | Mutation active/history classification and the UI's Killed status | Preserve killed-state reporting on capable servers. A 23.8 fallback may use `0 AS raw_is_killed`, but the UI must not imply that killed-state detection exists on that server. |
| `system.query_log.peak_threads_usage` is absent | No product consumer; it is only selected and declared on `QueryDetail` | Remove it, following the same rule as CH-COMPAT-007. Reintroduce it behind a column capability only when a UI or service consumes it. |
| A real code-60 unknown-table error is categorized as `unknown` | `HttpAdapter` error reporting | Fix the adapter to use the ClickHouse error object's structured `code`/`type`, with message matching only as a fallback. This is a test-exposed adapter defect, not a ClickHouse support boundary. |

The adjacent 24.3 full core run passed after the earlier compatibility work.
Direct schema probes also confirmed that 24.3 exposes
`system.mutations.is_killed`,
`system.query_log.peak_threads_usage`, and
`ProfileEvent_DistributedAsyncInsertionFailures`, while 23.8 does not.

**Implemented action:**

- removed unused `peak_threads_usage` from the product query and interface;
- classified structured numeric ClickHouse errors before falling back to
  message matching;
- marked the four nonessential imported presets as requiring ClickHouse 24.3,
  so the existing UI compatibility state prevents execution on 23.8;
- added a cluster-aware `system.columns` capability probe for
  `system.mutations.is_killed`;
- retained mutation active/history data on 23.8 with a typed false fallback,
  returned `is_killed_supported = false`, and displayed a UI warning rather
  than implying killed-state detection is complete.

Focused verification passed on both sides of the boundary:

- `23.8.16.40-alpine`: 367 passed, 33 intentional skips across the six
  previously failing files;
- `24.3.18.7-alpine`: 372 passed, 28 intentional skips across the same files.

The complete 23.8 core suite then passed with 718 passed and 105 intentional
skips. The distributed-query topology currently treats async-insert-log
evidence as optional and omits it when the 23.8 `data_kind` query fails; adding
a column-level fallback there remains a separate enhancement if that badge is
required on 23.8.

### CH-COMPAT-015: direct browser CORS is absent from the stock 23.8 image

**Classification:** runtime-probed HTTP transport capability.

**Observed on:** `23.8.16.40-alpine`; adjacent verification checkpoints
`24.3.18.7-alpine` and `24.8.14.39-alpine`.

The stock 23.8 HTTP endpoint accepts ordinary HTTP queries but closes a browser
preflight `OPTIONS` request without a response. It does not return
`Access-Control-Allow-*` headers. The same preflight returns `204` with the
required headers on the pinned 24.3 and 24.8 images. Consequently, all
Playwright tests that connect from the Vite origin fail on 23.8 with
`Failed to fetch`, while browser-independent tests continue to pass.

This is not a general ClickHouse connectivity failure and does not make
TraceHouse incompatible with 23.8. Direct browser mode requires a
CORS-capable endpoint; the bundled or standalone TraceHouse proxy does not.
The E2E harness now probes the actual endpoint rather than inferring support
from a version string. It keeps direct-browser coverage when CORS is available
and enables the TraceHouse proxy when it is not. This also handles custom
deployments whose CORS configuration differs from the stock image.

The complete 23.8 Playwright suite passed through the runtime-selected proxy
fallback with 23 passed and 5 intentional mobile skips. The adjacent 24.3
suite selected direct-CORS mode and also passed with 23 passed and 5
intentional mobile skips.

### CH-COMPAT-016: supported compatibility floor is ClickHouse 23.8

**Classification:** accepted unsupported version boundary.

**Observed on:** `23.3.22.3-alpine`; lower untested checkpoint
`22.8.21.38-alpine`.

The exploratory 23.3 core run failed at a foundational test-environment
assumption: the expected `test` database was unavailable. That caused 19
shared suite setup failures and skipped most feature assertions. The final
result was 24 failed files, 6 passed files, 3 skipped files; 15 failed tests,
140 passed tests, and 668 skipped tests. The run also reproduced authorization
behavior that differs from the explicitly characterized 23.8 boundary.

This signal is too broad to justify capability substitutions in production
queries: the suite did not reach enough feature code to distinguish individual
product capabilities. TraceHouse therefore adopts the ClickHouse 23.8 release
line as its supported floor. The default pinned matrix retains both the
`23.8.16.40` checkpoint and the early `23.8.2.7` regression checkpoint, and
does not run 23.3 or 22.8. The runner still accepts any exact image as an
explicit argument for exploratory work.

## Compatibility policy

- Keep the default pinned matrix at or above the 23.8 supported floor.
- Use explicit one-off runs, not the release gate, to explore older versions.
- Run newest to oldest so the first regression boundary is immediately visible.
- Do not rewrite every dashboard query for every historical version.
- Use `-- @requires: clickhouse>=<version>` for intentionally modern preset
  queries.
- Use runtime capability checks for optional tables, columns, functions, and
  configuration-dependent facilities.
- Use fallbacks only when they preserve the feature's semantics and safety.
- Do not add global experimental ClickHouse settings solely to make tests pass.
- Distinguish root failures from skipped suites caused by a failed setup hook.
