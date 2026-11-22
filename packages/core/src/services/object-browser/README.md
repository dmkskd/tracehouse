# Object browser service

Read-only collection of ClickHouse objects and the relationships
between them. The public API is `index.ts`, exported as
`@tracehouse/core/services/object-browser`. All SQL lives in
`src/queries/access-queries.ts`. No existing app service depends on this module,
and nothing runs at import time.

The service shows configured relationships. It does not decide whether a user is
allowed to run a query. Paths are provenance, revokes are shown as evidence, and
the capture verdict is always `not-checked`.

## Where relationships come from

ClickHouse is asked first. TraceHouse parses text only where the server exposes
nothing usable, and every derived link records how it was obtained in
`evidence.method`.

| Method | Source | Used for |
|---|---|---|
| (none) | Rows of `system.users`, `roles`, `role_grants`, `grants`, `row_policies`, `tables` | Access links and native table links |
| `native` | `system.tables` dependency and target fields | Materialized view insert triggers and targets |
| `analyzer` | `SELECT explain FROM (EXPLAIN AST <as_select>)`, then, when the parse has no effects, `SELECT explain FROM (EXPLAIN QUERY TREE <as_select>)` | View and refreshable MV sources |
| `engine-arguments` | TraceHouse cuts the engine call out of `engine_full`, then `SELECT explain FROM (EXPLAIN AST SELECT <call>)` | Buffer and Distributed destinations |
| `engine-location` | Same engine-call parse; TraceHouse then takes scheme and host from the location argument | Storage servers of Iceberg, DeltaLake, Hudi, S3, HDFS and URL tables |

Access links come from their rows and carry no method. Every table and storage
link records its method in `evidence.method`. The detail view shows the text
from `REFERENCE_METHODS` (`model/methods.ts`) under each link whose kind has
`methods` set in `RELATIONSHIPS`.

The engine-call step is the only text TraceHouse parses. It finds the closing
parenthesis of the engine call, skipping quoted text, and drops the engine's
`SETTINGS`. It has to, because `EXPLAIN` inside a subquery accepts only SELECT,
and engine settings in a SELECT are applied as query settings and rejected.
ClickHouse then parses the call. Only literal and identifier arguments are
accepted. An expression argument is reported in the coverage notes.

## Relationships per object

### User

| Link | Direction | Source |
|---|---|---|
| has role | user → role | `system.role_grants` row with `user_name`. Label says whether the role is default (`granted_role_is_default`). |
| grant | user → privilege | `system.grants` row with `user_name` |
| configured selector | row policy → user | User named in `system.row_policies.apply_to_list` |

`ALL` and `EXCEPT` selectors stay in the policy's evidence. They do not create
links.

### Role

Same sources as a user, keyed by `role_name`. A role granted to another role
gives a role → role `has role` link. Role traversal follows `has role` links
from the principal, at most 16 levels.

### Privilege

A privilege node is TraceHouse's grouping of `system.grants` rows. ClickHouse has
no object for it, so its detail view has no `CREATE` statement. Its identity is
`access_type`, `access_object`, `database`, `table`, `column` and `is_wildcard`.
Principals holding the same privilege on the same target share one node.

Its group and level come from `system.privileges`. The group is the ancestor
just below `ALL` (`SHOW USERS` → `SHOW ACCESS` → `ACCESS MANAGEMENT`). The level is
what the privilege can apply to (`COLUMN`, `TABLE`, `GLOBAL`, `SOURCE`). If
`system.privileges` cannot be read, the grants coverage note says so and the
card shows the raw privilege name.

| Link | Direction | Source |
|---|---|---|
| candidate name match | privilege → table | Computed by `projectCatalogGraph` from names |

Only `SELECT` and `INSERT` privileges without `access_object` or wildcard link
to tables. Without a focus object, only exact table privileges link. With one,
database-wide and global privileges link too, over the tables `FOCUS_TABLES`
(`projections/graph.ts`) gives for the focus kind:

| Focus | Tables considered |
|---|---|
| Table | That table |
| Storage server | Tables stored on it |
| Row policy | Its target tables |
| User, role, privilege | Every table |

At most 100 tables are linked per privilege, and the full count of a capped
privilege is kept in `graph.scopeMatches`. Scoping a table or storage focus to
its own tables keeps the cap from dropping them. A partial revoke keeps its
link with the label `excluded scope · evidence`. No grant and revoke union is
computed.

### Row policy

| Link | Direction | Source |
|---|---|---|
| configured filter target | row policy → table | `system.row_policies.database` and `table`. An empty table means every table in that database in the catalog. |
| configured selector | row policy → user or role | `apply_to_list` |

### Table, by engine

The engine decides which links a table can have. `model/engines.ts` holds one
entry per engine.

| Engine | Links | Method | Source |
|---|---|---|---|
| MaterializedView | source → MV, `triggers on insert` | native | Source table's `dependencies_database` / `dependencies_table`. Linked only when the dependent is an insert-triggered engine on the same host. |
| MaterializedView | MV → target, `writes to` | native | `target_database` / `target_table` |
| MaterializedView | source → MV, `read by` | analyzer | Sources of `as_select` with no native link, such as refreshable MV sources and joined tables |
| View | source → view, `read by` | analyzer | Sources of `as_select` |
| Buffer | buffer → destination, `flushes to` | engine-arguments | Arguments 0 and 1 |
| Distributed | distributed → local table, `forwards to` | engine-arguments | Arguments 1 and 2 |
| Iceberg, IcebergS3, IcebergHDFS, IcebergLocal | table → storage server, `stored on` | engine-location | Argument 0 |
| DeltaLake, DeltaLakeS3, DeltaLakeLocal, Hudi | table → storage server, `stored on` | engine-location | Argument 0 |
| S3, S3Queue, HDFS, URL | table → storage server, `stored on` | engine-location | Argument 0 |
| Azure variants, MergeTree family, Dictionary, others | none | | |

The analyzer resolves CTEs, joins and subqueries. ClickHouse stores view queries
with qualified names (checked on 26.10: a view created as `FROM t` under `USE d1`
stores `FROM d1.t`), so no database context is needed. Table functions are listed in the coverage notes
and not linked. A native link takes precedence over a derived link between the
same two objects.

Resolving a query is more than reading metadata (checked on 26.9.7). The analyzer asks
each table function for its columns, which for `url()` without columns or `remote()`
connects to the source, and it runs scalar subqueries, including `WITH (SELECT …) AS x`.
It does not run `IN`, `NOT IN`, `GLOBAL IN` or `exists` subqueries, FROM subqueries or
table CTEs. So each view query is parsed first, and `analysisEffects` lists table
functions (other than `numbers`, `zeros`, `generate_series` and `values`) and scalar
subqueries. A query with effects is not analyzed during collection: its table record
gets `analysisEffects`, the coverage notes name it, and the detail view offers
"Tables read" on request, labeled with the effects.

A named collection in place of the location (an identifier argument) is
reported in the coverage notes and not resolved.

A referenced table that the catalog did not collect becomes a placeholder node
with `unresolvedReference`. Tables in `system` and `information_schema` are
excluded from the catalog, so they get `outsideCatalog` as well and the card
says "outside catalog".

### Storage server

A storage server is TraceHouse's grouping of table locations. ClickHouse has no
object for it, so its detail view has no statement. `storageServer` in
`collectors/locations.ts` takes scheme and host from a URL location
(`http://minio:9000`) and drops credentials. A plain path becomes
`local disk · <host>`, one node per ClickHouse host. Tables stored on the same
server share one node. The full location and the definition stay in each
link's evidence.

| Link | Direction | Source |
|---|---|---|
| stored on | table → storage server | Location argument of `engine_full`, method `engine-location` |

Locations come from the table definition only. Collection makes no storage or
catalog calls, and the metadata integration test checks `query_log` for zero
S3 requests. ClickHouse shows URL credentials and key arguments as `[HIDDEN]`
in `engine_full` (checked on 26.10), so locations carry no secrets. Reading
Iceberg snapshots (`system.iceberg_history`) reads the table's metadata from
storage, so the detail view runs it only when the user asks.

A storage server is not expected on every host (`KINDS.location.everyHost` is
false in `model/kinds.ts`). It is never marked partial or different for hosts
that do not name it, and its card shows no host count.

## Bounds and failures

- Each source reads at most 1,000 rows (up to 5,000 with `limit`). Truncation is
  reported in coverage.
- Optional `system.tables` fields are selected only when every host has them.
  A missing field is named in the tables coverage note.
- Definition references analyze each distinct definition once, at most 300 per
  capture, three at a time, with a 10 second limit each. Failures, table
  functions and unsupported arguments are added to the tables coverage note.
- Every source records `readable`, `schema-unavailable`, `failed` or `truncated`.
  A failed read never becomes an empty catalog.

## Version notes

| Version | Difference |
|---|---|
| 26.9.7.9 | All sources and fields present |
| 26.3.62.3 | `system.grants.is_wildcard` is absent, so grants are `schema-unavailable` and there are no privilege nodes. `target_database` / `target_table` are absent, so MV `writes to` links are missing and the tables coverage note names the fields. Analyzer and engine-argument links work. |

## Not collected yet

Named collections used by engines, `DataLakeCatalog` database engines, dictionary
sources, the semantics of source privileges (`READ`, `WRITE`, `S3`), settings
profiles, quotas, columns and storage policies. Extraction probes in the
metadata integration test show the server data for profiles, quotas, columns and
storage is readable. The collector does not use it yet.

## Where behavior is defined

Behavior that depends on a type lives in one table per concept. Other code reads
the table and does not branch on engine names or relationship kinds.

| Table | File | Holds |
|---|---|---|
| `ENGINES` / `engineSpec` | `model/engines.ts` | Object type per engine, native insert-trigger and target fields, and how missing links are read |
| `KINDS` | `model/kinds.ts` | Whether an object kind is expected on every host |
| `RELATIONSHIPS` | `model/relationships.ts` | Family, provenance, column ranking (including targets kept in the last column, such as storage servers), recorded method and card facts per relationship kind |
| `REFERENCE_METHODS` | `model/methods.ts` | Text shown for each link method |
| `STRATEGIES` | `collectors/references.ts` | One reader per reference source. `request` builds the EXPLAIN and `read` turns its output into links. |
| `resolve` | `collectors/tables.ts` | How each reference target becomes a table key or a storage location |
| `privilegeGroup` | `model/privileges.ts` | Privilege group from the server's privilege tree |
| `FOCUS_TABLES` | `projections/graph.ts` | Tables a broad privilege may link to, per focus kind |
| `describers` | `presentation/cards.ts` | Card title, qualifier, summary line and facts per object type |
| `statements` | `presentation/details.ts` | Server statements in the detail view per object type |
| `KIND_LAYOUT` | `presentation/layout.ts` | Catalog lane for access objects and band (access, data, outside ClickHouse) per object kind; the page draws a divider where the band changes. Catalog data objects use the focus layout. |
| `GUIDES` | `presentation/guides.ts` | Guided questions on the page: what to pick, footer counts, and whether to focus or collect provenance |

Graph traversal is in core too: `lineage` and `focusGraphFrom` build the focus
graph, `searchGraph` the search view (`projections/focus.ts`). `routeEdges` (`presentation/layout.ts`) adds waypoints
so lines that skip columns run between cards. The frontend
renders the columns `layoutCatalog` or `layoutFocus` return, and holds only
symbols and colors per type (`objectStyle.ts`).

Adding an engine whose destination is in its arguments is one `ENGINES` entry
with `references: { reader: 'table-arguments', database, table, label }`. An
engine with a storage location is one entry with a `storedOn(backend)` source.

## Tests

| Relationship | Test |
|---|---|
| Users, roles, nested roles, grants, partial revokes, row policies | `object-browser-metadata.integration.test.ts`, `object-browser-cluster.integration.test.ts` |
| MV insert triggers and targets | metadata integration (`reads MV dependency pairs…`, `retains both cascade views…`) and `object-model.test.ts` |
| View, refreshable MV, Buffer and Distributed links with their methods | metadata integration (`links view sources through the analyzer…`) |
| No HTTP request or scalar subquery during collection; analysis on request | metadata integration (`does not analyze view queries whose analysis…`) |
| Privilege group and level | metadata integration (`labels grant scopes…`) and `presentation.test.ts` |
| Detail statements, secret hiding, denied observer | metadata integration (`reads each detail statement…`) |
| Storage servers, shared nodes, no S3 calls | metadata integration (`builds storage locations…`) and `locations.test.ts` |
| Focus scoping past the cap, storage servers on several hosts | `locations.test.ts` |
| Parsing of analyzer and AST output, unqualified sources | `references.test.ts`, using output captured from ClickHouse 26.10 |
| Name matching, caps, layouts, focus, search, cards | `presentation.test.ts`, `object-model.test.ts` |

Each new relationship source needs an integration fixture that asserts the link
on a real server, in addition to unit tests on captured output.

```bash
npm test --workspace @tracehouse/core -- src/services/object-browser/__tests__
CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:26.9.7 npm run test:integration --workspace @tracehouse/core -- \
  src/__tests__/integration/object-browser-metadata.integration.test.ts \
  src/__tests__/integration/object-browser-cluster.integration.test.ts
CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:26.3.62.3 npm run test:integration --workspace @tracehouse/core -- \
  src/__tests__/integration/object-browser-metadata.integration.test.ts
```

The integration suites create uniquely prefixed objects in their own containers
and drop them afterwards. `node packages/core/scripts/verify-object-browser.mjs
<test-container> [http-port]` runs the same kind of check against an existing
test server, creating and removing only its own prefixed objects.
