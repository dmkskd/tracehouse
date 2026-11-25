# Object Browser

Technical reference for TraceHouse's Object Browser and the ClickHouse access and metadata mechanisms it reads.

The Object Browser collects ClickHouse access definitions, tables, views, and external storage servers, then projects the configured relationships between them into a catalog graph and rooted focus graphs. All collection is read-only.

- Core service and public API: `packages/core/src/services/object-browser/` ([service README](../packages/core/src/services/object-browser/README.md))
- SQL statements and probes: `packages/core/src/queries/access-queries.ts`
- Frontend renderer and detail modal: `frontend/src/components/object-browser/`
- User-facing connection and grant notes: [Connecting to ClickHouse — Object browser](site/docs/guides/connecting.md#object-browser)

**Claim boundary:** the Object Browser evaluates configured access: for a user, a table and an access type, it reads the grants, partial revokes and role assignments it collected and combines them the way ClickHouse does (see [Access answers](#access-answers)). It does not check whether a query would succeed. Row policies, settings constraints and other execution gates are shown but not applied, and the capture verdict is always `not-checked`.

---

## 1. Collection model

### System tables read

Collection reads seven system tables. Access tables are optional: if the connecting user lacks `SELECT` on any of them, capability detection marks those sources `not-granted` and skips them without issuing a query. Table and data-flow browsing still works with `SELECT` on `system.tables` alone.

| Source | Fields collected | Role in the browser |
|---|---|---|
| `system.tables` | `database`, `name`, `uuid`, `engine`, keys, `storage_policy`, plus schema-probed optional fields (`dependencies_database`, `dependencies_table`, `target_database`, `target_table`, `as_select`, `engine_full`) | Table/view inventory, native MV links, inputs for view and engine-argument analysis |
| `system.users` | `name`, `id`, `storage`, `default_roles_all`, `default_roles_list`, `default_roles_except`, `host_ip`, `host_names`, `host_names_regexp`, `host_names_like` | User inventory and host/default-role metadata. `auth_params` is not selected. |
| `system.roles` | `name`, `id`, `storage` | Role inventory |
| `system.role_grants` | `user_name`, `role_name`, `granted_role_name`, `granted_role_id`, `granted_role_is_default`, `with_admin_option` | User-to-role and nested role-to-role assignments |
| `system.grants` | `user_name`, `role_name`, `access_type`, `access_object`, `database`, `table`, `column`, `is_partial_revoke`, `grant_option`, `is_wildcard` | Direct grants and partial revokes on users and roles |
| `system.row_policies` | `name`, `short_name`, `id`, `storage`, `database`, `table`, `select_filter`, `is_restrictive`, `apply_to_all`, `apply_to_list`, `apply_to_except` | Row-filter definitions, target tables/databases, and principal selectors |
| `system.privileges` | `privilege`, `level`, `parent_group` | Server's privilege hierarchy used to group and label privilege nodes |

Catalog reads exclude `system`, `information_schema`, and `INFORMATION_SCHEMA` from `system.tables`. If a collected view references a table in those databases, that table is fetched by name afterwards (`systemTablesByNameQuery`).

### On-demand detail statements

Opening an object's detail view executes statements against the connected host only on request:

- Table / view: `SELECT create_table_query FROM system.tables`
- User: `SHOW CREATE USER <name> SETTINGS format_display_secrets_in_show_and_select = 0`
- Role: `SHOW CREATE ROLE <name>`
- Grants: `SHOW GRANTS FOR <name>`
- Row policy: `SHOW CREATE ROW POLICY <name> ON <db>.<table>` (or `ON <db>.*` for database-wide policies)
- Iceberg snapshots: `SELECT ... FROM system.iceberg_history` (reads metadata from storage, so never run during catalog collection)

User statements need `SHOW USERS`, role statements `SHOW ROLES`, and the row policy statement `SHOW ROW POLICIES`. `SHOW GRANTS FOR` needs no privilege for the connected user or for a role granted to it. The capability probe checks each privilege (`show_users`, `show_roles`, `show_row_policies`).

### Host scoping and schema probing

In clustered setups, catalog queries run through `clusterAllReplicas` (`{{cluster_aware:...}}`) with `skip_unavailable_shards = 0` and label every row with `hostName()`.

- **Observation identity is host-scoped.** Matching names or UUIDs across hosts do not prove identical definitions or shared storage.
- **Paths stay within a host.** A role assignment observed on host A is never combined with a grant observed on host B.
- **Schema probing precedes collection.** `ACCESS_SCHEMA` reads `system.columns` per host. Optional `system.tables` fields are included only when present on every observed host; missing fields are recorded in coverage notes.
- **Explicit source status.** Every source records `readable`, `schema-unavailable`, `failed`, `truncated`, or `not-granted`. A failed query never collapses into an empty catalog.

---

## 2. Relationship extraction

ClickHouse is queried first. Text is parsed in TraceHouse only where the server exposes no structured field. Every derived table or storage link records its extraction method in `evidence.method`.

### Access relationships

| Link | Direction | Source and rules |
|---|---|---|
| `has role` | user/role → role | `system.role_grants`. Direction is recipient → assigned role (`GRANT role1 TO role2` yields `role2 → role1`). Carries `granted_role_is_default` and `with_admin_option`. Nested traversal is bounded at 16 levels with cycle protection. |
| `grant` | user/role → privilege | `system.grants`. Principals holding the same `(access_type, access_object, database, table, column, is_wildcard)` share one privilege node. Group and level come from `system.privileges` (ancestor directly below `ALL`). |
| `candidate name match` | privilege → table | Computed in `projectCatalogGraph` for privileges that include one of the selected access types (`SELECT` by default) through the privilege tree in `system.privileges` (such as `ALL ON *.*`), without `access_object`. Prefix scopes (`db.tri*`, `db*.*`) link to the tables they name. In the catalog view, only exact table grants link. Under focus, database-wide and global grants also link across tables relevant to the focus kind (capped at 100 tables per privilege; full count retained in `graph.scopeMatches`). Partial revokes stay linked as `excluded scope · evidence`. |
| `configured filter target` | row policy → table | `system.row_policies.database` and `table`. An empty `table` matches every catalog table in that database. |
| `configured selector` | row policy → user/role | `system.row_policies.apply_to_list`. `ALL` and `EXCEPT` remain in the policy's evidence without generating edges. |

### Data-flow and storage relationships

Engine behavior is defined declaratively in `model/engines.ts`. Native links take precedence over derived links between the same pair of objects.

| Engine | Link | Method | How it is obtained |
|---|---|---|---|
| `MaterializedView` | source → MV (`triggers on insert`) | `native` | Source table's `dependencies_database` / `dependencies_table`, linked when the dependent is an insert-triggered engine on the same host |
| `MaterializedView` | MV → target (`writes to`) | `native` | `target_database` / `target_table` (covers explicit `TO` and implicit `.inner.*` targets) |
| `MaterializedView` | source → MV (`read by`) | `analyzer` | Sources in `as_select` not already covered by a native link (e.g., refreshable MVs and joined tables) |
| `View` | source → view (`read by`) | `analyzer` | Sources resolved from `as_select` |
| `Buffer` | buffer → destination (`flushes to`) | `engine-arguments` | Arguments 0 and 1 of the engine call |
| `Distributed` | distributed → local table (`forwards to`) | `engine-arguments` | Arguments 1 and 2 of the engine call |
| `Iceberg*`, `DeltaLake*`, `Hudi`, `S3`, `S3Queue`, `HDFS`, `URL` | table → storage server (`stored on`) | `engine-location` | Argument 0 of the engine call, normalized to scheme + host (`http://minio:9000`) or `local disk · <host>` |

#### Analyzer safety (`EXPLAIN AST` and `EXPLAIN QUERY TREE`)

`EXPLAIN` runs as a standalone statement through `executeRawQuery`. Wrapping `EXPLAIN` inside `SELECT ... FROM (EXPLAIN ...)` invokes ClickHouse's `viewExplain` table function, which requires `CREATE TEMPORARY TABLE`.

Running `EXPLAIN QUERY TREE` is not purely passive: the ClickHouse analyzer asks table functions without explicit column lists (such as `url()` or `remote()`) for their schema by contacting the remote source, and it evaluates scalar subqueries including `WITH (SELECT ...) AS x`. It does not execute `IN`, `NOT IN`, `GLOBAL IN`, `EXISTS`, `FROM` subqueries, or table CTEs.

To avoid unintended network calls or subquery execution during catalog load:

1. TraceHouse runs `EXPLAIN AST <as_select>` first (pure parse, no I/O or execution).
2. `analysisEffects` flags table functions (excluding `numbers`, `zeros`, `generate_series`, `values`) and scalar subqueries.
3. Views with flagged effects are not passed to `EXPLAIN QUERY TREE` during collection. Their detail view offers on-request analysis labeled with the detected effects.
4. Safe views run `EXPLAIN QUERY TREE (<as_select>) SETTINGS max_execution_time = 10`.

#### Analyzer links on a cluster

`EXPLAIN` cannot run through `clusterAllReplicas`, so every view query is analyzed on the connected node (`SELECT hostName()`), and names resolve against that node's catalog. Identical view texts on several hosts are analyzed once. On the connected node, the analyzer's sources are linked as found. On another host, a source is linked only when an object with that name exists there with the same engine; otherwise the link is omitted and the tables coverage note says which view, host and source, for example `db.reads_src on ch-2: analyzed on ch-1; db.src has another engine there, link omitted`. Kept links record the node in `evidence.analyzedOn`, and the detail view shows it with the method.

#### Engine-argument parsing and storage servers

`engine-arguments` and `engine-location` are the only places TraceHouse parses text directly: it isolates the engine call from `engine_full` (finding the matching closing parenthesis while skipping quoted strings and stripping `SETTINGS`), then passes `EXPLAIN AST SELECT <call>` to ClickHouse. Only literal and identifier arguments are accepted; expressions and named-collection location identifiers are recorded in coverage notes and not resolved.

For storage servers, credentials in URLs are stripped and ClickHouse already masks secrets in `engine_full` as `[HIDDEN]`. Collection issues zero S3 or external storage requests. Unresolved table references remain visible as placeholder nodes (`unresolvedReference`).

---

## 3. ClickHouse permissions foundation

Understanding why the Object Browser separates configuration evidence from authorization verdicts requires these ClickHouse access-control properties.

### Grants describe operation and scope, not ownership

An authorization check asks whether a specific principal can perform a specific operation on specific objects/columns in a specific execution context. ClickHouse has no object-ownership authorization model:

- Scopes cover columns, tables, databases (`db.*`), global (`*.*`), and trailing-prefix wildcards (`db.prefix*`, available since 24.10). A wildcard is a prefix match, not a glob or regex.
- Privileges form a hierarchy exposed by `system.privileges` (for example, `ALTER` implies narrower `ALTER ...` privileges). Operation names, not Enum ordinal numbers, are the stable contract across versions.
- Grants can target non-existent names and persist after `DROP TABLE`; recreating a table with the same name reuses existing grants.
- `WITH GRANT OPTION` and `WITH ADMIN OPTION` govern delegation, not read/write access.

### Role assignment and role activation are separate

Roles can be granted to users and nested inside other roles:

```sql
GRANT SELECT ON sales.orders TO orders_reader;
GRANT orders_reader TO analyst;
GRANT analyst TO alice;
```

- Default roles (`default_roles_all`, `default_roles_list`, `default_roles_except` in `system.users`) activate at login.
- `SET ROLE` changes a session's active roles (explicit roles, defaults, `ALL`, or `NONE`). `SET ROLE NONE` does not remove direct user grants.
- Being assigned a role does not prove that role was active during a given query.
- `system.current_roles` and `system.enabled_roles` report only the querying session (TraceHouse's connection), not another user's session.

### Partial revokes are not global deny rules

```sql
GRANT SELECT ON sales.* TO sales_reader;
REVOKE SELECT ON sales.payroll FROM sales_reader;
```

A partial revoke narrows that single principal's grant. In ClickHouse (`src/Access/ContextAccess.cpp`, `mixAccessRightsFromUserAndRoles`), user grants and enabled-role grants combine by union. An independent grant on another enabled role restores access that was revoked from `sales_reader`. Evaluating effective access therefore requires evaluating each source's exclusions before unioning all active sources.

### Access answers

`access/evaluate.ts` evaluates configured rights for a user, a table and an access type; `presentation/access.ts` formats the result. Both use a capture: the catalog, or a read of the user and table by name when the catalog does not hold them.

- Each host is evaluated on its own records. A role assignment on one host does not connect to a grant on another.
- The holders are the user and every role it holds on that host. Roles active at login are the default roles and the roles they hold. The rest are reachable only after `SET ROLE`.
- For each holder, the most specific matching rule decides the table: global, database prefix, database, table prefix, table, with a longer prefix ranking higher and a revoke winning a tie. Column rules then narrow it. `system.grants` records a table prefix as `table` with `is_wildcard = 1` and a database prefix as `database` with `is_wildcard = 1`.
- Holders combine by union. A revoke takes back only its own holder's grant.
- The answer is `granted`, `columns` (only some columns, or all but some), `set-role`, `revoked`, `no-path`, `incomplete` or `differs`. `incomplete` replaces a negative answer when users, roles, role grants, grants or tables were not fully read, or the privilege tree was unavailable. `differs` lists each host's outcome when hosts disagree on the state or the columns.
- `reachFor` counts tables across the catalog from the same per-table answers, keeping tables that differ by host, need `SET ROLE` or are incomplete as their own notes.

The focus view shows one chip per selected access type with this answer, and the diagram marks a privilege-to-table line cancelled only when every holder of that privilege, on every host, has revoked it.

### Execution gates outside table grants

A matching `SELECT` or `INSERT` grant does not guarantee execution succeeds:

| Mechanism | Effect |
|---|---|
| Row policy | Filters readable rows (`PERMISSIVE` combines with `OR`, `RESTRICTIVE` with `AND`); does not itself grant `SELECT`. Behavior when no policy matches depends on server access-control settings. Write privileges can bypass `SELECT` row filters. |
| Setting constraints | `readonly`, `allow_ddl`, `allow_introspection_functions`, and session auth-method grant limits (`auth_grants`) reject operations with distinct error codes even when a table grant matches. |
| Quotas | Reject queries when interval resource budgets are exhausted. |
| Workload scheduler | Queues or rejects queries via the `workload` setting (configured in profiles/constraints) mapped to `system.workloads` and `system.resources`. |
| Authentication and host rules | Reject connections before query authorization (`host_ip`, `host_names`, `valid_until`). |

### Identity boundaries across views and distributed queries

- **Views (`SQL SECURITY`):** `SELECT` permission on a view is separate from permission on its base tables; there is no grant inheritance from source to view. Under `INVOKER` (the default for ordinary views), base-table privileges are checked against the caller. Under `DEFINER`, they are checked against the view's `definer`. Materialized views execute inserts under their own context and definer rules.
- **Distributed queries:** Distributed table execution can authenticate to remote shards as a configured remote user rather than `initial_user`. Remote privilege failures apply to that remote identity on the remote host's access state.

### Secrets and observer visibility

- **`system.users`** does not expose passwords or password hashes.
- **`system.tables` and `SHOW CREATE`** mask embedded credentials (passwords, access keys, tokens) as `[HIDDEN]` by default. Unmasking them requires all three of: server setting `display_secrets_in_show_and_select = 1`, session setting `format_display_secrets_in_show_and_select = 1`, and the `displaySecretsInShowAndSelect` privilege (introduced in ClickHouse 23.6/23.7). TraceHouse does not request this privilege and forces `format_display_secrets_in_show_and_select = 0` on `SHOW CREATE USER`.
- **Metadata visibility:** When `select_from_system_db_requires_grant` is disabled, `system.grants` and `system.role_grants` require `SHOW USERS` and `SHOW ROLES` (`StorageSystemGrants.cpp`, `StorageSystemRoleGrants.cpp`). When enabled, `SELECT` grants on the system tables govern access. An empty result from a restricted observer is not proof that no entities exist.

---

## 4. Graph projections and UI

All graph traversal and layout logic runs in `@tracehouse/core/services/object-browser`; `frontend/src/components/object-browser/` renders the returned columns, routed edges, detail modal, and guided questions.

- **Catalog view:** Groups objects into three visual bands (`access`, `data`, `external`) with fixed lane positions. Display is bounded per lane while full collected evidence remains searchable (`searchGraph`).
- **Focus view (`lineage` / `focusGraphFrom`):** Opens rooted at a single object. Upstream and downstream traversals run independently and recursively so traversal does not reverse sideways through shared roles or tables to pull in unrelated principals. Broad privileges (`database.*`, `*.*`) expand only for tables relevant to the focus object.
- **Guided questions (`GUIDES`):** Entry points for common lookups (e.g., tracing a user's privileges or a table's upstream/downstream flow) backed by the same capture model.

---

## 5. Bounds, version behavior, and current limits

### Collection and projection bounds

| Dimension | Bound | Behavior on hit |
|---|---|---|
| Rows per system table | 1,000 default (5,000 max) | Source marked `truncated` in coverage |
| Distinct definitions analyzed | 300 per capture, 3 concurrent, 10s `max_execution_time` | Remaining definitions skipped and reported in tables coverage note |
| Linked tables per privilege | 100 | Capped; full match count kept in `graph.scopeMatches` |
| Role nesting depth | 16 levels | Traversal stops; cycle-protected |

### Verified ClickHouse version behavior

| Version | Status |
|---|---|
| `26.9.7.9` | All sources and optional fields present. Single-host and two-host `clusterAllReplicas` integration suites pass. |
| `26.3.62.3` | Partial support. `system.grants.is_wildcard` is absent (grants reported `schema-unavailable`, no privilege nodes). `system.tables.target_database` / `target_table` are absent (MV `writes to` links omitted and named in coverage). Native MV triggers, `EXPLAIN` analyzer links, and `engine-arguments` links work. |

### Not implemented / deferred

- **Server-side check:** answers come from collected configuration only. `EXECUTE AS <user> CHECK GRANT` (25.11+) would let the server answer for the host it runs on, but it needs `IMPERSONATE`, which lets the observer run any statement as that user, and it is unsupported in Cloud. If added, it would be an opt-in cross-check, not the source of answers.
- **Uncollected metadata families:** Settings profiles (`system.settings_profiles`, `system.settings_profile_elements`), quotas (`system.quotas`, `system.quota_limits`), columns, storage policies (`system.storage_policies`), workloads/resources (`system.workloads`, `system.resources`), named-collection resolution for table engines, `DataLakeCatalog` database engines, dictionary sources, and source privileges (`READ`, `WRITE`, `S3`).
- **Historical failure correlation:** Query-level permission failures in `system.query_log` (`used_privileges`, `missing_privileges`, `Settings`) and login roles in `system.session_log` are not yet linked into the Object Browser. Any future integration must keep three states distinct: **observed at execution** (server-reported missing privileges on a host), **reconstructed now** (current grant paths under stated role assumptions), and **unknown historically** (active roles after `SET ROLE`, grants, or remote identity not captured at execution time).

---

## 6. Entry points and outbound links

Status: §6.1 and §6.2 are implemented. The rest is proposed.

### 6.1 Opening contract (implemented)

`/objects` opens focus on one object from URL parameters. Core builds and reads them (`objectLinkUrl`, `parseObjectLink` in `presentation/links.ts`):

```
/objects?ob_kind=table&ob_db=<db>&ob_tbl=<table>[&ob_user=<user>]
/objects?ob_kind=user&ob_user=<user>
/objects?ob_kind=role&ob_role=<role>
```

- The page loads the catalog as on any entry, then `resolveObjectLink` finds the object by database and table name, or by user or role name. Views are tables here. An object or user the catalog does not hold is named in an alert, and focus stays closed.
- `ob_user` on a table puts that user in context. `withPrincipalContext` keeps the access objects on the user's configured paths to the table (`edgesBetween`, per host, with no path limit) and row policies that name a principal on those paths. Data-flow and storage objects stay. The focus header shows the user, with "Show all principals" to drop the context, and one answer per selected access type. Re-rooting focus on another object drops it as well.
- Opened with `state.from`, closing focus navigates back to `from.path`. Without it, closing removes the `ob_*` parameters and stays on the catalog.
- Grafana data links open in a new tab, following the Grafana deep-link convention.
- Not implemented: `ob_host` (focus merges the hosts an object was observed on) and row-policy targets.

### 6.2 From the object detail to other pages (implemented)

The detail of a table or view (double-click a card in focus) has buttons under its title that open other pages filtered to it. The links come from `DESTINATIONS` in `model/destinations.ts`, one list per object type. Only tables and views have links:

| Type | Buttons |
|---|---|
| MergeTree table | Queries · Time Travel · Merges |
| Other tables and views | Queries · Time Travel |
| User, role, privilege, row policy, storage server | none |

- **User filter:** a `User` select before the buttons chooses whose activity Queries and Time Travel show: all users, the user a link put in context (`ob_user`) or the focus trail started from (the default), or any user with a configured path to the table (at most 25). Merges ignore it: merges have no user. Each button's tooltip says what the page will show.
- **Parameters:** Queries gets `user` and `tableName=<db>.<table>`, and Merges gets `database` and `table`, as their share schemas read them. Time Travel gets its tracking parameters, `tt_table` and `tt_user`, opens on matched operations only (`tt_matched_only=1`), and with a user also sets `tt_failed_start=1`, so queries that failed before start, such as access denied, appear.
- **Return:** each link navigates with `state.from` set to the URL that reopens this focus (`objectLinkFor`, with the chain user on a table). Queries, Merges and Time Travel show the back link.

### 6.3 From Analytics: `@object_link`

```sql
-- @object_link: on=<col> kind=table database=<col> table=<col> user=<col>
```

Parsed in `meta-language.ts` next to `@query_link` and `@merge_link`, and turned into a URL in `deep-link-resolution.ts`. A chart click or cell click opens the focus modal with `state.from`, and closing it returns to the panel. `GrafanaExportDialog` lists it as not exported, like `@merge_link`.

### 6.4 From Query Details

- **Failed query** (exception codes 497 `ACCESS_DENIED`, 164 `READONLY`, 452 `SETTING_CONSTRAINT_VIOLATION`, 201 `QUOTA_EXCEEDED`): an "Inspect access" action opens focus rooted at the table named in the failure, with the executing user as context. The modal shows the server-reported missing privileges (observed at execution) beside the current grant, role and policy paths (reconstructed now). It does not state whether the query would succeed now and issues no `GRANT`.
- **Successful query:** an "Objects" action on the `tables` list. For an `INSERT`, focus shows the materialized views triggered downstream.

First step before building this: an integration test that records what a denied query writes to `query_log` (`exception_code`, `missing_privileges`, `tables`, `user`).

### 6.5 From Grafana panels

The provisioned **TraceHouse · Panel links** dashboard (`tracehouse-primitives`)
demonstrates these entry points with native Grafana panels. A User cell opens
user focus; a Table cell opens table focus. Its failed-query, query-details,
time-series and merge panels also invoke Time Travel, Queries, Events, Merges
and Explorer. Each data link preserves the selected ClickHouse datasource and
cluster. See the [dashboard and link reference](../infra/docker/grafana/dashboards/TraceHouse/README.md).


Recipes with no plugin-specific code beyond the opening contract, using data links to `/a/<plugin-id>/objects?th_ds=${th_datasource}&ob_...`:

- Failed-queries panel: the user and table cells open focus with `ob_ask=access`.
- Table-size or inserts panel: the table cell opens focus with `ob_ask=lineage`.

### 6.6 Access dashboard

An Analytics dashboard for users, roles and grants. Each panel is one literal SQL preset with `@object_link` on its rows. Panel ideas taken from the Snowflake security and role-auditing guides, Acceldata "Unused Resources" and Veza role mining, mapped to ClickHouse sources:

| Panel | Source |
|---|---|
| Access failures over time by user and exception code | `system.query_log` |
| Login failures by user and client address | `system.session_log` (`type = 'LoginFailure'`) |
| Dormant users: no session or query activity in N days | `system.users` with `system.session_log` / `system.query_log`. Bounded by log TTL, which the panel states. |
| Orphan roles: granted to no user or role | `system.roles`, `system.role_grants` |
| Privileged principals: `ALL`, `*.*`, `ACCESS MANAGEMENT`, `WITH GRANT OPTION`, `WITH ADMIN OPTION` | `system.grants`, `system.role_grants` |
| Grants on names with no current table (grants survive `DROP TABLE`) | `system.grants` with `system.tables` |
| Users without host restriction | `system.users` host fields |
| Tables accessed per user | `system.query_log.tables` × `user`, linked to focus for comparison with configured grants |

The dashboard needs the same access-table grants as collection (§1). Panels whose source is unreadable show the source status instead of an empty result.

### 6.7 Order

1. Opening contract (§6.1): done. Callers navigate to `/objects`; focus is not yet a modal other pages can host in place.
2. Outbound links from the object detail (§6.2): done.
3. `@object_link` and the Access dashboard (§6.3, §6.6).
4. Query Details actions, after the denied-query integration test (§6.4).
5. Grafana recipes (§6.5).
