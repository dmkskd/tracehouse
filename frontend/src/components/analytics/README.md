# Analytics: queries & dashboards

This directory is tracehouse's query-explorer + dashboard engine. **Dashboards here are
pure data** - a dashboard is a titled grid of references to preset SQL queries. Adding one
needs **no React/service/hook/store code**: you write SQL with directive comments, register
it, and list it in a dashboard.

> Layering note: the project rule (see root `CLAUDE.md`) is that SQL lives in
> `packages/core/src/queries/`. The Analytics **preset queries** are the deliberate
> exception - they are user-visible, editable SQL shown in the query explorer, so they live
> here as plain strings.

## The data flow

```text
queries/<group>.ts   →  queries/index.ts  →  presetQueries.ts  →  dashboards.ts
(SQL + directives)      (registers module)    (parses @meta)       (lists panels)
```

Directives are parsed by [`metaLanguage.ts`](./metaLanguage.ts); template placeholders
(`{{time_range}}`, `{{cluster_aware:…}}`, `{{drill_value:…}}`) are resolved by
[`templateResolution.ts`](./templateResolution.ts).

## Recipe - add a dashboard

### 1. Write the queries - `queries/<group>.ts`

Export an array of SQL strings. Each query is a `--`-comment header of directives followed
by the SQL:

```sql
-- @meta: title='CPU by Query Shape' group='Knowledge Base' interval='1 DAY' description='…'
-- @chart: type=bar group_by=query_hash value=cpu_seconds unit=s style=2d
-- @drill: on=query_hash into='CPU Query Executions'
-- @source: https://kb.altinity.com/…
SELECT lower(hex(normalized_query_hash)) AS query_hash,
       round(sum(ProfileEvents['UserTimeMicroseconds']) / 1e6, 2) AS cpu_seconds
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish' AND event_time > {{time_range}}
GROUP BY query_hash ORDER BY cpu_seconds DESC LIMIT 20
```

**Directives** (full grammar in [`metaLanguage.ts`](./metaLanguage.ts)):

| Directive | Purpose |
| --- | --- |
| `@meta:` | `title='…' group='…'` (required), optional `description='…'`, `interval='…'` (default for `{{time_range}}`) |
| `@chart:` | `type=bar\|line\|area\|pie\|grouped_bar\|stacked_bar\|grouped_line` + `group_by=COL value=COL[,COL] series=COL unit=X color=#hex`. Omit to render a table. |
| `@cell:` | table-cell decoration: `type=rag green<N amber<N` (or `green=a,b amber=c red=d`), `type=gauge max=N\|col`, `type=sparkline` |
| `@drill:` | `on=COL into='Other Query Title'` - click a value to open another query in-place |
| `@link:` | `on=COL into='Other Query Title'` - same, but opens in a modal |
| `@source:` | attribution URL shown on the panel |

**Templates**: `{{time_range}}` → `now() - INTERVAL <interval>`; `{{cluster_aware:system.X}}`
expands per cluster topology; `{{drill_value:col | 'default'}}` is replaced by the clicked
value (quoted) on a drill/link target. A query is a valid drill/link **target** only if it
contains a `{{drill_value:…}}` (or `{{drill:…}}`).

If you introduce a brand-new `group=` value, add it to `QueryGroup` + `QUERY_GROUPS` in
[`metaLanguage.ts`](./metaLanguage.ts) (a colour + `builtin: true`). Unknown groups still
work - they just get a default colour.

### 2. Register the module - `queries/index.ts`

```ts
import altinityKb from './altinityKb';
export const RAW_QUERIES: string[] = [ …, ...altinityKb ];
```

### 3. List it in a dashboard - `dashboards.ts`

Add a `Dashboard` to `BUILTIN_DASHBOARDS`. Panels reference queries by `'Group#Title'`.
Use `section:` on the first panel of a group to start a collapsible section.

```ts
{
  id: 'altinity-kb',
  title: 'Altinity Knowledge Base',
  group: 'Knowledge Base',       // add new groups to DashboardGroup + DASHBOARD_GROUPS
  category: 'Altinity',
  columns: 2,
  panels: [
    { queryName: 'Knowledge Base#Merges', section: 'Who Ate My CPU' },
    { queryName: 'Knowledge Base#Mutations' },
    // …
  ],
}
```

Drill/link **target** queries (those gated by `{{drill_value:…}}`) should **not** be listed
as panels - they are reached by clicking, and need a drill value to run.

### 4. Verify

```bash
npm run typecheck
npx vitest run src/components/analytics
```

Then open the app → **Analytics** → the dashboard list; the new dashboard appears under its
group. Every `queryName` must resolve to an existing `Group#Title`, or the panel is blank.

## Worked example

The **Altinity Knowledge Base** dashboard is a complete worked example of this recipe:
[`queries/altinityKb.ts`](./queries/altinityKb.ts) (the SQL) + the `altinity-kb` entry in
[`dashboards.ts`](./dashboards.ts). It is a single dashboard with collapsible sections, and
it reuses existing `Memory#` panels alongside its own queries.
