import { ACCESS_HOSTS, ACCESS_PRIVILEGES, ACCESS_SCHEMA, ACCESS_CATALOGS, ACCESS_TABLE_DEPENDENCY_FIELDS, ACCESS_TABLE_SUMMARY_FIELDS, accessTableCatalogQuery, systemTablesByNameQuery, ACTIVE_PARTS_BY_TABLE } from '../../../queries/access-queries.js';
import { buildQuery, tagQuery } from '../../../queries/builder.js';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { objectKey } from '../model/types.js';
import type { Attributes, Capture, Investigation, Observation, Relationship, Source } from '../model/types.js';
import { canonical, sortedSet } from '../model/canonical.js';
import { tableLinks } from './tables.js';
import { privilegeGroup, privilegeTree, type PrivilegeTree } from '../model/privileges.js';
import type { MonitoringCapability } from '../../../types/monitoring-capabilities.js';

type Row = Record<string, unknown>;
export interface CollectOptions { limit?: number; mode?: 'catalog' | 'investigation'; cancellation?: { readonly aborted: boolean };
  /** Sources the capability probe found without SELECT (notGrantedSources); skipped without a query. */
  notGranted?: readonly Source[] }
const setFields = new Set(['default_roles_list','default_roles_except','host_ip','host_names',
  'host_names_regexp','host_names_like','apply_to_list','apply_to_except']);
function attributes(row: Row): Attributes {
  return Object.fromEntries(Object.entries(row).filter(([k]) => k !== 'host')
    .map(([k, v]) => [k, setFields.has(k) ? sortedSet(v) : v]));
}
const text = (v: unknown) => String(v ?? '');
/** Databases the catalog skips; their tables are read by name when a view reads from them. */
const SYSTEM_DATABASES = new Set(['system', 'information_schema', 'INFORMATION_SCHEMA']);
const flag = (v: unknown) => Number(v) === 1;

/** Catalog sources whose system table the capability probe (capability `system_<source>`)
 * found without SELECT. Sources it did not probe are read and report their own errors. */
export function notGrantedSources(capabilities: readonly MonitoringCapability[]): Source[] {
  return (Object.keys(ACCESS_CATALOGS) as Source[]).filter(source =>
    capabilities.some(c => c.id === `system_${source}` && !c.available && c.unavailableReason === 'grant'));
}

/** Read-only, manual capture; schema probes precede every optional catalog SELECT. */
export async function collectObjects(adapter: IClickHouseAdapter, investigation: Investigation,
  options: CollectOptions = {}): Promise<Capture> {
  const browse = options.mode === 'catalog';
  const limit = Math.max(1, Math.min(5000, options.limit ?? 1000));
  const check = () => { if (options.cancellation?.aborted) throw new Error('Object capture disposed'); };
  const run = async (sql: string, source: string) => {
    check();
    const rows = await adapter.executeQuery(tagQuery(buildQuery(sql, { ...investigation, browse: browse ? 1 : 0, limit: limit + 1 }),
      `TraceHouse:Objects:${source}`));
    check();return rows;
  };
  const capture: Capture = { schemaVersion: 1, mode: browse ? 'catalog' : 'investigation', investigation, startedAt: new Date().toISOString(),
    completedAt: '', hosts: [], observations: [], relationships: [], coverage: [],
    verdict: { kind: 'not-checked', reason: `Not checked for ${investigation.principal}: configured paths only.` } };
  const hosts = await run(ACCESS_HOSTS, 'hosts');
  capture.hosts = [...new Map(hosts.map(r => [text(r.host), { host: text(r.host), version: text(r.version) }])).values()];
  if (!capture.hosts.length || capture.hosts.some(h => !h.host || !h.version)) throw new Error('Host/version discovery returned incomplete metadata');
  const schema = await run(ACCESS_SCHEMA, 'schema');
  // Privilege groups label grant scopes. Without them scopes keep their raw privilege name.
  let privileges: PrivilegeTree | null = null, privilegesError: string | null = null;
  try { privileges = privilegeTree(await run(ACCESS_PRIVILEGES, 'privileges')); }
  catch (e) { check();privilegesError = e instanceof Error ? e.message : String(e); }
  const onEveryHost = (name: string) => capture.hosts.every(host =>
    schema.some(row => row.host === host.host && row.table === 'tables' && row.name === name));
  const tableFields = new Set(ACCESS_TABLE_DEPENDENCY_FIELDS.filter(onEveryHost));
  const summaryFields = ACCESS_TABLE_SUMMARY_FIELDS.filter(onEveryHost);
  const sources = Object.keys(ACCESS_CATALOGS) as Source[];
  const data = new Map<Source, Row[]>();
  // Three at a time, rather than an unbounded catalog fan-out.
  for (let offset = 0; offset < sources.length; offset += 3) {
    check();
    await Promise.all(sources.slice(offset, offset + 3).map(async source => {
      const spec = ACCESS_CATALOGS[source];
      if (options.notGranted?.includes(source)) {
        capture.coverage.push({ source, state: 'not-granted', hosts: [], visibility: 'unknown', reason: `Missing SELECT permission on system.${source}` });return;
      }
      const missing = capture.hosts.flatMap(h => spec.columns.filter(name =>
        !schema.some(r => r.host === h.host && r.table === source && r.name === name)).map(name => `${h.host}:${name}`));
      if (missing.length) {
        capture.coverage.push({ source, state: 'schema-unavailable', hosts: [], visibility: 'unknown', reason: `Missing/unreadable schema: ${missing.join(', ')}` });return;
      }
      try {
        const rows = await run(source === 'tables' ? accessTableCatalogQuery(new Set([...tableFields, ...summaryFields])) : spec.sql, source);
        if (rows.some(r => !capture.hosts.some(h => h.host === r.host) || spec.columns.some(k => !(k in r)) || Object.entries(r).some(([k, v]) => setFields.has(k) && !Array.isArray(v)))) throw new Error('Malformed catalog result or unexpected host');
        data.set(source, rows.slice(0, limit));
        capture.coverage.push({ source, state: rows.length > limit ? 'truncated' : 'readable',
          hosts: capture.hosts.map(h => h.host), visibility: 'unknown',
          ...(rows.length > limit ? { reason: `Row limit ${limit} reached; more rows exist.` } :
            source === 'tables' && tableFields.size < ACCESS_TABLE_DEPENDENCY_FIELDS.length ? { reason: `Fields missing on some hosts: ${ACCESS_TABLE_DEPENDENCY_FIELDS.filter(field => !tableFields.has(field)).join(', ')}; table links are partial.` } : {}) });
      } catch (e) {
        check();capture.coverage.push({ source, state: 'failed', hosts: [], visibility: 'unknown', reason: e instanceof Error ? e.message : String(e) });
      }
    }));
  }
  const k = (kind: Observation['kind'], ...names: string[]) => objectKey(investigation.connection, kind, ...names);
  const add = (kind: Observation['kind'], name: string, source: Source, row: Row, key = k(kind, name)) => {
    capture.observations.push({ key, kind, name, host: text(row.host), source, attributes: attributes(row) });return key;
  };
  const rel = (from: string, to: string, kind: Relationship['kind'], label: string, source: Source, row: Row) => {
    const evidence = attributes(row), host = text(row.host);
    capture.relationships.push({ id: canonical([host, from, to, kind, evidence]), host, from, to, kind, label, source, evidence });
  };
  for (const r of data.get('users') ?? []) add('user', text(r.name), 'users', r);
  for (const r of data.get('roles') ?? []) add('role', text(r.name), 'roles', r);
  const tableKey = k('table', investigation.database, investigation.table);
  for (const r of data.get('tables') ?? []) add('table', `${r.database}.${r.name}`, 'tables', r, k('table', text(r.database), text(r.name)));
  const links = await tableLinks(adapter, investigation.connection, data.get('tables') ?? [], tableFields, check);
  capture.observations.push(...links.observations);
  // Kept on the view's record, so its detail can offer analysis on request.
  for (const { table, host, effects } of links.unanalyzed) {
    const record = capture.observations.find(o => o.key === table && o.host === host);
    if (record) record.attributes = { ...record.attributes, analysisEffects: effects };
  }
  capture.relationships.push(...links.relationships);
  const tablesCoverage = capture.coverage.find(entry => entry.source === 'tables');
  // System tables that views read from are read by name, so they show as ordinary tables.
  const systemTables = [...new Set(links.relationships.flatMap(r => [r.from, r.to]))]
    .map(key => JSON.parse(key) as string[]).filter(([, kind, database]) => kind === 'table' && SYSTEM_DATABASES.has(database));
  if (systemTables.length && data.has('tables')) {
    try {
      const rows = await run(buildQuery(systemTablesByNameQuery(new Set(summaryFields)),
        { tables: JSON.stringify(systemTables.map(([, , database, name]) => [database, name])) }), 'tables');
      for (const r of rows) add('table', `${r.database}.${r.name}`, 'tables', r, k('table', text(r.database), text(r.name)));
    } catch (e) {
      check();links.notes.push(`System tables read by views unavailable: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  // Part counts join the table records of the same host; they are measurements, not definition.
  if (data.has('tables')) {
    try {
      for (const r of await run(ACTIVE_PARTS_BY_TABLE, 'tables')) {
        const key = k('table', text(r.database), text(r.table));
        const record = capture.observations.find(o => o.key === key && o.host === text(r.host));
        if (record) record.attributes = { ...record.attributes, active_parts: r.active_parts, newest_part: r.newest_part };
      }
    } catch (e) {
      check();links.notes.push(`Part counts unavailable: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  if (tablesCoverage && links.notes.length) tablesCoverage.reason = [tablesCoverage.reason, ...links.notes].filter(Boolean).join(' ');
  for (const r of data.get('role_grants') ?? []) {
    const recipient = r.user_name != null ? k('user', text(r.user_name)) : k('role', text(r.role_name));
    rel(recipient, k('role', text(r.granted_role_name)), 'has-role', flag(r.granted_role_is_default) ? 'has role · default' : 'has role · not default', 'role_grants', r);
  }
  if (privilegesError) {
    const coverage = capture.coverage.find(c => c.source === 'grants');
    if (coverage) coverage.reason = [coverage.reason, `Privilege groups unavailable: ${privilegesError}`].filter(Boolean).join(' ');
  }
  // Keep only the selected user's role closure, independently on each host.
  const closure = new Map(capture.hosts.map(h => [h.host, new Set([k('user', investigation.principal)])]));
  if (browse) for (const record of capture.observations) closure.get(record.host)?.add(record.key);
  for (const [host, found] of closure) {
    for (let depth = 0; depth < 16; depth++) {
      const before = found.size;
      for (const r of capture.relationships) if (r.host === host && r.kind === 'has-role' && found.has(r.from)) found.add(r.to);
      if (found.size === before) break;
      if (depth === 15) {
        const coverage = capture.coverage.find(c => c.source === 'role_grants');
        if (coverage) { coverage.state = 'truncated';coverage.reason = 'Role nesting deeper than 16 levels; paths may be incomplete.'; }
      }
    }
  }
  capture.relationships = capture.relationships.filter(r => r.source === 'tables' || closure.get(r.host)?.has(r.from));
  // The principal closure narrows users and roles only; tables and locations stay.
  capture.observations = capture.observations.filter(r => (r.kind !== 'user' && r.kind !== 'role') || closure.get(r.host)?.has(r.key));
  for (const r of data.get('grants') ?? []) {
    const recipient = r.user_name != null ? k('user', text(r.user_name)) : k('role', text(r.role_name));
    if (!closure.get(text(r.host))?.has(recipient)) continue;
    const scope = { access_type: r.access_type, access_object: r.access_object, database: r.database,
      table: r.table, column: r.column, is_wildcard: r.is_wildcard,
      ...(privileges ? { privilege_group: privilegeGroup(privileges, text(r.access_type)), privilege_level: privileges.get(text(r.access_type))?.level ?? null } : {}) };
    const scopeKey = k('grant-scope', canonical(scope));
    const name = `${r.access_type}${r.column == null ? '' : `(${r.column})`} ON ${r.database ?? '*'}.${r.table ?? '*'}`;
    add('grant-scope', name, 'grants', { host: r.host, ...scope }, scopeKey);
    rel(recipient, scopeKey, 'grant', flag(r.is_partial_revoke) ? 'partial revoke · evidence' : `${r.access_type} grant`, 'grants', r);
    // Candidate name coverage only: no privilege hierarchy, prefix, revoke or union evaluator.
    if (!browse && r.access_type === investigation.operation && !flag(r.is_wildcard) && !text(r.access_object)
      && (r.database == null || r.database === investigation.database) && (r.table == null || r.table === investigation.table)) {
      rel(scopeKey, tableKey, 'candidate-scope', flag(r.is_partial_revoke) ? 'excluded scope · evidence' : 'candidate name match', 'grants', r);
    }
  }
  for (const r of data.get('row_policies') ?? []) {
    const key = add('row-policy', text(r.name), 'row_policies', r, k('row-policy', text(r.database), text(r.table), text(r.short_name)));
    const targets = browse ? (data.get('tables') ?? []).filter(t => t.host === r.host && t.database === r.database && (r.table === '' || t.name === r.table)).map(t => k('table', text(t.database), text(t.name))) : [tableKey];
    if (browse && r.table && !targets.length) targets.push(k('table', text(r.database), text(r.table)));
    for (const target of targets) rel(key, target, 'policy-target', 'configured filter target', 'row_policies', r);
    // Selector links, never effective-filter evaluation. ALL/EXCEPT stay in evidence.
    for (const name of sortedSet(r.apply_to_list)) {
      for (const kind of ['user','role'] as const) {
        const to = k(kind, name);
        if (closure.get(text(r.host))?.has(to)) rel(key, to, 'policy-recipient', 'configured selector', 'row_policies', r);
      }
    }
  }
  // Preserve unresolved endpoint names instead of dropping assignments when an
  // inventory source is unreadable or the granted target does not exist.
  const observed = new Set(capture.observations.map(r => canonical([r.key,r.host])));
  for (const r of capture.relationships) for (const key of [r.from,r.to]) {
    const identity = canonical([key,r.host]);if (observed.has(identity)) continue;
    const [,kind,...names] = JSON.parse(key) as string[];
    capture.observations.push({ key, kind: kind as Observation['kind'], name: names.join('.'), host: r.host,
      source: r.source, attributes: { unresolvedReference: true, referencedBy: r.source } });
    observed.add(identity);
  }
  // Deduplicate repeated scope/metadata rows within a host without collapsing host records.
  capture.observations = [...new Map(capture.observations.map(r => [canonical([r.key, r.host, r.attributes]), r])).values()];
  capture.completedAt = new Date().toISOString();check();return capture;
}
