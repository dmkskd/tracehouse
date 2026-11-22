import { explainEngineCall, explainViewAst, explainViewSources } from '../../../queries/access-queries.js';
import { tagQuery } from '../../../queries/builder.js';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { engineSpec, type ReferenceSource } from '../model/engines.js';
import type { ReferenceMethod } from '../model/methods.js';

/** Table names the analyzer resolved. CTEs are inlined as subqueries and table
 * functions are separate nodes, so neither appears here. */
export function analyzerTableNames(lines: string[]): { tables: string[]; tableFunctions: string[] } {
  const tables: string[] = [], tableFunctions: string[] = [];
  for (const line of lines) {
    const table = /^\s*TABLE id: \d+, .*?table_name: (.+)$/.exec(line);
    if (table) tables.push(table[1]);
    const fn = /^\s*TABLE_FUNCTION id: \d+, .*?table_function_name: (.+)$/.exec(line);
    if (fn) tableFunctions.push(fn[1]);
  }
  return { tables: [...new Set(tables)], tableFunctions: [...new Set(tableFunctions)] };
}

/** Table functions with a fixed structure that read nothing outside the server's memory. */
const LOCAL_TABLE_FUNCTIONS = new Set(['numbers', 'numbers_mt', 'zeros', 'zeros_mt', 'generate_series', 'generateSeries', 'values']);
/** Subqueries in these arguments are not run by the analyzer; any other expression subquery is a scalar it runs. */
const SET_FUNCTIONS = new Set(['in', 'notIn', 'globalIn', 'globalNotIn', 'nullIn', 'notNullIn', 'globalNullIn', 'globalNotNullIn', 'exists']);
/** What resolving a stored query with the analyzer (EXPLAIN QUERY TREE) does beyond reading
 * metadata, read from its parse (EXPLAIN AST): table functions, which are asked for their
 * columns and can contact their source, and scalar subqueries, which run. */
export function analysisEffects(lines: string[]): string[] {
  const effects: string[] = [];
  const parents: { depth: number; text: string }[] = [];
  for (const line of lines) {
    const depth = line.length - line.trimStart().length, text = line.trim();
    while (parents.length && parents[parents.length - 1].depth >= depth) parents.pop();
    const parent = parents.at(-1)?.text ?? '', grandparent = parents.at(-2)?.text ?? '';
    const fn = /^Function (\S+)/.exec(text);
    if (fn && parent.startsWith('TableExpression') && !LOCAL_TABLE_FUNCTIONS.has(fn[1])) effects.push(`${fn[1]}() table function`);
    if (text.startsWith('Subquery') && !parent.startsWith('TableExpression') && !parent.startsWith('WithElement')
      && !(parent.startsWith('ExpressionList') && SET_FUNCTIONS.has(/^Function (\S+)/.exec(grandparent)?.[1] ?? ''))) effects.push('scalar subquery');
    parents.push({ depth, text });
  }
  return [...new Set(effects)];
}

/** The engine call at the start of engine_full, without trailing engine SETTINGS.
 * TraceHouse finds the closing parenthesis itself, skipping quoted text. */
export function engineCall(engineFull: string): string | null {
  const open = engineFull.indexOf('(');
  if (open < 1) return null;
  let depth = 0, quote: string | null = null;
  for (let i = open; i < engineFull.length; i++) {
    const c = engineFull[i];
    if (quote) { if (c === '\\') i++;else if (c === quote) quote = null;continue; }
    if (c === "'" || c === '`' || c === '"') quote = c;
    else if (c === '(') depth++;
    else if (c === ')' && --depth === 0) return engineFull.slice(0, i + 1);
  }
  return null;
}

const UNESCAPE: Record<string, string> = { n: '\n', t: '\t', r: '\r', '0': '\0' };
/** A direct argument of the parsed call. An identifier in first position is usually a named collection. */
export type CallArgument = { kind: 'literal'; value: string } | { kind: 'identifier'; value: string } | { kind: 'expression' };
/** Direct arguments of the parsed call: string literals and identifiers by value, anything else as an expression. */
export function astCallArguments(lines: string[], engine: string): CallArgument[] | null {
  const indent = (line: string) => line.length - line.trimStart().length;
  const at = lines.findIndex(line => line.trim().startsWith(`Function ${engine} `) || line.trim() === `Function ${engine}`);
  if (at < 0) return null;
  const depth = indent(lines[at]);
  const args: CallArgument[] = [];
  for (const line of lines.slice(at + 2)) {
    const d = indent(line);
    if (d <= depth + 1) break;
    if (d !== depth + 2) continue;
    const text = line.trim();
    const literal = /^Literal '(.*)'$/.exec(text);
    const identifier = /^Identifier (\S+)$/.exec(text);
    args.push(literal ? { kind: 'literal', value: literal[1].replace(/\\(.)/g, (_, c: string) => UNESCAPE[c] ?? c) }
      : identifier ? { kind: 'identifier', value: identifier[1] } : { kind: 'expression' });
  }
  return args;
}

/** What a reference points at: a table by its two names, a table as the analyzer printed it, or a storage location. */
export type ReferenceTarget =
  | { kind: 'table'; database: string; table: string }
  | { kind: 'qualified'; qualified: string }
  | { kind: 'location'; location: string; backend: string };
export interface DefinitionReference {
  /** Index into the table rows given to `collectDefinitionReferences`. */
  row: number;
  /** upstream: the target feeds the row's object. downstream: the object feeds the target. */
  flow: 'upstream' | 'downstream';
  kind: 'reads-from' | 'writes-to' | 'points-to';
  label: string;
  target: ReferenceTarget;
  method: ReferenceMethod;
  /** The stored definition text that was analyzed. */
  definition: string;
}
/** A view query not analyzed, with what analysis would do (analysisEffects). Index into the rows. */
export interface UnanalyzedQuery { row: number; effects: string[] }
export interface ReferenceCollection { references: DefinitionReference[]; notes: string[]; unanalyzed: UnanalyzedQuery[] }

type Row = Record<string, unknown>;
type Found = Omit<DefinitionReference, 'row'>;
/** Runs one EXPLAIN statement and returns its output lines. */
type Explain = (sql: string) => Promise<string[]>;
interface ReferenceStrategy<S extends ReferenceSource> {
  /** The definition to read; `key` deduplicates identical definitions. */
  request(row: Row, source: S): { key: string } | { note: string } | null;
  read(row: Row, source: S, explain: Explain): Promise<{ found: Found[]; notes: string[]; effects?: string[] }>;
}
const objectName = (row: Row) => `${row.database}.${row.name}`;

/** The engine call, parsed by the server, shared by the argument readers. */
const engineCallRequest = (row: Row) => {
  if (typeof row.engine_full !== 'string') return null;
  const call = engineCall(row.engine_full);
  return call ? { key: `engine:${call}` } : { note: `${objectName(row)}: engine call not found in engine_full` };
};
/** Server parse of the engine call, as AST lines. engineCallRequest checked that it exists. */
const parsedEngineCall = (row: Row, explain: Explain) => explain(explainEngineCall(engineCall(String(row.engine_full))!));
const STRATEGIES: { [R in ReferenceSource['reader']]: ReferenceStrategy<Extract<ReferenceSource, { reader: R }>> } = {
  'query-sources': {
    request: row => typeof row.as_select === 'string' && row.as_select ? { key: `analyzer:${row.as_select}` } : null,
    // Parsed first: a query with effects is not analyzed during collection.
    read: async (row, source, explain) => {
      const effects = analysisEffects(await explain(explainViewAst(String(row.as_select))));
      if (effects.length) return { found: [], notes: [`${objectName(row)}: not analyzed (${effects.join(', ')})`], effects };
      const { tables, tableFunctions } = analyzerTableNames(await explain(explainViewSources(String(row.as_select))));
      return {
        found: tables.map(qualified => ({ flow: 'upstream', kind: 'reads-from', label: source.label, target: { kind: 'qualified', qualified }, method: 'analyzer', definition: String(row.as_select) })),
        notes: tableFunctions.length ? [`${objectName(row)}: table functions not linked (${tableFunctions.join(', ')})`] : [],
      };
    },
  },
  'table-arguments': {
    request: engineCallRequest,
    read: async (row, source, explain) => {
      const args = astCallArguments(await parsedEngineCall(row, explain), String(row.engine));
      const database = args?.[source.database], table = args?.[source.table];
      return database && 'value' in database && table && 'value' in table
        ? { found: [{ flow: 'downstream', kind: 'writes-to', label: source.label, target: { kind: 'table', database: database.value, table: table.value }, method: 'engine-arguments', definition: String(row.engine_full) }], notes: [] }
        : { found: [], notes: [`${objectName(row)}: ${row.engine} destination is not a literal or identifier`] };
    },
  },
  'location-argument': {
    request: engineCallRequest,
    read: async (row, source, explain) => {
      const arg = astCallArguments(await parsedEngineCall(row, explain), String(row.engine))?.[source.argument];
      if (arg?.kind === 'literal') return { found: [{ flow: 'downstream', kind: 'points-to', label: source.label,
        target: { kind: 'location', location: arg.value, backend: source.backend }, method: 'engine-location', definition: String(row.engine_full) }], notes: [] };
      const reason = arg?.kind === 'identifier' ? `named collection ${arg.value}, not resolved` : 'not a string literal';
      return { found: [], notes: [`${objectName(row)}: ${row.engine} location is ${reason}`] };
    },
  },
};
/** The mapped type pairs each method with its own source shape; lookups widen it again. */
const strategyFor = (source: ReferenceSource) => STRATEGIES[source.reader] as ReferenceStrategy<ReferenceSource>;

const MAX_DEFINITIONS = 300;
const CONCURRENCY = 3;

/** Reads links the native fields do not cover, using each engine's reference source.
 * Each distinct definition is analyzed once. Failures are reported in notes. */
export async function collectDefinitionReferences(adapter: IClickHouseAdapter, rows: Row[],
  check: () => void): Promise<ReferenceCollection> {
  const jobs = new Map<string, { rows: number[]; source: ReferenceSource }>();
  const notes: string[] = [];
  rows.forEach((row, index) => {
    const source = engineSpec(row.engine).references;
    const request = source && strategyFor(source).request(row, source);
    if (!source || !request) return;
    if ('note' in request) { notes.push(request.note);return; }
    const job = jobs.get(request.key) ?? { rows: [], source };
    job.rows.push(index);jobs.set(request.key, job);
  });
  const queue = [...jobs.values()];
  if (queue.length > MAX_DEFINITIONS) notes.push(`Definition references bounded to ${MAX_DEFINITIONS} of ${queue.length} definitions.`);
  const references: DefinitionReference[] = [], unanalyzed: UnanalyzedQuery[] = [];
  const failures: string[] = [];
  // EXPLAIN runs as its own statement; executeQuery would append FORMAT, which EXPLAIN AST parses as part of the query.
  const explain: Explain = async sql => {
    if (!adapter.executeRawQuery) throw new Error('connection cannot run EXPLAIN statements');
    const lines = await adapter.executeRawQuery(tagQuery(sql, 'TraceHouse:Objects:references'));
    check();return lines;
  };
  const work = queue.slice(0, MAX_DEFINITIONS);
  const worker = async () => {
    for (let job = work.shift(); job; job = work.shift()) {
      check();
      const first = rows[job.rows[0]];
      try {
        const result = await strategyFor(job.source).read(first, job.source, explain);
        notes.push(...result.notes);
        for (const row of job.rows) for (const found of result.found) references.push({ row, ...found });
        if (result.effects) for (const row of job.rows) unanalyzed.push({ row, effects: result.effects });
      } catch (e) {
        failures.push(`${objectName(first)}: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
  };
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  if (failures.length) notes.push(`Definition references failed for ${failures.length} object(s): ${failures.slice(0, 3).join('; ')}`);
  return { references, notes, unanalyzed };
}
