import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { analyzerTableNames, astCallArguments, engineCall } from '../collectors/references.js';
import { tableLinks } from '../collectors/tables.js';

// Output captured from ClickHouse 26.10.7.24 (clickhouse local), JSONEachRow `explain` values.
const QUERY_TREE = [
  '            LEFT TABLE EXPRESSION',
  '              QUERY id: 5, alias: __table2, is_subquery: 1, is_cte: 1, cte_name: c',
  '                  TABLE id: 11, alias: __table3, table_name: default.t',
  '            RIGHT TABLE EXPRESSION',
  '              TABLE id: 7, alias: __table4, table_name: system.one',
  '          TABLE_FUNCTION id: 16, alias: __table6, table_function_name: numbers',
];
const BUFFER_AST = [
  'SelectWithUnionQuery (children 1)', ' ExpressionList (children 1)', '  SelectQuery (children 2)', '   ExpressionList (children 1)',
  '    Function Buffer (children 1)', '     ExpressionList (children 9)',
  "      Literal 'trace\\'house'", "      Literal 'merges_history'", '      Literal UInt64_1',
  '   TablesInSelectQuery (children 1)', '    TablesInSelectQueryElement (children 1)', '     TableExpression (children 1)', '      TableIdentifier system.one',
];
const EXPRESSION_AST = [
  '  Function Buffer (children 1)', '   ExpressionList (children 3)',
  '    Function currentDatabase (children 1)', '     ExpressionList', '    Identifier tbl', '    Literal UInt64_1',
];

describe('definition reference parsing', () => {
  it('takes analyzer-resolved tables and reports table functions separately', () => {
    expect(analyzerTableNames(QUERY_TREE)).toEqual({ tables: ['default.t', 'system.one'], tableFunctions: ['numbers'] });
  });
  it('isolates the engine call before engine SETTINGS, skipping quoted parentheses', () => {
    expect(engineCall("Distributed('c', 'db', 't', rand()) SETTINGS fsync_after_insert = 0")).toBe("Distributed('c', 'db', 't', rand())");
    expect(engineCall("Buffer('a)b', 't', 1, 1, 10, 1, 100, 1, 1000)")).toBe("Buffer('a)b', 't', 1, 1, 10, 1, 100, 1, 1000)");
    expect(engineCall('MergeTree')).toBeNull();
  });
  it('reads literal and identifier arguments by value and expressions as unknown', () => {
    expect(astCallArguments(BUFFER_AST, 'Buffer')).toEqual([
      { kind: 'literal', value: "trace'house" }, { kind: 'literal', value: 'merges_history' }, { kind: 'expression' }]);
    expect(astCallArguments(EXPRESSION_AST, 'Buffer')).toEqual([{ kind: 'expression' }, { kind: 'identifier', value: 'tbl' }, { kind: 'expression' }]);
    expect(astCallArguments(BUFFER_AST, 'Distributed')).toBeNull();
  });
});

describe('table links', () => {
  it('links qualified analyzer sources and reports an unqualified one', async () => {
    const lines = ['  TABLE id: 3, alias: __table1, table_name: default.t', '  TABLE id: 5, alias: __table2, table_name: tmp'];
    // EXPLAIN runs as its own statement; both the parse and the analysis get these lines.
    const adapter = { executeRawQuery: async () => lines } as unknown as IClickHouseAdapter;
    const rows = [{ host: 'ch-1', database: 'default', name: 'v', engine: 'View', as_select: 'SELECT * FROM default.t, tmp' }];
    const links = await tableLinks(adapter, 'c', rows, new Set(['as_select']), () => {});
    expect(links.relationships.map(r => [r.from, r.to, r.kind, r.evidence.method])).toEqual([
      [JSON.stringify(['c', 'table', 'default', 't']), JSON.stringify(['c', 'table', 'default', 'v']), 'reads-from', 'analyzer']]);
    expect(links.notes).toEqual(['default.v: unqualified source tmp not linked']);
  });
});
