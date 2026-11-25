import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { accessFixture } from '../fixtures/access.js';
import { collectObjects } from '../collectors/collect.js';
import { objectKey, type Source } from '../model/types.js';

function setup(failLookup = false) {
  const fixture = accessFixture(), queries: string[] = [];
  const tables = fixture.rows.tables.map(row => ({ ...row, name: 'sampler', engine: 'View', as_select: 'SELECT * FROM system.merges' }));
  const systemRows = fixture.rows.tables.map(row => ({ ...row, database: 'system', name: 'merges', engine: 'SystemMerges' }));
  const schema = [...fixture.schema, ...fixture.hosts.map(host => ({ host, table: 'tables', name: 'as_select' }))];
  const adapter = {
    executeQuery: async (query: string) => {
      queries.push(query);
      const source = /TraceHouse:Objects:(\w+)/.exec(query)?.[1];
      if (source === 'hosts') return query.includes('version()') ? fixture.hosts.map(host => ({ host, version: '26.9' })) : [{ host: fixture.hosts[0] }];
      if (source === 'schema') return schema;
      if (source === 'privileges') return [{ privilege: 'SELECT', level: 'COLUMN', parent_group: 'ALL' }];
      if (source === 'tables') {
        if (query.includes('JSONExtract(')) {
          if (failLookup) throw new Error('system table lookup denied');
          return systemRows;
        }
        if (query.includes('active_parts')) return [];
        return tables;
      }
      return fixture.rows[source as Source] ?? [];
    },
    executeRawQuery: async (query: string) => query.includes('QUERY TREE') ? [' TABLE id: 1, table_name: system.merges'] : ['SelectQuery'],
  } as unknown as IClickHouseAdapter;
  return { fixture, queries, systemRows, collect: () => collectObjects(adapter,
    { connection: 'test', principal: '', database: '', table: '', operation: 'SELECT' }, { mode: 'catalog' }) };
}

describe('referenced system table inventory before analyzer validation', () => {
  it('loads omitted system metadata before validating links on every host', async () => {
    const s = setup(), capture = await s.collect();
    const source = objectKey('test', 'table', 'system', 'merges');
    expect(capture.observations.filter(o => o.key === source && !o.attributes.unresolvedReference).map(o => o.host).sort()).toEqual(s.fixture.hosts);
    expect(capture.relationships.filter(r => r.kind === 'reads-from' && r.from === source).map(r => r.host).sort()).toEqual(s.fixture.hosts);
    expect(capture.coverage.find(c => c.source === 'tables')?.reason ?? '').not.toContain('link omitted');
    expect(s.queries.filter(query => query.includes('JSONExtract('))).toHaveLength(1);
  });

  it('still omits a source whose enriched inventory has a different engine on another host', async () => {
    const s = setup();s.systemRows[1].engine = 'Memory';
    const capture = await s.collect();
    expect(capture.relationships.filter(r => r.kind === 'reads-from').map(r => r.host).sort()).toEqual(s.fixture.hosts.filter(host => host !== s.fixture.hosts[1]));
    expect(capture.coverage.find(c => c.source === 'tables')?.reason).toContain('has another engine there, link omitted');
  });

  it('reports a failed lookup as unavailable inventory, without claiming the server lacks the table', async () => {
    const s = setup(true), capture = await s.collect();
    const reason = capture.coverage.find(c => c.source === 'tables')?.reason ?? '';
    expect(reason).toContain('system table lookup denied');
    expect(reason).toContain('not in the collected inventory');
    expect(reason).not.toContain('is missing there');
    expect(capture.relationships.filter(r => r.kind === 'reads-from').map(r => r.host)).toEqual([s.fixture.hosts[0]]);
  });
});
