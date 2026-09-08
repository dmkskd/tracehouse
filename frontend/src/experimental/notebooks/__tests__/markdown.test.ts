import { describe, expect, it } from 'vitest';
import { memoryLimitNotebook } from '../example';
import { notebookToMarkdown } from '../markdown';
import type { NotebookDocument } from '../model';

const md = (document: NotebookDocument = memoryLimitNotebook) => notebookToMarkdown(document);

describe('notebookToMarkdown', () => {
  it('leads with the title, question and scope', () => {
    const out = md();
    expect(out).toContain(`# ${memoryLimitNotebook.title}`);
    expect(out).toContain(`> ${memoryLimitNotebook.question}`);
    expect(out).toContain('**Investigation**');
    expect(out).toContain('hosts: ch-02');
  });

  it('renders evidence rows as a table with units in the header', () => {
    expect(md()).toContain('| timestamp | memory_gib (GiB) |');
  });

  it('marks only the highlighted row', () => {
    // rowMatchesKey is vacuously true for an absent key, so a cell that
    // highlights a timestamp rather than a row once marked every row.
    const out = md();
    const marked = out.split('\n').filter(line => line.includes('**←**'));
    const cellsWithRowKey = memoryLimitNotebook.cells.filter(s => s.highlight?.rowKey).length;
    expect(marked.length).toBe(cellsWithRowKey);
  });

  it('escapes pipes and newlines so a value cannot break the table', () => {
    const document: NotebookDocument = {
      ...memoryLimitNotebook,
      evidence: {
        ...memoryLimitNotebook.evidence,
        hostile: {
          title: 'Hostile', mode: 'snapshot',
          columns: ['text'],
          rows: [{ text: 'a | b\nc' }],
        },
      },
      cells: [{ ...memoryLimitNotebook.cells[0], evidence: 'hostile', encoding: { label: 'text', value: 'text' } }],
    };
    const row = md(document).split('\n').find(line => line.includes('a \\| b'));
    expect(row).toBeDefined();
    expect(row).not.toContain('\n');
  });


  it('names missing evidence rather than emitting an empty section', () => {
    const document: NotebookDocument = {
      ...memoryLimitNotebook,
      cells: [{ ...memoryLimitNotebook.cells[0], evidence: 'gone' }],
    };
    expect(md(document)).toContain('_Missing evidence: `gone`_');
  });

  it('lists limitations when present and omits the heading when not', () => {
    expect(md()).toContain('## Limitations');
    expect(md({ ...memoryLimitNotebook, limitations: [] })).not.toContain('## Limitations');
  });

  it('states the block and encoding each cell asked for', () => {
    // Two cells can carry identical evidence and render differently — a ranked
    // table against a grid of fact tiles. Only `block` explains that, so the
    // source view has to name it.
    const out = md();
    for (const cell of memoryLimitNotebook.cells) {
      expect(out).toContain(`block \`${cell.block}\``);
      expect(out).toContain(`id \`${cell.id}\``);
    }
    expect(out).toContain('x: timestamp · y: memory_gib');
    expect(out).toContain('label: fact · value: value');
  });

  it('names a timestamp highlight, which has no row to mark', () => {
    const cell = memoryLimitNotebook.cells.find(s => s.highlight?.timestamp);
    expect(cell).toBeDefined();
    expect(md()).toContain(`highlight: ${cell!.highlight!.timestamp}`);
  });

  it('names a row highlight alongside the row marker', () => {
    expect(md()).toContain('highlight: actor_id=q-123');
  });

  it('carries the evidence key, provenance and actions', () => {
    const out = md();
    expect(out).toContain('(`query-mechanism`, captured evidence)');
    expect(out).toContain('Provenance: kind=fixture · diagnostic=memory-attribution');
    expect(out).toContain('Actions: open-evidence → `host-memory`');
  });

  it('maps renamed display columns back to their evidence fields', () => {
    const document: NotebookDocument = {
      ...memoryLimitNotebook,
      cells: [{
        ...memoryLimitNotebook.cells[1],
        columns: [{ field: 'actor', label: 'Actor' }, { field: 'peak_gib', label: 'Peak', type: 'bytes' }],
      }],
    };
    expect(md(document)).toContain('Displayed columns: `actor` → Actor · `peak_gib` → Peak (bytes)');
  });

  it('leaves no cell field out of the source view', () => {
    // The parity guarantee itself: a field added to a cell without a matching
    // line here is a field a reader can only find by opening the JSON.
    const rendered = new Set(['headline', 'claimType', 'block', 'encoding', 'evidence',
      'takeaway', 'highlight', 'actions', 'id', 'columns']);
    const declared = new Set(memoryLimitNotebook.cells.flatMap(cell => Object.keys(cell)));
    expect([...declared].filter(field => !rendered.has(field))).toEqual([]);
  });
});
