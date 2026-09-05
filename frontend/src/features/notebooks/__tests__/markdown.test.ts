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
    // rowMatchesKey is vacuously true for an absent key, so a stage that
    // highlights a timestamp rather than a row once marked every row.
    const out = md();
    const marked = out.split('\n').filter(line => line.includes('**←**'));
    const stagesWithRowKey = memoryLimitNotebook.stages.filter(s => s.highlight?.rowKey).length;
    expect(marked.length).toBe(stagesWithRowKey);
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
      stages: [{ ...memoryLimitNotebook.stages[0], evidence: 'hostile', encoding: { label: 'text', value: 'text' } }],
    };
    const row = md(document).split('\n').find(line => line.includes('a \\| b'));
    expect(row).toBeDefined();
    expect(row).not.toContain('\n');
  });

  it('keeps the caveat as a callout', () => {
    const inferred = memoryLimitNotebook.stages.find(s => s.caveat);
    expect(inferred).toBeDefined();
    expect(md()).toContain(`> **Inference boundary.** ${inferred!.caveat}`);
  });

  it('names missing evidence rather than emitting an empty section', () => {
    const document: NotebookDocument = {
      ...memoryLimitNotebook,
      stages: [{ ...memoryLimitNotebook.stages[0], evidence: 'gone' }],
    };
    expect(md(document)).toContain('_Missing evidence: `gone`_');
  });

  it('lists limitations when present and omits the heading when not', () => {
    expect(md()).toContain('## Limitations');
    expect(md({ ...memoryLimitNotebook, limitations: [] })).not.toContain('## Limitations');
  });
});
