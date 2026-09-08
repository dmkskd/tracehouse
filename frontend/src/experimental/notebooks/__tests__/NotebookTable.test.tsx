import { describe, expect, it } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { NotebookTable } from '../NotebookTable';
import { validateNotebook } from '../validate';
import { memoryLimitNotebook } from '../example';
import type { NotebookEvidence, NotebookCell } from '../model';

const evidence: NotebookEvidence = {
  title: 'Queries', mode: 'snapshot', columns: ['query_id', 'description', 'failures', 'hash'],
  rows: [
    { query_id: 'q-1', description: 'Count distinct users', failures: 3, hash: 'secret-hash-1' },
    { query_id: 'q-2', description: 'Read recent events', failures: 1, hash: 'secret-hash-2' },
  ],
};
const cell: NotebookCell = {
  id: 'queries', headline: 'Distinct counting failed', takeaway: 'Three failures',
  claimType: 'observed', block: 'table.ranked', evidence: 'queries',
  encoding: { label: 'description', rankBy: 'failures' },
  columns: [{ field: 'description', label: 'Query' }, { field: 'query_id', label: 'Details', type: 'query' }, { field: 'failures', label: 'Failures' }],
};

describe('notebook table presentation', () => {
  it('shows readable columns, preserves raw details, and respects user sorting', () => {
    render(<MemoryRouter><NotebookTable cell={cell} evidence={evidence} /></MemoryRouter>);
    expect(screen.queryByText('secret-hash-1')).toBeNull();
    expect(screen.getByRole('columnheader', { name: 'Query' })).toBeDefined();
    const link = screen.getByTitle('Open query details: q-1');
    expect(link.getAttribute('href')).toBe('/queries?qd_id=q-1');
    expect(link.getAttribute('target')).toBe('_blank');
    fireEvent.click(screen.getByRole('columnheader', { name: /Failures/ }));
    expect(within(screen.getAllByRole('row')[1]).getByText('Read recent events')).toBeDefined();
    fireEvent.click(screen.getByText('Count distinct users'));
    expect(screen.getByText('secret-hash-1')).toBeDefined();
  });

  it('rejects presentation fields that are not in the captured evidence', () => {
    const result = validateNotebook({ ...memoryLimitNotebook, evidence: { queries: evidence }, cells: [{ ...cell, columns: [{ field: 'missing', label: 'Missing' }] }] });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.errors.join(' ')).toContain('must reference an evidence column');
  });
});
