import { describe, expect, it } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { NotebookView } from '../NotebookView';
import { NotebookErrorBoundary } from '../NotebookErrorBoundary';
import { memoryLimitNotebook } from '../example';
import type { NotebookDocument } from '../model';

const wrap = (document: NotebookDocument) =>
  render(<MemoryRouter><NotebookView document={document} /></MemoryRouter>);

describe('NotebookView', () => {
  it('renders the example notebook', () => {
    const { container } = wrap(memoryLimitNotebook);
    expect(container.textContent).toContain(memoryLimitNotebook.title);
  });

  it('labels the notebook by kind', () => {
    const { container } = wrap({ ...memoryLimitNotebook, kind: 'runbook' });
    expect(container.textContent).toContain('Runbook');
  });

  it('does not render a stray 0 when limitations is empty', () => {
    // `limitations?.length && (...)` evaluates to 0 for an empty array, and
    // React renders that 0 as text.
    const { container } = wrap({ ...memoryLimitNotebook, limitations: [] });
    const stray = [...container.querySelectorAll('main')]
      .flatMap(main => [...main.childNodes])
      .filter(node => node.nodeType === Node.TEXT_NODE && node.textContent === '0');
    expect(stray).toHaveLength(0);
  });

  it('still renders limitations when present', () => {
    const { container } = wrap({ ...memoryLimitNotebook, limitations: ['sampled at 1s'] });
    expect(container.textContent).toContain('sampled at 1s');
  });
});

describe('stage panel actions', () => {
  it('groups Source, Evidence and Focus in one toolbar', () => {
    wrap(memoryLimitNotebook);
    // Previously Evidence sat alone in a footer while Focus was in the header.
    expect(screen.getAllByText('Focus').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Source').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Evidence ↗').length).toBeGreaterThan(0);
  });

  it('shows that step as Markdown on demand', () => {
    wrap(memoryLimitNotebook);
    expect(screen.queryByLabelText('Source for step 1')).toBeNull();

    fireEvent.click(screen.getAllByText('Source')[0]);
    const source = screen.getByLabelText('Source for step 1').textContent ?? '';
    expect(source).toContain(memoryLimitNotebook.stages[0].headline);
    expect(source).toContain('| ---');

    fireEvent.click(screen.getAllByText('Source')[0]);
    expect(screen.queryByLabelText('Source for step 1')).toBeNull();
  });

  it('marks only the highlighted fact tile', () => {
    // rowMatchesKey is vacuously true for an absent key, so a facts stage with
    // no rowKey rendered every tile highlighted, which highlights nothing.
    const factsStage = memoryLimitNotebook.stages.find(s => s.block === 'facts.list');
    expect(factsStage).toBeDefined();
    const document: NotebookDocument = {
      ...memoryLimitNotebook,
      stages: [{ ...factsStage!, highlight: undefined }],
    };
    const { container } = wrap(document);
    const highlighted = [...container.querySelectorAll('div')]
      .filter(el => el.getAttribute('style')?.includes('210,153,34'));
    expect(highlighted).toHaveLength(0);
  });
});

describe('NotebookErrorBoundary', () => {
  it('contains a renderer crash instead of losing the route', () => {
    // Validation should stop this reaching the renderer; the boundary is for
    // the failures we did not predict.
    const broken: NotebookDocument = {
      ...memoryLimitNotebook,
      stages: [{ ...memoryLimitNotebook.stages[0], evidence: 'does-not-exist' }],
    };

    const { container } = render(
      <MemoryRouter>
        <NotebookErrorBoundary title="broken.json">
          <NotebookView document={broken} />
        </NotebookErrorBoundary>
      </MemoryRouter>,
    );

    expect(container.textContent).toContain('could not be rendered');
    expect(container.textContent).toContain('broken.json');
  });
});
