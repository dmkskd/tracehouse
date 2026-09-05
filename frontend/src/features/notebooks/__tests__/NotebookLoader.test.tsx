import { describe, expect, it } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { NotebookLoader } from '../NotebookLoader';
import { memoryLimitNotebook } from '../example';

const renderLoader = () => render(<MemoryRouter><NotebookLoader /></MemoryRouter>);

/** Type into the paste box and submit, as a user handed a notebook by an agent would. */
function paste(text: string) {
  fireEvent.change(screen.getByLabelText('or paste a notebook'), { target: { value: text } });
  fireEvent.click(screen.getByText('Load pasted notebook'));
}

describe('NotebookLoader', () => {
  it('starts empty rather than showing a fixture as if it were real data', () => {
    const { container } = renderLoader();
    expect(container.textContent).not.toContain(memoryLimitNotebook.title);
  });

  it('renders a pasted notebook', async () => {
    renderLoader();
    paste(JSON.stringify(memoryLimitNotebook));
    expect(await screen.findByText(memoryLimitNotebook.title)).toBeDefined();
  });

  it('reports validation errors and renders nothing', () => {
    const broken = { ...memoryLimitNotebook, stages: [{ ...memoryLimitNotebook.stages[0], evidence: 'nope' }] };
    const { container } = renderLoader();
    paste(JSON.stringify(broken));

    expect(container.textContent).toContain('is not a valid notebook');
    expect(container.textContent).toContain('unknown evidence nope');
    // The renderer must not have mounted at all: a half-rendered document is
    // the crash this path exists to prevent. Checked via the view's own header
    // rather than document text, since the textarea still holds the pasted JSON.
    expect(container.textContent).not.toContain('TRACEHOUSE NOTEBOOK');
  });

  it('reports malformed JSON without throwing', () => {
    const { container } = renderLoader();
    paste('{ not json');
    expect(container.textContent).toContain('not valid JSON');
  });

  it('closes a loaded notebook and returns to the empty state', async () => {
    renderLoader();
    paste(JSON.stringify(memoryLimitNotebook));
    await screen.findByText(memoryLimitNotebook.title);

    fireEvent.click(screen.getByText('Close'));
    expect(screen.queryByText(memoryLimitNotebook.title)).toBeNull();
    expect(screen.getByLabelText('or paste a notebook')).toBeDefined();
  });

  it('shows the source as Markdown, not JSON', async () => {
    renderLoader();
    paste(JSON.stringify(memoryLimitNotebook));
    await screen.findByText(memoryLimitNotebook.title);

    expect(screen.queryByLabelText('Notebook source')).toBeNull();
    fireEvent.click(screen.getByText('Full source'));

    const source = screen.getByLabelText('Notebook source').textContent ?? '';
    expect(source).toContain(`# ${memoryLimitNotebook.title}`);
    expect(source).toContain('| ---');
    expect(source).not.toContain('"schemaVersion"');

    fireEvent.click(screen.getByText('Hide full source'));
    expect(screen.queryByLabelText('Notebook source')).toBeNull();
  });

  it('still offers the example explicitly', async () => {
    renderLoader();
    fireEvent.click(screen.getByText('View example'));
    expect(await screen.findByText(memoryLimitNotebook.title)).toBeDefined();
  });
});
