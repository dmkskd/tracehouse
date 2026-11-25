import { cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Coverage } from '@tracehouse/core/services/object-browser';
import { CollectionNotice } from '../CollectionNotice';

const coverage = (source: Coverage['source'], reason: string, state: Coverage['state'] = 'readable'): Coverage =>
  ({ source, reason, state, hosts: ['ch-1', 'ch-2'], visibility: 'unknown' });
afterEach(() => { cleanup();vi.unstubAllGlobals(); });

describe('object collection notice', () => {
  it('keeps repeated memory errors collapsed, groups the affected sources, and preserves the original messages for copying', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined), retry = vi.fn();
    vi.stubGlobal('navigator', { clipboard: { writeText } });
    const reason = 'Definition references failed for 2 object(s): tracehouse.processes_sampler: MEMORY_LIMIT_EXCEEDED RSS 6.73 GiB; demo.payments_to_minute_mv: MEMORY_LIMIT_EXCEEDED. System tables read by views unavailable: MEMORY_LIMIT_EXCEEDED. demo.sampler on ch-2: system.merges is not in the collected inventory there, link omitted';
    render(<CollectionNotice coverage={[coverage('tables', reason), coverage('grants', 'MEMORY_LIMIT_EXCEEDED grants query', 'failed')]} disabled={false} onRetry={retry} />);
    expect(screen.getByText('Capture incomplete')).toBeTruthy();
    const details = screen.getByText('Show details').closest('details')!;
    expect(details.open).toBe(false);
    fireEvent.click(screen.getByText('Show details'));
    expect(details.open).toBe(true);
    expect(screen.getAllByRole('heading', { name: 'ClickHouse server memory limit exceeded' })).toHaveLength(1);
    expect(screen.getByText(/Definition analysis failed for 2 object/)).toBeTruthy();
    expect(screen.getByText(/Referenced system tables could not be read/)).toBeTruthy();
    const technical = screen.getByText('Technical errors and coverage').closest('details')!;
    expect(technical.open).toBe(false);
    fireEvent.click(screen.getByText('Technical errors and coverage'));
    expect(within(technical).getByText(/tracehouse.processes_sampler/).textContent).toContain(reason);
    fireEvent.click(screen.getByRole('button', { name: 'Copy technical details' }));
    await screen.findByText('Copied');
    expect(writeText.mock.calls[0][0]).toContain(reason);
    expect(writeText.mock.calls[0][0]).toContain('Observed hosts: ch-1, ch-2');
    fireEvent.click(screen.getByRole('button', { name: 'Retry collection' }));
    expect(retry).toHaveBeenCalledOnce();
  });

  it('describes table-only gaps without declaring access answers incomplete', () => {
    render(<CollectionNotice coverage={[coverage('tables', 'Part counts unavailable: TIMEOUT_EXCEEDED')]} disabled onRetry={vi.fn()} />);
    expect(screen.getByText(/some table metadata or dependency links may be missing/)).toBeTruthy();
    expect(screen.queryByText(/Access answers/)).toBeNull();
    expect(screen.getByRole('button', { name: 'Retry collection' }).hasAttribute('disabled')).toBe(true);
    fireEvent.click(screen.getByText('Show details'));
    expect(screen.getByRole('heading', { name: 'Collection timed out' })).toBeTruthy();
    expect(screen.getByText(/Part counts could not be collected/)).toBeTruthy();
  });

  it('retains unknown errors and handles clipboard failure without exposing the full error by default', async () => {
    vi.stubGlobal('navigator', { clipboard: { writeText: vi.fn().mockRejectedValue(new Error('Denied')) } });
    render(<CollectionNotice error="Unexpected adapter failure: secretless diagnostic text" disabled={false} onRetry={vi.fn()} />);
    expect(screen.getByText('Collection failed')).toBeTruthy();
    expect(screen.getByText('Show details').closest('details')!.open).toBe(false);
    fireEvent.click(screen.getByText('Show details'));
    fireEvent.click(screen.getByText('Technical errors and coverage'));
    fireEvent.click(screen.getByRole('button', { name: 'Copy technical details' }));
    expect(await screen.findByText(/Could not copy/)).toBeTruthy();
    expect(screen.getByText(/Unexpected adapter failure/)).toBeTruthy();
  });

  it('renders nothing for readable sources without collection notes', () => {
    const { container } = render(<CollectionNotice coverage={[{ source: 'tables', state: 'readable', hosts: ['ch-1'], visibility: 'unknown' }]} disabled={false} onRetry={vi.fn()} />);
    expect(container.textContent).toBe('');
  });
});
