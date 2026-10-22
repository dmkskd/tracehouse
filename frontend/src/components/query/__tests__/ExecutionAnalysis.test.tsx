import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import {
  ExecutionAnalysisDialog,
  ExecutionAnalysisPanel,
} from '../ExecutionAnalysis';

describe('ExecutionAnalysisDialog', () => {
  it('makes query execution explicit before confirmation', () => {
    const onConfirm = vi.fn();

    render(
      <ExecutionAnalysisDialog
        onConfirm={onConfirm}
        onCancel={vi.fn()}
      />,
    );

    expect(screen.getByText('Confirm query execution?')).toBeInTheDocument();
    expect(screen.getByText('ClickHouse will execute this SELECT.')).toBeInTheDocument();
    expect(screen.queryByText('SELECT count() FROM events')).not.toBeInTheDocument();
    expect(screen.queryByRole('checkbox')).not.toBeInTheDocument();
    expect(screen.queryByRole('link')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }));
    expect(onConfirm).toHaveBeenCalledOnce();
  });
});

describe('ExecutionAnalysisPanel', () => {
  it('shows the visual plan first and preserves raw output in the last tab', () => {
    render(
      <ExecutionAnalysisPanel
        requestDurationMs={12.5}
        result={{
          kind: 'explain_analyze',
          query: 'SELECT 1',
          output: [
            'Query summary:',
            '  Time:        1.00 ms (planning 0.40 ms · execution 0.60 ms)',
            '  Read:        10 rows, 80 B (10 thousand rows/s., 80 KB/s.)',
            '  Peak memory: 1.00 KiB',
            '',
            'Output: count()',
            '',
            'Expression (Projection)',
            '│  I/O: rows 1 → 1 · 8 B → 8 B',
            '│    time 10.00 us (1.7%) · parallelism 1.00/1',
            '└──ReadFromMergeTree (events)',
            '   Read type: Default',
            '   Parts: 2 | Granules: 1',
            '   Output: event_time, event_type',
            "   Prewhere filter column: event_type = 'NewPart'",
            '   Indexes:',
            '     PrimaryKey',
            '       Keys:',
            '         event_time',
            '       Condition: (event_time in [1, +Inf))',
            '       Parts: 2/2',
            '       Granules: 1/2',
            '│  I/O: rows 0 → 1 · 0 B → 8 B',
            '│    time 100.00 us (16.7%) · parallelism 1.00/1',
          ].join('\n'),
          processors: true,
        }}
      />,
    );

    expect(screen.getByRole('tab', { name: 'Visual plan' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByText('Total')).toBeInTheDocument();
    expect(screen.getByText('1.00 ms')).toBeInTheDocument();
    expect(screen.getByText(/ReadFromMergeTree/)).toBeInTheDocument();
    expect(screen.getByText('Data flow')).toBeInTheDocument();
    expect(screen.getByText(/source → result/)).toBeInTheDocument();
    const source = screen.getByText(/ReadFromMergeTree/);
    const result = screen.getByText('Expression');
    expect(source.compareDocumentPosition(result) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.queryByText(/Most time is in/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/Query summary:/)).not.toBeInTheDocument();

    fireEvent.click(source);
    expect(screen.getByText('Index pruning')).toBeInTheDocument();
    expect(screen.getAllByText('PrimaryKey').length).toBeGreaterThan(0);
    expect(screen.getByText('50.0% pruned')).toBeInTheDocument();
    expect(screen.getByText('event_type')).toBeInTheDocument();

    const orderToggle = screen.getByRole('button', { name: 'Toggle operator order' });
    expect(orderToggle).toHaveAttribute('aria-pressed', 'false');
    expect(orderToggle).toHaveAttribute('title', expect.stringContaining('original ClickHouse plan order'));
    fireEvent.click(orderToggle);
    expect(orderToggle).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByText('ClickHouse plan')).toBeInTheDocument();
    expect(screen.getByText(/result → source/)).toBeInTheDocument();
    expect(result.compareDocumentPosition(source) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(result.closest('button')?.parentElement).toHaveStyle({ paddingLeft: '12px' });
    expect(source.closest('button')?.parentElement).toHaveStyle({ paddingLeft: '30px' });
    // Expanded details stay attached to their operator when its position changes.
    expect(screen.getByText('Index pruning')).toBeInTheDocument();
    fireEvent.click(orderToggle);
    expect(orderToggle).toHaveAttribute('aria-pressed', 'false');
    expect(source.compareDocumentPosition(result) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(source.closest('button')?.parentElement).toHaveStyle({ paddingLeft: '12px' });
    expect(result.closest('button')?.parentElement).toHaveStyle({ paddingLeft: '30px' });

    const tabs = screen.getAllByRole('tab');
    expect(tabs.map(tab => tab.textContent)).toEqual(['Visual plan', 'Raw plan']);
    fireEvent.click(screen.getByRole('tab', { name: 'Raw plan' }));

    expect(screen.getByText(/Query summary:/)).toHaveTextContent('Peak memory: 1.00 KiB');
    expect(screen.getByRole('tabpanel', { name: 'Raw plan' })).toHaveStyle({ overflow: 'auto' });
    expect(screen.getByText('PROCESSORS')).toBeInTheDocument();
    expect(screen.getByText('request 12.5ms')).toBeInTheDocument();
  });
});
