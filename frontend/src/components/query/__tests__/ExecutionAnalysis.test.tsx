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
  it('renders ClickHouse runtime-plan output without restructuring it', () => {
    render(
      <ExecutionAnalysisPanel
        requestDurationMs={12.5}
        result={{
          kind: 'explain_analyze',
          query: 'SELECT 1',
          output: 'Query summary:\n  Peak memory: 1.00 KiB\nReadFromSystemOne',
          processors: true,
        }}
      />,
    );

    expect(screen.getByText(/Query summary:/)).toHaveTextContent('Peak memory: 1.00 KiB');
    expect(screen.getByText(/Query summary:/)).toHaveStyle({ overflow: 'auto' });
    expect(screen.getByText('PROCESSORS')).toBeInTheDocument();
    expect(screen.getByText('request 12.5ms')).toBeInTheDocument();
  });
});
