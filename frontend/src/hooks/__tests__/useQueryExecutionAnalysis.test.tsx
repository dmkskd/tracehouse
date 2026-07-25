import type { PropsWithChildren } from 'react';
import { act, renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import {
  ClickHouseContext,
  type ClickHouseServices,
} from '@tracehouse/ui-shared';
import { useQueryExecutionAnalysis } from '../useQueryExecutionAnalysis';

describe('useQueryExecutionAnalysis', () => {
  it('owns query ID correlation, timing, and result state', async () => {
    const analyze = vi.fn().mockResolvedValue({
      kind: 'explain_analyze',
      query: 'SELECT 1',
      output: 'Query summary:',
      processors: false,
      queryId: 'generated-id',
    });
    const services = {
      queryExecutionAnalysisService: {
        supportsExplicitQueryId: () => true,
        analyze,
      },
    } as unknown as ClickHouseServices;
    const wrapper = ({ children }: PropsWithChildren) => (
      <ClickHouseContext.Provider value={services}>
        {children}
      </ClickHouseContext.Provider>
    );
    const hook = renderHook(() => useQueryExecutionAnalysis(), { wrapper });

    await act(async () => {
      await hook.result.current.analyze({
        query: 'SELECT 1',
        source: 'TraceHouse:Queries:test',
        processors: false,
      });
    });

    expect(analyze).toHaveBeenCalledWith(
      'SELECT 1',
      'TraceHouse:Queries:test',
      {
        database: undefined,
        processors: false,
        queryId: expect.any(String),
      },
    );
    expect(hook.result.current.result?.output).toBe('Query summary:');
    expect(hook.result.current.error).toBeNull();
    expect(hook.result.current.isAnalyzing).toBe(false);
    expect(hook.result.current.requestDurationMs).toBeGreaterThanOrEqual(0);

    act(() => hook.result.current.reset());
    expect(hook.result.current.result).toBeNull();
    expect(hook.result.current.requestDurationMs).toBe(0);
  });

  it('ignores an in-flight response after reset', async () => {
    let resolveAnalysis!: (value: {
      kind: 'explain_analyze';
      query: string;
      output: string;
      processors: boolean;
    }) => void;
    const pendingAnalysis = new Promise<{
      kind: 'explain_analyze';
      query: string;
      output: string;
      processors: boolean;
    }>(resolve => {
      resolveAnalysis = resolve;
    });
    const services = {
      queryExecutionAnalysisService: {
        supportsExplicitQueryId: () => false,
        analyze: vi.fn().mockReturnValue(pendingAnalysis),
      },
    } as unknown as ClickHouseServices;
    const wrapper = ({ children }: PropsWithChildren) => (
      <ClickHouseContext.Provider value={services}>
        {children}
      </ClickHouseContext.Provider>
    );
    const hook = renderHook(() => useQueryExecutionAnalysis(), { wrapper });

    let request!: Promise<unknown>;
    act(() => {
      request = hook.result.current.analyze({
        query: 'SELECT sleep(1)',
        source: 'TraceHouse:Queries:test',
      });
    });
    expect(hook.result.current.isAnalyzing).toBe(true);

    act(() => hook.result.current.reset());
    expect(hook.result.current.isAnalyzing).toBe(false);

    await act(async () => {
      resolveAnalysis({
        kind: 'explain_analyze',
        query: 'SELECT sleep(1)',
        output: 'stale result',
        processors: false,
      });
      await request;
    });

    expect(hook.result.current.result).toBeNull();
    expect(hook.result.current.error).toBeNull();
    expect(hook.result.current.requestDurationMs).toBe(0);
  });
});
