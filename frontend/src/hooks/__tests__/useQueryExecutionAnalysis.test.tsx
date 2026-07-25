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

  it('restores a completed result for the same connection and query only', async () => {
    const analyze = vi.fn().mockResolvedValue({
      kind: 'explain_analyze',
      query: 'SELECT 1',
      output: 'cached plan',
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

    const first = renderHook(
      () => useQueryExecutionAnalysis('historical-query:SELECT 1'),
      { wrapper },
    );
    await act(async () => {
      await first.result.current.analyze({
        query: 'SELECT 1',
        source: 'TraceHouse:Queries:test',
      });
    });
    first.unmount();

    const restored = renderHook(
      () => useQueryExecutionAnalysis('historical-query:SELECT 1'),
      { wrapper },
    );
    expect(restored.result.current.result?.output).toBe('cached plan');
    expect(analyze).toHaveBeenCalledOnce();
    restored.unmount();

    const changedQuery = renderHook(
      () => useQueryExecutionAnalysis('historical-query:SELECT 2'),
      { wrapper },
    );
    expect(changedQuery.result.current.result).toBeNull();
    changedQuery.unmount();

    const otherServices = {
      queryExecutionAnalysisService: {
        supportsExplicitQueryId: () => true,
        analyze: vi.fn(),
      },
    } as unknown as ClickHouseServices;
    const otherConnectionWrapper = ({ children }: PropsWithChildren) => (
      <ClickHouseContext.Provider value={otherServices}>
        {children}
      </ClickHouseContext.Provider>
    );
    const changedConnection = renderHook(
      () => useQueryExecutionAnalysis('historical-query:SELECT 1'),
      { wrapper: otherConnectionWrapper },
    );
    expect(changedConnection.result.current.result).toBeNull();
    changedConnection.unmount();

    const clearable = renderHook(
      () => useQueryExecutionAnalysis('historical-query:SELECT 1'),
      { wrapper },
    );
    act(() => clearable.result.current.reset());
    clearable.unmount();

    const afterClear = renderHook(
      () => useQueryExecutionAnalysis('historical-query:SELECT 1'),
      { wrapper },
    );
    expect(afterClear.result.current.result).toBeNull();
  });

  it('keeps an in-flight analysis across unmounts and deduplicates the request', async () => {
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
    const analyze = vi.fn().mockReturnValue(pendingAnalysis);
    const services = {
      queryExecutionAnalysisService: {
        supportsExplicitQueryId: () => false,
        analyze,
      },
    } as unknown as ClickHouseServices;
    const wrapper = ({ children }: PropsWithChildren) => (
      <ClickHouseContext.Provider value={services}>
        {children}
      </ClickHouseContext.Provider>
    );
    const request = {
      query: 'SELECT sleep(1)',
      source: 'TraceHouse:Queries:test',
    };

    const first = renderHook(
      () => useQueryExecutionAnalysis('historical-query:pending'),
      { wrapper },
    );
    act(() => {
      void first.result.current.analyze(request);
    });
    expect(first.result.current.isAnalyzing).toBe(true);
    expect(analyze).toHaveBeenCalledOnce();
    first.unmount();

    const restored = renderHook(
      () => useQueryExecutionAnalysis('historical-query:pending'),
      { wrapper },
    );
    expect(restored.result.current.isAnalyzing).toBe(true);

    let duplicateRequest!: Promise<unknown>;
    act(() => {
      duplicateRequest = restored.result.current.analyze(request);
    });
    expect(analyze).toHaveBeenCalledOnce();

    await act(async () => {
      resolveAnalysis({
        kind: 'explain_analyze',
        query: request.query,
        output: 'completed plan',
        processors: false,
      });
      await duplicateRequest;
    });

    expect(restored.result.current.isAnalyzing).toBe(false);
    expect(restored.result.current.result?.output).toBe('completed plan');
  });
});
