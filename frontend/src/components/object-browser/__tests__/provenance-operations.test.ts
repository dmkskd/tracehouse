import { renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { Capture } from '@tracehouse/core/services/object-browser';
import { useProvenanceOperations } from '../useProvenanceOperations';

const capture = (operation: string): Capture => ({
  schemaVersion: 1, mode: 'investigation', investigation: { connection: 'test', principal: 'alice', database: 'demo', table: 'trips', operation },
  startedAt: '', completedAt: 'done', hosts: [{ host: 'ch-1', version: '26.9.7.9' }], coverage: [], verdict: { kind: 'not-checked', reason: 'provenance' },
  observations: [], relationships: [],
});

describe('provenance operations', () => {
  it('starts on several access types: collects the first, then each next one as the previous arrives', () => {
    const collect = vi.fn(), show = vi.fn();
    const { result, rerender } = renderHook(({ current }) => useProvenanceOperations(current, collect, show), { initialProps: { current: null as Capture | null } });
    result.current.start({ principal: 'alice', database: 'demo', table: 'trips' }, ['SELECT', 'INSERT']);
    expect(collect).toHaveBeenLastCalledWith({ principal: 'alice', database: 'demo', table: 'trips', operation: 'SELECT' });
    rerender({ current: capture('SELECT') });
    expect(collect).toHaveBeenLastCalledWith({ principal: 'alice', database: 'demo', table: 'trips', operation: 'INSERT' });
    rerender({ current: capture('INSERT') });
    expect(collect).toHaveBeenCalledTimes(2);
    expect(result.current.operations).toEqual(['SELECT', 'INSERT']);
    // The picker removes a type; the shown one moves to the first left.
    result.current.toggle('INSERT');
    expect(show).toHaveBeenCalledWith(expect.objectContaining({ investigation: expect.objectContaining({ operation: 'SELECT' }) }));
  });
});
