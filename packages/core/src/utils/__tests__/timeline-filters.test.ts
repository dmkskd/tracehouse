import { describe, expect, it } from 'vitest';
import { DEFAULT_TIMELINE_FILTERS, describeTimelineFilters, explainListedOperations, readTimelineFilters, resolveTimelineFilters } from '../timeline-filters.js';

describe('timeline filters', () => {
  it('layers admin, personal and URL settings over the defaults, dropping invalid values', () => {
    const url = readTimelineFilters(name => ({ tt_min_dur: '1000', tt_failed_start: '1', tt_min_mem: 'lots' } as Record<string, string>)[name] ?? null);
    expect(url).toEqual({ minDurationMs: 1000, includeFailedBeforeStart: true });
    expect(resolveTimelineFilters({ minMemoryBytes: 16_777_216, includeFailedBeforeStart: false }, { minMemoryBytes: -1 }, url))
      .toEqual({ ...DEFAULT_TIMELINE_FILTERS, minMemoryBytes: 16_777_216, minDurationMs: 1000, includeFailedBeforeStart: true });
  });
  it('names what is listed', () => {
    expect(describeTimelineFilters(DEFAULT_TIMELINE_FILTERS)).toBe('over 1 MB');
    expect(describeTimelineFilters({ ...DEFAULT_TIMELINE_FILTERS, minDurationMs: 1000, includeFailedBeforeStart: true })).toBe('over 1 MB or 1 s · failed before start shown');
    expect(describeTimelineFilters({ ...DEFAULT_TIMELINE_FILTERS, minMemoryBytes: 0, includeTraceHouse: true })).toBe('all sizes · TraceHouse queries shown');
    expect(explainListedOperations(250, 'CPU', { ...DEFAULT_TIMELINE_FILTERS, minMemoryBytes: 0, includeFailedBeforeStart: true }))
      .toBe('The 250 queries, 250 merges and 250 mutations with the highest CPU, of any size. Queries that failed before start included.');
    expect(explainListedOperations(100, 'memory', { ...DEFAULT_TIMELINE_FILTERS, minDurationMs: 1000 }))
      .toBe('The 100 queries, 100 merges and 100 mutations with the highest memory, using over 1 MB of memory or running over 1 s.');
  });
});
