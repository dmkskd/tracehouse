import { describe, expect, it } from 'vitest';
import { globalRefreshLabel } from '../layout-refresh-model';

describe('globalRefreshLabel', () => {
  it('does not describe an idle screen as connecting', () => {
    expect(globalRefreshLabel(5, null, 'idle')).toBe('Ready');
  });

  it('prioritizes paused and failed refresh states', () => {
    expect(globalRefreshLabel(0, null, 'idle')).toBe('Paused');
    expect(globalRefreshLabel(5, null, 'error')).toBe('Refresh failed');
  });

  it('reports the age of the latest successful refresh', () => {
    const now = Date.parse('2026-07-26T12:00:30.000Z');
    expect(globalRefreshLabel(
      5,
      new Date('2026-07-26T12:00:25.000Z'),
      'polling',
      now,
    )).toBe('5s ago');
  });
});
