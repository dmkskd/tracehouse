import { describe, expect, it } from 'vitest';
import { legacyEventsRangeCenter } from '../events-page-model';

describe('legacyEventsRangeCenter', () => {
  it('migrates an older Time Travel event time into a stable range center', () => {
    expect(legacyEventsRangeCenter(
      'timetravel',
      undefined,
      '2026-07-25T19:00:54.808Z',
    )).toBe('2026-07-25T19:00:54.808Z');
  });

  it('does not replace an explicit range center', () => {
    expect(legacyEventsRangeCenter(
      'timetravel',
      '2026-07-25T18:00:00.000Z',
      '2026-07-25T19:00:54.808Z',
    )).toBeUndefined();
  });

  it('does not derive the range from a normal event selection', () => {
    expect(legacyEventsRangeCenter(
      undefined,
      undefined,
      '2026-07-25T19:00:54.808Z',
    )).toBeUndefined();
  });
});
