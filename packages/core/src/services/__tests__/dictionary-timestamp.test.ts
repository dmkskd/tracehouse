import { describe, expect, it, vi } from 'vitest';
import { EngineInternalsService } from '../engine-internals.js';

describe('Dictionary update timestamps', { tags: ['observability'] }, () => {
  it.each([
    ['2026-11-08 17:00:00Z', '2026-11-08T17:00:00.000Z'],
    ['2026-11-08 12:00:00-05:00', '2026-11-08T17:00:00.000Z'],
    ['0000-00-00 00:00:00Z', null],
    ['1970-01-01 00:00:00Z', null],
    ['1970-01-01T01:00:00+01:00', null],
    ['', null],
  ])('normalizes or hides the update clock %s', async (raw, expected) => {
    const adapter = { executeQuery: vi.fn().mockResolvedValue([{ name: 'fixture', last_successful_update_time: raw }]) };
    const dictionaries = await new EngineInternalsService(adapter).getDictionaries();
    expect(dictionaries[0].lastSuccessfulUpdate).toBe(expected);
  });
});
