import { describe, expect, it } from 'vitest';
import { buildErrorCodeSuggestions } from '../errorCodeFilterModel';

describe('buildErrorCodeSuggestions', () => {
  it('groups, names, and sorts positive error codes', () => {
    const records = [
      { code: 395, exception: 'Code: 395. failure (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)' },
      { code: 241, exception: 'Code: 241. failure (MEMORY_LIMIT_EXCEEDED)' },
      { code: 395, exception: 'Code: 395. repeated failure' },
      { code: 0, exception: '' },
    ];

    expect(buildErrorCodeSuggestions(
      records,
      record => record.code,
      record => record.exception,
    )).toEqual([
      { code: 395, label: 'Code 395 · FUNCTION_THROW_IF_VALUE_IS_NON_ZERO (2)' },
      { code: 241, label: 'Code 241 · MEMORY_LIMIT_EXCEEDED (1)' },
    ]);
  });
});
