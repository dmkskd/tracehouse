import { describe, expect, it } from 'vitest';
import { defineShareSchema, parseShareParam, serializeShareParam } from './shareSchema';

describe('share schema', () => {
  const schema = defineShareSchema({
    limit: { type: 'number', default: 100, persistDefault: true },
    hosts: { type: 'string[]' },
    enabled: { type: 'boolean', default: true },
  });

  it('keeps explicitly shared defaults when requested', () => {
    expect(serializeShareParam(100, schema.limit)).toBe('100');
    expect(serializeShareParam(true, schema.enabled)).toBeNull();
  });

  it('round-trips repeated coordinates', () => {
    const params = new URLSearchParams('hosts=a&hosts=b');
    expect(parseShareParam(params, 'hosts', schema.hosts)).toEqual(['a', 'b']);
  });
});
