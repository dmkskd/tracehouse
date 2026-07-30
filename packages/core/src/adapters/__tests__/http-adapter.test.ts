import { describe, expect, it } from 'vitest';
import { HttpAdapter } from '../http-adapter.js';
import { AdapterError } from '../types.js';

function wrap(error: unknown): AdapterError {
  const prototype = HttpAdapter.prototype as unknown as {
    wrapError(error: unknown): AdapterError;
  };
  return prototype.wrapError(error);
}

describe('HttpAdapter error categorization', () => {
  it('uses a structured ClickHouse error code when legacy messages omit Code:', () => {
    const error = Object.assign(
      new Error("Table default.missing doesn't exist"),
      { code: '60', type: 'UNKNOWN_TABLE' },
    );

    expect(wrap(error).category).toBe('query');
  });

  it('does not treat Node network error codes as ClickHouse query codes', () => {
    const error = Object.assign(
      new Error('connect ECONNREFUSED 127.0.0.1:8123'),
      { code: 'ECONNREFUSED' },
    );

    expect(wrap(error).category).toBe('network');
  });
});
