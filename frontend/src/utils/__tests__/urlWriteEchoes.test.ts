import { describe, expect, it } from 'vitest';
import { createUrlWriteEchoes } from '../urlWriteEchoes';

describe('createUrlWriteEchoes', () => {
  it('recognizes each of two quick writes when the router reports them back one by one', () => {
    const echoes = createUrlWriteEchoes();
    echoes.recordWrite('tt_pin=1&tt_scope=window');
    echoes.recordWrite('tt_pin=1&tt_scope=pin');

    expect(echoes.consumeEcho('tt_pin=1&tt_scope=window')).toBe(true);
    expect(echoes.consumeEcho('tt_pin=1&tt_scope=pin')).toBe(true);
  });

  it('drops older writes when the router reports only the latest one', () => {
    const echoes = createUrlWriteEchoes();
    echoes.recordWrite('a=1');
    echoes.recordWrite('a=2');

    expect(echoes.consumeEcho('a=2')).toBe(true);
    expect(echoes.consumeEcho('a=1')).toBe(false);
  });

  it('recognizes a write reported back with its parameters in a different order', () => {
    const echoes = createUrlWriteEchoes();
    // The page re-appends tt_ops; Grafana keeps it where it was.
    echoes.recordWrite('tt_dir=desc&tt_scope=pin&tt_ops=all');

    expect(echoes.consumeEcho('tt_dir=desc&tt_ops=all&tt_scope=pin')).toBe(true);
  });

  it('keeps the order of a repeated parameter significant', () => {
    const echoes = createUrlWriteEchoes();
    echoes.recordWrite('tt_host=a&tt_host=b');

    expect(echoes.consumeEcho('tt_host=b&tt_host=a')).toBe(false);
  });

  it('treats a URL the page did not write as an outside change', () => {
    const echoes = createUrlWriteEchoes();
    echoes.recordWrite('a=1');

    expect(echoes.consumeEcho('a=pasted')).toBe(false);
    expect(echoes.consumeEcho('a=1')).toBe(true);
  });
});
