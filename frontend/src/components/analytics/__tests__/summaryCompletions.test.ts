import { EditorState } from '@codemirror/state';
import { CompletionContext } from '@codemirror/autocomplete';
import { describe, expect, it } from 'vitest';
import { createDirectiveCompletionSource } from '../editor/directiveCompletions';

const source = createDirectiveCompletionSource({ columns: { current: ['total_elapsed', 'total_count'] }, queries: { current: [] } });
function complete(text: string) {
  return source(new CompletionContext(EditorState.create({ doc: text }), text.length, true))?.options.map(option => option.label) ?? [];
}

describe('@summary editor completions', () => {
  it('offers the standalone directive and its selected numeric result column', () => {
    expect(complete('-- @')).toContain('@summary');
    expect(complete('-- @summary: value=')).toEqual(['total_elapsed', 'total_count']);
  });
  it('offers labels and shared time units without retaining the old chart option', () => {
    expect(complete('-- @summary: value=total_elapsed ')).toEqual(['label=', 'unit=']);
    expect(complete('-- @summary: value=total_elapsed unit=')).toContain('s');
    expect(complete('-- @chart: type=bar ')).not.toContain('summary=');
  });
});
