import { describe, it, expect } from 'vitest';
import { buildInspectorModel } from '../arena-inspector-model';
import type { BlockEntry } from '../arena-types';

const base: BlockEntry = {
  id: 'm:h1:db.t.all_1_6_1', kind: 'MERGE', color: 'hsl(38, 90%, 57%)', label: 'db.t', tableHint: 'db.t',
  isMerge: true, deck: 'merge', lane: 0, startTime: 0, endTime: null,
  cpu: 0.3, mem: 48_150_000, elapsed: 1.1, progress: 0.374,
  ioReadRate: 0, rowsRead: 0, bytesRead: 0, readBytesPerSec: 49_900_000, writeBytesPerSec: 63_000_000,
  numParts: 6, mergeType: 'Regular', hostname: 'h1',
};

const labels = (vals: { label: string }[] | null) => (vals ?? []).map(v => v.label);

describe('buildInspectorModel', () => {
  it('running merge: progress shown, placeholder CPU left out', () => {
    const m = buildInspectorModel(base);
    expect(m.phase).toBe('running');
    expect(m.progress).toEqual({ pct: 37.4, caption: 'merged' });
    expect(labels(m.resources)).not.toContain('Avg CPU');
  });

  it('merge that finished between polls: no progress, last-sample note', () => {
    const m = buildInspectorModel({ ...base, endTime: 1000 });
    expect(m.phase).toBe('last-sample');
    expect(m.progress).toBeNull();
    expect(m.hero).toBeNull();
    expect(m.note).toMatch(/last live sample/);
  });

  it('finished query with its final record: duration, avg CPU and peak memory lead, no progress', () => {
    const m = buildInspectorModel({
      ...base, id: 'q1', kind: 'SELECT', isMerge: false, deck: 'select', endTime: 1000, final: true,
      cpu: 1.58, mem: 36_870_000, elapsed: 14.8, progress: 100, rowsRead: 0, ioReadRate: 17_800,
    });
    expect(m.progress).toBeNull();
    expect(m.hero).toEqual([
      { label: 'duration', value: '14.8s' },
      { label: 'avg CPU cores', value: '1.58' },
      { label: 'peak memory', value: expect.stringContaining('MB') },
    ]);
    expect(labels(m.resources)).toEqual(['Avg read']);
  });
});
