import { describe, it, expect } from 'vitest';
import {
  summarizePipelineStall,
  mapPipelineStallRow,
  pipelineStallHint,
  type PipelineStallRow,
} from '../pipeline-stall.js';

const row = (over: Partial<PipelineStallRow>): PipelineStallRow => ({
  name: 'X', input_wait_us: 0, output_wait_us: 0, active_us: 0, ...over,
});

describe('summarizePipelineStall', { tags: ['observability'] }, () => {
  it('names the worst-blocked stage and which side it blocked on', () => {
    // Shape taken from a real coordinator's Pipeline tab.
    const stall = summarizePipelineStall([
      row({ name: 'ExpressionTransform', input_wait_us: 54_300_000, active_us: 2_700 }),
      row({ name: 'LimitsCheckingTransform', input_wait_us: 23_500_000, active_us: 2 }),
      row({ name: 'Remote', output_wait_us: 491_500, active_us: 142_900 }),
    ]);

    expect(stall?.processor).toBe('ExpressionTransform');
    expect(stall?.kind).toBe('input');
    expect(stall?.summary).toBe('ExpressionTransform blocked on input');
  });

  it('reports back-pressure when the output side dominates', () => {
    const stall = summarizePipelineStall([
      row({ name: 'Remote', input_wait_us: 1_000, output_wait_us: 491_500, active_us: 142_900 }),
    ]);
    expect(stall?.kind).toBe('output');
    expect(stall?.summary).toBe('Remote blocked on output');
  });

  it('skips stages that worked more than they waited', () => {
    // A busy aggregation is doing its job, not holding the pipeline up.
    const stall = summarizePipelineStall([
      row({ name: 'AggregatingTransform', input_wait_us: 100, active_us: 9_000_000 }),
      row({ name: 'Resize', input_wait_us: 10_400_000, active_us: 0 }),
    ]);
    expect(stall?.processor).toBe('Resize');
  });

  it('returns nothing when no stage stalled', () => {
    expect(summarizePipelineStall([])).toBeUndefined();
    expect(summarizePipelineStall([row({ name: 'A', active_us: 500 })])).toBeUndefined();
    expect(summarizePipelineStall([row({ input_wait_us: 5, name: '' })])).toBeUndefined();
  });

  it('parses string numerics from the HTTP interface', () => {
    const mapped = mapPipelineStallRow({
      name: 'ExpressionTransform',
      input_wait_us: '54300000',
      output_wait_us: '110400',
      active_us: '2700',
    });
    expect(mapped.input_wait_us).toBe(54_300_000);
    expect(summarizePipelineStall([mapped])?.kind).toBe('input');
  });

  it('treats negative or unparseable counters as zero', () => {
    const mapped = mapPipelineStallRow({ name: 'A', input_wait_us: -5, output_wait_us: 'x', active_us: null });
    expect(mapped.input_wait_us).toBe(0);
    expect(summarizePipelineStall([mapped])).toBeUndefined();
  });

  it('explains both stall kinds', () => {
    expect(pipelineStallHint('input')).toContain('starved');
    expect(pipelineStallHint('output')).toContain('back-pressured');
  });
});
