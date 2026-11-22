import { describe, expect, it } from 'vitest';
import { stepTo, visibleTrail } from '../focusTrail';

describe('focus trail', () => {
  it('appends new steps and cuts back to a step already on the trail', () => {
    const trail = ['user', 'role', 'privilege', 'table'].reduce(stepTo, [] as readonly string[]);
    expect(trail).toEqual(['user', 'role', 'privilege', 'table']);
    expect(stepTo(trail, 'role')).toEqual(['user', 'role']);
    expect(stepTo(trail, 'table')).toEqual(trail);
  });
  it('keeps the origin and the last steps, folding the middle once the trail is long', () => {
    expect(visibleTrail(['a', 'b', 'c', 'd'])).toEqual(['a', 'b', 'c', 'd'].map(key => ({ kind: 'step', key })));
    expect(visibleTrail(['a', 'b', 'c', 'd', 'e', 'f'])).toEqual([{ kind: 'step', key: 'a' }, { kind: 'gap', hidden: ['b', 'c', 'd'] },
      { kind: 'step', key: 'e' }, { kind: 'step', key: 'f' }]);
  });
});
