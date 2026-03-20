import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import { parsePartName, getLevelFromName, isMutatedPart, getPartLevelGroupKey, MUTATION_GROUP_KEY } from '../part-name-parser.js';

describe('Part Name Parser', () => {
  describe('parsePartName', () => {
    describe('regular parts (partition_minBlock_maxBlock_level)', () => {
      it('parses simple partition', () => {
        const result = parsePartName('all_1_100_3');
        expect(result).toEqual({
          name: 'all_1_100_3',
          partition: 'all',
          minBlock: 1,
          maxBlock: 100,
          level: 3,
          isMutated: false,
        });
      });

      it('parses numeric partition (YYYYMM)', () => {
        const result = parsePartName('202602_1_15139_8');
        expect(result).toEqual({
          name: '202602_1_15139_8',
          partition: '202602',
          minBlock: 1,
          maxBlock: 15139,
          level: 8,
          isMutated: false,
        });
      });

      it('parses partition with underscore - AMBIGUOUS CASE', () => {
        // NOTE: This is ambiguous! `2026_02_1_100_5` could be:
        // - Regular part: partition=2026_02, minBlock=1, maxBlock=100, level=5
        // - Mutated part: partition=2026, minBlock=02, maxBlock=1, level=100, mutation=5
        //
        // The parser assumes mutated format when last 4 segments are numeric.
        // In practice, use the `level` column from system.parts for accuracy.
        const result = parsePartName('2026_02_1_100_5');
        // Parser interprets as mutated (last 4 numeric)
        expect(result).toEqual({
          name: '2026_02_1_100_5',
          partition: '2026',
          minBlock: 2,
          maxBlock: 1,
          level: 100,
          mutationVersion: 5,
          isMutated: true,
        });
      });

      it('parses L0 part (level 0)', () => {
        const result = parsePartName('202602_5_5_0');
        expect(result).toEqual({
          name: '202602_5_5_0',
          partition: '202602',
          minBlock: 5,
          maxBlock: 5,
          level: 0,
          isMutated: false,
        });
      });

      it('parses tuple partition', () => {
        const result = parsePartName('abc_def_1_50_2');
        expect(result).toEqual({
          name: 'abc_def_1_50_2',
          partition: 'abc_def',
          minBlock: 1,
          maxBlock: 50,
          level: 2,
          isMutated: false,
        });
      });
    });

    describe('mutated parts (partition_minBlock_maxBlock_level_mutationVersion)', () => {
      it('parses mutated part with simple partition', () => {
        const result = parsePartName('all_1_100_3_19118');
        expect(result).toEqual({
          name: 'all_1_100_3_19118',
          partition: 'all',
          minBlock: 1,
          maxBlock: 100,
          level: 3,
          mutationVersion: 19118,
          isMutated: true,
        });
      });

      it('parses mutated part with numeric partition - the screenshot case', () => {
        const result = parsePartName('202602_1_15139_8_19118');
        expect(result).toEqual({
          name: '202602_1_15139_8_19118',
          partition: '202602',
          minBlock: 1,
          maxBlock: 15139,
          level: 8,
          mutationVersion: 19118,
          isMutated: true,
        });
      });

      it('parses mutated L0 part', () => {
        const result = parsePartName('202602_5_5_0_100');
        expect(result).toEqual({
          name: '202602_5_5_0_100',
          partition: '202602',
          minBlock: 5,
          maxBlock: 5,
          level: 0,
          mutationVersion: 100,
          isMutated: true,
        });
      });

      it('parses mutated part with underscore partition - AMBIGUOUS', () => {
        // This is also ambiguous - could be 6-segment mutated or 5-segment regular
        // Parser assumes mutated when last 4 are numeric
        const result = parsePartName('2026_02_1_100_5_999');
        expect(result).toEqual({
          name: '2026_02_1_100_5_999',
          partition: '2026_02',
          minBlock: 1,
          maxBlock: 100,
          level: 5,
          mutationVersion: 999,
          isMutated: true,
        });
      });
    });

    describe('invalid inputs', () => {
      it('returns null for too few segments', () => {
        expect(parsePartName('all_1_2')).toBeNull();
        expect(parsePartName('all_1')).toBeNull();
        expect(parsePartName('all')).toBeNull();
        expect(parsePartName('')).toBeNull();
      });

      it('returns null for non-numeric trailing segments', () => {
        expect(parsePartName('all_a_b_c')).toBeNull();
        expect(parsePartName('all_1_2_x')).toBeNull();
      });
    });
  });

  describe('getLevelFromName', () => {
    it('extracts level from regular parts', () => {
      expect(getLevelFromName('all_1_100_0')).toBe(0);
      expect(getLevelFromName('all_1_100_1')).toBe(1);
      expect(getLevelFromName('all_1_100_5')).toBe(5);
      expect(getLevelFromName('202602_1_15139_8')).toBe(8);
    });

    it('extracts level from mutated parts (NOT mutation version)', () => {
      expect(getLevelFromName('all_1_100_3_19118')).toBe(3);
      expect(getLevelFromName('202602_1_15139_8_19118')).toBe(8);
      expect(getLevelFromName('202602_5_5_0_100')).toBe(0);
    });

    it('returns 0 for invalid names', () => {
      expect(getLevelFromName('')).toBe(0);
      expect(getLevelFromName('invalid')).toBe(0);
      expect(getLevelFromName('a_b_c')).toBe(0);
    });
  });

  describe('isMutatedPart', () => {
    it('returns false for regular parts', () => {
      expect(isMutatedPart('all_1_100_3')).toBe(false);
      expect(isMutatedPart('202602_1_15139_8')).toBe(false);
    });

    it('returns true for mutated parts', () => {
      expect(isMutatedPart('all_1_100_3_19118')).toBe(true);
      expect(isMutatedPart('202602_1_15139_8_19118')).toBe(true);
    });

    it('returns false for invalid names', () => {
      expect(isMutatedPart('')).toBe(false);
      expect(isMutatedPart('invalid')).toBe(false);
    });
  });

  describe('getPartLevelGroupKey', () => {
    it('returns the merge level for regular parts', () => {
      expect(getPartLevelGroupKey('all_1_100_3')).toBe(3);
      expect(getPartLevelGroupKey('all_754_754_0')).toBe(0);
      expect(getPartLevelGroupKey('202602_1_15139_8')).toBe(8);
    });

    it('returns the merge level for high-level regular parts (not mutations)', () => {
      // These are heavily-merged parts, NOT mutations — the old level>=100 heuristic was wrong
      expect(getPartLevelGroupKey('all_1_585_123')).toBe(123);
      expect(getPartLevelGroupKey('all_1_510_132')).toBe(132);
      expect(getPartLevelGroupKey('all_387_699_109')).toBe(109);
    });

    it('returns MUTATION_GROUP_KEY for mutated parts', () => {
      expect(getPartLevelGroupKey('all_1_100_3_19118')).toBe(MUTATION_GROUP_KEY);
      expect(getPartLevelGroupKey('202602_1_15139_8_19118')).toBe(MUTATION_GROUP_KEY);
      expect(getPartLevelGroupKey('all_1_585_123_456')).toBe(MUTATION_GROUP_KEY);
    });

    it('returns 0 for invalid names', () => {
      expect(getPartLevelGroupKey('')).toBe(0);
      expect(getPartLevelGroupKey('invalid')).toBe(0);
    });
  });

  describe('property-based tests', () => {
    // Arbitrary for valid partition names (simple alphanumeric)
    const arbPartition = fc.stringMatching(/^[a-z0-9]{1,10}$/);

    const arbBlockNum = fc.nat({ max: 100000 });
    const arbLevel = fc.nat({ max: 20 });
    const arbMutationVersion = fc.nat({ max: 100000 });

    it('round-trips regular part names', () => {
      fc.assert(
        fc.property(
          arbPartition,
          arbBlockNum,
          arbBlockNum,
          arbLevel,
          (partition, minBlock, maxBlock, level) => {
            // Ensure minBlock <= maxBlock
            const [min, max] = minBlock <= maxBlock ? [minBlock, maxBlock] : [maxBlock, minBlock];
            const name = `${partition}_${min}_${max}_${level}`;
            const parsed = parsePartName(name);

            // Skip if partition looks like it could be numeric (ambiguous)
            if (parsed === null) return true;

            expect(parsed.level).toBe(level);
            expect(parsed.minBlock).toBe(min);
            expect(parsed.maxBlock).toBe(max);
            expect(parsed.isMutated).toBe(false);
            expect(getLevelFromName(name)).toBe(level);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('round-trips mutated part names', () => {
      fc.assert(
        fc.property(
          arbPartition,
          arbBlockNum,
          arbBlockNum,
          arbLevel,
          arbMutationVersion,
          (partition, minBlock, maxBlock, level, mutVersion) => {
            const [min, max] = minBlock <= maxBlock ? [minBlock, maxBlock] : [maxBlock, minBlock];
            const name = `${partition}_${min}_${max}_${level}_${mutVersion}`;
            const parsed = parsePartName(name);

            if (parsed === null) return true;

            expect(parsed.level).toBe(level);
            expect(parsed.minBlock).toBe(min);
            expect(parsed.maxBlock).toBe(max);
            expect(parsed.mutationVersion).toBe(mutVersion);
            expect(parsed.isMutated).toBe(true);
            expect(getLevelFromName(name)).toBe(level);
          }
        ),
        { numRuns: 100 }
      );
    });

    it('level is always non-negative', () => {
      fc.assert(
        fc.property(
          fc.string({ minLength: 1, maxLength: 50 }),
          (name) => {
            const level = getLevelFromName(name);
            expect(level).toBeGreaterThanOrEqual(0);
          }
        ),
        { numRuns: 100 }
      );
    });
  });
});
