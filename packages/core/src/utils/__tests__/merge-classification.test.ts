import { describe, it, expect } from 'vitest';
import {
  classifyActiveMerge,
  classifyMergeHistory,
  refineCategoryWithRowDiff,
  isPatchPart,
  isDeduplicatingEngine,
  markReplicaMerges,
  markReplicaMergeHistory,
  categoryToPartLogCondition,
} from '../merge-classification.js';

describe('merge-classification', { tags: ['merge-engine'] }, () => {
  // ── classifyActiveMerge ──────────────────────────────────────────

  describe('classifyActiveMerge', () => {
    it('classifies regular merge', () => {
      expect(classifyActiveMerge('Regular', false)).toBe('Regular');
    });

    it('classifies TTL delete', () => {
      expect(classifyActiveMerge('TTLDelete', false)).toBe('TTLDelete');
    });

    it('classifies TTL recompress', () => {
      expect(classifyActiveMerge('TTLRecompress', false)).toBe('TTLRecompress');
    });

    it('classifies mutation via is_mutation flag', () => {
      expect(classifyActiveMerge('Regular', true)).toBe('Mutation');
    });

    it('classifies patch part as LightweightUpdate even if is_mutation=false', () => {
      expect(classifyActiveMerge('Regular', false, 'patch-abc123-202602_1_5_0_1934')).toBe('LightweightUpdate');
    });

    it('classifies patch part as LightweightUpdate over is_mutation=true', () => {
      expect(classifyActiveMerge('Regular', true, 'patch-abc123-202602_1_5_0_1934')).toBe('LightweightUpdate');
    });

    it('falls back to Regular for unknown merge_type', () => {
      expect(classifyActiveMerge('SomethingNew', false)).toBe('Regular');
    });
  });

  // ── classifyMergeHistory ─────────────────────────────────────────

  describe('classifyMergeHistory', () => {
    it('classifies MovePart as TTLMove', () => {
      expect(classifyMergeHistory('MovePart', '')).toBe('TTLMove');
    });

    it('classifies MutatePart as Mutation', () => {
      expect(classifyMergeHistory('MutatePart', 'NotAMerge')).toBe('Mutation');
    });

    it('classifies patch part as LightweightUpdate regardless of event_type', () => {
      expect(classifyMergeHistory('MergeParts', 'RegularMerge', 'patch-abc-all_1_5_0')).toBe('LightweightUpdate');
      expect(classifyMergeHistory('MutatePart', 'NotAMerge', 'patch-abc-all_1_5_0')).toBe('LightweightUpdate');
    });

    it('classifies DownloadPart for a regular part as Regular', () => {
      expect(classifyMergeHistory('DownloadPart', 'RegularMerge', '202602_1_100_3')).toBe('Regular');
    });

    it('classifies DownloadPart for a patch part as LightweightUpdate', () => {
      expect(classifyMergeHistory('DownloadPart', '', 'patch-abc-202602_1_5_0')).toBe('LightweightUpdate');
    });

    it('classifies TTLDeleteMerge', () => {
      expect(classifyMergeHistory('MergeParts', 'TTLDeleteMerge')).toBe('TTLDelete');
    });

    it('classifies TTLRecompressMerge', () => {
      expect(classifyMergeHistory('MergeParts', 'TTLRecompressMerge')).toBe('TTLRecompress');
    });

    it('falls back to Regular for unknown merge_reason', () => {
      expect(classifyMergeHistory('MergeParts', 'SomethingNew')).toBe('Regular');
    });

    it('falls back to Regular when no merge_reason', () => {
      expect(classifyMergeHistory('MergeParts', '')).toBe('Regular');
    });
  });

  // ── refineCategoryWithRowDiff ────────────────────────────────────

  describe('refineCategoryWithRowDiff', () => {
    it('refines Regular with negative row diff to LightweightDelete', () => {
      expect(refineCategoryWithRowDiff('Regular', -500)).toBe('LightweightDelete');
    });

    it('refines Regular with negative row diff on plain MergeTree', () => {
      expect(refineCategoryWithRowDiff('Regular', -500, 'MergeTree')).toBe('LightweightDelete');
    });

    it('leaves Regular unchanged with zero row diff', () => {
      expect(refineCategoryWithRowDiff('Regular', 0)).toBe('Regular');
    });

    it('leaves Mutation unchanged even with negative row diff', () => {
      expect(refineCategoryWithRowDiff('Mutation', -100)).toBe('Mutation');
    });

    it('skips LightweightDelete for ReplacingMergeTree (natural dedup)', () => {
      expect(refineCategoryWithRowDiff('Regular', -500, 'ReplacingMergeTree')).toBe('Regular');
    });

    it('skips LightweightDelete for ReplicatedReplacingMergeTree', () => {
      expect(refineCategoryWithRowDiff('Regular', -500, 'ReplicatedReplacingMergeTree')).toBe('Regular');
    });

    it('skips LightweightDelete for CollapsingMergeTree', () => {
      expect(refineCategoryWithRowDiff('Regular', -500, 'CollapsingMergeTree')).toBe('Regular');
    });

    it('skips LightweightDelete for VersionedCollapsingMergeTree', () => {
      expect(refineCategoryWithRowDiff('Regular', -500, 'VersionedCollapsingMergeTree')).toBe('Regular');
    });

    it('skips LightweightDelete for AggregatingMergeTree', () => {
      expect(refineCategoryWithRowDiff('Regular', -500, 'AggregatingMergeTree')).toBe('Regular');
    });

    it('skips LightweightDelete for SharedReplacingMergeTree', () => {
      expect(refineCategoryWithRowDiff('Regular', -500, 'SharedReplacingMergeTree')).toBe('Regular');
    });

    it('still classifies LightweightDelete when no engine provided', () => {
      expect(refineCategoryWithRowDiff('Regular', -500)).toBe('LightweightDelete');
      expect(refineCategoryWithRowDiff('Regular', -500, undefined)).toBe('LightweightDelete');
    });
  });

  // ── isDeduplicatingEngine ──────────────────────────────────────

  describe('isDeduplicatingEngine', () => {
    it('detects ReplacingMergeTree variants', () => {
      expect(isDeduplicatingEngine('ReplacingMergeTree')).toBe(true);
      expect(isDeduplicatingEngine('ReplicatedReplacingMergeTree')).toBe(true);
      expect(isDeduplicatingEngine('SharedReplacingMergeTree')).toBe(true);
    });

    it('detects CollapsingMergeTree variants', () => {
      expect(isDeduplicatingEngine('CollapsingMergeTree')).toBe(true);
      expect(isDeduplicatingEngine('ReplicatedCollapsingMergeTree')).toBe(true);
    });

    it('detects VersionedCollapsingMergeTree', () => {
      expect(isDeduplicatingEngine('VersionedCollapsingMergeTree')).toBe(true);
      expect(isDeduplicatingEngine('ReplicatedVersionedCollapsingMergeTree')).toBe(true);
    });

    it('detects AggregatingMergeTree', () => {
      expect(isDeduplicatingEngine('AggregatingMergeTree')).toBe(true);
      expect(isDeduplicatingEngine('ReplicatedAggregatingMergeTree')).toBe(true);
    });

    it('returns false for plain MergeTree', () => {
      expect(isDeduplicatingEngine('MergeTree')).toBe(false);
      expect(isDeduplicatingEngine('ReplicatedMergeTree')).toBe(false);
    });

    it('returns false for SummingMergeTree', () => {
      expect(isDeduplicatingEngine('SummingMergeTree')).toBe(false);
    });
  });

  // ── isPatchPart ──────────────────────────────────────────────────

  describe('isPatchPart', () => {
    it('detects patch parts', () => {
      expect(isPatchPart('patch-1e5b2e238fbe84a8-202602_1_5_0_1934')).toBe(true);
    });

    it('rejects regular parts', () => {
      expect(isPatchPart('202602_1_100_3')).toBe(false);
      expect(isPatchPart('all_1_5_0')).toBe(false);
    });
  });

  // ── categoryToPartLogCondition ──────────────────────────────────

  describe('categoryToPartLogCondition', () => {
    it('returns SQL for Regular (event_type + merge_reason)', () => {
      const cond = categoryToPartLogCondition('Regular')!;
      expect(cond).toContain('MergeParts');
      expect(cond).toContain('RegularMerge');
    });

    it('returns SQL for TTLDelete covering all CH variants', () => {
      const cond = categoryToPartLogCondition('TTLDelete')!;
      expect(cond).toContain('TTLDeleteMerge');
      expect(cond).toContain('TTLDropMerge');
      expect(cond).toContain('TTLMerge');
    });

    it('returns SQL for TTLRecompress', () => {
      const cond = categoryToPartLogCondition('TTLRecompress')!;
      expect(cond).toContain('TTLRecompressMerge');
    });

    it('returns SQL for TTLMove (event_type = MovePart)', () => {
      const cond = categoryToPartLogCondition('TTLMove')!;
      expect(cond).toContain('MovePart');
    });

    it('returns SQL for Mutation (event_type = MutatePart)', () => {
      const cond = categoryToPartLogCondition('Mutation')!;
      expect(cond).toContain('MutatePart');
    });

    it('returns undefined for LightweightDelete (needs client-side row diff)', () => {
      expect(categoryToPartLogCondition('LightweightDelete')).toBeUndefined();
    });

    it('returns SQL for LightweightUpdate (patch- prefix)', () => {
      const cond = categoryToPartLogCondition('LightweightUpdate')!;
      expect(cond).toContain('patch-');
    });
  });

  // ── markReplicaMerges ────────────────────────────────────────────

  describe('markReplicaMerges', () => {
    const makeMerge = (db: string, table: string, result: string, progress: number, host: string) => ({
      database: db, table, result_part_name: result, progress, hostname: host,
      is_replica_merge: undefined as boolean | undefined,
    });

    it('does not mark when only one replica', () => {
      const merges = [makeMerge('db', 't', 'all_1_5_1', 0.5, 'host-0')];
      markReplicaMerges(merges);
      expect(merges[0].is_replica_merge).toBeUndefined();
    });

    it('marks the less-progressed merge as replica', () => {
      const merges = [
        makeMerge('db', 't', 'all_1_5_1', 0.3, 'host-0'),
        makeMerge('db', 't', 'all_1_5_1', 0.7, 'host-1'),
      ];
      markReplicaMerges(merges);
      // host-1 has more progress → primary
      const primary = merges.find(m => m.hostname === 'host-1')!;
      const replica = merges.find(m => m.hostname === 'host-0')!;
      expect(primary.is_replica_merge).toBeUndefined();
      expect(replica.is_replica_merge).toBe(true);
    });

    it('handles 3+ replicas', () => {
      const merges = [
        makeMerge('db', 't', 'all_1_5_1', 0.1, 'host-0'),
        makeMerge('db', 't', 'all_1_5_1', 0.9, 'host-1'),
        makeMerge('db', 't', 'all_1_5_1', 0.5, 'host-2'),
      ];
      markReplicaMerges(merges);
      expect(merges.filter(m => m.is_replica_merge).length).toBe(2);
      const primary = merges.find(m => !m.is_replica_merge)!;
      expect(primary.hostname).toBe('host-1');
    });

    it('does not cross-contaminate different tables', () => {
      const merges = [
        makeMerge('db', 't1', 'all_1_5_1', 0.3, 'host-0'),
        makeMerge('db', 't2', 'all_1_5_1', 0.7, 'host-1'),
      ];
      markReplicaMerges(merges);
      expect(merges[0].is_replica_merge).toBeUndefined();
      expect(merges[1].is_replica_merge).toBeUndefined();
    });

    it('does not cross-contaminate different result_part_names', () => {
      const merges = [
        makeMerge('db', 't', 'all_1_5_1', 0.3, 'host-0'),
        makeMerge('db', 't', 'all_6_10_1', 0.7, 'host-1'),
      ];
      markReplicaMerges(merges);
      expect(merges[0].is_replica_merge).toBeUndefined();
      expect(merges[1].is_replica_merge).toBeUndefined();
    });
  });

  // ── markReplicaMergeHistory ──────────────────────────────────────

  describe('markReplicaMergeHistory', () => {
    const makeRecord = (db: string, table: string, part: string, eventType: string, time: string, host: string) => ({
      database: db, table, part_name: part, event_type: eventType, event_time: time, hostname: host,
      is_replica_merge: undefined as boolean | undefined,
    });

    it('marks DownloadPart as replica regardless of grouping', () => {
      const records = [
        makeRecord('db', 't', 'all_1_5_1', 'DownloadPart', '2026-03-25 10:00:00', 'host-0'),
      ];
      markReplicaMergeHistory(records);
      expect(records[0].is_replica_merge).toBe(true);
    });

    it('marks later MergeParts for the same part as replica', () => {
      const records = [
        makeRecord('db', 't', 'all_1_5_1', 'MergeParts', '2026-03-25 10:00:00', 'host-0'),
        makeRecord('db', 't', 'all_1_5_1', 'MergeParts', '2026-03-25 10:00:02', 'host-1'),
      ];
      markReplicaMergeHistory(records);
      expect(records[0].is_replica_merge).toBeUndefined();
      expect(records[1].is_replica_merge).toBe(true);
    });

    it('does not mark unique merge events', () => {
      const records = [
        makeRecord('db', 't', 'all_1_5_1', 'MergeParts', '2026-03-25 10:00:00', 'host-0'),
        makeRecord('db', 't', 'all_6_10_1', 'MergeParts', '2026-03-25 10:00:01', 'host-0'),
      ];
      markReplicaMergeHistory(records);
      expect(records[0].is_replica_merge).toBeUndefined();
      expect(records[1].is_replica_merge).toBeUndefined();
    });

    it('DownloadPart does not interfere with MergeParts grouping', () => {
      const records = [
        makeRecord('db', 't', 'all_1_5_1', 'MergeParts', '2026-03-25 10:00:00', 'host-0'),
        makeRecord('db', 't', 'all_1_5_1', 'DownloadPart', '2026-03-25 10:00:01', 'host-1'),
      ];
      markReplicaMergeHistory(records);
      // MergeParts is the only one in its group → not replica
      expect(records[0].is_replica_merge).toBeUndefined();
      // DownloadPart is always replica
      expect(records[1].is_replica_merge).toBe(true);
    });

    it('handles mix of merge + download + duplicate merge', () => {
      const records = [
        makeRecord('db', 't', 'all_1_5_1', 'MergeParts', '2026-03-25 10:00:00', 'host-0'),
        makeRecord('db', 't', 'all_1_5_1', 'MergeParts', '2026-03-25 10:00:02', 'host-1'),
        makeRecord('db', 't', 'all_1_5_1', 'DownloadPart', '2026-03-25 10:00:03', 'host-2'),
      ];
      markReplicaMergeHistory(records);
      expect(records[0].is_replica_merge).toBeUndefined(); // primary merge
      expect(records[1].is_replica_merge).toBe(true);       // replica merge
      expect(records[2].is_replica_merge).toBe(true);       // replica fetch
    });
  });
});
