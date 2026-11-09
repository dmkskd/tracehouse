import { describe, expect, it } from 'vitest';
import type { OperationalEvent } from '@tracehouse/core';
import { buildMergeDetailsUrl, legacyEventsRangeCenter, linkTimeToEventsRange } from '../events-page-model';
import { readLinkTime } from '../../utils/linkCoordinates';

const mergeFailureEvent = (overrides: Partial<OperationalEvent> = {}): OperationalEvent => ({
  id: 'merge-failure',
  occurred_at: '2026-07-31T18:59:43.228Z',
  kind: 'merge_failure',
  category: 'merges',
  severity: 'error',
  precision: 'exact',
  title: 'Merge failed',
  source: 'system.part_log',
  capability: 'part_log',
  hostname: 'replica-01',
  database: 'uk_price_paid',
  table: 'uk_price_paid_local',
  part_name: '1785506400_465_488_3',
  operation: 'MergeParts',
  ...overrides,
});

describe('legacyEventsRangeCenter', () => {
  it('migrates an older Time Travel event time into a stable range center', () => {
    expect(legacyEventsRangeCenter(
      'timetravel',
      undefined,
      '2026-07-25T19:00:54.808Z',
    )).toBe('2026-07-25T19:00:54.808Z');
  });

  it('does not replace an explicit range center', () => {
    expect(legacyEventsRangeCenter(
      'timetravel',
      '2026-07-25T18:00:00.000Z',
      '2026-07-25T19:00:54.808Z',
    )).toBeUndefined();
  });

  it('does not derive the range from a normal event selection', () => {
    expect(legacyEventsRangeCenter(
      undefined,
      undefined,
      '2026-07-25T19:00:54.808Z',
    )).toBeUndefined();
  });
});

describe('buildMergeDetailsUrl', () => {
  it('builds a Merges detail deep link from a merge failure event', () => {
    expect(buildMergeDetailsUrl(mergeFailureEvent())).toBe(
      '/merges?md_db=uk_price_paid&md_tbl=uk_price_paid_local&md_part=1785506400_465_488_3'
      + '&md_host=replica-01&md_time=2026-07-31T18%3A59%3A43.228Z&md_type=MergeParts',
    );
  });

  it('builds the same detail deep link for a failed mutation record', () => {
    expect(buildMergeDetailsUrl(mergeFailureEvent({
      kind: 'mutation_failure',
      operation: 'MutatePart',
    }))).toBe(
      '/merges?md_db=uk_price_paid&md_tbl=uk_price_paid_local&md_part=1785506400_465_488_3'
      + '&md_host=replica-01&md_time=2026-07-31T18%3A59%3A43.228Z&md_type=MutatePart',
    );
  });

  it('encodes merge identifiers safely', () => {
    expect(buildMergeDetailsUrl(mergeFailureEvent({
      database: 'database name',
      table: 'table/name',
      part_name: 'part+name',
    }))).toBe(
      '/merges?md_db=database+name&md_tbl=table%2Fname&md_part=part%2Bname'
      + '&md_host=replica-01&md_time=2026-07-31T18%3A59%3A43.228Z&md_type=MergeParts',
    );
  });

  it('does not link unrelated or incomplete events', () => {
    expect(buildMergeDetailsUrl(mergeFailureEvent({ kind: 'part_move_failure' }))).toBeUndefined();
    expect(buildMergeDetailsUrl(mergeFailureEvent({ part_name: undefined }))).toBeUndefined();
    expect(buildMergeDetailsUrl(mergeFailureEvent({ hostname: undefined }))).toBeUndefined();
  });
});

describe('linkTimeToEventsRange', () => {
  it('centres the Events range on a linked instant, pad on each side', () => {
    const time = readLinkTime(new URLSearchParams('th_at=2026-11-09T12:00:00Z&th_pad=15m'))!;
    expect(linkTimeToEventsRange(time)).toEqual({ range_center: '2026-11-09T12:00:00.000Z', event_range: 0.5 });
  });

  it('spans a linked dashboard range', () => {
    const time = readLinkTime(new URLSearchParams('th_from=2026-11-09T06:00:00Z&th_to=2026-11-09T12:00:00Z'))!;
    expect(linkTimeToEventsRange(time)).toEqual({ range_center: '2026-11-09T09:00:00.000Z', event_range: 6 });
  });
});
