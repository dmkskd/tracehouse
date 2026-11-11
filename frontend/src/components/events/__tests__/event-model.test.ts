import { describe, expect, it } from 'vitest';
import type { EventCategory, EventKind, OperationalEvent } from '@tracehouse/core';
import {
  emptyEventFilter,
  eventCategoryCheckState,
  eventKindCheckState,
  filterEvents,
  onlyEventCategory,
  onlyEventKind,
  setAllEventCategories,
  setAllEventKinds,
  toggleEventCategory,
  toggleEventKind,
} from '../event-model';

function event(id: string, kind: EventKind, category: EventCategory): OperationalEvent {
  return {
    id,
    occurred_at: '2026-11-11T13:30:00Z',
    kind,
    category,
    severity: 'error',
    precision: 'exact',
    title: id,
    source: 'test',
    capability: 'test',
  };
}

const events = [
  event('oom', 'query_oom', 'queries'),
  event('timeout', 'query_timeout', 'queries'),
  event('merge', 'merge_failure', 'merges'),
];
const shownIds = (filter: ReturnType<typeof emptyEventFilter>) => filterEvents(events, filter).map(e => e.id);

describe('event filter cascade', () => {
  it('unchecks the kinds of a hidden category, and checking one kind brings back only that kind', () => {
    const queriesHidden = toggleEventCategory(emptyEventFilter(), 'queries', events);
    expect(eventKindCheckState('query_oom', queriesHidden, events)).toBe('unchecked');
    expect(eventKindCheckState('merge_failure', queriesHidden, events)).toBe('checked');
    expect(shownIds(queriesHidden)).toEqual(['merge']);

    const oomBack = toggleEventKind(queriesHidden, 'query_oom', events);
    expect(shownIds(oomBack)).toEqual(['oom', 'merge']);
    expect(eventCategoryCheckState('queries', oomBack, events)).toBe('partial');
    expect(eventKindCheckState('query_timeout', oomBack, events)).toBe('unchecked');

    const queriesBack = toggleEventCategory(oomBack, 'queries', events);
    expect(shownIds(queriesBack)).toEqual(['oom', 'timeout', 'merge']);
    expect(eventCategoryCheckState('queries', queriesBack, events)).toBe('checked');
  });

  it('shows a kind that spans categories as partial when only some of them are hidden', () => {
    const bursts = [
      event('replication-burst', 'error_burst', 'replication'),
      event('storage-burst', 'error_burst', 'storage'),
    ];
    const storageHidden = toggleEventCategory(emptyEventFilter(), 'storage', bursts);
    expect(eventKindCheckState('error_burst', storageHidden, bursts)).toBe('partial');
    expect(filterEvents(bursts, toggleEventKind(storageHidden, 'error_burst', bursts)).map(e => e.id))
      .toEqual(['replication-burst', 'storage-burst']);
  });
});

describe('event filter shortcuts', () => {
  it('keeps only one category in one step, and "all" on event types brings everything back', () => {
    const onlyQueries = onlyEventCategory(emptyEventFilter(), 'queries', events);
    expect(shownIds(onlyQueries)).toEqual(['oom', 'timeout']);

    const onlyOom = onlyEventKind(onlyQueries, 'query_oom', events);
    expect(shownIds(onlyOom)).toEqual(['oom']);

    expect(shownIds(setAllEventKinds(onlyOom, true, events))).toEqual(['oom', 'timeout', 'merge']);
    expect(shownIds(setAllEventCategories(emptyEventFilter(), false, events))).toEqual([]);
  });
});
