import { describe, expect, it } from 'vitest';
import {
  EVENT_KIND_DEFINITIONS,
  EVENT_SOURCE_DEFINITIONS,
} from '../events.js';
import { EventsService } from '../../services/events-service.js';

describe('event domain catalog', () => {
  it('defines every detector source once and derives not-requested coverage', () => {
    const ids = EVENT_SOURCE_DEFINITIONS.map(source => source.id);
    expect(new Set(ids).size).toBe(ids.length);

    expect(EventsService.notRequested().coverage).toEqual(
      EVENT_SOURCE_DEFINITIONS.map(source => ({
        source: source.coverageLabel ?? source.source,
        capability: source.capability,
        status: 'not_requested',
        event_count: 0,
      })),
    );
  });

  it('gives every supported kind complete semantic metadata', () => {
    const supportedKinds = new Set(
      EVENT_SOURCE_DEFINITIONS.flatMap(source => source.kinds),
    );

    for (const kind of supportedKinds) {
      const definition = EVENT_KIND_DEFINITIONS[kind];
      expect(definition.label).not.toBe('');
      expect(definition.shortLabel).not.toBe('');
      expect(definition.description).not.toBe('');
      expect(definition.categories.length).toBeGreaterThan(0);
      expect(definition.severities.length).toBeGreaterThan(0);
    }
  });
});
