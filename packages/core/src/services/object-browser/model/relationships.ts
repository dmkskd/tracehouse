import type { Relationship } from './types.js';

export type RelationshipKind = Relationship['kind'];
export type RelationshipFamily = 'access' | 'candidate' | 'policy' | 'data-flow' | 'storage';

export interface RelationshipSpec {
  family: RelationshipFamily;
  /** Part of a configured privilege path (user → role → scope → table). */
  provenance: boolean;
  /** Orders focus columns: the `to` object sits right of the `from` object. */
  ranks: boolean;
  /** All `to` objects of this kind share one focus column (privileges, tables in storage). */
  alignTargets?: boolean;
  /** `to` objects sit in the last focus column, after every other object: what lies
   * outside ClickHouse ends the chain. */
  lastColumn?: boolean;
  /** Links record how they were obtained in `evidence.method` (REFERENCE_METHODS). */
  methods?: boolean;
  /** Card fact naming the neighbor: on the `to` object for incoming, on the `from` object for outgoing.
   * Outgoing `label` uses the edge label, which differs per engine (writes to, flushes to, forwards to). */
  fact?: { incoming?: string; outgoing?: string };
}

/** One entry per relationship kind. Code that needs a property of a kind reads it here. */
export const RELATIONSHIPS: Record<RelationshipKind, RelationshipSpec> = {
  'has-role': { family: 'access', provenance: true, ranks: true },
  grant: { family: 'access', provenance: true, ranks: true, alignTargets: true },
  'candidate-scope': { family: 'candidate', provenance: true, ranks: false },
  'policy-target': { family: 'policy', provenance: false, ranks: false },
  'policy-recipient': { family: 'policy', provenance: false, ranks: false },
  'insert-trigger': { family: 'data-flow', provenance: false, ranks: true, methods: true, fact: { incoming: 'Reads from' } },
  'reads-from': { family: 'data-flow', provenance: false, ranks: true, methods: true, fact: { incoming: 'Reads from' } },
  'writes-to': { family: 'data-flow', provenance: false, ranks: true, methods: true, fact: { outgoing: 'label' } },
  /** ClickHouse table → the storage server outside ClickHouse named by its engine arguments. */
  'points-to': { family: 'storage', provenance: false, ranks: true, alignTargets: true, lastColumn: true, methods: true, fact: { outgoing: 'Stored on' } },
};
