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
  /** `to` objects sit in the last focus column, after every other object: external
   * storage ends the chain. */
  lastColumn?: boolean;
  /** Links record how they were obtained in `evidence.method` (REFERENCE_METHODS). */
  methods?: boolean;
  /** Card fact naming the neighbor: on the `to` object for incoming, on the `from` object for outgoing.
   * Outgoing `label` uses the edge label, which differs per engine (writes to, flushes to, forwards to). */
  fact?: { incoming?: string; outgoing?: string };
  /** Object detail: the group the link is listed in, and its wording from each end. `outgoing` is
   * read from the `from` object ("events Triggers on insert events_to_minute_mv"), `incoming` from
   * the `to` object. `'label'` uses the edge label, which differs per engine. */
  detail: { group: DetailGroup; outgoing: string; incoming: string };
}

/** A link that records a partial revoke: a principal's revoke, or what that revoke removes. */
export const isRevokeEdge = (edge: { records: { evidence: Record<string, unknown> }[] }): boolean =>
  edge.records.some(r => Number(r.evidence.is_partial_revoke) === 1);

/** Object detail groups, in display order. */
export const DETAIL_GROUPS = ['Data flow', 'Storage', 'Access'] as const;
export type DetailGroup = typeof DETAIL_GROUPS[number];

/** One entry per relationship kind. Code that needs a property of a kind reads it here. */
export const RELATIONSHIPS: Record<RelationshipKind, RelationshipSpec> = {
  'has-role': { family: 'access', provenance: true, ranks: true, detail: { group: 'Access', outgoing: 'Has role', incoming: 'Granted to' } },
  grant: { family: 'access', provenance: true, ranks: true, alignTargets: true, detail: { group: 'Access', outgoing: 'Holds', incoming: 'Held by' } },
  // Incoming wording is refined by the privilege (describeDetail): readable, writable, revoked.
  'candidate-scope': { family: 'candidate', provenance: true, ranks: false, detail: { group: 'Access', outgoing: 'May cover', incoming: 'Covered by' } },
  'policy-target': { family: 'policy', provenance: false, ranks: false, detail: { group: 'Access', outgoing: 'Filters rows of', incoming: 'Row policies' } },
  'policy-recipient': { family: 'policy', provenance: false, ranks: false, detail: { group: 'Access', outgoing: 'Applies to', incoming: 'Row policies' } },
  'insert-trigger': { family: 'data-flow', provenance: false, ranks: true, methods: true, fact: { incoming: 'Reads from' },
    detail: { group: 'Data flow', outgoing: 'Triggers on insert', incoming: 'Triggered by inserts into' } },
  'reads-from': { family: 'data-flow', provenance: false, ranks: true, methods: true, fact: { incoming: 'Reads from' },
    detail: { group: 'Data flow', outgoing: 'Read by', incoming: 'Reads from' } },
  'writes-to': { family: 'data-flow', provenance: false, ranks: true, methods: true, fact: { outgoing: 'label' },
    detail: { group: 'Data flow', outgoing: 'label', incoming: 'Written by' } },
  /** ClickHouse table → the external storage server named by its engine arguments, or a remote
   * disk of its storage policy. The label says how data gets there (stored on, moves after TTL …). */
  'points-to': { family: 'storage', provenance: false, ranks: true, alignTargets: true, lastColumn: true, methods: true, fact: { outgoing: 'Stored on' },
    detail: { group: 'Storage', outgoing: 'label', incoming: 'Stores' } },
};
