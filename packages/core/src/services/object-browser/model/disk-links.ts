import type { Attributes } from './types.js';

/** Evidence of a table → remote disk link (collectors/storage.ts): the table's storage policy, the
 * volume and disk the link goes through, and the TTL expressions that move parts there. */
export interface DiskLinkEvidence { method: 'storage-policy'; policy: string; volume: string; disk: string; ttl?: string[] }

/** The evidence of a table → remote disk link, or null for any other link. */
export function diskLinkEvidence(evidence: Attributes): DiskLinkEvidence | null {
  return evidence.method === 'storage-policy' ? evidence as unknown as DiskLinkEvidence : null;
}

/** TTL expressions that move parts along a link, over all its host records. */
export function movingTTL(records: readonly { evidence: Attributes }[]): string[] {
  return [...new Set(records.flatMap(record => diskLinkEvidence(record.evidence)?.ttl ?? []))];
}
