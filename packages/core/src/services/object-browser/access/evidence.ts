import type { Capture, Source } from '../model/types.js';
import type { AccessEvaluation } from './types.js';

const ACCESS_SOURCES: Source[] = ['users', 'roles', 'role_grants', 'grants', 'tables'];

/** Evidence an access answer depends on that the collection did not fully read: a source that
 * failed, was truncated or not granted, or a privilege tree it could not read. */
export function evidenceGaps(capture: Capture): string[] {
  const gaps = capture.coverage.filter(c => ACCESS_SOURCES.includes(c.source) && c.state !== 'readable').map(c => `${c.source} ${c.state}`);
  return capture.operations ? gaps : [...gaps, 'privilege tree unavailable'];
}

/** Objects of `keys` a capture lacks although the source listing them was read in full: absent
 * from the server. One whose source failed or was truncated is unknown, not absent. */
export function absentFrom(capture: Capture, keys: string[]): string[] {
  const sourceOf: Partial<Record<string, Source>> = { user: 'users', role: 'roles', table: 'tables' };
  return keys.filter(key => {
    const [, kind] = JSON.parse(key) as string[];
    const source = sourceOf[kind];
    const read = !!source && capture.coverage.some(c => c.source === source && c.state === 'readable');
    return read && !capture.observations.some(o => o.key === key && !o.attributes.unresolvedReference);
  });
}

/** Scopes whose line to the table every shown access type cancels: across the answers for the
 * selected access types, those cancelled in each answer that holds them. */
export function cancelledAcross(answers: readonly AccessEvaluation[]): string[] {
  const held = (a: AccessEvaluation) => new Set(a.hosts.flatMap(h => h.holders.flatMap(x => x.grants)));
  const scopes = new Set(answers.flatMap(a => a.cancelledScopes));
  return [...scopes].filter(scope => answers.every(a => !held(a).has(scope) || a.cancelledScopes.includes(scope)));
}

