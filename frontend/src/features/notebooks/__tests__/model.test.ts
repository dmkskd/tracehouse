import { describe, expect, it } from 'vitest';
import { memoryLimitNotebook } from '../example';
import { evidenceTarget, notebookKind, notebookKindLabel, rowMatchesKey } from '../model';

describe('notebook model', () => {
  it('keeps TraceHouse evidence links host-independent', () => {
    const targets = Object.values(memoryLimitNotebook.evidence).map(evidenceTarget);
    expect(targets.every(target => target?.kind === 'route')).toBe(true);
    expect(targets.map(target => target?.value)).toContain(
      '/queries?qd_id=q-123&hostname=ch-02&status=Exception',
    );
  });

  it('rejects protocol-relative and executable routes', () => {
    expect(evidenceTarget({
      title: 'bad', mode: 'live-link', columns: ['x'], rows: [],
      view: { route: '//attacker.example/path', descriptorVersion: 1 },
    })).toBeUndefined();
    expect(evidenceTarget({
      title: 'bad', mode: 'live-link', columns: ['x'], rows: [],
      view: { href: 'javascript:alert(1)', descriptorVersion: 1 },
    })).toBeUndefined();
  });

  it('uses stable row keys for highlights', () => {
    const row = memoryLimitNotebook.evidence['memory-contributors'].rows[0];
    expect(rowMatchesKey(row, { actor_id: 'q-123' })).toBe(true);
    expect(rowMatchesKey(row, { actor_id: 'q-456' })).toBe(false);
  });

  it('reads the declared kind and labels it', () => {
    expect(notebookKind(memoryLimitNotebook)).toBe('investigation');
    expect(notebookKindLabel({ ...memoryLimitNotebook, kind: 'runbook' })).toBe('Runbook');
  });

  it('treats a document without a kind as an investigation', () => {
    const { kind, ...withoutKind } = memoryLimitNotebook;
    expect(notebookKind(withoutKind)).toBe('investigation');
  });
});
