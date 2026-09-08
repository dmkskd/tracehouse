import { describe, expect, it } from 'vitest';
import { memoryLimitNotebook } from '../example';
import { parseNotebook, validateNotebook } from '../validate';

/** The shipped example is the reference document; it must always pass. */
const valid = () => JSON.parse(JSON.stringify(memoryLimitNotebook));

describe('validateNotebook', () => {
  it('accepts the example notebook', () => {
    const result = validateNotebook(valid());
    expect(result.ok ? [] : result.errors).toEqual([]);
    expect(result.ok).toBe(true);
  });

  it('rejects a stage naming evidence that does not exist', () => {
    // The crash this module exists to prevent: the renderer reads
    // document.evidence[stage.evidence].title and white-screens the route.
    const doc = valid();
    doc.stages[0].evidence = 'does-not-exist';
    const result = validateNotebook(doc);
    expect(result.ok).toBe(false);
    expect(result.ok === false && result.errors.join('\n')).toContain('unknown evidence does-not-exist');
  });

  it('rejects duplicate stage IDs', () => {
    const doc = valid();
    doc.stages = [doc.stages[0], { ...doc.stages[0] }];
    const result = validateNotebook(doc);
    expect(result.ok === false && result.errors.join('\n')).toMatch(/duplicates/);
  });

  it('rejects protocol-relative and executable evidence links', () => {
    const doc = valid();
    const key = Object.keys(doc.evidence)[0];
    doc.evidence[key].view = { route: '//attacker.example/path', descriptorVersion: 1 };
    expect(validateNotebook(doc).ok).toBe(false);

    const other = valid();
    other.evidence[key].view = { href: 'javascript:alert(1)', descriptorVersion: 1 };
    expect(validateNotebook(other).ok).toBe(false);
  });

  it('rejects relative timestamps in scope', () => {
    const doc = valid();
    doc.scope.from = '2026-07-28 12:55:00';
    expect(validateNotebook(doc).ok).toBe(false);
  });

  it('rejects an inferred claim with no caveat', () => {
    const doc = valid();
    doc.stages[0].claimType = 'inferred';
    delete doc.stages[0].caveat;
    expect(validateNotebook(doc).ok).toBe(false);
  });

  it('rejects an unknown kind but allows an absent one', () => {
    const doc = valid();
    doc.kind = 'postmortem';
    expect(validateNotebook(doc).ok).toBe(false);

    const withoutKind = valid();
    delete withoutKind.kind;
    expect(validateNotebook(withoutKind).ok).toBe(true);
  });

  it('rejects row-position highlights', () => {
    const doc = valid();
    doc.stages[0].highlight = { row: 0 };
    expect(validateNotebook(doc).ok).toBe(false);
  });

  it('rejects encodings referencing a column the evidence lacks', () => {
    const doc = valid();
    doc.stages[0].encoding = { ...doc.stages[0].encoding, label: 'no_such_column' };
    expect(validateNotebook(doc).ok).toBe(false);
  });

  it('reports every problem at once rather than the first', () => {
    const result = validateNotebook({ schemaVersion: '0.1' });
    expect(result.ok).toBe(false);
    expect(result.ok === false && result.errors.length).toBeGreaterThan(2);
  });

  it('rejects non-objects without throwing', () => {
    for (const input of [null, undefined, 42, 'notebook', []]) {
      expect(validateNotebook(input).ok).toBe(false);
    }
  });
});

describe('parseNotebook', () => {
  it('reports malformed JSON as an error', () => {
    const result = parseNotebook('{ not json');
    expect(result.ok).toBe(false);
    expect(result.ok === false && result.errors[0]).toContain('not valid JSON');
  });

  it('round-trips the example through text', () => {
    expect(parseNotebook(JSON.stringify(memoryLimitNotebook)).ok).toBe(true);
  });
});
