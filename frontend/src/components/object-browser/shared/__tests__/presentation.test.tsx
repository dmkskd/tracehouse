import { renderHook } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { projectCatalogGraph } from '@tracehouse/core/services/object-browser';
import { captureFixture, key } from './captureFixture';
import { databaseGroups } from '../databaseGroups';
import { useObjectPresentation } from '../useObjectPresentation';
import { selectionEmphasis } from '../selectionEmphasis';

describe('renderer-independent object presentation', () => {
  it('keeps canonical database membership and distinguishes core access from data-flow emphasis', () => {
    const capture = captureFixture(), alice = key('user', 'alice');
    const graph = projectCatalogGraph(capture, alice);
    const { result } = renderHook(() => useObjectPresentation(graph, ['SELECT'], capture, alice));
    expect(databaseGroups(graph)).toMatchObject([{ name: 'db.with.dots', count: 3 }]);
    expect(result.current.accessForTable?.(key('table', 'db.with.dots', 'payments'))[0].state).toBe('granted');
    expect(result.current.accessForTable?.(key('table', 'db.with.dots', 'payments_view'))[0].state).toBe('no-path');
    const emphasis = selectionEmphasis({ graph, selected: alice });
    expect(emphasis.dimmed(key('table', 'db.with.dots', 'payments_view'))).toBe(false);
    expect(emphasis.dimmed(key('user', 'bob'))).toBe(true);
  });
  it('preserves SET ROLE, holder-specific revokes and host disagreement from the core evaluator', () => {
    const capture = captureFixture(), bob = key('user', 'bob');
    const graph = projectCatalogGraph(capture, bob);
    const { result, rerender } = renderHook(({ capture }) => useObjectPresentation(graph, ['SELECT'], capture, bob), { initialProps: { capture } });
    const payments = key('table', 'db.with.dots', 'payments'), audit = key('table', 'db.with.dots', 'audit_log');
    expect(result.current.accessForTable?.(payments)[0].state).toBe('set-role');
    expect(result.current.accessForTable?.(audit)[0].state).toBe('revoked');
    const changed = { ...capture, relationships: capture.relationships.filter(e => !(e.host === 'ch-2' && e.kind === 'grant' && e.to === key('grant-scope', 'revoke'))) };
    rerender({ capture: changed });
    expect(result.current.accessForTable?.(audit)[0].state).toBe('differs');
  });
  it('hover emphasis restores the selected database and does not traverse sideways through a shared role', () => {
    const capture = captureFixture(), graph = projectCatalogGraph(capture, key('user', 'alice'));
    const normal = selectionEmphasis({ graph, selected: null, selectedDatabase: 'db.with.dots' });
    expect(normal.databaseMembers.size).toBe(3);
    const hover = selectionEmphasis({ graph, selected: key('user', 'bob'), hovered: key('user', 'alice'), highlight: true });
    expect(hover.dimmed(key('user', 'bob'))).toBe(true);
    expect(hover.dimmed(key('role', 'reader'))).toBe(false);
  });
});
