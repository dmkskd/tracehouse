/**
 * Opens a panel's @query_link / @merge_link target on its page, in the same
 * tab, with a back link to this Analytics URL. Closing the opened item returns
 * here (useReturnToOrigin). The drill path is in memory, so it is not restored.
 */

import { useCallback, useMemo } from 'react';
import { useAppLocation, useNavigate } from '../../hooks/useAppLocation';
import { pageLinkUrl, type ParsedDirectives } from './metaLanguage';

type LinkKind = 'queryLink' | 'mergeLink';

export function usePageLink(
  directives: Pick<ParsedDirectives, 'queryLink' | 'mergeLink'> | undefined,
  rows: Record<string, unknown>[] | undefined,
) {
  const navigate = useNavigate();
  const location = useAppLocation();
  const fromPath = `${location.pathname}${location.search}`;

  const openRow = useCallback((kind: LinkKind, row: Record<string, unknown>) => {
    const url = directives ? pageLinkUrl(directives, kind, row) : null;
    if (url) navigate(url, { state: { from: { path: fromPath, label: 'Analytics' } } });
  }, [directives, navigate, fromPath]);

  /** Chart click: the label is the value of the link's `on` column. */
  const openLabel = useCallback((kind: LinkKind, label: string) => {
    const on = directives?.[kind]?.on;
    const row = on ? rows?.find(r => String(r[on]) === label) : undefined;
    if (row) openRow(kind, row);
  }, [directives, rows, openRow]);

  return useMemo(() => ({ openRow, openLabel }), [openRow, openLabel]);
}
