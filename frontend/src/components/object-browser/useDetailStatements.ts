import { useCallback, useEffect, useRef, useState } from 'react';
import { loadDetailStatement, loadTableActivity, loadTablePlacement, type DetailSection, type DetailStatement, type TableActivity, type TablePlacement } from '@tracehouse/core/services/object-browser';
import { useClickHouseServices } from '@tracehouse/ui-shared';

export type StatementResult = { text: string } | { activity: TableActivity | null } | { placement: TablePlacement[] } | { error: string };
type Adapter = NonNullable<ReturnType<typeof useClickHouseServices>>['adapter'];
/** A statement a section reads when the detail opens, and how its result is read. */
interface Load { statement: DetailStatement; run: (adapter: Adapter) => Promise<StatementResult> }

/** One loader per section kind; kinds without an entry read nothing. */
const LOADERS: { [K in DetailSection['kind']]?: (section: Extract<DetailSection, { kind: K }>) => Load[] } = {
  statement: ({ statement }) => [{ statement, run: async adapter => ({ text: await loadDetailStatement(adapter, statement) }) }],
  activity: ({ statement, table }) => [{ statement, run: async adapter => ({ activity: await loadTableActivity(adapter, statement, table) }) }],
  lifecycle: ({ statement }) => [{ statement, run: async adapter => ({ placement: await loadTablePlacement(adapter, statement) }) }],
};
const loadsOf = (sections: DetailSection[]): Load[] =>
  sections.flatMap(section => (LOADERS[section.kind] as ((s: DetailSection) => Load[]) | undefined)?.(section) ?? []);

/** Loads the statements of a detail's sections when it opens; results are keyed by SQL.
 * Statements marked `external` make ClickHouse contact storage or a catalog, so they load
 * only after `load(sql)`. Results belong to one adapter; late results from an old
 * connection or an unmounted detail are dropped. */
export function useDetailStatements(sections: DetailSection[]) {
  const services = useClickHouseServices();
  const adapter = services?.adapter;
  const [state, setState] = useState<{ adapter: unknown; results: Record<string, StatementResult> }>({ adapter: null, results: {} });
  const [requested, setRequested] = useState<ReadonlySet<string>>(new Set());
  const started = useRef<{ adapter: unknown; sqls: Set<string> }>({ adapter: null, sqls: new Set() });
  const current = useRef<unknown>(adapter);
  // The sections that load something, as JSON: it keeps multi-line SQL intact and is a stable
  // effect dependency. An external statement joins once it is requested.
  const key = JSON.stringify(sections.filter(section => LOADERS[section.kind]
    && loadsOf([section]).every(({ statement }) => !statement.external || requested.has(statement.sql))));
  useEffect(() => {
    current.current = adapter;
    return () => { current.current = null; };
  }, [adapter]);
  useEffect(() => {
    if (!adapter) return;
    if (started.current.adapter !== adapter) started.current = { adapter, sqls: new Set() };
    const publish = (sql: string, result: StatementResult) => {
      if (current.current === adapter) setState(previous => ({ adapter, results: { ...(previous.adapter === adapter ? previous.results : {}), [sql]: result } }));
    };
    for (const { statement, run } of loadsOf(JSON.parse(key) as DetailSection[])) {
      if (started.current.sqls.has(statement.sql)) continue;
      started.current.sqls.add(statement.sql);
      run(adapter).then(result => publish(statement.sql, result),
        (e: unknown) => publish(statement.sql, { error: e instanceof Error ? e.message : String(e) }));
    }
  }, [adapter, key]);
  const load = useCallback((sql: string) => setRequested(previous => new Set([...previous, sql])), []);
  return { results: state.adapter === adapter ? state.results : {}, requested, load };
}
