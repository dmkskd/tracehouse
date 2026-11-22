import { useCallback, useEffect, useRef, useState } from 'react';
import { loadDetailStatement, loadTableActivity, type DetailStatement, type TableActivity } from '@tracehouse/core/services/object-browser';
import { useClickHouseServices } from '@tracehouse/ui-shared';

export type StatementResult = { text: string } | { activity: TableActivity | null } | { error: string };

/** Loads detail statements when a detail opens. Statements marked `external` make
 * ClickHouse contact storage or a catalog, so they load only after `load(sql)`.
 * Results belong to one adapter; late results from an old connection or an
 * unmounted detail are dropped. */
/** `activityStatements` load as table activity (loadTableActivity) instead of text; `table` is `database.table`. */
export function useDetailStatements(statements: DetailStatement[], activityStatements: { statement: DetailStatement; table: string }[]) {
  const services = useClickHouseServices();
  const adapter = services?.adapter;
  const [state, setState] = useState<{ adapter: unknown; results: Record<string, StatementResult> }>({ adapter: null, results: {} });
  const [requested, setRequested] = useState<ReadonlySet<string>>(new Set());
  const started = useRef<{ adapter: unknown; sqls: Set<string> }>({ adapter: null, sqls: new Set() });
  const current = useRef<unknown>(adapter);
  // The statements to load, as JSON: it keeps multi-line SQL intact and is a stable effect dependency.
  const key = JSON.stringify([statements.filter(s => !s.external || requested.has(s.sql)), activityStatements]);
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
    const [texts, activity] = JSON.parse(key) as [DetailStatement[], { statement: DetailStatement; table: string }[]];
    const start = (statement: DetailStatement, run: () => Promise<StatementResult>) => {
      if (started.current.sqls.has(statement.sql)) return;
      started.current.sqls.add(statement.sql);
      run().then(result => publish(statement.sql, result),
        (e: unknown) => publish(statement.sql, { error: e instanceof Error ? e.message : String(e) }));
    };
    for (const statement of texts) start(statement, async () => ({ text: await loadDetailStatement(adapter, statement) }));
    for (const { statement, table } of activity) start(statement, async () => ({ activity: await loadTableActivity(adapter, statement, table) }));
  }, [adapter, key]);
  const load = useCallback((sql: string) => setRequested(previous => new Set([...previous, sql])), []);
  return { results: state.adapter === adapter ? state.results : {}, requested, load };
}
