import type { IClickHouseAdapter } from '../adapters/types.js';
import { tagQuery } from '../queries/builder.js';

export interface InteractiveQueryOptions {
  queryId?: string;
}

export class InteractiveQueryService {
  constructor(private adapter: IClickHouseAdapter) {}

  supportsExplicitQueryId(): boolean {
    return this.adapter.supportsExplicitQueryId === true;
  }

  /**
   * Execute authored SQL with a traceability tag. Callers escape substituted
   * values before this boundary; this method does not enforce read-only SQL.
   * The connected ClickHouse user's permissions govern statement execution.
   */
  async run<T extends Record<string, unknown>>(sql: string, source: string, options?: InteractiveQueryOptions): Promise<T[]> {
    // Use the server's DateTime type / Grafana time-field metadata, never a
    // datetime-looking string heuristic. Authored SQL and calendar Dates remain
    // unchanged. Other services retain their explicit producer migration paths.
    return this.adapter.executeQuery<T>(tagQuery(sql, source), {
      utcDateTimeOutput: true,
      ...(options?.queryId ? { queryId: options.queryId } : {}),
    });
  }
}
