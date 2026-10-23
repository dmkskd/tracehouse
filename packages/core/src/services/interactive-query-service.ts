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
    return this.adapter.executeQuery<T>(tagQuery(sql, source), options?.queryId ? { queryId: options.queryId } : undefined);
  }
}
