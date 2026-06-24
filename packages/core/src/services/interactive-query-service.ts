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

  async run<T extends Record<string, unknown>>(sql: string, source: string, options?: InteractiveQueryOptions): Promise<T[]> {
    return this.adapter.executeQuery<T>(tagQuery(sql, source), options?.queryId ? { queryId: options.queryId } : undefined);
  }
}
