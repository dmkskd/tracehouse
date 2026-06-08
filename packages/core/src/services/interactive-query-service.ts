import type { IClickHouseAdapter } from '../adapters/types.js';
import { tagQuery } from '../queries/builder.js';

export class InteractiveQueryService {
  constructor(private adapter: IClickHouseAdapter) {}

  async run<T extends Record<string, unknown>>(sql: string, source: string): Promise<T[]> {
    return this.adapter.executeQuery<T>(tagQuery(sql, source));
  }
}
