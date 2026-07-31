import type { IClickHouseAdapter } from '../adapters/types.js';
import { OBSERVABILITY_COLUMN_COMMENTS, OBSERVABILITY_SYSTEM_TABLES } from '../queries/observability-map-queries.js';
import { tagQuery } from '../queries/builder.js';
import { TAB_OVERVIEW, sourceTag } from '../queries/source-tags.js';

export type ObservabilityServerTableInfo = Record<string, unknown> & {
  name: string;
  sorting_key: string;
  primary_key: string;
};

export type ObservabilityColumnCommentMap = Map<string, string>;

export class ObservabilityMapServiceError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'ObservabilityMapServiceError';
  }
}

export class ObservabilityMapService {
  private systemTablesRequest: Promise<Map<string, ObservabilityServerTableInfo>> | null = null;
  private columnCommentsRequest: Promise<ObservabilityColumnCommentMap> | null = null;

  constructor(private adapter: IClickHouseAdapter) {}

  async runDiagnostic<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
    try {
      return await this.adapter.executeQuery<T>(
        tagQuery(sql, sourceTag(TAB_OVERVIEW, 'observabilityDiagnostic')),
      );
    } catch (error) {
      throw new ObservabilityMapServiceError('Failed to run observability diagnostic query', error as Error);
    }
  }

  async getSystemTables(): Promise<Map<string, ObservabilityServerTableInfo>> {
    if (this.systemTablesRequest) return this.systemTablesRequest;

    this.systemTablesRequest = (async () => {
      try {
        const rows = await this.adapter.executeQuery<ObservabilityServerTableInfo>(
          tagQuery(OBSERVABILITY_SYSTEM_TABLES, sourceTag(TAB_OVERVIEW, 'observabilityTables')),
        );
        const map = new Map<string, ObservabilityServerTableInfo>();
        for (const row of rows) {
          map.set(`system.${row.name}`, {
            name: `system.${row.name}`,
            sorting_key: row.sorting_key || '',
            primary_key: row.primary_key || '',
          });
        }
        return map;
      } catch (error) {
        this.systemTablesRequest = null;
        throw new ObservabilityMapServiceError('Failed to load observability system tables', error as Error);
      }
    })();

    return this.systemTablesRequest;
  }

  async getColumnComments(): Promise<ObservabilityColumnCommentMap> {
    if (this.columnCommentsRequest) return this.columnCommentsRequest;

    this.columnCommentsRequest = (async () => {
      try {
        const rows = await this.adapter.executeQuery<{ table: string; name: string; comment: string }>(
          tagQuery(OBSERVABILITY_COLUMN_COMMENTS, sourceTag(TAB_OVERVIEW, 'observabilityColumnComments')),
        );
        const map: ObservabilityColumnCommentMap = new Map();
        for (const row of rows) {
          if (row.comment) map.set(`system.${row.table}.${row.name}`, row.comment);
        }
        return map;
      } catch (error) {
        this.columnCommentsRequest = null;
        throw new ObservabilityMapServiceError('Failed to load observability column comments', error as Error);
      }
    })();

    return this.columnCommentsRequest;
  }
}
