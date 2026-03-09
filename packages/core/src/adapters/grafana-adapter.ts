import type { IClickHouseAdapter } from './types.js';
import { AdapterError } from './types.js';

interface DsRef {
  uid: string;
  type?: string;
}

interface DsQueryResponse {
  results: Record<string, {
    status?: number;
    frames?: Array<{
      schema: { fields: Array<{ name: string }> };
      data: { values: unknown[][] };
    }>;
    error?: string;
  }>;
}

export class GrafanaAdapter implements IClickHouseAdapter {
  constructor(
    private ds: DsRef,
    private getBackendSrv: () => { post<T>(url: string, body: unknown): Promise<T> },
  ) {}

  async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
    const refId = 'q';
    try {
      const response = await this.getBackendSrv().post<DsQueryResponse>('/api/ds/query', {
        queries: [{
          refId,
          rawSql: sql,
          datasource: { uid: this.ds.uid, type: this.ds.type },
          format: 1,
          maxDataPoints: 1000,
          intervalMs: 1000,
        }],
        from: 'now',
        to: 'now',
      });
      return this.framesToRows<T>(response, refId);
    } catch (error) {
      throw this.wrapError(error);
    }
  }

  async executeRawQuery(sql: string, _database?: string): Promise<string[]> {
    // Route through the normal Grafana query path — the ClickHouse datasource
    // plugin handles EXPLAIN and returns results as a data frame.
    // We extract the first column's values as strings.
    try {
      const rows = await this.executeQuery<Record<string, unknown>>(sql);
      return rows.map(row => {
        const value = row.explain || row.EXPLAIN || Object.values(row)[0];
        return value !== undefined && value !== null ? String(value) : '';
      }).filter(Boolean);
    } catch (error) {
      throw this.wrapError(error);
    }
  }

  private framesToRows<T extends Record<string, unknown>>(response: DsQueryResponse, refId: string): T[] {
    const result = response.results?.[refId];
    if (!result?.frames?.length) return [];
    if (result.error) {
      const errMsg = typeof result.error === 'string'
        ? result.error
        : JSON.stringify(result.error);
      throw new Error(errMsg);
    }

    const frame = result.frames[0];
    const fields = frame.schema?.fields ?? [];
    const values = frame.data?.values ?? [];

    if (!fields.length || !values.length) return [];

    const rowCount = values[0]?.length ?? 0;
    const rows: T[] = [];

    for (let i = 0; i < rowCount; i++) {
      const row: Record<string, unknown> = {};
      for (let j = 0; j < fields.length; j++) {
        row[fields[j].name] = values[j]?.[i] ?? null;
      }
      rows.push(row as T);
    }

    return rows;
  }

  private wrapError(error: unknown): AdapterError {
    // Grafana backend errors can be plain objects like { data: { message: '...' }, status: 400 }
    let msg: string;
    if (error instanceof Error) {
      msg = error.message;
    } else if (error && typeof error === 'object') {
      const e = error as Record<string, unknown>;
      const data = e.data as Record<string, unknown> | undefined;
      msg = (data?.message as string)
        || (e.message as string)
        || (e.statusText as string)
        || JSON.stringify(error);
    } else {
      msg = String(error);
    }
    const cause = error instanceof Error ? error : undefined;

    if (msg.includes('Unauthorized') || msg.includes('403') || msg.includes('authentication')) {
      return new AdapterError(msg, 'authentication', cause);
    }
    if (msg.includes('timeout') || msg.includes('Timeout')) {
      return new AdapterError(msg, 'timeout', cause);
    }
    if (msg.includes('network') || msg.includes('ECONNREFUSED') || msg.includes('Failed to fetch')) {
      return new AdapterError(msg, 'network', cause);
    }
    if (msg.includes('DB::Exception') || msg.includes('Syntax error') || msg.includes('Code:')) {
      return new AdapterError(msg, 'query', cause);
    }
    return new AdapterError(msg, 'unknown', cause);
  }
}
