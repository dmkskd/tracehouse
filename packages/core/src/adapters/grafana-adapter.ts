import type { IClickHouseAdapter } from './types.js';
import { AdapterError } from './types.js';

export interface AdapterField {
  name: string;
  type?: string;
  values: unknown[] | { length: number; get(index: number): unknown };
}

export interface AdapterFrame {
  fields: AdapterField[];
  length?: number;
}

export type AdapterQueryFn = (sql: string, refId: string) => Promise<AdapterFrame[]>;

export class GrafanaAdapter implements IClickHouseAdapter {
  constructor(private readonly query: AdapterQueryFn) {}

  async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
    const refId = 'q';
    try {
      const frames = await this.query(sql, refId);
      return this.framesToRows<T>(frames);
    } catch (error) {
      throw this.wrapError(error);
    }
  }

  async executeRawQuery(sql: string, _database?: string): Promise<string[]> {
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

  private framesToRows<T extends Record<string, unknown>>(frames: AdapterFrame[]): T[] {
    if (!frames.length) return [];

    const frame = frames[0];
    const fields = frame.fields ?? [];
    if (!fields.length) return [];

    const firstValues = fields[0].values;
    const defaultLen = Array.isArray(firstValues)
      ? firstValues.length
      : (firstValues as { length: number }).length ?? 0;
    const rowCount = frame.length ?? defaultLen;

    const timeFieldIndices = new Set(
      fields.map((f, i) => f.type === 'time' ? i : -1).filter(i => i >= 0)
    );

    const getValue = (field: AdapterField, i: number): unknown => {
      const values = field.values;
      if (Array.isArray(values)) return values[i] ?? null;
      return (values as { get(i: number): unknown }).get(i) ?? null;
    };

    const rows: T[] = [];
    for (let i = 0; i < rowCount; i++) {
      const row: Record<string, unknown> = {};
      for (let j = 0; j < fields.length; j++) {
        let v = getValue(fields[j], i);
        // Grafana's ClickHouse datasource returns DateTime columns as epoch-ms
        // with schema type "time". Normalize these to ISO-like strings so
        // downstream code can treat all adapter output uniformly.
        if (timeFieldIndices.has(j) && typeof v === 'number') {
          v = new Date(v).toISOString().replace('T', ' ').replace('Z', '');
        }
        row[fields[j].name] = v;
      }
      rows.push(row as T);
    }

    return rows;
  }

  private wrapError(error: unknown): AdapterError {
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
    if (msg.includes('DB::Exception') || msg.includes('Syntax error') || msg.includes('Code:') || msg.includes('[Code ')) {
      return new AdapterError(msg, 'query', cause);
    }
    return new AdapterError(msg, 'unknown', cause);
  }
}
