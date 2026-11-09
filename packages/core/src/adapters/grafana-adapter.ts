import type { IClickHouseAdapter, QueryExecutionOptions, TaggedQuery } from './types.js';
import { AdapterError } from './types.js';
import { describeResultTypesQuery } from '../queries/result-type-queries.js';

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

/** Shared by ServiceProvider, calendar-Date decoding, and real transport tests. */
export const GRAFANA_QUERY_TIMEZONE = 'browser' as const;

export class GrafanaAdapter implements IClickHouseAdapter {
  constructor(private readonly query: AdapterQueryFn) {}

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery, options?: QueryExecutionOptions): Promise<T[]> {
    const refId = 'q';
    try {
      // Grafana collapses ClickHouse Date and DateTime into the same time-field
      // schema. Time-bearing Analytics results need native types to distinguish
      // calendar dates from instants. Do not run metadata SQL for non-time results
      // (including DDL/EXPLAIN). Analysis may evaluate scalar subqueries, so avoid
      // claiming that the metadata lookup cannot evaluate query expressions.
      // PERFORMANCE: qualifying Analytics queries pay one extra sequential
      // datasource request on every execution; no metadata cache exists.
      // Behavior and diagnosis: docs/site/docs/development/grafana-plugin.md
      // #analytics-result-types-and-metadata-lookup
      const frames = await this.query(sql, refId);
      const needsTypes = options?.utcDateTimeOutput && frames.some(frame => frame.fields.some(field => field.type === 'time'));
      const nativeTypes = needsTypes ? await this.describeColumnTypes(sql) : undefined;
      return this.framesToRows<T>(frames, !!nativeTypes, nativeTypes);
    } catch (error) {
      throw this.wrapError(error);
    }
  }

  async executeRawQuery(sql: TaggedQuery, _database?: string, _options?: QueryExecutionOptions): Promise<string[]> {
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

  private async describeColumnTypes(sql: TaggedQuery): Promise<Map<string, string>> {
    try {
      const frames = await this.query(describeResultTypesQuery(sql), 'types');
      const rows = this.framesToRows<{ name: string; type: string }>(frames);
      if (!rows.length) throw new Error('No native column types were returned');
      return new Map(rows.map(row => [row.name, row.type]));
    } catch (error) {
      throw new Error(`Cannot safely interpret Grafana time fields: native column-type lookup failed: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
    }
  }

  private framesToRows<T extends Record<string, unknown>>(frames: AdapterFrame[], utcDateTimeOutput = false, nativeTypes?: Map<string, string>): T[] {
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
    // Resolve and unwrap once per column, rather than once per result cell.
    const nativeColumnTypes = fields.map(field => {
      let type = nativeTypes?.get(field.name);
      for (let wrapper = type?.match(/^(?:Nullable|LowCardinality)\((.*)\)$/); wrapper; wrapper = type?.match(/^(?:Nullable|LowCardinality)\((.*)\)$/)) type = wrapper[1];
      return type;
    });

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
          const iso = new Date(v).toISOString();
          const nativeType = nativeColumnTypes[j];
          if (utcDateTimeOutput && nativeType !== 'Date' && nativeType !== 'Date32' && !nativeType?.startsWith('DateTime')) {
            throw new Error(`Cannot safely interpret Grafana time field ${fields[j].name}: unsupported native type ${nativeType ?? '(missing)'}`);
          }
          // With timezone:'browser', the datasource represents a calendar Date
          // as browser-local midnight. Slicing its UTC ISO value loses a day in
          // positive-offset browsers, so recover its local calendar components.
          const calendar = new Date(v);
          const calendarText = `${calendar.getFullYear()}-${String(calendar.getMonth() + 1).padStart(2, '0')}-${String(calendar.getDate()).padStart(2, '0')}`;
          v = nativeType === 'Date' || nativeType === 'Date32'
            ? calendarText
            : utcDateTimeOutput && nativeType?.startsWith('DateTime')
              ? iso : iso.replace('T', ' ').replace('Z', '');
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
