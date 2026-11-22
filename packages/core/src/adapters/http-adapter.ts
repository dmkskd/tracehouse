import { createClient, type ClickHouseClient } from '@clickhouse/client';
import { rawResponseLines, responseFormat } from './raw-lines.js';
import type { IClickHouseAdapter, QueryExecutionOptions, TaggedQuery } from './types.js';
import { AdapterError, CLIENT_COMPRESSION } from './types.js';
import type { ConnectionConfig } from '../types/connection.js';
import { applyStickyRouting } from './sticky-routing.js';
import { randomUUID } from '../utils/uuid.js';

export class HttpAdapter implements IClickHouseAdapter {
  readonly supportsExplicitQueryId = true;

  private client: ClickHouseClient;
  private sessionPrefix: string;

  constructor(config: ConnectionConfig) {
    const host = applyStickyRouting(config.host, config.useCloudStickyRouting);
    this.sessionPrefix = `chm-${randomUUID().slice(0, 8)}`;
    this.client = createClient({
      url: `${config.secure ? 'https' : 'http'}://${host}:${config.port}`,
      username: config.user,
      password: config.password,
      database: config.database,
      request_timeout: config.send_receive_timeout * 1000,
      compression: CLIENT_COMPRESSION,
      clickhouse_settings: {
        connect_timeout: config.connect_timeout,
      },
    });
  }

  private sessionIdFor(sql: string): string {
    const m = sql.match(/\/\*\s*source:TraceHouse:([^*]+?)\s*\*\//);
    const tag = m ? m[1].replace(/:/g, '-') : 'untagged';
    return `${this.sessionPrefix}-${tag}-${randomUUID().slice(0, 4)}`;
  }

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery, options?: QueryExecutionOptions): Promise<T[]> {
    try {
      const result = await this.client.query({
        query: sql,
        format: 'JSONEachRow',
        session_id: this.sessionIdFor(sql),
        ...(options?.queryId ? { query_id: options.queryId } : {}),
        ...(options?.utcDateTimeOutput ? { clickhouse_settings: { date_time_output_format: 'iso' as const } } : {}),
      });
      return await result.json<T>();
    } catch (error) {
      throw this.wrapError(error);
    }
  }

  async close(): Promise<void> {
    await this.client.close();
  }

  async executeRawQuery(sql: TaggedQuery, database?: string, options?: QueryExecutionOptions): Promise<string[]> {
    try {
      const { stream, response_headers } = await this.client.exec({
        query: sql,
        session_id: this.sessionIdFor(sql),
        ...(database ? { clickhouse_settings: { database } } : {}),
        ...(options?.queryId ? { query_id: options.queryId } : {}),
      });
      const chunks: Buffer[] = [];
      for await (const chunk of stream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }
      return rawResponseLines(Buffer.concat(chunks).toString('utf-8'), responseFormat(response_headers));
    } catch (error) {
      throw this.wrapError(error);
    }
  }

  private wrapError(error: unknown): AdapterError {
    const msg = error instanceof Error ? error.message : String(error);
    const cause = error instanceof Error ? error : undefined;
    const errorRecord = typeof error === 'object' && error !== null
      ? error as { code?: unknown; type?: unknown }
      : undefined;
    const code = errorRecord?.code == null ? '' : String(errorRecord.code);
    const type = errorRecord?.type == null ? '' : String(errorRecord.type);

    if (
      type === 'NETWORK_ERROR' ||
      msg.includes('ECONNREFUSED') ||
      msg.includes('ENOTFOUND') ||
      msg.includes('CORS') ||
      msg.includes('fetch failed') ||
      msg.includes('network')
    ) {
      return new AdapterError(msg, 'network', cause);
    }
    if (
      type === 'AUTHENTICATION_FAILED' ||
      msg.includes('Authentication') ||
      msg.includes('authentication') ||
      msg.includes('Access denied') ||
      msg.includes('wrong password') ||
      msg.includes('AUTHENTICATION_FAILED')
    ) {
      return new AdapterError(msg, 'authentication', cause);
    }
    if (
      type === 'TIMEOUT_EXCEEDED' ||
      msg.includes('timeout') ||
      msg.includes('Timeout') ||
      msg.includes('ETIMEDOUT') ||
      msg.includes('ESOCKETTIMEDOUT')
    ) {
      return new AdapterError(msg, 'timeout', cause);
    }
    if (
      /^\d+$/.test(code) ||
      msg.includes('Syntax error') ||
      msg.includes('DB::Exception') ||
      msg.includes('Unknown') ||
      msg.includes('Code:') ||
      msg.includes('[Code ')
    ) {
      return new AdapterError(msg, 'query', cause);
    }
    return new AdapterError(msg, 'unknown', cause);
  }
}
