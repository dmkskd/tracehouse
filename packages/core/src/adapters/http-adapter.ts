import { createClient, type ClickHouseClient } from '@clickhouse/client';
import type { IClickHouseAdapter } from './types.js';
import { AdapterError, CLIENT_COMPRESSION } from './types.js';
import type { ConnectionConfig } from '../types/connection.js';
import { applyStickyRouting } from './sticky-routing.js';
import { randomUUID } from '../utils/uuid.js';

export class HttpAdapter implements IClickHouseAdapter {
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
    const m = sql.match(/\/\*\s*source:CHM:([^*]+?)\s*\*\//);
    const tag = m ? m[1].replace(/:/g, '-') : 'untagged';
    return `${this.sessionPrefix}-${tag}-${randomUUID().slice(0, 4)}`;
  }

  async executeQuery<T extends Record<string, unknown>>(
    sql: string,
  ): Promise<T[]> {
    try {
      const result = await this.client.query({
        query: sql,
        format: 'JSONEachRow',
        session_id: this.sessionIdFor(sql),
      });
      return await result.json<T>();
    } catch (error) {
      throw this.wrapError(error);
    }
  }

  async close(): Promise<void> {
    await this.client.close();
  }

  async executeRawQuery(sql: string, database?: string): Promise<string[]> {
    try {
      const { stream } = await this.client.exec({
        query: sql,
        session_id: this.sessionIdFor(sql),
        ...(database ? { clickhouse_settings: { database } } : {}),
      });
      const chunks: Buffer[] = [];
      for await (const chunk of stream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }
      const text = Buffer.concat(chunks).toString('utf-8');
      const rawLines = text.split('\n').filter(line => line !== '');

      // Server may return JSONEachRow depending on default_format setting.
      // Detect and extract the field value if so.
      if (rawLines.length > 0 && rawLines[0].trimStart().startsWith('{')) {
        return rawLines.map(line => {
          try {
            const obj = JSON.parse(line);
            const vals = Object.values(obj);
            return vals.length > 0 ? String(vals[0]) : line;
          } catch {
            return line;
          }
        });
      }

      return rawLines;
    } catch (error) {
      throw this.wrapError(error);
    }
  }

  private wrapError(error: unknown): AdapterError {
    const msg = error instanceof Error ? error.message : String(error);
    const cause = error instanceof Error ? error : undefined;

    if (
      msg.includes('ECONNREFUSED') ||
      msg.includes('ENOTFOUND') ||
      msg.includes('CORS') ||
      msg.includes('fetch failed') ||
      msg.includes('network')
    ) {
      return new AdapterError(msg, 'network', cause);
    }
    if (
      msg.includes('Authentication') ||
      msg.includes('authentication') ||
      msg.includes('Access denied') ||
      msg.includes('wrong password') ||
      msg.includes('AUTHENTICATION_FAILED')
    ) {
      return new AdapterError(msg, 'authentication', cause);
    }
    if (
      msg.includes('timeout') ||
      msg.includes('Timeout') ||
      msg.includes('ETIMEDOUT') ||
      msg.includes('ESOCKETTIMEDOUT')
    ) {
      return new AdapterError(msg, 'timeout', cause);
    }
    if (
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
