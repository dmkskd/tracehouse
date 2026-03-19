import { createClient, type ClickHouseClient } from '@clickhouse/client-web';
import type { IClickHouseAdapter } from './types.js';
import { AdapterError, CLIENT_COMPRESSION } from './types.js';
import type { AdapterErrorCategory } from './types.js';
import type { ConnectionConfig } from '../types/connection.js';
import { applyStickyRouting } from './sticky-routing.js';

/**
 * Browser-compatible adapter that uses @clickhouse/client-web.
 * This is the official ClickHouse browser client that handles CORS, auth, etc.
 */
export class BrowserAdapter implements IClickHouseAdapter {
  private client: ClickHouseClient;
  private sessionPrefix: string;

  constructor(config: ConnectionConfig) {
    const host = applyStickyRouting(config.host, config.useCloudStickyRouting);
    // Stable prefix for this connection instance — combined with the query
    // source tag to produce a unique-per-purpose session_id on each request.
    // This avoids the "session is locked" error from concurrent queries while
    // still giving the LB a consistent key per query type for routing affinity.
    this.sessionPrefix = `chm-${crypto.randomUUID().slice(0, 8)}`;
    this.client = createClient({
      url: `${config.secure ? 'https' : 'http'}://${host}:${config.port}`,
      username: config.user,
      password: config.password,
      database: config.database,
      request_timeout: (config.send_receive_timeout ?? 300) * 1000,
      compression: CLIENT_COMPRESSION,
      keep_alive: { enabled: false },
    });
  }

  /** Extract the source tag from a tagged query, e.g. "/* source:CHM:Overview:merges *​/" → "Overview-merges" */
  private sessionIdFor(sql: string): string {
    const m = sql.match(/\/\*\s*source:CHM:([^*]+?)\s*\*\//);
    const tag = m ? m[1].replace(/:/g, '-') : 'untagged';
    // Unique per call to avoid "session is locked" on concurrent queries,
    // but prefixed with the source tag so the LB sees a consistent routing key.
    return `${this.sessionPrefix}-${tag}-${crypto.randomUUID().slice(0, 4)}`;
  }

  async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
    try {
      const result = await this.client.query({
        query: sql,
        format: 'JSONEachRow',
        session_id: this.sessionIdFor(sql),
      });
      return await result.json<T>();
    } catch (error) {
      throw this.wrapError(error, sql);
    }
  }

  async close(): Promise<void> {
    await this.client.close();
  }

  async executeCommand(sql: string): Promise<void> {
    await this.client.command({ query: sql, session_id: this.sessionIdFor(sql) });
  }

  async executeRawQuery(sql: string, database?: string): Promise<string[]> {
    try {
      // EXPLAIN and similar statements don't support FORMAT clauses.
      // client.query() always appends FORMAT, so we use exec() which
      // sends the query as-is and returns a raw stream.
      const { stream } = await this.client.exec({
        query: sql,
        session_id: this.sessionIdFor(sql),
        ...(database ? { clickhouse_settings: { database } } : {}),
      });
      const reader = stream.getReader();
      const decoder = new TextDecoder();
      const chunks: string[] = [];
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(decoder.decode(value, { stream: true }));
      }
      chunks.push(decoder.decode()); // flush
      const text = chunks.join('');
      const rawLines = text.split('\n').filter(line => line !== '');

      // The server may return TabSeparated plain text or JSONEachRow
      // depending on default_format. Detect and handle both.
      if (rawLines.length > 0 && rawLines[0].trimStart().startsWith('{')) {
        // JSONEachRow — extract the first (typically 'explain') field value
        return rawLines.map(line => {
          try {
            const obj = JSON.parse(line);
            // Return the first string value found
            const vals = Object.values(obj);
            return vals.length > 0 ? String(vals[0]) : line;
          } catch {
            return line;
          }
        });
      }

      return rawLines;
    } catch (error) {
      throw this.wrapError(error, sql);
    }
  }

  private wrapError(error: unknown, sql?: string): AdapterError {
    // Try to extract the most useful error message
    let msg = 'Unknown error';
    
    if (error instanceof Error) {
      msg = error.message;
      
      // Check for ClickHouse-specific error properties
      const anyError = error as unknown as Record<string, unknown>;
      
      // The @clickhouse/client-web may include additional details
      if (anyError.cause && anyError.cause instanceof Error) {
        msg = anyError.cause.message || msg;
      }
      
      // Check for response body with error details
      if (typeof anyError.body === 'string' && anyError.body) {
        msg = anyError.body;
      }
      
      // Some errors have a 'code' property from ClickHouse
      if (typeof anyError.code === 'string' || typeof anyError.code === 'number') {
        msg = `[Code ${anyError.code}] ${msg}`;
      }
    } else if (typeof error === 'string') {
      msg = error;
    } else if (error && typeof error === 'object') {
      // Try to extract message from object
      const obj = error as Record<string, unknown>;
      if (typeof obj.message === 'string') msg = obj.message;
      else if (typeof obj.error === 'string') msg = obj.error;
      else if (typeof obj.body === 'string') msg = obj.body;
      else msg = JSON.stringify(error);
    }
    
    const cause = error instanceof Error ? error : undefined;

    // Categorize the error first so we can adjust log level
    let category: AdapterErrorCategory;
    if (
      msg.includes('ECONNREFUSED') ||
      msg.includes('ENOTFOUND') ||
      msg.includes('CORS') ||
      msg.includes('fetch failed') ||
      msg.includes('Failed to fetch') ||
      msg.includes('Load failed') ||
      msg.includes('network') ||
      msg.includes('NetworkError')
    ) {
      category = 'network';
    } else if (
      msg.includes('Authentication') ||
      msg.includes('authentication') ||
      msg.includes('Access denied') ||
      msg.includes('wrong password') ||
      msg.includes('AUTHENTICATION_FAILED') ||
      msg.includes('401')
    ) {
      category = 'authentication';
    } else if (
      msg.includes('timeout') ||
      msg.includes('Timeout') ||
      msg.includes('ETIMEDOUT')
    ) {
      category = 'timeout';
    } else if (
      msg.includes('Syntax error') ||
      msg.includes('DB::Exception') ||
      msg.includes('Unknown') ||
      msg.includes('Code:')
    ) {
      category = 'query';
    } else {
      category = 'unknown';
    }

    // Log at debug level for query errors (often expected from feature probes),
    // error level for connection/auth/timeout issues.
    const log = category === 'query' ? console.debug : console.error;
    log('[BrowserAdapter] Query error:', msg);
    if (sql) {
      log('[BrowserAdapter] SQL:', sql.substring(0, 500) + (sql.length > 500 ? '...' : ''));
    }

    return new AdapterError(msg, category, cause);
  }
}
