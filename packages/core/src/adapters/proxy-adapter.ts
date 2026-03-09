/**
 * ProxyAdapter — routes ClickHouse queries through a local CORS proxy.
 *
 * Instead of connecting directly to ClickHouse (which fails due to CORS
 * when the browser page and ClickHouse are on different origins), this
 * adapter sends queries to a lightweight local proxy server that forwards
 * them server-side.
 *
 * Connection details are passed as headers on every request so the proxy
 * is stateless.
 */

import type { IClickHouseAdapter } from './types.js';
import { AdapterError } from './types.js';
import type { AdapterErrorCategory } from './types.js';
import type { ConnectionConfig } from '../types/connection.js';
import { applyStickyRouting } from './sticky-routing.js';

export class ProxyAdapter implements IClickHouseAdapter {
  private proxyUrl: string;
  private headers: Record<string, string>;

  constructor(
    config: ConnectionConfig,
    proxyUrl: string = 'http://localhost:8990/proxy',
  ) {
    this.proxyUrl = proxyUrl.replace(/\/$/, '');
    const host = applyStickyRouting(config.host, config.useCloudStickyRouting);
    this.headers = {
      'x-ch-host': host,
      'x-ch-port': String(config.port),
      'x-ch-user': config.user,
      'x-ch-password': config.password,
      'x-ch-database': config.database,
      'x-ch-secure': String(config.secure),
    };
  }

  async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
    try {
      const resp = await fetch(`${this.proxyUrl}/query?format=JSONEachRow`, {
        method: 'POST',
        headers: this.headers,
        body: sql,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
      }

      const text = await resp.text();
      if (!text.trim()) return [];

      // JSONEachRow: one JSON object per line
      return text
        .trim()
        .split('\n')
        .filter(line => line.trim())
        .map(line => JSON.parse(line) as T);
    } catch (error) {
      throw this.wrapError(error, sql);
    }
  }

  async executeCommand(sql: string): Promise<void> {
    try {
      const resp = await fetch(`${this.proxyUrl}/command`, {
        method: 'POST',
        headers: this.headers,
        body: sql,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
      }
    } catch (error) {
      throw this.wrapError(error, sql);
    }
  }

  async executeRawQuery(sql: string, database?: string): Promise<string[]> {
    try {
      const headers = database
        ? { ...this.headers, 'x-ch-database': database }
        : this.headers;
      const resp = await fetch(`${this.proxyUrl}/query`, {
        method: 'POST',
        headers,
        body: sql,
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(text || `HTTP ${resp.status}`);
      }

      const text = await resp.text();
      const rawLines = text.split('\n').filter(line => line !== '');

      // Detect JSONEachRow and extract first field value
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
      throw this.wrapError(error, sql);
    }
  }

  async close(): Promise<void> {
    // No persistent connection to close
  }

  private wrapError(error: unknown, sql?: string): AdapterError {
    let msg = 'Unknown error';

    if (error instanceof Error) {
      msg = error.message;
    } else if (typeof error === 'string') {
      msg = error;
    }

    let category: AdapterErrorCategory;
    if (msg.includes('Failed to fetch') || msg.includes('NetworkError') || msg.includes('ECONNREFUSED') || msg.includes('fetch')) {
      category = 'network';
    } else if (msg.includes('Authentication') || msg.includes('Access denied') || msg.includes('AUTHENTICATION_FAILED')) {
      category = 'authentication';
    } else if (msg.includes('timeout') || msg.includes('Timeout') || msg.includes('ETIMEDOUT')) {
      category = 'timeout';
    } else if (msg.includes('Syntax error') || msg.includes('DB::Exception') || msg.includes('Code:')) {
      category = 'query';
    } else {
      category = 'unknown';
    }

    const log = category === 'query' ? console.debug : console.error;
    log('[ProxyAdapter] Error:', msg);
    if (sql) log('[ProxyAdapter] SQL:', sql.substring(0, 500));

    return new AdapterError(msg, category, error instanceof Error ? error : undefined);
  }
}
