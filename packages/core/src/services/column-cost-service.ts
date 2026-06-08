import type { IClickHouseAdapter } from '../adapters/types.js';
import { HostTargetedAdapter } from '../adapters/host-targeted-adapter.js';
import { escapeValue, tagQuery } from '../queries/builder.js';
import { TAB_QUERIES, sourceTag } from '../queries/source-tags.js';

export interface ColumnCost {
  column: string;
  bytes: number;
  pct: number;
}

export interface ServerColumnCost {
  column: string;
  readBytes: number;
  pct: number;
}

export interface ServerProgress {
  total: number;
  completed: number;
  currentColumn: string;
  flushCountdown?: number;
  flushIntervalMs?: number;
}

export interface ColumnCostTarget {
  clusterName?: string | null;
  hostname?: string | null;
}

export interface ClientColumnCostResult {
  costs: ColumnCost[];
  total: number;
}

export interface ServerColumnCostResult {
  costs: ServerColumnCost[];
  total: number;
}

export interface ServerColumnCostOptions extends ColumnCostTarget {
  flushIntervalMs?: number | null;
  onProgress?: (progress: ServerProgress) => void;
  sleep?: (ms: number) => Promise<void>;
}

export class ColumnCostServiceError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'ColumnCostServiceError';
  }
}

export class ColumnCostService {
  constructor(private adapter: IClickHouseAdapter) {}

  private adapterForTarget(target?: ColumnCostTarget): IClickHouseAdapter {
    if (target?.hostname && target.clusterName) {
      return new HostTargetedAdapter(this.adapter, target.clusterName, target.hostname);
    }
    return this.adapter;
  }

  async discoverOutputColumns(query: string, target?: ColumnCostTarget): Promise<string[] | null> {
    const queryAdapter = this.adapterForTarget(target);
    const stripped = query.replace(/;\s*$/, '');

    try {
      const describeResult = await queryAdapter.executeQuery<{ name: string; type: string }>(
        tagQuery(`DESCRIBE (${stripped})`, sourceTag(TAB_QUERIES, 'columnCostDescribe')),
      );
      if (describeResult.length > 0) return describeResult.map(row => row.name);
    } catch { /* fall through */ }

    try {
      const probeResult = await queryAdapter.executeQuery<Record<string, unknown>>(
        tagQuery(`SELECT * FROM (${stripped}) LIMIT 1`, sourceTag(TAB_QUERIES, 'columnCostProbe')),
      );
      if (probeResult.length > 0) return Object.keys(probeResult[0]);
    } catch { /* fall through */ }

    return null;
  }

  async runClientAnalysis(query: string, target?: ColumnCostTarget): Promise<ClientColumnCostResult> {
    const queryAdapter = this.adapterForTarget(target);
    const outputColumns = await this.discoverOutputColumns(query, target);
    if (!outputColumns || outputColumns.length === 0) {
      throw new ColumnCostServiceError('Could not determine output columns for this query.');
    }

    const byteSizeExprs = outputColumns.map(col => {
      const escaped = col.replace(/`/g, '\\`');
      return `sum(byteSize(\`${escaped}\`)) AS \`__bytes_${escaped}\``;
    });

    const analysisSql = `SELECT ${byteSizeExprs.join(', ')} FROM (${query.replace(/;\s*$/, '')})`;
    const result = await queryAdapter.executeQuery<Record<string, number>>(
      tagQuery(analysisSql, sourceTag(TAB_QUERIES, 'columnCostClient')),
    );
    const row = result[0] ?? {};

    let total = 0;
    const costs: ColumnCost[] = outputColumns.map(col => {
      const bytes = Number(row[`__bytes_${col}`] || 0);
      total += bytes;
      return { column: col, bytes, pct: 0 };
    });

    for (const cost of costs) {
      cost.pct = total > 0 ? (cost.bytes / total) * 100 : 0;
    }
    costs.sort((a, b) => b.bytes - a.bytes);

    return { costs, total };
  }

  async runServerAnalysis(query: string, options: ServerColumnCostOptions = {}): Promise<ServerColumnCostResult> {
    const queryAdapter = this.adapterForTarget(options);
    const outputColumns = await this.discoverOutputColumns(query, options);
    if (!outputColumns || outputColumns.length === 0) {
      throw new ColumnCostServiceError('Could not determine output columns for this query.');
    }

    const strippedQuery = query.replace(/;\s*$/, '');
    const runTag = `__ccost_${Date.now()}`;
    const columnTags: { col: string; tag: string; failed: boolean }[] = [];

    for (let i = 0; i < outputColumns.length; i++) {
      const col = outputColumns[i];
      const escaped = col.replace(/`/g, '\\`');
      options.onProgress?.({ total: outputColumns.length, completed: i, currentColumn: col });

      const tag = `${runTag}_${i}`;
      const analysisSql = `SELECT count() AS \`${tag}\` FROM (SELECT \`${escaped}\` FROM (${strippedQuery}))`;

      let failed = false;
      try {
        await queryAdapter.executeQuery<Record<string, number>>(
          tagQuery(analysisSql, sourceTag(TAB_QUERIES, 'columnCostServerColumn')),
        );
      } catch {
        failed = true;
      }
      columnTags.push({ col, tag, failed });
    }

    const flushMs = options.flushIntervalMs ?? 7500;
    const sleep = options.sleep ?? ((ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms)));
    const lastSuccessTag = [...columnTags].reverse().find(t => !t.failed)?.tag;

    if (lastSuccessTag) {
      const waitSec = Math.ceil(flushMs / 1000);
      for (let remaining = waitSec; remaining > 0; remaining--) {
        options.onProgress?.({
          total: outputColumns.length,
          completed: outputColumns.length,
          currentColumn: `waiting for query_log flush (${remaining}s)`,
          flushCountdown: remaining,
          flushIntervalMs: flushMs,
        });
        await sleep(1000);
      }

      options.onProgress?.({
        total: outputColumns.length,
        completed: outputColumns.length,
        currentColumn: 'checking query_log...',
        flushIntervalMs: flushMs,
      });

      const pollStart = Date.now();
      while (Date.now() - pollStart < 15_000) {
        try {
          const checkSql = tagQuery(`
            SELECT count() AS c
            FROM {{cluster_aware:system.query_log}}
            WHERE query LIKE '%${escapeValue(lastSuccessTag)}%'
              AND query NOT LIKE '%system.query_log%'
              AND type = 'QueryFinish'
          `, sourceTag(TAB_QUERIES, 'columnCostLogCheck'));
          const checkResult = await this.adapter.executeQuery<{ c: number }>(checkSql);
          if (checkResult.length > 0 && Number(checkResult[0].c) > 0) break;
        } catch { /* ignore */ }
        await sleep(1000);
      }
    }

    const costs = await this.fetchServerCosts(columnTags);
    let total = 0;
    for (const cost of costs) total += cost.readBytes;
    for (const cost of costs) {
      cost.pct = total > 0 ? (cost.readBytes / total) * 100 : 0;
    }
    costs.sort((a, b) => b.readBytes - a.readBytes);

    return { costs, total };
  }

  private async fetchServerCosts(columnTags: { col: string; tag: string; failed: boolean }[]): Promise<ServerColumnCost[]> {
    const successTags = columnTags.filter(t => !t.failed);
    if (successTags.length === 0) {
      return columnTags.map(t => ({ column: t.col, readBytes: 0, pct: 0 }));
    }

    const likeConditions = successTags
      .map(t => `query LIKE '%${escapeValue(t.tag)}%'`)
      .join(' OR ');

    const logSql = tagQuery(`
      SELECT query, read_bytes
      FROM {{cluster_aware:system.query_log}}
      WHERE (${likeConditions})
        AND query NOT LIKE '%system.query_log%'
        AND type = 'QueryFinish'
      ORDER BY event_time_microseconds DESC
    `, sourceTag(TAB_QUERIES, 'columnCostLogLookup'));

    try {
      const logResults = await this.adapter.executeQuery<{ query: string; read_bytes: number }>(logSql);
      const tagToBytes = new Map<string, number>();
      for (const row of logResults) {
        for (const tag of successTags) {
          if (row.query.includes(tag.tag) && !tagToBytes.has(tag.tag)) {
            tagToBytes.set(tag.tag, Number(row.read_bytes));
          }
        }
      }

      return columnTags.map(tag => ({
        column: tag.col,
        readBytes: tag.failed ? 0 : (tagToBytes.get(tag.tag) ?? 0),
        pct: 0,
      }));
    } catch {
      return columnTags.map(tag => ({ column: tag.col, readBytes: 0, pct: 0 }));
    }
  }
}
