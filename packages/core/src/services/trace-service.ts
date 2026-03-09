/**
 * TraceService - Fetches query trace logs, EXPLAIN results, and OpenTelemetry spans.
 */
import type { IClickHouseAdapter } from '../adapters/types.js';
import type { TraceLog, ExplainType, ExplainResult, OpenTelemetrySpan, FlamegraphSample, FlamegraphNode, ProcessorProfile } from '../types/trace.js';
import { buildQuery, tagQuery, eventDateBound } from '../queries/builder.js';
import { TAB_QUERIES, sourceTag } from '../queries/source-tags.js';
import { QUERY_TRACE_LOGS, QUERY_FLAMEGRAPH_CPU, QUERY_FLAMEGRAPH_REAL, QUERY_FLAMEGRAPH_MEMORY, QUERY_FLAMEGRAPH_DATA, QUERY_FLAMEGRAPH_REAL_LEGACY, QUERY_FLAMEGRAPH_MEMORY_LEGACY, QUERY_PROCESSORS_PROFILE } from '../queries/trace-queries.js';

export type FlamegraphType = 'CPU' | 'Real' | 'Memory';

export class TraceServiceError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'TraceServiceError';
  }
}

export class TraceService {
  constructor(private adapter: IClickHouseAdapter) {}

  /**
   * Fetch trace logs for a specific query from system.text_log.
   * @param eventDate - Optional query_start_time for precise partition pruning.
   */
  async getQueryLogs(queryId: string, logLevels?: string[], eventDate?: string, limit?: number): Promise<TraceLog[]> {
    try {
      const dateBound = eventDateBound(eventDate);
      const rowLimit = limit ?? 1000;
      let sql: string;

      if (logLevels && logLevels.length > 0) {
        // Build IN clause for log levels
        const levelsStr = logLevels.map(l => `'${l}'`).join(', ');
        sql = `
          SELECT
            toString(event_time) AS event_time,
            toString(event_time_microseconds) AS event_time_microseconds,
            query_id,
            level,
            message,
            logger_name AS source,
            thread_id,
            thread_name
          FROM {{cluster_aware:system.text_log}}
          WHERE query_id = {query_id}
            AND level IN (${levelsStr})
            AND event_date >= ${dateBound}
          ORDER BY event_time_microseconds ASC
          LIMIT ${rowLimit}
        `;
        sql = buildQuery(sql, { query_id: queryId });
      } else {
        sql = buildQuery(QUERY_TRACE_LOGS.replace('{event_date_bound}', dateBound).replace('LIMIT 1000', `LIMIT ${rowLimit}`), { query_id: queryId });
      }

      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'queryLogs')));
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        return {
          event_time: String(row.event_time || ''),
          event_time_microseconds: String(row.event_time_microseconds || row.event_time || ''),
          query_id: String(row.query_id || ''),
          level: String(row.level || ''),
          message: String(row.message || ''),
          source: String(row.source || ''),
          thread_id: Number(row.thread_id || 0),
          thread_name: String(row.thread_name || ''),
        };
      });
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      console.error('[TraceService] Failed to get query logs:', msg);
      throw new TraceServiceError(`Failed to get query logs: ${msg}`, error as Error);
    }
  }

  /**
   * Execute EXPLAIN query and return the result
   */
  async executeExplain(query: string, explainType: ExplainType, database?: string): Promise<ExplainResult> {
      try {
        const explainQuery = `EXPLAIN ${explainType} ${query}`;

        // EXPLAIN doesn't support FORMAT clauses appended by executeQuery,
        // so prefer executeRawQuery which uses exec() and handles both
        // plain text and JSONEachRow server responses.
        let lines: string[];
        if (this.adapter.executeRawQuery) {
          lines = await this.adapter.executeRawQuery(explainQuery, database);
        } else {
          // Fallback for adapters without executeRawQuery
          const rows = await this.adapter.executeQuery(explainQuery);
          lines = rows.map(r => {
            const row = r as Record<string, unknown>;
            const value = row.explain || row.EXPLAIN || Object.values(row)[0];
            return value !== undefined && value !== null ? String(value) : '';
          }).filter(Boolean);
        }

        const output = lines.join('\n');

        // Try to parse as JSON for tree structure (only PLAN supports json=1)
        let parsedTree: Record<string, unknown> | null = null;
        if (explainType === 'PLAN') {
          try {
            const jsonQuery = `EXPLAIN ${explainType} json=1 ${query}`;
            let jsonLines: string[];
            if (this.adapter.executeRawQuery) {
              jsonLines = await this.adapter.executeRawQuery(jsonQuery, database);
            } else {
              const jsonRows = await this.adapter.executeQuery(jsonQuery);
              jsonLines = jsonRows.map(r => {
                const row = r as Record<string, unknown>;
                return String(row.explain || row.EXPLAIN || Object.values(row)[0] || '');
              });
            }
            const jsonStr = jsonLines.join('');
            if (jsonStr.trim().startsWith('{') || jsonStr.trim().startsWith('[')) {
              parsedTree = JSON.parse(jsonStr);
            }
          } catch (err) {
            console.warn('[TraceService] EXPLAIN JSON parse failed:', err);
          }
        }

        return {
          explain_type: explainType,
          output,
          parsed_tree: parsedTree,
        };
      } catch (error) {
        const msg = error instanceof Error
          ? error.message
          : (error && typeof error === 'object')
            ? (error as Record<string, unknown>).message
              ? String((error as Record<string, unknown>).message)
              : JSON.stringify(error)
            : String(error);
        console.error('[TraceService] Failed to execute EXPLAIN:', msg);
        throw new TraceServiceError(`Failed to execute EXPLAIN: ${msg}`, error instanceof Error ? error : undefined);
      }
    }

  /**
   * Execute EXPLAIN with automatic fallback strategies for database resolution.
   *
   * In a cluster the load balancer may route to a node that doesn't have the
   * database, so this method tries multiple strategies:
   * 1. Run with the original database context
   * 2. Strip database qualifiers and rely on connection-level database
   * 3. Qualify bare table references with the database name
   */
  async executeExplainWithFallback(
    query: string,
    explainType: ExplainType,
    database?: string,
  ): Promise<ExplainResult> {
    // Strategy 0: try as-is with database context
    try {
      return await this.executeExplain(query, explainType, database);
    } catch (firstErr) {
      const msg = firstErr instanceof Error ? firstErr.message : String(firstErr);
      const isDatabaseMissing = msg.includes('UNKNOWN_DATABASE') || msg.includes('does not exist');
      if (!isDatabaseMissing || !database) throw firstErr;
    }

    // Strategy 1: strip the database qualifier from table names and let
    // the connection-level database setting resolve them.
    const stripped = TraceService.unqualifyTables(query, database);
    if (stripped !== query) {
      try {
        return await this.executeExplain(stripped, explainType, database);
      } catch (err) {
        console.warn('[TraceService] EXPLAIN with unqualified tables failed, trying qualified:', err);
      }
    }

    // Strategy 2: qualify unqualified FROM/JOIN table references with the database
    const qualified = TraceService.qualifyTables(query, database);
    return await this.executeExplain(qualified, explainType);
  }

  /**
   * Qualify unqualified table references (FROM/JOIN) with the given database name.
   * Skips already-qualified references, subqueries, table functions, and system.* tables.
   */
  private static qualifyTables(sql: string, database: string): string {
    return sql.replace(
      /\b(FROM|JOIN)\s+(?!system\.)(`[^`]+`|[a-zA-Z_]\w*)(?!\s*\()\b(?!\.)/gi,
      (_match, keyword: string, table: string) =>
        `${keyword} ${database}.${table}`,
    );
  }

  /**
   * Strip a specific database qualifier from table references so the query
   * can run with the database set at the connection level instead.
   * Only strips the given database name; leaves other qualifiers intact.
   */
  private static unqualifyTables(sql: string, database: string): string {
    const escaped = database.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    // nosemgrep: detect-non-literal-regexp — `database` is regex-escaped above
    const re = new RegExp(`\\b(FROM|JOIN)\\s+${escaped}\\.`, 'gi');
    return sql.replace(re, (_match, keyword: string) => `${keyword} `);
  }

  /**
   * Fetch OpenTelemetry spans for a query - Work in Progress
   */
  async getOpenTelemetrySpans(_queryId: string): Promise<OpenTelemetrySpan[]> {
    // TODO: Implement span fetching with proper level filtering
    return [];
  }

  /**
   * Fetch CPU or Memory profiling data from system.trace_log for flamegraph visualization
   * Uses ClickHouse's built-in flameGraph() function which outputs collapsed stack format
   */
  async getFlamegraphData(queryId: string, type: FlamegraphType = 'CPU', eventDate?: string): Promise<FlamegraphNode> {
    try {
      // Check if trace_log table exists
      const checkSql = tagQuery(`SELECT 1 FROM {{cluster_metadata:system.tables}} WHERE database = 'system' AND name = 'trace_log' GROUP BY name LIMIT 1`, sourceTag(TAB_QUERIES, 'traceLogCheck'));
      const checkResult = await this.adapter.executeQuery(checkSql);
      if (checkResult.length === 0) {
        console.log('[TraceService] trace_log table does not exist');
        return { name: 'root', value: 0, children: [] };
      }

      // Try the built-in flameGraph() function first, fall back to legacy manual stacks
      let rows: Record<string, unknown>[];
      let usedLegacy = false;
      const dateBound = eventDateBound(eventDate);
      const injectDate = (tpl: string) => tpl.replace('{event_date_bound}', dateBound);

      try {
        const queryTemplate = type === 'Memory'
          ? QUERY_FLAMEGRAPH_MEMORY
          : type === 'Real'
            ? QUERY_FLAMEGRAPH_REAL
            : QUERY_FLAMEGRAPH_CPU;
        const sql = buildQuery(injectDate(queryTemplate), { query_id: queryId });
        rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'flamegraph')));
      } catch (primaryError) {
        const primaryMsg = primaryError instanceof Error ? primaryError.message : String(primaryError);
        // Introspection disabled (Code 446) — don't bother with legacy fallback, it also needs introspection
        if (primaryMsg.includes('allow_introspection_functions') || primaryMsg.includes('Code: 446')) {
          throw primaryError;
        }
        // flameGraph() not available — fall back to legacy demangle/addressToSymbol approach
        if (primaryMsg.includes('flameGraph') || primaryMsg.includes('not implemented') || primaryMsg.includes('Unknown function')) {
          console.log('[TraceService] flameGraph() not available, using legacy stack building');
          const legacyTemplate = type === 'Memory'
            ? QUERY_FLAMEGRAPH_MEMORY_LEGACY
            : type === 'Real'
              ? QUERY_FLAMEGRAPH_REAL_LEGACY
              : QUERY_FLAMEGRAPH_DATA;
          const sql = buildQuery(injectDate(legacyTemplate), { query_id: queryId });
          rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'flamegraphLegacy')));
          usedLegacy = true;
        } else {
          throw primaryError;
        }
      }

      if (usedLegacy) {
        // Legacy format: { stack: "func1;func2;func3", value: 42 }
        const samples: FlamegraphSample[] = rows.map(r => ({
          stack: String((r as Record<string, unknown>).stack || ''),
          value: Number((r as Record<string, unknown>).value) || 1,
        }));
        return this.buildFlamegraphTree(samples);
      }
      
      // Parse flameGraph() output: each line is "stack count" (space-separated)
      // e.g. "func1;func2;func3 42"
      const samples: FlamegraphSample[] = rows.map(r => {
        const row = r as Record<string, unknown>;
        const line = String(row.line || '');
        // Split on last space to separate stack from count
        const lastSpaceIdx = line.lastIndexOf(' ');
        if (lastSpaceIdx === -1) {
          return { stack: line, value: 1 };
        }
        const stack = line.substring(0, lastSpaceIdx);
        const value = parseInt(line.substring(lastSpaceIdx + 1), 10) || 1;
        return { stack, value };
      });

      // Build hierarchical tree from stack samples
      return this.buildFlamegraphTree(samples);
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      // Check for introspection functions error (Code 446)
      if (msg.includes('allow_introspection_functions') || msg.includes('addressToSymbol') || msg.includes('Code: 446')) {
        console.log('[TraceService] Introspection functions not enabled');
        return {
          name: 'root',
          value: 0,
          children: [],
          unavailableReason: 'Introspection functions are not enabled on this server. Enable allow_introspection_functions in your ClickHouse config to use flamegraphs.',
        };
      }
      // Check for no data
      if (msg.includes('Empty query')) {
        return { name: 'root', value: 0, children: [] };
      }
      console.error('[TraceService] Failed to get flamegraph data:', msg);
      throw new TraceServiceError(`Failed to get flamegraph data: ${msg}`, error as Error);
    }
  }

  /**
   * Build hierarchical flamegraph tree from stack samples
   */
  private buildFlamegraphTree(samples: FlamegraphSample[]): FlamegraphNode {
    const root: FlamegraphNode = { name: 'root', value: 0, children: [] };
    
    for (const sample of samples) {
      if (!sample.stack) continue;
      
      // Stack is semicolon-separated, from bottom (root) to top (leaf)
      const frames = sample.stack.split(';').filter(f => f.trim());
      let currentNode = root;
      
      for (const frame of frames) {
        const simplifiedName = this.simplifyFunctionName(frame);
        let childNode = currentNode.children.find(c => c.name === simplifiedName);
        
        if (!childNode) {
          childNode = { name: simplifiedName, value: 0, children: [] };
          currentNode.children.push(childNode);
        }
        
        currentNode = childNode;
      }
      
      // Add sample value to the leaf node
      currentNode.value += sample.value;
    }
    
    // Propagate values up the tree (inclusive time)
    this.propagateValues(root);
    
    return root;
  }

  /**
   * Propagate values from leaves up to parents (inclusive time calculation)
   */
  private propagateValues(node: FlamegraphNode): number {
    const childrenValue = node.children.reduce((sum, child) => sum + this.propagateValues(child), 0);
    node.value += childrenValue;
    return node.value;
  }

  /**
   * Simplify C++ function names for better readability
   */
  private simplifyFunctionName(name: string): string {
    if (!name) return 'unknown';
    
    let simplified = name;
    
    // Remove template arguments <...>
    simplified = simplified.replace(/<[^<>]*>/g, '<>');
    
    // Remove lambda definitions
    simplified = simplified.replace(/::'lambda'.*/, '::lambda');
    
    // Simplify std::__1:: to std::
    simplified = simplified.replace(/std::__1::/g, 'std::');
    
    // Truncate if too long
    if (simplified.length > 80) {
      simplified = simplified.substring(0, 77) + '...';
    }
    
    return simplified;
  }


    /**
     * Fetch per-processor execution stats from system.processors_profile_log
     */
    async getProcessorProfiles(queryId: string, eventDate?: string): Promise<ProcessorProfile[]> {
      try {
        const sql = buildQuery(QUERY_PROCESSORS_PROFILE.replace('{event_date_bound}', eventDateBound(eventDate)), { query_id: queryId });
        const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'processorProfile')));
        return rows.map(r => {
          const row = r as Record<string, unknown>;
          return {
            name: String(row.name || ''),
            elapsed_us: Number(row.elapsed_us || 0),
            input_wait_us: Number(row.input_wait_us || 0),
            output_wait_us: Number(row.output_wait_us || 0),
            input_rows: Number(row.input_rows || 0),
            input_bytes: Number(row.input_bytes || 0),
            output_rows: Number(row.output_rows || 0),
            output_bytes: Number(row.output_bytes || 0),
            instances: Number(row.instances || 1),
          };
        });
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        console.error('[TraceService] Failed to get processor profiles:', msg);
        throw new TraceServiceError(`Failed to get processor profiles: ${msg}`, error as Error);
      }
    }

}
