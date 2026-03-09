/**
 * Ordering Key Diagnostics — analyzes why a query has poor ordering key pruning.
 *
 * Compares the query's WHERE clause columns against the table's ORDER BY
 * columns to produce actionable diagnostics.
 *
 * This runs client-side on the sample_query text — no extra SQL needed.
 *
 * ## How ClickHouse ordering key pruning actually works
 *
 * ClickHouse uses a sparse primary index (one entry per granule of ~8192 rows).
 * At query time, two algorithms are used to select granules:
 *
 * 1. **Binary search** — used for the leftmost key column. O(log n), very efficient.
 * 2. **Generic exclusion search** — used for non-leftmost key columns. Iterates
 *    over marks trying to *exclude* granules where the condition is provably
 *    impossible. Effectiveness depends on the cardinality of predecessor columns:
 *    - Low-cardinality predecessor → same value spans many granules → non-leftmost
 *      column values are locally sorted → effective pruning.
 *    - High-cardinality predecessor → each mark has different predecessor value →
 *      almost no granules can be excluded.
 *
 * This means:
 * - Filtering on the leftmost key column is always efficient (binary search).
 * - Filtering on non-leftmost columns CAN help, but it's data-dependent.
 * - The actual pruning ratio from query_log (SelectedMarks/SelectedMarksTotal)
 *   is the real data. Our analysis is a heuristic explanation.
 *
 * ## WHERE column extraction
 *
 * We use regex-based extraction to find column references in WHERE clauses.
 * This is a heuristic fallback — when EXPLAIN indexes data is available,
 * diagnoseOrderingKeyUsage() uses the actual key columns reported by the
 * ClickHouse optimizer instead.
 *
 * Reference: https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes
 * Verified with: EXPLAIN indexes = 1 (shows Keys used + Granules selected/total)
 *
 * ## What ClickHouse system tables tell us
 *
 * - `system.query_log.ProfileEvents['SelectedMarks']` — granules actually read
 * - `system.query_log.ProfileEvents['SelectedMarksTotal']` — total granules available
 * - `system.query_log.ProfileEvents['SelectedParts']` / `SelectedPartsTotal` — part-level pruning
 * - `system.query_log.ProfileEvents['FilteringMarksWithPrimaryKeyMicroseconds']` — time spent on index filtering
 * - `system.query_log.columns` — all columns referenced in the query (not just WHERE)
 *
 * There is NO column in query_log that tells us which key columns were used for
 * index filtering. That information is only available via:
 * - `EXPLAIN indexes = 1` (shows Keys: [...] and Granules: X/Y)
 * - Server trace log (shows "binary search" vs "generic exclusion search")
 *
 * Since we can't run EXPLAIN for historical queries, we combine:
 * 1. Regex-based WHERE column extraction (what the query filters on)
 * 2. Actual pruning ratio from ProfileEvents (what actually happened)
 * 3. Knowledge of ClickHouse's index algorithms (to explain why)
 */


export type OrderingKeyDiagnosticSeverity = 'good' | 'warning' | 'poor';

export interface OrderingKeyDiagnostic {
  severity: OrderingKeyDiagnosticSeverity;
  /** Short label for badges */
  label: string;
  /** Detailed explanation */
  reason: string;
  /** ORDER BY columns parsed from the sorting key */
  orderByColumns: string[];
  /** WHERE clause columns extracted from the query */
  whereColumns: string[];
  /** Which ORDER BY columns are matched by WHERE */
  matchedColumns: string[];
  /** Whether the leftmost ORDER BY column is used in WHERE */
  usesLeftmostKey: boolean;
  /** How many key columns form a contiguous prefix from the left */
  prefixLength: number;
  /**
   * Which index algorithm ClickHouse would use:
   * - 'binary_search' — leftmost key column is in WHERE (efficient)
   * - 'generic_exclusion' — only non-leftmost key columns in WHERE (data-dependent)
   * - 'none' — no key columns in WHERE
   */
  indexAlgorithm: 'binary_search' | 'generic_exclusion' | 'none';
}


/**
 * Parse a ClickHouse sorting key string into individual column names.
 * Handles expressions like "toDate(timestamp)" by extracting the inner column.
 *
 * Examples:
 *   "tenant_id, timestamp" → ["tenant_id", "timestamp"]
 *   "toDate(event_time), user_id" → ["event_time", "user_id"]
 *   "cityHash64(url)" → ["url"]
 */
export function parseSortingKey(sortingKey: string): string[] {
  if (!sortingKey || !sortingKey.trim()) return [];

  return sortingKey.split(',').map(part => {
    const trimmed = part.trim();
    // Extract column from function calls like toDate(col), cityHash64(col)
    const funcMatch = trimmed.match(/^\w+\(([^)]+)\)$/);
    if (funcMatch) return funcMatch[1].trim();
    return trimmed;
  }).filter(Boolean);
}


// ─── Regex-based WHERE column extraction (fallback) ─────────────────────────

/**
 * Extract column names referenced in WHERE/PREWHERE/HAVING clauses of a SQL query.
 *
 * This is regex-based and intentionally conservative — it catches the common
 * patterns (col = val, col IN (...), col > val, col BETWEEN, etc.) without
 * trying to be a full SQL parser. When EXPLAIN indexes data is available,
 * diagnoseOrderingKeyUsage() uses the actual optimizer keys instead.
 */
function extractWhereColumnsRegex(query: string): string[] {
  if (!query) return [];

  // Normalize: collapse whitespace, remove string literals to avoid false matches
  const normalized = query
    .replace(/--[^\n]*/g, '')           // remove line comments
    .replace(/\/\*[\s\S]*?\*\//g, '')   // remove block comments
    .replace(/'[^']*'/g, "'_STR_'")     // replace string literals
    .replace(/\s+/g, ' ');

  // Find WHERE/PREWHERE/HAVING clause content
  const clauseRegex = /\b(?:WHERE|PREWHERE|HAVING)\b\s+([\s\S]*?)(?:\b(?:GROUP\s+BY|ORDER\s+BY|LIMIT|UNION|SETTINGS|FORMAT)\b|$)/gi;

  const columns = new Set<string>();
  let match: RegExpExecArray | null;

  while ((match = clauseRegex.exec(normalized)) !== null) {
    const clause = match[1];

    const colPatterns = [
      /\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|!=|<>|>=?|<=?)\s*/g,
      /\b([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:NOT\s+)?IN\s*\(/gi,
      /\b([a-zA-Z_][a-zA-Z0-9_]*)\s+BETWEEN\b/gi,
      /\b([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:NOT\s+)?I?LIKE\b/gi,
      /\b([a-zA-Z_][a-zA-Z0-9_]*)\s+IS\s+(?:NOT\s+)?NULL\b/gi,
      /\b\w+\(([a-zA-Z_][a-zA-Z0-9_]*)\)/g,
    ];

    for (const pattern of colPatterns) {
      let colMatch: RegExpExecArray | null;
      pattern.lastIndex = 0;
      while ((colMatch = pattern.exec(clause)) !== null) {
        const col = colMatch[1].toLowerCase();
        if (!SQL_KEYWORDS.has(col)) {
          columns.add(col);
        }
      }
    }
  }

  return [...columns];
}

/**
 * Extract column names referenced in WHERE/PREWHERE/HAVING clauses.
 *
 * Uses regex-based extraction. When EXPLAIN indexes data is available,
 * diagnoseOrderingKeyUsage() prefers that over this heuristic.
 */
export function extractWhereColumns(query: string): string[] {
  return extractWhereColumnsRegex(query);
}

const SQL_KEYWORDS = new Set([
  'select', 'from', 'where', 'and', 'or', 'not', 'in', 'is', 'null',
  'between', 'like', 'ilike', 'true', 'false', 'as', 'on', 'join',
  'left', 'right', 'inner', 'outer', 'cross', 'having', 'group',
  'order', 'by', 'limit', 'offset', 'union', 'all', 'any', 'exists',
  'case', 'when', 'then', 'else', 'end', 'distinct', 'prewhere',
  'global', 'settings', 'format', 'with', 'array', 'tuple',
]);


/**
 * Diagnose why a query pattern has poor (or good) ordering key pruning.
 *
 * When `explainKeys` is provided (from EXPLAIN indexes = 1), uses the actual
 * key columns reported by the ClickHouse optimizer. Otherwise falls back to
 * heuristic WHERE-column extraction from the query text.
 *
 * @param sortingKey - The table's sorting key (e.g. "tenant_id, timestamp")
 * @param sampleQuery - A sample query text for this pattern
 * @param avgPruningPct - The average pruning percentage (0-100, null if no data)
 * @param explainKeys - Key columns from EXPLAIN indexes = 1 PrimaryKey entry (optional)
 */
export function diagnoseOrderingKeyUsage(
  sortingKey: string | null,
  sampleQuery: string,
  avgPruningPct: number | null,
  explainKeys?: string[],
): OrderingKeyDiagnostic {
  const orderByColumns = parseSortingKey(sortingKey ?? '');
  const whereColumns = extractWhereColumns(sampleQuery);

  // Normalize ORDER BY for comparison (lowercase)
  const orderByLower = orderByColumns.map(c => c.toLowerCase());

  // When EXPLAIN keys are available, use them as the matched columns.
  // EXPLAIN tells us exactly which key columns the optimizer used.
  // Otherwise, fall back to heuristic WHERE-column matching.
  let matchedColumns: string[];
  if (explainKeys && explainKeys.length > 0) {
    const explainLower = new Set(explainKeys.map(c => c.toLowerCase()));
    matchedColumns = orderByLower.filter(c => explainLower.has(c));
  } else {
    const whereLower = new Set(whereColumns.map(c => c.toLowerCase()));
    matchedColumns = orderByLower.filter(c => whereLower.has(c));
  }

  const usesLeftmostKey = orderByLower.length > 0 && matchedColumns.length > 0 && matchedColumns.includes(orderByLower[0]);

  // Count contiguous prefix from the left
  const matchedSet = new Set(matchedColumns);
  let prefixLength = 0;
  for (const col of orderByLower) {
    if (matchedSet.has(col)) {
      prefixLength++;
    } else {
      break;
    }
  }

  // Determine which index algorithm ClickHouse would use
  const indexAlgorithm: OrderingKeyDiagnostic['indexAlgorithm'] =
    matchedColumns.length === 0 ? 'none' :
    usesLeftmostKey ? 'binary_search' :
    'generic_exclusion';

  const base = { orderByColumns, whereColumns, matchedColumns, usesLeftmostKey, prefixLength, indexAlgorithm };

  // No ORDER BY defined
  if (orderByColumns.length === 0) {
    return {
      ...base,
      severity: 'warning',
      label: 'No ORDER BY',
      reason: 'Table has no sorting key defined. All scans are full table scans.',
    };
  }

  // No WHERE clause at all
  if (whereColumns.length === 0) {
    return {
      ...base,
      severity: avgPruningPct != null && avgPruningPct < 50 ? 'poor' : 'warning',
      label: 'No WHERE clause',
      reason: 'Query has no WHERE clause — full table scan. This is expected for aggregations over all data.',
    };
  }

  // WHERE exists but matches no ORDER BY columns
  if (matchedColumns.length === 0) {
    return {
      ...base,
      severity: 'poor',
      label: 'No key match',
      reason: `WHERE filters on ${whereColumns.join(', ')} but ORDER BY is (${orderByColumns.join(', ')}). None of the filter columns match the sorting key — ClickHouse cannot use the primary index for pruning.`,
    };
  }

  // WHERE matches some ORDER BY columns but NOT the leftmost
  if (!usesLeftmostKey) {
    // ClickHouse uses "generic exclusion search" here — it CAN still prune,
    // but effectiveness depends on predecessor column cardinality.
    // Use actual pruning data to determine severity.
    const actuallyPrunes = avgPruningPct != null && avgPruningPct >= 50;
    const severity: OrderingKeyDiagnosticSeverity = actuallyPrunes ? 'warning' : 'poor';

    const baseReason = `WHERE filters on ${matchedColumns.join(', ')} but skips the leftmost key column "${orderByColumns[0]}". ClickHouse uses generic exclusion search (not binary search) for non-leftmost columns — effectiveness depends on the cardinality of preceding key columns.`;
    const actualNote = actuallyPrunes
      ? ` However, actual pruning is ${avgPruningPct!.toFixed(0)}% — the predecessor column likely has low cardinality, allowing some granule exclusion.`
      : '';

    return {
      ...base,
      severity,
      label: 'Skips leftmost key',
      reason: baseReason + actualNote,
    };
  }

  // Uses leftmost key but not all key columns
  if (matchedColumns.length < orderByColumns.length) {
    const hasGap = prefixLength < matchedColumns.length;
    const effectiveLabel = hasGap
      ? `Partial key (${prefixLength}/${orderByColumns.length})`
      : `Partial key (${matchedColumns.length}/${orderByColumns.length})`;

    let reason = `WHERE uses ${matchedColumns.join(', ')} from ORDER BY (${orderByColumns.join(', ')}). Binary search on the leftmost column provides efficient pruning.`;

    if (hasGap) {
      const gapCol = orderByColumns[prefixLength];
      const extraCols = matchedColumns.slice(prefixLength).join(', ');
      reason += ` Note: WHERE also filters on ${extraCols} but skips "${gapCol}" — ClickHouse uses binary search for the prefix (${orderByColumns.slice(0, prefixLength).join(', ')}) and generic exclusion search for ${extraCols}. The extra columns may or may not improve pruning depending on "${gapCol}" cardinality.`;
    }

    return {
      ...base,
      severity: avgPruningPct != null && avgPruningPct >= 90 ? 'good' : 'warning',
      label: effectiveLabel,
      reason,
    };
  }

  // Full key match — all ORDER BY columns are in WHERE
  return {
    ...base,
    severity: 'good',
    label: 'Full key match',
    reason: `WHERE filters on all ORDER BY columns (${orderByColumns.join(', ')}). Binary search on the full key prefix provides maximum pruning.`,
  };
}
