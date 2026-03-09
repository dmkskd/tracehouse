/**
 * query-literals — extract and diff literal values from queries sharing
 * the same normalized_query_hash.
 *
 * Since ClickHouse's normalizeQuery() replaces literals with '?', queries
 * with the same normalized hash have identical structure but different
 * literal values (strings, numbers, dates, IN-lists, etc.).
 *
 * We tokenize the SQL and extract just the literal tokens, then diff them
 * across executions to surface what changed.
 */

/** A single literal extracted from a query */
export interface QueryLiteral {
  /** The literal value as it appears in the SQL */
  value: string;
  /** Position index among all literals in the query (0-based) */
  index: number;
  /** Contextual label — the column/keyword preceding this literal, e.g. "country_code", "LIMIT", "INTERVAL" */
  context: string;
}

/**
 * Tokenize SQL and extract literal values in order, with surrounding context.
 *
 * Handles:
 * - Single-quoted strings: 'hello', '2026-01-01', 'it''s'
 * - Numeric literals: 42, 3.14, 1e6, -7, 0xFF
 * - Backtick-quoted identifiers are NOT literals (skipped)
 * - Double-quoted identifiers are NOT literals (skipped)
 * - Comments (-- and /* ... *​/) are skipped
 *
 * Context extraction looks backward from each literal to find the nearest
 * identifier or keyword, giving labels like "country_code =", "LIMIT",
 * "INTERVAL", "date >=", etc.
 */
export function extractLiterals(sql: string): QueryLiteral[] {
  const literals: QueryLiteral[] = [];
  let i = 0;
  const len = sql.length;

  while (i < len) {
    const ch = sql[i];

    // Skip single-line comments
    if (ch === '-' && i + 1 < len && sql[i + 1] === '-') {
      while (i < len && sql[i] !== '\n') i++;
      continue;
    }

    // Skip block comments
    if (ch === '/' && i + 1 < len && sql[i + 1] === '*') {
      i += 2;
      while (i + 1 < len && !(sql[i] === '*' && sql[i + 1] === '/')) i++;
      i += 2;
      continue;
    }

    // Single-quoted string literal
    if (ch === "'") {
      const start = i;
      i++; // skip opening quote
      while (i < len) {
        if (sql[i] === "'" && i + 1 < len && sql[i + 1] === "'") {
          i += 2; // escaped quote
        } else if (sql[i] === "'") {
          break;
        } else {
          i++;
        }
      }
      i++; // skip closing quote
      const ctx = extractContext(sql, start);
      literals.push({ value: sql.slice(start, i), index: literals.length, context: ctx });
      continue;
    }

    // Skip double-quoted identifiers
    if (ch === '"') {
      i++;
      while (i < len && sql[i] !== '"') i++;
      i++;
      continue;
    }

    // Skip backtick-quoted identifiers
    if (ch === '`') {
      i++;
      while (i < len && sql[i] !== '`') i++;
      i++;
      continue;
    }

    // Numeric literal — must be preceded by a non-alphanumeric char (or start of string)
    // to avoid matching parts of identifiers like "col1"
    if (
      (ch >= '0' && ch <= '9') &&
      (i === 0 || !isAlphaUnderscore(sql[i - 1]))
    ) {
      const start = i;
      // Hex: 0x...
      if (ch === '0' && i + 1 < len && (sql[i + 1] === 'x' || sql[i + 1] === 'X')) {
        i += 2;
        while (i < len && isHexDigit(sql[i])) i++;
      } else {
        // Decimal / float / scientific
        while (i < len && sql[i] >= '0' && sql[i] <= '9') i++;
        if (i < len && sql[i] === '.') {
          i++;
          while (i < len && sql[i] >= '0' && sql[i] <= '9') i++;
        }
        if (i < len && (sql[i] === 'e' || sql[i] === 'E')) {
          i++;
          if (i < len && (sql[i] === '+' || sql[i] === '-')) i++;
          while (i < len && sql[i] >= '0' && sql[i] <= '9') i++;
        }
      }
      // Only count as literal if not followed by alpha/underscore (would be identifier)
      if (i >= len || !isAlphaUnderscore(sql[i])) {
        const ctx = extractContext(sql, start);
        literals.push({ value: sql.slice(start, i), index: literals.length, context: ctx });
      }
      continue;
    }

    i++;
  }

  return literals;
}

/**
 * Look backward from a literal's start position to find the nearest
 * meaningful context — typically a column name, operator, or SQL keyword.
 *
 * Examples of what this returns:
 *   "WHERE country_code = 'US'"  → "country_code"
 *   "AND date >= '2026-01-01'"   → "date"
 *   "LIMIT 100"                  → "LIMIT"
 *   "INTERVAL 2 DAY"             → "INTERVAL"
 *   "IN (1, 2, 3)"              → "IN"  (for the first), "" for subsequent
 *   "user_id = 42"               → "user_id"
 *   "today() - 7"                → "today() -"
 */
function extractContext(sql: string, literalStart: number): string {
  // Walk backward, skipping whitespace and operators to find the identifier/keyword
  let j = literalStart - 1;

  // Skip whitespace
  while (j >= 0 && isWhitespace(sql[j])) j--;
  if (j < 0) return '';

  // Check for operator (=, >=, <=, !=, <>, <, >, -)
  // We want to skip past the operator to get the identifier before it
  const opChars = new Set(['=', '>', '<', '!', '-']);
  let operator = '';
  if (opChars.has(sql[j])) {
    const opEnd = j;
    while (j >= 0 && opChars.has(sql[j])) j--;
    operator = sql.slice(j + 1, opEnd + 1).trim();
    // Skip whitespace before operator
    while (j >= 0 && isWhitespace(sql[j])) j--;
  }

  // Check for comma (inside IN list or function args) — return empty context for list items
  if (sql[j] === ',' || sql[j] === '(') {
    // Walk further back to see if this is an IN(...) or function call
    let k = j;
    if (sql[k] === ',') {
      // Find the opening paren
      let depth = 0;
      while (k >= 0) {
        if (sql[k] === ')') depth++;
        else if (sql[k] === '(') {
          if (depth === 0) break;
          depth--;
        }
        k--;
      }
    }
    // k is now at '(' — look for keyword before it
    if (k >= 0 && sql[k] === '(') {
      let m = k - 1;
      while (m >= 0 && isWhitespace(sql[m])) m--;
      if (m >= 0) {
        const wordEnd = m + 1;
        while (m >= 0 && isIdentChar(sql[m])) m--;
        const word = sql.slice(m + 1, wordEnd);
        if (word.toUpperCase() === 'IN') return 'IN';
      }
    }
    return '';
  }

  if (j < 0) return operator || '';

  // Now we should be at the end of an identifier or keyword or ')' for function calls
  // Handle function call: today() - 7 → "today() -"
  if (sql[j] === ')') {
    // Find matching '('
    let depth = 1;
    let k = j - 1;
    while (k >= 0 && depth > 0) {
      if (sql[k] === ')') depth++;
      else if (sql[k] === '(') depth--;
      k--;
    }
    // k is now before '(' — get the function name
    while (k >= 0 && isWhitespace(sql[k])) k--;
    if (k >= 0 && isIdentChar(sql[k])) {
      const funcEnd = k + 1;
      while (k >= 0 && isIdentChar(sql[k])) k--;
      const funcName = sql.slice(k + 1, funcEnd);
      return operator ? `${funcName}() ${operator}` : `${funcName}()`;
    }
    return operator || '';
  }

  // Read the identifier/keyword
  if (isIdentChar(sql[j])) {
    const wordEnd = j + 1;
    while (j >= 0 && isIdentChar(sql[j])) j--;
    const word = sql.slice(j + 1, wordEnd);

    // If the word is a SQL keyword that precedes a value directly, use it as-is
    const upper = word.toUpperCase();
    const directKeywords = new Set(['LIMIT', 'OFFSET', 'INTERVAL', 'TOP', 'IN', 'BETWEEN', 'VALUES']);
    if (directKeywords.has(upper)) {
      return upper;
    }

    // Otherwise it's likely a column name — return it (without operator for cleaner display)
    return word;
  }

  return operator || '';
}

function isWhitespace(ch: string): boolean {
  return ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r';
}

function isAlphaUnderscore(ch: string): boolean {
  return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch === '_';
}

function isIdentChar(ch: string): boolean {
  return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch === '_';
}

function isHexDigit(ch: string): boolean {
  return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F');
}

/** A parameter that changed between a reference query and another execution */
export interface LiteralDiff {
  /** 0-based position among literals */
  index: number;
  /** Value in the reference query */
  reference: string;
  /** Value in this execution */
  current: string;
  /** Contextual label from the reference query (column name, keyword, etc.) */
  context: string;
}

/**
 * Compare literals between a reference query and another execution.
 * Returns only the positions where values differ, with context labels.
 */
export function diffLiterals(referenceSql: string, currentSql: string): LiteralDiff[] {
  const refLiterals = extractLiterals(referenceSql);
  const curLiterals = extractLiterals(currentSql);
  const diffs: LiteralDiff[] = [];

  const len = Math.min(refLiterals.length, curLiterals.length);
  for (let i = 0; i < len; i++) {
    if (refLiterals[i].value !== curLiterals[i].value) {
      diffs.push({
        index: i,
        reference: refLiterals[i].value,
        current: curLiterals[i].value,
        context: refLiterals[i].context,
      });
    }
  }

  // If one query has more literals (e.g. longer IN list), flag extras
  for (let i = len; i < curLiterals.length; i++) {
    diffs.push({
      index: i,
      reference: '',
      current: curLiterals[i].value,
      context: curLiterals[i].context,
    });
  }

  return diffs;
}

/**
 * Strip quotes from a literal value and optionally truncate for display.
 */
export function formatLiteral(raw: string, maxLen = 30): string {
  let val = raw;
  // Strip surrounding single quotes and unescape
  if (val.startsWith("'") && val.endsWith("'")) {
    val = val.slice(1, -1).replace(/''/g, "'");
  }
  if (maxLen > 0 && val.length > maxLen) {
    return val.slice(0, maxLen) + '…';
  }
  return val;
}

/**
 * Extract all literal values from a SQL query as simple strings.
 * Convenience wrapper around extractLiterals for callers that just need values.
 */
export function extractQueryParameters(sql: string): string[] {
  return extractLiterals(sql).map(l => l.value);
}

