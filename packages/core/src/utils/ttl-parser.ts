/**
 * TTL parser for ClickHouse CREATE TABLE DDL.
 *
 * Extracts the table-level TTL expression from a CREATE TABLE statement
 * and converts it to a human-readable duration string.
 *
 * ClickHouse keeps `create_table_query` in `system.tables` in sync with
 * ALTER TABLE ... MODIFY TTL, so this always reflects the current TTL.
 *
 * Supported forms:
 *   TTL event_date + toIntervalDay(30)
 *   TTL event_date + INTERVAL 30 DAY
 *   TTL event_date + toIntervalMonth(3)
 *   TTL event_date + toIntervalHour(24)
 */

/**
 * Extract a human-readable TTL duration from a CREATE TABLE DDL.
 * Returns null if no TTL clause is found.
 */
export function parseTTL(ddl: string): string | null {
  if (!ddl) return null;

  const ttlStart = findKeywordOutsideQuoted(ddl, 'TTL');
  if (ttlStart === -1) return null;

  const exprStart = ttlStart + 'TTL'.length;
  const exprEnd = findTTLClauseEnd(ddl, exprStart);
  const expr = ddl.slice(exprStart, exprEnd).trim();
  if (!expr) return null;

  // Try toIntervalDay(N), toIntervalMonth(N), toIntervalHour(N), etc.
  const funcMatch = expr.match(/toInterval(\w+)\((\d+)\)/i);
  if (funcMatch) {
    const unit = funcMatch[1].toLowerCase();
    const value = parseInt(funcMatch[2], 10);
    return formatTTLDuration(value, unit);
  }

  // Try INTERVAL N UNIT syntax
  const intervalMatch = expr.match(/INTERVAL\s+(\d+)\s+(\w+)/i);
  if (intervalMatch) {
    const value = parseInt(intervalMatch[1], 10);
    const unit = intervalMatch[2].toLowerCase();
    return formatTTLDuration(value, unit);
  }

  // Fallback: return the raw expression (trimmed)
  return expr;
}

function findTTLClauseEnd(ddl: string, start: number): number {
  const stopKeywords = ['DELETE', 'SETTINGS'];
  let end = ddl.length;

  for (const keyword of stopKeywords) {
    const index = findKeywordOutsideQuoted(ddl, keyword, start);
    if (index !== -1 && index < end) end = index;
  }

  for (const keyword of ['TO DISK', 'TO VOLUME']) {
    const index = findPhraseOutsideQuoted(ddl, keyword, start);
    if (index !== -1 && index < end) end = index;
  }

  return end;
}

function findPhraseOutsideQuoted(input: string, phrase: string, start = 0): number {
  const [firstWord, ...rest] = phrase.split(/\s+/);
  let index = findKeywordOutsideQuoted(input, firstWord, start);

  while (index !== -1) {
    const afterFirstWord = index + firstWord.length;
    const remainingPattern = new RegExp(`^\\s+${rest.map(escapeRegExp).join('\\s+')}(?![A-Za-z0-9_])`, 'i');
    if (remainingPattern.test(input.slice(afterFirstWord))) return index;
    index = findKeywordOutsideQuoted(input, firstWord, afterFirstWord);
  }

  return -1;
}

function findKeywordOutsideQuoted(input: string, keyword: string, start = 0): number {
  const lowerInput = input.toLowerCase();
  const lowerKeyword = keyword.toLowerCase();
  let quote: "'" | '"' | '`' | null = null;

  for (let i = start; i <= input.length - keyword.length; i++) {
    const char = input[i];

    if (quote) {
      if (char === '\\') {
        i++;
        continue;
      }
      if (char === quote) {
        if (input[i + 1] === quote) {
          i++;
        } else {
          quote = null;
        }
      }
      continue;
    }

    if (char === "'" || char === '"' || char === '`') {
      quote = char;
      continue;
    }

    if (lowerInput.slice(i, i + keyword.length) !== lowerKeyword) continue;

    const before = input[i - 1] ?? '';
    const after = input[i + keyword.length] ?? '';
    if (!isIdentifierChar(before) && !isIdentifierChar(after)) return i;
  }

  return -1;
}

function isIdentifierChar(char: string): boolean {
  return /[A-Za-z0-9_]/.test(char);
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export function formatTTLDuration(value: number, unit: string): string {
  // Normalize unit to singular
  const u = unit.replace(/s$/, '');
  switch (u) {
    case 'day': return value === 1 ? '1 day' : `${value} days`;
    case 'month': return value === 1 ? '1 month' : `${value} months`;
    case 'hour': return value === 1 ? '1 hour' : `${value} hours`;
    case 'week': return value === 1 ? '1 week' : `${value} weeks`;
    case 'year': return value === 1 ? '1 year' : `${value} years`;
    case 'minute': return value === 1 ? '1 minute' : `${value} minutes`;
    case 'second': return value === 1 ? '1 second' : `${value} seconds`;
    default: return `${value} ${unit}`;
  }
}
