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

/** One table TTL rule as `system.tables.engine_full` stores it. `expression` is the server's
 * normalized text (`d + toIntervalDay(7)`). A rule with no action keyword deletes rows. */
export interface TTLRule {
  expression: string;
  action: 'delete' | 'move' | 'recompress' | 'group-by';
  /** Target of a move: `TO DISK 'name'` or `TO VOLUME 'name'`. */
  destination?: { type: 'disk' | 'volume'; name: string; ifExists: boolean };
  /** Row filter of a delete (`DELETE WHERE`, stored as `WHERE`). */
  where?: string;
  /** Codec of a recompress, e.g. `CODEC(ZSTD(9))`. */
  codec?: string;
  /** Key and assignments of a group-by rollup. */
  groupBy?: string;
  set?: string;
}

/** `text` with quoted text and everything inside parentheses replaced by spaces, at the same
 * length: keywords and commas found in it are at the top level of the original. */
function topLevel(text: string): string {
  let quote: string | null = null, depth = 0, out = '';
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (quote) {
      if (char === '\\') { out += '  '; i++; continue; }
      if (char === quote) {
        if (text[i + 1] === quote) { out += '  '; i++; continue; }
        quote = null;
      }
      out += ' '; continue;
    }
    if (char === "'" || char === '"' || char === '`') { quote = char; out += ' '; continue; }
    if (char === '(') depth++;
    out += depth > 0 ? ' ' : char;
    if (char === ')') depth = Math.max(0, depth - 1);
  }
  return out;
}

const IDENTIFIER = /^\s*(`(?:[^`\\]|\\.)*`|[A-Za-z_][\w.]*)\s*/;
const unquote = (text: string) => {
  const trimmed = text.trim();
  return /^'.*'$/s.test(trimmed) ? trimmed.slice(1, -1).replace(/\\(.)|''/g, (m, c) => c ?? "'") : trimmed;
};
const SUFFIX = /\s(TO\s+(?:DISK|VOLUME)(?:\s+IF\s+EXISTS)?|RECOMPRESS|DELETE|WHERE|GROUP\s+BY|SET)\b/g;
/** Start of the last top-level `SETTINGS` keyword at or after `from`: the clause ends the definition. */
function settingsStart(mask: string, from: number): number {
  let found = -1;
  for (const match of mask.slice(from).matchAll(/\bSETTINGS\b/g)) found = from + match.index!;
  return found;
}

/**
 * Every table-level TTL rule of an engine definition (`system.tables.engine_full`), in order.
 * Column TTLs are in the column list, which `engine_full` does not hold. Returns [] when the
 * definition has no TTL clause. Keywords match in upper case, as the server prints them, so a
 * column named `ttl` or `settings` is not read as one; a column named `TTL` outside parentheses is.
 */
export function parseTTLRules(definition: string): TTLRule[] {
  const mask = topLevel(definition);
  const start = mask.search(/\bTTL\b/);
  if (start < 0) return [];
  const from = start + 3, settings = settingsStart(mask, from);
  const to = settings < 0 ? definition.length : settings;
  // Elements are separated by top-level commas. GROUP BY keys and SET assignments are comma
  // lists too, so a piece that continues them joins the element before it.
  const pieces: [number, number][] = [];
  let begin = from;
  for (let i = from; i <= to; i++) if (i === to || mask[i] === ',') { pieces.push([begin, i]); begin = i + 1; }
  const elements: [number, number][] = [];
  for (const [a, b] of pieces) {
    const last = elements[elements.length - 1], previous = last ? mask.slice(last[0], last[1]) : '';
    const text = definition.slice(a, b), id = IDENTIFIER.exec(text);
    const continues = /\sGROUP\s+BY\s/.test(previous) && id !== null && (/\sSET\s/.test(previous)
      ? /^=(?!=)/.test(text.slice(id[0].length))
      : text.slice(id[0].length) === '' || /^SET\b/.test(text.slice(id[0].length)));
    if (continues) last[1] = b; else if (text.trim()) elements.push([a, b]);
  }
  return elements.map(([a, b]) => {
    const text = definition.slice(a, b), mark = mask.slice(a, b);
    const found = [...mark.matchAll(SUFFIX)].map(m => ({ keyword: m[1].replace(/\s+/g, ' ').toUpperCase(), at: m.index!, end: m.index! + m[0].length }));
    const part = (keyword: string) => {
      const index = found.findIndex(f => f.keyword.startsWith(keyword));
      if (index < 0) return undefined;
      return text.slice(found[index].end, found[index + 1]?.at ?? text.length).trim();
    };
    const rule: TTLRule = { expression: text.slice(0, found[0]?.at ?? text.length).trim(), action: 'delete' };
    const move = found.find(f => f.keyword.startsWith('TO '));
    if (move) {
      rule.action = 'move';
      rule.destination = { type: move.keyword.startsWith('TO DISK') ? 'disk' : 'volume', name: unquote(part('TO ')!), ifExists: move.keyword.endsWith('IF EXISTS') };
    }
    const codec = part('RECOMPRESS');
    if (codec !== undefined) { rule.action = 'recompress'; rule.codec = codec; }
    const groupBy = part('GROUP BY');
    if (groupBy !== undefined) {
      rule.action = 'group-by'; rule.groupBy = groupBy;
      const set = part('SET');
      if (set !== undefined) rule.set = set;
    }
    const where = part('WHERE');
    if (where !== undefined) rule.where = where;
    return rule;
  });
}

const SHORT_UNITS: Record<string, string> = { second: 's', minute: 'min', hour: 'h', day: 'd', week: 'w', month: 'mo', quarter: 'q', year: 'y' };
/** Delay of a `<column> + toInterval<Unit>(n)` TTL expression, as the server normalizes it: the
 * column, and the interval long ("5 minutes") and short ("5 min"). null for other expressions. */
export function ttlDelay(expression: string): { column: string; long: string; short: string } | null {
  const match = /^(.+?)\s*\+\s*toInterval([A-Za-z]+)\((\d+)\)$/.exec(expression.trim());
  if (!match) return null;
  const unit = match[2].toLowerCase(), value = Number(match[3]);
  return { column: match[1], long: formatTTLDuration(value, unit), short: `${value} ${SHORT_UNITS[unit] ?? unit}` };
}

/** Settings of an engine definition's SETTINGS clause whose name mentions TTL
 * (merge_with_ttl_timeout, ttl_only_drop_parts), as [name, value] in definition order. */
export function ttlSettings(definition: string): [string, string][] {
  const mask = topLevel(definition), start = settingsStart(mask, 0);
  if (start < 0) return [];
  const settings: [string, string][] = [];
  let begin = start + 'SETTINGS'.length;
  for (let i = begin; i <= definition.length; i++) {
    if (i < definition.length && mask[i] !== ',') continue;
    const [name, ...value] = definition.slice(begin, i).split('=');
    if (/ttl/i.test(name) && value.length) settings.push([name.trim(), value.join('=').trim()]);
    begin = i + 1;
  }
  return settings;
}
