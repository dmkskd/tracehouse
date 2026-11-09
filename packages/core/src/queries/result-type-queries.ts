/** Native result metadata for Grafana, whose time fields conflate Date and
 * DateTime. Strip only top-level output syntax; leave expressions untouched.
 * This scanner recognizes comments/quotes so a literal FORMAT or semicolon
 * cannot truncate the query. ClickHouse remains the final SQL parser. */
export function describeResultTypesQuery(sql: string): string {
  const tokens: { text: string; start: number; end: number; depth: number }[] = [];
  let depth = 0;
  for (let i = 0; i < sql.length;) {
    if (/\s/.test(sql[i])) { i++; continue; }
    if (sql.startsWith('--', i) || sql[i] === '#') {
      const end = sql.indexOf('\n', i); i = end < 0 ? sql.length : end + 1; continue;
    }
    if (sql.startsWith('/*', i)) {
      let nesting = 1; i += 2;
      while (i < sql.length && nesting) {
        if (sql.startsWith('/*', i)) { nesting++; i += 2; }
        else if (sql.startsWith('*/', i)) { nesting--; i += 2; }
        else i++;
      }
      if (nesting) throw new Error('Unterminated SQL comment in result metadata query');
      continue;
    }
    const start = i;
    const quote = sql[i];
    if (quote === "'" || quote === '"' || quote === '`') {
      i++;
      let closed = false;
      while (i < sql.length) {
        if (sql[i] === '\\') { i += 2; continue; }
        if (sql[i++] === quote) {
          if (sql[i] === quote) { i++; continue; }
          closed = true; break;
        }
      }
      if (!closed) throw new Error('Unterminated SQL quote in result metadata query');
    } else {
      const heredoc = sql.slice(i).match(/^\$[A-Za-z_0-9]*\$/)?.[0];
      if (heredoc) {
        const end = sql.indexOf(heredoc, i + heredoc.length);
        if (end < 0) throw new Error('Unterminated SQL heredoc in result metadata query');
        i = end + heredoc.length;
      } else if (/[A-Za-z_]/.test(sql[i])) {
        while (i < sql.length && /[A-Za-z_0-9]/.test(sql[i])) i++;
      } else i++;
    }
    const text = sql.slice(start, i);
    if (text === ')' || text === ']') depth--;
    tokens.push({ text, start, end: i, depth });
    if (text === '(' || text === '[') depth++;
  }
  if (tokens.at(-1)?.text === ';') tokens.pop();
  if (!tokens.length || tokens.some(t => t.depth === 0 && t.text === ';')) {
    throw new Error('Result metadata requires one SQL statement');
  }
  let body = sql.slice(0, tokens.at(-1)!.end);
  // FORMAT is an output clause only at the outer statement level. SETTINGS may
  // follow it; retain those settings so type analysis uses the same semantics.
  const formatIndex = tokens.findIndex((t, index) => t.depth === 0 && t.text.toUpperCase() === 'FORMAT'
    && /^[A-Za-z_][A-Za-z_0-9]*$/.test(tokens[index + 1]?.text ?? '')
    && (index + 2 === tokens.length || tokens[index + 2].text.toUpperCase() === 'SETTINGS'));
  if (formatIndex >= 0) {
    const format = tokens[formatIndex];
    body = body.slice(0, format.start) + body.slice(tokens[formatIndex + 1].end);
  }
  return `DESCRIBE TABLE (\n${body.trim()}\n)`;
}
