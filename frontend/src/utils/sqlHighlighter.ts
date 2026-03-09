/**
 * SQL syntax highlighting utilities.
 *
 * Extracted from QueryExplorer.tsx so the component stays focused on
 * rendering and the tokenizer/highlighter logic is independently testable.
 */

const SQL_KEYWORDS = new Set([
  'SELECT','FROM','WHERE','AND','OR','NOT','IN','IS','NULL','LIKE','BETWEEN',
  'JOIN','LEFT','RIGHT','INNER','OUTER','ON','GROUP','BY','HAVING','ORDER',
  'ASC','DESC','LIMIT','OFFSET','AS','DISTINCT','UNION','INSERT','INTO',
  'VALUES','UPDATE','SET','DELETE','CREATE','DROP','ALTER','TABLE','CASE',
  'WHEN','THEN','ELSE','END','CAST','WITH','OVER','PARTITION','INTERVAL',
  'ILIKE','ARRAY','EXISTS',
]);

const SQL_FUNCTIONS = new Set([
  'COUNT','SUM','AVG','MIN','MAX','ROUND','FLOOR','CEIL','ABS',
  'LENGTH','LOWER','UPPER','TRIM','SUBSTRING','REPLACE','CONCAT',
  'COALESCE','NULLIF','EXTRACT','QUANTILE','FORMATREADABLESIZE',
  'TOSTARTOFMINUTE','TOSTARTOFHOUR','NOW','TODAY','TOINTERVALDAY',
  'TOINTERVALHOUR','ARRAYJOIN','MULTIIF','ANY',
]);

export function escapeHtml(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/**
 * Tokenize raw SQL and return an HTML string with syntax highlighting spans.
 *
 * Character counts are preserved between the raw text and the output so that
 * a `<pre>` overlay stays in sync with a `<textarea>` underneath.
 */
export function highlightSQL(raw: string): string {
  const tokens: { type: string; value: string }[] = [];
  let rest = raw;
  while (rest.length > 0) {
    // Template variables: {{time_range}}, {{cluster_aware:...}}, etc.
    const tm = rest.match(/^(\{\{[^}]+\}\})/);
    if (tm) { tokens.push({ type:'template', value:tm[1] }); rest=rest.slice(tm[1].length); continue; }
    const cm = rest.match(/^(--[^\n]*)/);
    if (cm) { tokens.push({ type:'comment', value:cm[1] }); rest=rest.slice(cm[1].length); continue; }
    const sm = rest.match(/^('(?:[^'\\]|\\.)*')/);
    if (sm) { tokens.push({ type:'string', value:sm[1] }); rest=rest.slice(sm[1].length); continue; }
    const nm = rest.match(/^(\d+\.?\d*)/);
    if (nm) { tokens.push({ type:'number', value:nm[1] }); rest=rest.slice(nm[1].length); continue; }
    const wm = rest.match(/^([a-zA-Z_][a-zA-Z0-9_]*)/);
    if (wm) {
      const u = wm[1].toUpperCase();
      tokens.push({ type: SQL_KEYWORDS.has(u) ? 'keyword' : SQL_FUNCTIONS.has(u) ? 'function' : 'text', value: wm[1] });
      rest=rest.slice(wm[1].length); continue;
    }
    tokens.push({ type:'text', value:rest[0] }); rest=rest.slice(1);
  }
  const styles: Record<string,string> = {
    keyword: 'color:#c678dd;font-weight:500',
    function: 'color:#61afef',
    string: 'color:#98c379',
    number: 'color:#d19a66',
    comment: 'color:var(--text-muted);font-style:italic',
    template: 'color:#d19a66;background:rgba(209,154,102,0.1);border-radius:2px;padding:0 1px',
  };
  return tokens.map(t => {
    const escaped = escapeHtml(t.value);
    const s = styles[t.type];
    if (t.type === 'template') {
      return `<span class="sql-template-var" style="${s}" data-var="${escaped}">${escaped}</span>`;
    }
    return s ? `<span style="${s}">${escaped}</span>` : escaped;
  }).join('');
}
