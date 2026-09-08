/**
 * Render a notebook as Markdown.
 *
 * The source view exists so a reader can check a claim against the data behind
 * it. JSON is a poor medium for that — evidence rows are arrays of objects with
 * the column names repeated on every row, so the shape of the data is invisible
 * until you have mentally transposed it. The same rows as a Markdown table are
 * readable at a glance.
 *
 * The view is a full account of the cell, not a summary of it. Every field a
 * cell declares in the manifest appears here, including the ones that only the
 * renderer consumes — `block`, `encoding`, `highlight`. Those decide whether a
 * cell draws as a table, a chart or a grid of tiles, so a reader comparing two
 * cells that look different has no way to explain the difference unless the
 * source view says which block each one asked for.
 */

import type {
  EvidenceRow,
  EvidenceValue,
  NotebookDocument,
  NotebookEvidence,
  NotebookCell,
} from './model';
import { notebookKindLabel, rowMatchesKey } from './model';

/** Pipes would break the table; newlines would break the row. */
function cell(value: EvidenceValue | undefined): string {
  if (value === null || value === undefined) return '';
  return String(value).replace(/\|/g, '\\|').replace(/\n/g, ' ');
}

function table(evidence: NotebookEvidence, highlight?: Record<string, EvidenceValue>, columns?: NotebookCell['columns']): string[] {
  const fields = columns ?? evidence.columns.map(field => ({ field, label: field }));
  const header = fields.map(column => {
    const unit = evidence.units?.[column.field];
    return unit ? `${column.label} (${unit})` : column.label;
  });

  const lines = [
    `| ${header.map(cell).join(' | ')} |`,
    `| ${header.map(() => '---').join(' | ')} |`,
  ];

  for (const row of evidence.rows as EvidenceRow[]) {
    const cells = fields.map(column => cell(row[column.field]));
    // The highlighted row is the one the claim rests on, so it has to survive
    // the trip into Markdown or the table stops supporting the headline.
    //
    // Guarded on `highlight` because rowMatchesKey is vacuously true for an
    // absent key — a cell highlighting a timestamp instead of a row would
    // otherwise mark every row, which marks nothing.
    const marker = highlight && rowMatchesKey(row, highlight) ? ' **←**' : '';
    lines.push(`| ${cells.join(' | ')} |${marker}`);
  }

  return lines;
}

/**
 * The encoding names which evidence columns drive the visual. `y` is the one
 * field that may be a list, so it is joined rather than special-cased upstream.
 */
function encodingParts(encoding: NotebookCell['encoding']): string[] {
  return Object.entries(encoding)
    .filter(([, value]) => value !== undefined && value !== null)
    .map(([key, value]) => `${key}: ${Array.isArray(value) ? value.join(', ') : String(value)}`);
}

/**
 * A row highlight also shows up as `←` on its table row. It is named here as
 * well because the marker says which row, not what made it the one that matters
 * — and a timestamp highlight has no row to mark at all.
 */
function highlightParts(highlight: NotebookCell['highlight']): string[] {
  if (!highlight) return [];
  const parts: string[] = [];
  if (highlight.timestamp) parts.push(`highlight: ${highlight.timestamp}`);
  if (highlight.rowKey) {
    const key = Object.entries(highlight.rowKey).map(([field, value]) => `${field}=${cell(value)}`);
    if (key.length > 0) parts.push(`highlight: ${key.join(', ')}`);
  }
  return parts;
}

/** Presentation renames hide the underlying field, so state the mapping. */
function columnParts(columns: NotebookCell['columns']): string[] {
  if (!columns?.length) return [];
  const renames = columns.map(column => {
    const type = column.type ? ` (${column.type})` : '';
    return `\`${column.field}\` → ${column.label}${type}`;
  });
  return ['', `Displayed columns: ${renames.join(' · ')}`];
}

function provenanceParts(evidence: NotebookEvidence): string[] {
  const entries = Object.entries(evidence.provenance ?? {});
  if (entries.length === 0) return [];
  return ['', `Provenance: ${entries.map(([key, value]) => `${key}=${cell(value as EvidenceValue)}`).join(' · ')}`];
}

/** One cell as Markdown lines. Exported so a panel can show its own source. */
export function cellToMarkdown(cell: NotebookCell, evidence: NotebookEvidence | undefined, index: number): string[] {
  const lines = [
    `## ${String(index + 1).padStart(2, '0')} · ${cell.headline}`,
    '',
    [
      `block \`${cell.block}\``,
      ...encodingParts(cell.encoding),
      ...highlightParts(cell.highlight),
      `id \`${cell.id}\``,
    ].join(' · '),
    '',
    cell.takeaway,
  ];

  if (!evidence) {
    // Validation rejects this, so it only shows up for documents rendered
    // outside the loader. Say so rather than printing an empty section.
    // Markdown inline code span, not a ClickHouse identifier.
    // nosemgrep: clickhouse-unescaped-identifier-interpolation
    lines.push('', `_Missing evidence: \`${cell.evidence}\`_`);
    return lines;
  }

  const mode = evidence.mode === 'live-link' ? 'linked evidence' : 'captured evidence';
  // Markdown inline code span, not a ClickHouse identifier.
  // nosemgrep: clickhouse-unescaped-identifier-interpolation
  lines.push('', `**Evidence — ${evidence.title}** (\`${cell.evidence}\`, ${mode})`);
  lines.push(...provenanceParts(evidence));
  lines.push(...columnParts(cell.columns));
  lines.push('');
  lines.push(...table(evidence, cell.highlight?.rowKey, cell.columns));

  if (cell.actions?.length) {
    lines.push('', `Actions: ${cell.actions.map(action => `${action.type} → \`${action.evidence}\``).join(' · ')}`);
  }

  const route = evidence.view?.route ?? evidence.view?.href;
  if (route) lines.push('', `[Open evidence](${route})`);

  return lines;
}

export function notebookToMarkdown(document: NotebookDocument): string {
  const { scope } = document;
  const lines: string[] = [
    `# ${document.title}`,
    '',
    `> ${document.question}`,
    '',
    `**${notebookKindLabel(document)}** · schema ${document.schemaVersion} · ${scope.from} → ${scope.to}`,
  ];

  const context = [
    scope.sourceLabel && `source: ${scope.sourceLabel}`,
    scope.cluster && `cluster: ${scope.cluster}`,
    scope.hosts?.length && `hosts: ${scope.hosts.join(', ')}`,
  ].filter(Boolean);
  if (context.length > 0) lines.push('', context.join(' · '));

  document.cells.forEach((cell, index) => {
    lines.push('', ...cellToMarkdown(cell, document.evidence[cell.evidence], index));
  });

  if (document.limitations?.length) {
    lines.push('', '## Limitations', '');
    lines.push(...document.limitations.map(item => `- ${item}`));
  }

  return `${lines.join('\n')}\n`;
}
