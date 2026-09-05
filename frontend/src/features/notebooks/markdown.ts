/**
 * Render a notebook as Markdown.
 *
 * The source view exists so a reader can check a claim against the data behind
 * it. JSON is a poor medium for that — evidence rows are arrays of objects with
 * the column names repeated on every row, so the shape of the data is invisible
 * until you have mentally transposed it. The same rows as a Markdown table are
 * readable at a glance.
 *
 * This is a lossy view by design: it carries what a human needs to audit the
 * notebook, not what a machine needs to reconstruct it. Fields that only matter
 * to the renderer (encodings, descriptor versions, action lists) are omitted.
 */

import type {
  EvidenceRow,
  EvidenceValue,
  NotebookDocument,
  NotebookEvidence,
  NotebookStage,
} from './model';
import { notebookKindLabel, rowMatchesKey } from './model';

/** Pipes would break the table; newlines would break the row. */
function cell(value: EvidenceValue | undefined): string {
  if (value === null || value === undefined) return '';
  return String(value).replace(/\|/g, '\\|').replace(/\n/g, ' ');
}

function table(evidence: NotebookEvidence, highlight?: Record<string, EvidenceValue>): string[] {
  const header = evidence.columns.map(column => {
    const unit = evidence.units?.[column];
    return unit ? `${column} (${unit})` : column;
  });

  const lines = [
    `| ${header.map(cell).join(' | ')} |`,
    `| ${header.map(() => '---').join(' | ')} |`,
  ];

  for (const row of evidence.rows as EvidenceRow[]) {
    const cells = evidence.columns.map(column => cell(row[column]));
    // The highlighted row is the one the claim rests on, so it has to survive
    // the trip into Markdown or the table stops supporting the headline.
    //
    // Guarded on `highlight` because rowMatchesKey is vacuously true for an
    // absent key — a stage highlighting a timestamp instead of a row would
    // otherwise mark every row, which marks nothing.
    const marker = highlight && rowMatchesKey(row, highlight) ? ' **←**' : '';
    lines.push(`| ${cells.join(' | ')} |${marker}`);
  }

  return lines;
}

/** One stage as Markdown lines. Exported so a panel can show its own source. */
export function stageToMarkdown(stage: NotebookStage, evidence: NotebookEvidence | undefined, index: number): string[] {
  const lines = [
    `## ${String(index + 1).padStart(2, '0')} · ${stage.headline}`,
    '',
    // Markdown inline code spans, not ClickHouse identifiers.
    // nosemgrep: clickhouse-unescaped-identifier-interpolation
    `\`${stage.claimType}\` · \`${stage.block}\``,
    '',
    stage.takeaway,
  ];

  if (stage.caveat) {
    lines.push('', `> **Inference boundary.** ${stage.caveat}`);
  }

  if (!evidence) {
    // Validation rejects this, so it only shows up for documents rendered
    // outside the loader. Say so rather than printing an empty section.
    // Markdown inline code span, not a ClickHouse identifier.
    // nosemgrep: clickhouse-unescaped-identifier-interpolation
    lines.push('', `_Missing evidence: \`${stage.evidence}\`_`);
    return lines;
  }

  lines.push('', `**Evidence — ${evidence.title}** (${evidence.mode})`, '');
  lines.push(...table(evidence, stage.highlight?.rowKey));

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
    `**${notebookKindLabel(document)}** · ${scope.from} → ${scope.to}`,
  ];

  const context = [
    scope.sourceLabel && `source: ${scope.sourceLabel}`,
    scope.cluster && `cluster: ${scope.cluster}`,
    scope.hosts?.length && `hosts: ${scope.hosts.join(', ')}`,
  ].filter(Boolean);
  if (context.length > 0) lines.push('', context.join(' · '));

  document.stages.forEach((stage, index) => {
    lines.push('', ...stageToMarkdown(stage, document.evidence[stage.evidence], index));
  });

  if (document.limitations?.length) {
    lines.push('', '## Limitations', '');
    lines.push(...document.limitations.map(item => `- ${item}`));
  }

  return `${lines.join('\n')}\n`;
}
