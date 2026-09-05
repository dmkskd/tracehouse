import { readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  QUERY_KIND_LABELS,
  QUERY_ROW_LABELS,
  THREAD_LABELS,
  TOPOLOGY_ACTOR_LABELS,
  TOPOLOGY_EVENT_LABELS,
  TOPOLOGY_ROLE_LABELS,
} from '../topology-labels.js';

/**
 * The reason topology-labels.ts exists is that these words were written in five
 * places and drifted apart. Collecting them once fixes today; this test is what
 * stops a sixth place appearing tomorrow, when someone types 'Coordinator' into
 * a new component because that is what the neighbouring code appeared to do.
 *
 * It reads the label table, then scans the source for string literals that are
 * exactly one of those labels, and fails naming the file.
 *
 * Exact match rather than substring, deliberately. The failure it is built to
 * catch is a label written somewhere new, which always looks like
 * `label="Coordinator"` or `role: 'Remote child'`. Matching substrings instead
 * would flag every enum identifier, every evidence sentence mentioning a
 * coordinator in passing, and `MergeTreeReader`; a check that cries wolf gets
 * exemptions sprinkled over it until it means nothing. Prose that embeds a
 * label is left alone - composing sentences from the table is a bigger change
 * than this test should force.
 *
 * A literal legitimately identical to a label is exempted by a comment on the
 * line or the line above, which keeps the exception visible in review rather
 * than buried in an allowlist here.
 */

const EXEMPTION = 'labels-exempt';

const HERE = dirname(fileURLToPath(import.meta.url));
/** __tests__ -> services -> src -> core -> packages -> repo root */
const REPO_ROOT = join(HERE, '..', '..', '..', '..', '..');

const SCANNED_TREES = [
  join(REPO_ROOT, 'packages', 'core', 'src'),
  join(REPO_ROOT, 'frontend', 'src'),
];

/** The table itself is where these words are supposed to be written. */
const TABLE_FILE = 'topology-labels.ts';

/**
 * Labels too generic to guard on. 'Local' and 'Unknown' are ordinary English
 * appearing all over the codebase for unrelated reasons, and every lower-case
 * noun collides with the role identifier it is named after - the string
 * 'coordinator' is both a label and an enum value, and the enum value is far
 * more common. Flagging either would train people to sprinkle exemptions,
 * which defeats the point. The title-case forms, which is how a label is
 * actually written into a component, stay guarded.
 */
const TOO_GENERIC = new Set([
  'Local', 'local', 'Unknown', 'unknown', 'Distributed', 'remote', 'Worker', 'worker',
  'initiator', 'shard initiator', 'nested initiator', 'local replica',
  'remote node', 'independent child', 'object worker', 'hybrid segment', 'insert client',
  'remote table INSERT', 'async insert flush', 'child query',
  // "Replica" is ordinary vocabulary in a tool for ClickHouse - the Replication
  // page and Cluster Overview are full of it in a sense that has nothing to do
  // with a query participant. This is the guard's blind spot: it protects
  // distinctive labels well and single common words not at all. The qualified
  // forms it cannot see either ("Shard 2 replica") are built by perShard rather
  // than typed as literals, so they cannot be copied by hand in the first place.
  'Replica', 'replica',
]);

function collectLabels(): string[] {
  const labels = new Set<string>();
  const add = (value: unknown) => {
    if (typeof value === 'string' && value.length > 3 && !TOO_GENERIC.has(value)) labels.add(value);
  };
  for (const role of Object.values(TOPOLOGY_ROLE_LABELS)) {
    add(role.title);
    add(role.noun);
    add(role.remoteExecution);
  }
  for (const event of Object.values(TOPOLOGY_EVENT_LABELS) as { title: string; headline?: string; detail?: string }[]) {
    add(event.title);
    add(event.headline);
    add(event.detail);
  }
  Object.values(TOPOLOGY_ACTOR_LABELS).forEach(add);
  for (const row of Object.values(QUERY_ROW_LABELS)) {
    add(row.title);
    add(row.noun);
  }
  for (const thread of Object.values(THREAD_LABELS)) {
    add(thread.title);
    add(thread.thread);
    add(thread.start);
  }
  Object.values(QUERY_KIND_LABELS).forEach(add);
  // Longest first, so a report names the most specific label that matched
  // rather than a shorter one contained inside it.
  return [...labels].sort((a, b) => b.length - a.length);
}

function sourceFiles(dir: string): string[] {
  const found: string[] = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) {
      if (entry === 'node_modules' || entry === 'dist' || entry === '__tests__') continue;
      found.push(...sourceFiles(full));
      continue;
    }
    if (!/\.tsx?$/.test(entry) || entry === TABLE_FILE) continue;
    found.push(full);
  }
  return found;
}

/** Single- and double-quoted literals, and the text between JSX tags. */
const LITERAL = /'([^'\\\n]{4,200})'|"([^"\\\n]{4,200})"|>([^<>{}\n]{4,200})</g;

/**
 * Whether a line carries an exemption, on itself or anywhere in the comment
 * block directly above it. The whole block counts because an exemption worth
 * writing usually needs a sentence of reason, and forcing that onto one line
 * either truncates the reason or hides the marker at the end of it.
 */
function isExempt(lines: string[], index: number): boolean {
  if (lines[index]?.includes(EXEMPTION)) return true;
  for (let above = index - 1; above >= 0; above -= 1) {
    const line = lines[above].trim();
    if (!line.startsWith('//') && !line.startsWith('*') && !line.startsWith('/*')) return false;
    if (line.includes(EXEMPTION)) return true;
  }
  return false;
}

function violationsIn(file: string, labels: string[]): string[] {
  const source = readFileSync(file, 'utf8');
  const lines = source.split('\n');
  const found: string[] = [];
  for (const [index, line] of lines.entries()) {
    if (isExempt(lines, index)) continue;
    for (const match of line.matchAll(LITERAL)) {
      const literal = match[1] ?? match[2] ?? match[3] ?? '';
      const hit = labels.find(label => literal === label);
      if (hit) found.push(`${relative(REPO_ROOT, file)}:${index + 1}  ${JSON.stringify(hit)} in ${JSON.stringify(literal)}`);
    }
  }
  return found;
}

describe('topology labels are written in one place', () => {
  const labels = collectLabels();

  it('collected the labels it is going to guard', () => {
    // A regression that emptied the table would make this test pass vacuously.
    expect(labels.length).toBeGreaterThan(20);
    expect(labels).toContain('Initiator');
  });

  it('finds no label written outside the label table', () => {
    const violations = SCANNED_TREES
      .flatMap(sourceFiles)
      .flatMap(file => violationsIn(file, labels));

    expect(violations, [
      '',
      'These files write a label that belongs to topology-labels.ts:',
      ...violations.map(entry => `  ${entry}`),
      '',
      'Import the label from @tracehouse/core instead. If the text genuinely is',
      `not one of our labels - a ClickHouse identifier, a quoted doc sentence -`,
      `add a "${EXEMPTION}: <reason>" comment on the line or the line above.`,
      '',
    ].join('\n')).toEqual([]);
  });
});
