/**
 * Validate a notebook document before it reaches the renderer.
 *
 * This is the only validator. Documents arrive from an agent, a colleague, or a
 * text editor, and the renderer trusts its input completely: a stage naming an
 * evidence key that does not exist reads `undefined.title` and white-screens
 * the route.
 *
 * It returns errors rather than throwing, because the useful response to a bad
 * notebook is showing the author what is wrong with it.
 *
 * The contract it enforces is notebook.schema.json, which ships with the
 * compose skill at .agents/skills/compose-notebook/. When the schema moves,
 * both move.
 */

import type {
  EvidenceRow,
  EvidenceValue,
  NotebookBlock,
  NotebookDocument,
} from './model';
import { rowMatchesKey } from './model';

const KINDS = ['investigation', 'runbook', 'report'];
const MODES = ['snapshot', 'live-link', 'snapshot-with-live-link'];
const CLAIM_TYPES = ['observed', 'derived', 'inferred', 'recommended'];

/** Mirrors visual-catalog.json. Blocks the renderer can actually draw. */
const BLOCKS: Record<NotebookBlock, readonly string[]> = {
  'timeseries.annotated': ['x', 'y'],
  'table.ranked': ['label', 'rankBy'],
  'facts.list': ['label', 'value'],
};

export type NotebookValidation =
  | { ok: true; document: NotebookDocument }
  | { ok: false; errors: string[] };

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/** Absolute instants only — a notebook without a timezone cannot be replayed. */
function absoluteDate(value: unknown): number | null {
  if (typeof value !== 'string' || !/(Z|[+-]\d\d:\d\d)$/.test(value)) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function safeHref(value: unknown): boolean {
  if (typeof value !== 'string' || !value.trim()) return false;
  try {
    const url = new URL(value);
    return url.protocol === 'https:' || url.protocol === 'http:';
  } catch {
    return false;
  }
}

/** One leading slash: `//host` is protocol-relative and leaves the app. */
function safeRoute(value: unknown): boolean {
  return typeof value === 'string' && /^\/[^/]/.test(value);
}

function encodedFields(encoding: unknown): string[] {
  if (!isObject(encoding)) return [];
  return Object.values(encoding).flatMap(value => {
    if (typeof value === 'string') return [value];
    if (Array.isArray(value)) return value.filter((item): item is string => typeof item === 'string');
    return [];
  });
}

export function validateNotebook(input: unknown): NotebookValidation {
  const errors: string[] = [];
  const error = (location: string, message: string) => errors.push(`${location}: ${message}`);

  if (!isObject(input)) return { ok: false, errors: ['notebook: must be an object'] };

  if (input.schemaVersion !== '0.1') error('schemaVersion', 'must equal "0.1"');
  if (typeof input.title !== 'string' || !input.title.trim()) error('title', 'is required');
  if (typeof input.question !== 'string' || !input.question.trim()) error('question', 'is required');
  if (input.kind !== undefined && !KINDS.includes(input.kind as string)) {
    error('kind', `must be one of ${KINDS.join(', ')}`);
  }

  const scope = isObject(input.scope) ? input.scope : {};
  const from = absoluteDate(scope.from);
  const to = absoluteDate(scope.to);
  if (from === null) error('scope.from', 'must be an absolute ISO-8601 timestamp');
  if (to === null) error('scope.to', 'must be an absolute ISO-8601 timestamp');
  if (from !== null && to !== null && from >= to) error('scope', 'from must be before to');

  const evidence = isObject(input.evidence) ? input.evidence : {};
  if (Object.keys(evidence).length === 0) error('evidence', 'must contain at least one item');

  for (const [id, raw] of Object.entries(evidence)) {
    const at = `evidence.${id}`;
    if (!isObject(raw)) {
      error(at, 'must be an object');
      continue;
    }
    if (typeof raw.title !== 'string' || !raw.title.trim()) error(`${at}.title`, 'is required');
    if (!MODES.includes(raw.mode as string)) {
      error(`${at}.mode`, `must be one of ${MODES.join(', ')}`);
    }
    if (!Array.isArray(raw.columns) || raw.columns.length === 0
      || raw.columns.some(column => typeof column !== 'string')) {
      error(`${at}.columns`, 'must contain field names');
    }
    if (!Array.isArray(raw.rows)) error(`${at}.rows`, 'must be an array');

    const view = isObject(raw.view) ? raw.view : undefined;
    if (typeof raw.mode === 'string' && raw.mode.includes('live-link')
      && (!view || (!view.href && !view.route))) {
      error(`${at}.view`, 'is required for live-link evidence');
    }
    if (view?.href !== undefined && !safeHref(view.href)) {
      error(`${at}.view.href`, 'must be an absolute HTTP(S) URL');
    }
    if (view?.route !== undefined && !safeRoute(view.route)) {
      error(`${at}.view.route`, 'must be an app-relative route starting with a single slash');
    }
    if (view && (!Number.isInteger(view.descriptorVersion) || (view.descriptorVersion as number) < 1)) {
      error(`${at}.view.descriptorVersion`, 'must be a positive integer');
    }

    if (Array.isArray(raw.rows) && Array.isArray(raw.columns)) {
      raw.rows.forEach((row, index) => {
        if (!isObject(row)) {
          error(`${at}.rows[${index}]`, 'must be an object');
          return;
        }
        for (const column of raw.columns as string[]) {
          if (!(column in row)) error(`${at}.rows[${index}]`, `is missing column ${column}`);
        }
      });
    }
  }

  if (!Array.isArray(input.stages) || input.stages.length === 0) {
    error('stages', 'must contain at least one stage');
  }

  const seenIds = new Set<string>();
  const stages = Array.isArray(input.stages) ? input.stages : [];
  stages.forEach((raw, index) => {
    const at = `stages[${index}]`;
    if (!isObject(raw)) {
      error(at, 'must be an object');
      return;
    }

    // Stage IDs become React keys and anchor hrefs, so duplicates break both.
    if (typeof raw.id !== 'string' || !/^[a-z0-9][a-z0-9-]*$/.test(raw.id)) {
      error(`${at}.id`, 'must be a stable kebab-case ID');
    } else if (seenIds.has(raw.id)) {
      error(`${at}.id`, `duplicates ${raw.id}`);
    } else {
      seenIds.add(raw.id);
    }

    if (typeof raw.headline !== 'string' || !raw.headline.trim()) error(`${at}.headline`, 'is required');
    if (!CLAIM_TYPES.includes(raw.claimType as string)) error(`${at}.claimType`, 'is invalid');
    if (raw.claimType === 'inferred' && (typeof raw.caveat !== 'string' || !raw.caveat.trim())) {
      error(`${at}.caveat`, 'is required for inferred claims');
    }
    if (typeof raw.takeaway !== 'string' || !raw.takeaway.trim()) error(`${at}.takeaway`, 'is required');

    const required = BLOCKS[raw.block as NotebookBlock];
    if (!required) error(`${at}.block`, `unknown block ${String(raw.block)}`);

    // The crash this whole module exists to prevent.
    const item = typeof raw.evidence === 'string'
      ? (evidence[raw.evidence] as Record<string, unknown> | undefined)
      : undefined;
    if (!item) error(`${at}.evidence`, `unknown evidence ${String(raw.evidence)}`);

    if (!isObject(raw.encoding)) {
      error(`${at}.encoding`, 'must be an object');
    } else if (required) {
      for (const key of required) {
        if (!(key in raw.encoding)) error(`${at}.encoding`, `is missing ${key}`);
      }
      const columns = new Set((item?.columns as string[] | undefined) ?? []);
      for (const field of encodedFields(raw.encoding)) {
        if (!columns.has(field)) error(`${at}.encoding`, `references missing column ${field}`);
      }
    }

    const highlight = isObject(raw.highlight) ? raw.highlight : undefined;
    if (highlight && 'row' in highlight) {
      error(`${at}.highlight`, 'row positions are unstable; use rowKey');
    }
    if (highlight?.rowKey !== undefined) {
      const rows = (item?.rows as EvidenceRow[] | undefined) ?? [];
      const key = highlight.rowKey as Record<string, EvidenceValue>;
      if (item && !rows.some(row => rowMatchesKey(row, key))) {
        error(`${at}.highlight.rowKey`, 'does not match an evidence row');
      }
    }
    if (highlight?.timestamp !== undefined && absoluteDate(highlight.timestamp) === null) {
      error(`${at}.highlight.timestamp`, 'must be an absolute timestamp');
    }

    if (raw.actions !== undefined) {
      if (!Array.isArray(raw.actions)) {
        error(`${at}.actions`, 'must be an array');
      } else {
        raw.actions.forEach((action, actionIndex) => {
          if (!isObject(action) || action.type !== 'open-evidence') {
            error(`${at}.actions[${actionIndex}]`, 'has an unsupported type');
            return;
          }
          if (!evidence[action.evidence as string]) {
            error(`${at}.actions[${actionIndex}]`, 'references unknown evidence');
          }
        });
      }
    }
  });

  if (errors.length > 0) return { ok: false, errors };
  return { ok: true, document: input as unknown as NotebookDocument };
}

/** Parse and validate in one step, so callers handle bad JSON the same way. */
export function parseNotebook(text: string): NotebookValidation {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (cause) {
    return { ok: false, errors: [`notebook: is not valid JSON (${(cause as Error).message})`] };
  }
  return validateNotebook(parsed);
}
