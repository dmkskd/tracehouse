/**
 * A notebook is the container; `kind` says what this one is for.
 *
 * - `investigation`: answers one question about a past incident, usually agent-composed.
 * - `runbook`: a reusable operational procedure, authored ahead of time and re-run.
 * - `report`: a periodic or one-off summary meant to be read rather than acted on.
 */
export type NotebookKind = 'investigation' | 'runbook' | 'report';

export type NotebookClaimType = 'observed' | 'derived' | 'inferred' | 'recommended';
export type NotebookBlock = 'timeseries.annotated' | 'table.ranked' | 'facts.list';
export type EvidenceValue = string | number | boolean | null;
export type EvidenceRow = Record<string, EvidenceValue>;

export interface NotebookEvidence {
  title: string;
  mode: 'snapshot' | 'live-link' | 'snapshot-with-live-link';
  columns: string[];
  rows: EvidenceRow[];
  units?: Record<string, string>;
  provenance?: Record<string, unknown>;
  view?: {
    /** TraceHouse-owned route. The host shell translates it for standalone or Grafana. */
    route?: string;
    /** External URLs are supported for imported evidence, but are not used by the example. */
    href?: string;
    descriptorVersion: number;
  };
}

export interface NotebookStage {
  id: string;
  headline: string;
  claimType: NotebookClaimType;
  block: NotebookBlock;
  evidence: string;
  encoding: {
    x?: string;
    y?: string | string[];
    label?: string;
    value?: string;
    rankBy?: string;
  };
  takeaway: string;
  caveat?: string;
  highlight?: {
    timestamp?: string;
    rowKey?: Record<string, EvidenceValue>;
  };
  actions?: Array<{ type: 'open-evidence'; evidence: string }>;
}

export interface NotebookDocument {
  schemaVersion: '0.1';
  /** Absent on documents written before `kind` existed; treat those as investigations. */
  kind?: NotebookKind;
  title: string;
  question: string;
  scope: {
    from: string;
    to: string;
    sourceLabel?: string;
    cluster?: string | null;
    hosts?: string[];
  };
  evidence: Record<string, NotebookEvidence>;
  stages: NotebookStage[];
  limitations?: string[];
}

const KIND_LABELS: Record<NotebookKind, string> = {
  investigation: 'Investigation',
  runbook: 'Runbook',
  report: 'Report',
};

export function notebookKind(document: NotebookDocument): NotebookKind {
  return document.kind ?? 'investigation';
}

export function notebookKindLabel(document: NotebookDocument): string {
  return KIND_LABELS[notebookKind(document)];
}

export function rowMatchesKey(row: Record<string, unknown>, key?: Record<string, EvidenceValue>): boolean {
  return Object.entries(key ?? {}).every(([field, value]) => row[field] === value);
}

export function evidenceTarget(evidence: NotebookEvidence):
  | { kind: 'route'; value: string }
  | { kind: 'external'; value: string }
  | undefined {
  if (evidence.view?.route?.startsWith('/') && !evidence.view.route.startsWith('//')) {
    return { kind: 'route', value: evidence.view.route };
  }
  if (evidence.view?.href && /^https?:\/\//i.test(evidence.view.href)) {
    return { kind: 'external', value: evidence.view.href };
  }
  return undefined;
}
