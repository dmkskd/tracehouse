import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { provenanceAnswer, type Capture, type Investigation, type ProvenanceAnswer } from '@tracehouse/core/services/object-browser';

// One answer per capture: the chip and the diagram read the same one.
const answers = new WeakMap<Capture, ProvenanceAnswer>();
function answerOf(capture: Capture): ProvenanceAnswer {
  let answer = answers.get(capture);
  if (!answer) { answer = provenanceAnswer(capture);answers.set(capture, answer); }
  return answer;
}

/** Provenance captures of one user and table, one per operation asked since the catalog was last
 * shown. `pick` shows an operation asked before, or collects a new one; `toggle` (the picker)
 * also removes one, except the last. Another user or table, or the catalog, starts a new list,
 * so no answer outlives its catalog. */
export function useProvenanceOperations(capture: Capture | null,
  collect: (input: Omit<Investigation, 'connection'>) => void, show: (capture: Capture) => void) {
  const [asked, setAsked] = useState<{ target: string; captures: Capture[] }>({ target: '', captures: [] });
  // Access types still to collect after the first (start), for one user and table.
  const queued = useRef<{ target: string; operations: string[] }>({ target: '', operations: [] });
  // Each capture is handled once, so a repeated effect run does not collect twice.
  const handled = useRef<Capture | null>(null);
  useEffect(() => {
    if (!capture || handled.current === capture) return;
    handled.current = capture;
    if (capture.mode === 'catalog') { setAsked({ target: '', captures: [] });queued.current = { target: '', operations: [] }; return; }
    const { principal, database, table, operation } = capture.investigation;
    const target = JSON.stringify([capture.investigation.connection, principal, database, table]);
    if (queued.current.target === JSON.stringify([principal, database, table]) && queued.current.operations.length) {
      const [next, ...rest] = queued.current.operations;
      queued.current = { ...queued.current, operations: rest };
      collect({ principal, database, table, operation: next });
    }
    setAsked(prev => {
      if (prev.target !== target) return { target, captures: [capture] };
      const known = prev.captures.some(c => c.investigation.operation === operation);
      return { target, captures: known ? prev.captures.map(c => c.investigation.operation === operation ? capture : c) : [...prev.captures, capture] };
    });
  }, [capture, collect]);
  const current = useMemo(() => capture && capture.mode !== 'catalog' ? answerOf(capture) : null, [capture]);
  const list = useMemo(() => asked.captures.map(answerOf), [asked]);
  const pick = useCallback((operation: string) => {
    if (!capture || capture.mode === 'catalog') return;
    const earlier = asked.captures.find(c => c.investigation.operation === operation);
    if (earlier) { show(earlier); return; }
    const { principal, database, table } = capture.investigation;
    collect({ principal, database, table, operation });
  }, [capture, asked, collect, show]);
  const toggle = useCallback((operation: string) => {
    if (!capture || capture.mode === 'catalog') return;
    const known = asked.captures.some(c => c.investigation.operation === operation);
    if (!known) { pick(operation); return; }
    if (asked.captures.length === 1) return;
    const rest = asked.captures.filter(c => c.investigation.operation !== operation);
    setAsked(prev => ({ ...prev, captures: prev.captures.filter(c => c.investigation.operation !== operation) }));
    if (capture.investigation.operation === operation) show(rest[0]);
  }, [capture, asked, pick, show]);
  /** Opens a provenance on several access types: collects the first, then the rest in turn. */
  const start = useCallback((input: Omit<Investigation, 'connection' | 'operation'>, operations: readonly string[]) => {
    const [first = 'SELECT', ...rest] = operations;
    queued.current = { target: JSON.stringify([input.principal, input.database, input.table]), operations: rest };
    collect({ ...input, operation: first });
  }, [collect]);
  return { current, answers: list, operations: list.map(a => a.operation), pick, toggle, start };
}
