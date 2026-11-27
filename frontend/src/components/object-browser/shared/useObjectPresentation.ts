import { useMemo } from 'react';
import { accessEvaluator, coveredOperations, describeAccess, describeObjects, privilegesOutside,
  type Capture, type ObjectGraph, type ProvenanceAnswer } from '@tracehouse/core/services/object-browser';
import { databaseGroups } from './databaseGroups';

/** Shared descriptions and evidence, before either renderer simplifies or lays out edges. */
export function useObjectPresentation(graph: ObjectGraph | null, operations: readonly string[], capture?: Capture | null, selected?: string | null) {
  const cards = useMemo(() => describeObjects(graph ?? { nodes: [], edges: [] }), [graph]);
  const databases = useMemo(() => databaseGroups(graph), [graph]);
  const muted = useMemo(() => graph ? privilegesOutside(graph, operations) : undefined, [graph, operations]);
  const covered = useMemo(() => graph ? coveredOperations(graph, operations) : undefined, [graph, operations]);
  const principal = graph?.nodes.find(n => n.key === selected && n.kind === 'user')?.key;
  const accessForTable = useMemo(() => {
    if (!capture || !principal) return undefined;
    // The graph renderer does not request table answers. Build the private core
    // index only when a consumer requests one, avoiding work for the default view.
    let evaluator: ReturnType<typeof accessEvaluator> | undefined;
    const cache = new Map<string, readonly ProvenanceAnswer[]>();
    return (key: string) => {
      let answers = cache.get(key);
      if (!answers) {
        evaluator ??= accessEvaluator(capture);
        answers = operations.map(operation => describeAccess(evaluator!.answer(principal, key, operation)));
        cache.set(key, answers);
      }
      return answers;
    };
  }, [capture, principal, operations]);
  return { cards, databases, muted, covered, accessForTable };
}
