import type { Coverage, ObjectCard, ObjectGraph, ProvenanceAnswer } from '@tracehouse/core/services/object-browser';

export interface DatabaseGroup { name: string; count: number; keys: readonly string[] }

/** Semantic presentation input. No columns, camera, scene coordinates, routed lines,
 * aggregate identities or collection APIs cross this boundary. Graph edges have not
 * been simplified for a renderer. Original keys and host evidence remain intact. */
export interface ObjectViewModel {
  graph: ObjectGraph;
  cards: ReadonlyMap<string, ObjectCard>;
  databases: readonly DatabaseGroup[];
  coverage: readonly Coverage[];
  selected: string | null;
  selectedDatabase: string | null;
  muted?: ReadonlySet<string>;
  covered?: ReadonlyMap<string, readonly string[]>;
  /** Core evaluator answers for the selected user, evaluated lazily per visible table.
   * Missing for other selections. Scope-name links alone are never access verdicts. */
  accessForTable?: (key: string) => readonly ProvenanceAnswer[];
}

export interface ObjectViewActions {
  onSelect: (key: string) => void;
  onFocus: (key: string) => void;
  onDatabase: (name: string) => void;
  onClear: () => void;
}
