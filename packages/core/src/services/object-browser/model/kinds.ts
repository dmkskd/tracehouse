import type { ObjectKind } from './types.js';

export interface KindSpec {
  /** Expected on every host. A host without the object marks it partial, and host
   * records are compared. A storage location exists for the hosts whose tables name it. */
  everyHost: boolean;
}

/** One entry per object kind. Code that needs a property of a kind reads it here. */
export const KINDS: Record<ObjectKind, KindSpec> = {
  user: { everyHost: true },
  role: { everyHost: true },
  'grant-scope': { everyHost: true },
  table: { everyHost: true },
  'row-policy': { everyHost: true },
  location: { everyHost: false },
  // A remote disk exists on the hosts whose tables' storage policies use it.
  disk: { everyHost: false },
};
