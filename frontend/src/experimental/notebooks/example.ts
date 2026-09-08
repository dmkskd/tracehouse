import exampleDocument from './example-notebook.json';
import type { NotebookDocument } from './model';

/**
 * Demo document behind "View example".
 *
 * Lives inside the feature because the frontend is built from a Docker context
 * containing only packages/ and frontend/ — importing across the repo root
 * breaks the image build.
 */
export const memoryLimitNotebook = exampleDocument as NotebookDocument;
