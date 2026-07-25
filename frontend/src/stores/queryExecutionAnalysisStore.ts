import type {
  QueryExecutionAnalysisErrorCategory,
  QueryExecutionAnalysisResult,
} from '@tracehouse/core';

export const QUERY_EXECUTION_ANALYSIS_CACHE_LIMIT = 20;

export type QueryExecutionAnalysisFailureCategory =
  | QueryExecutionAnalysisErrorCategory
  | 'connection';

export interface QueryExecutionAnalysisFailure {
  message: string;
  category: QueryExecutionAnalysisFailureCategory;
}

export type QueryExecutionAnalysisStatus =
  | 'idle'
  | 'running'
  | 'success'
  | 'error';

export interface QueryExecutionAnalysisSnapshot {
  status: QueryExecutionAnalysisStatus;
  result: QueryExecutionAnalysisResult | null;
  requestDurationMs: number;
  failure: QueryExecutionAnalysisFailure | null;
}

interface QueryExecutionAnalysisEntry {
  snapshot: QueryExecutionAnalysisSnapshot;
  listeners: Set<() => void>;
  generation: number;
  promise?: Promise<QueryExecutionAnalysisResult | null>;
}

type FailureMapper = (error: unknown) => QueryExecutionAnalysisFailure;

const IDLE_SNAPSHOT: QueryExecutionAnalysisSnapshot = {
  status: 'idle',
  result: null,
  requestDurationMs: 0,
  failure: null,
};

const scopedSessions = new WeakMap<
  object,
  Map<string, QueryExecutionAnalysisEntry>
>();

function sessionsFor(scope: object): Map<string, QueryExecutionAnalysisEntry> {
  let sessions = scopedSessions.get(scope);
  if (!sessions) {
    sessions = new Map();
    scopedSessions.set(scope, sessions);
  }
  return sessions;
}

function sessionEntry(
  scope: object,
  key: string,
): QueryExecutionAnalysisEntry {
  const sessions = sessionsFor(scope);
  let entry = sessions.get(key);
  if (!entry) {
    entry = {
      snapshot: IDLE_SNAPSHOT,
      listeners: new Set(),
      generation: 0,
    };
    sessions.set(key, entry);
  }
  return entry;
}

function touch(
  sessions: Map<string, QueryExecutionAnalysisEntry>,
  key: string,
  entry: QueryExecutionAnalysisEntry,
): void {
  sessions.delete(key);
  sessions.set(key, entry);
}

function prune(sessions: Map<string, QueryExecutionAnalysisEntry>): void {
  while (sessions.size > QUERY_EXECUTION_ANALYSIS_CACHE_LIMIT) {
    const removable = [...sessions.entries()].find(([, entry]) =>
      entry.listeners.size === 0 && entry.snapshot.status !== 'running'
    );
    if (!removable) return;
    sessions.delete(removable[0]);
  }
}

function publish(entry: QueryExecutionAnalysisEntry): void {
  entry.listeners.forEach(listener => listener());
}

export function getQueryExecutionAnalysisSnapshot(
  scope: object,
  key: string,
): QueryExecutionAnalysisSnapshot {
  return sessionEntry(scope, key).snapshot;
}

export function subscribeQueryExecutionAnalysis(
  scope: object,
  key: string,
  listener: () => void,
): () => void {
  const sessions = sessionsFor(scope);
  const entry = sessionEntry(scope, key);
  touch(sessions, key, entry);
  entry.listeners.add(listener);

  return () => {
    entry.listeners.delete(listener);
    if (entry.snapshot.status === 'idle' && entry.listeners.size === 0) {
      sessions.delete(key);
    }
    prune(sessions);
  };
}

export function failQueryExecutionAnalysis(
  scope: object,
  key: string,
  failure: QueryExecutionAnalysisFailure,
): void {
  const sessions = sessionsFor(scope);
  const entry = sessionEntry(scope, key);
  entry.generation += 1;
  entry.promise = undefined;
  entry.snapshot = {
    status: 'error',
    result: null,
    requestDurationMs: 0,
    failure,
  };
  touch(sessions, key, entry);
  prune(sessions);
  publish(entry);
}

export function resetQueryExecutionAnalysis(
  scope: object,
  key: string,
): void {
  const sessions = sessionsFor(scope);
  const entry = sessions.get(key);
  if (!entry) return;

  entry.generation += 1;
  entry.promise = undefined;
  entry.snapshot = IDLE_SNAPSHOT;
  publish(entry);

  if (entry.listeners.size === 0) {
    sessions.delete(key);
  } else {
    touch(sessions, key, entry);
  }
}

export function runQueryExecutionAnalysis(
  scope: object,
  key: string,
  execute: () => Promise<QueryExecutionAnalysisResult>,
  mapFailure: FailureMapper,
): Promise<QueryExecutionAnalysisResult | null> {
  const sessions = sessionsFor(scope);
  const entry = sessionEntry(scope, key);
  touch(sessions, key, entry);

  if (entry.snapshot.status === 'running' && entry.promise) {
    return entry.promise;
  }

  const generation = entry.generation + 1;
  entry.generation = generation;
  entry.snapshot = {
    status: 'running',
    result: null,
    requestDurationMs: 0,
    failure: null,
  };

  const started = performance.now();
  const promise = (async () => {
    try {
      const result = await execute();
      if (entry.generation === generation) {
        entry.snapshot = {
          status: 'success',
          result,
          requestDurationMs: performance.now() - started,
          failure: null,
        };
        touch(sessions, key, entry);
        publish(entry);
      }
      return result;
    } catch (error) {
      if (entry.generation === generation) {
        entry.snapshot = {
          status: 'error',
          result: null,
          requestDurationMs: performance.now() - started,
          failure: mapFailure(error),
        };
        touch(sessions, key, entry);
        publish(entry);
      }
      return null;
    } finally {
      if (entry.generation === generation) {
        entry.promise = undefined;
        prune(sessions);
      }
    }
  })();

  entry.promise = promise;
  prune(sessions);
  publish(entry);
  return promise;
}
