export interface RequestGate {
  begin(): number;
  invalidate(): void;
  isLatest(requestId: number): boolean;
}

/**
 * Keeps asynchronous responses aligned with the latest request.
 * Starting or invalidating a request makes every older request stale.
 */
export function createRequestGate(): RequestGate {
  let latestRequestId = 0;
  return {
    begin: () => ++latestRequestId,
    invalidate: () => { latestRequestId += 1; },
    isLatest: requestId => requestId === latestRequestId,
  };
}
