export function updateTimeTravelHostSelection(
  selectedHosts: readonly string[],
  host: string,
  additive: boolean,
): string[] {
  if (!additive) return [host];
  if (selectedHosts.includes(host)) {
    return selectedHosts.filter(selected => selected !== host);
  }
  return [...selectedHosts, host];
}

export function timeTravelHostnameFilter(
  selectedHosts: readonly string[],
): readonly string[] | null {
  return selectedHosts.length > 0 ? selectedHosts : null;
}

export function timeTravelRowHosts(
  clusterHosts: readonly string[],
  selectedHosts: readonly string[],
  perServerView: boolean,
): string[] {
  if (!perServerView) return [];
  if (selectedHosts.length === 0) return [...clusterHosts];
  return clusterHosts.filter(host => selectedHosts.includes(host));
}

export interface TimeTravelRequestGate {
  begin(): number;
  invalidate(): void;
  isLatest(requestId: number): boolean;
}

/**
 * Keeps asynchronous timeline responses aligned with the current host scope.
 * Starting or invalidating a request makes every older request stale.
 */
export function createTimeTravelRequestGate(): TimeTravelRequestGate {
  let latestRequestId = 0;
  return {
    begin: () => ++latestRequestId,
    invalidate: () => { latestRequestId += 1; },
    isLatest: requestId => requestId === latestRequestId,
  };
}
