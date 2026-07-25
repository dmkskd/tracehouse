export interface ClickHouseVersion {
  major: number;
  minor: number;
  patch: number;
}

/**
 * Parse the numeric prefix of a ClickHouse version string.
 *
 * ClickHouse versions normally look like `26.7.1.1315`, but build metadata
 * may follow the numeric components. Unknown or malformed versions return
 * null so capability callers fail closed.
 */
export function parseClickHouseVersion(version: string): ClickHouseVersion | null {
  const match = version.trim().match(/^(\d+)\.(\d+)(?:\.(\d+))?/);
  if (!match) return null;

  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3] ?? 0),
  };
}

/** Compare a server version with a minimum supported version. */
export function isClickHouseVersionAtLeast(
  version: string,
  minimumMajor: number,
  minimumMinor: number,
  minimumPatch = 0,
): boolean {
  const parsed = parseClickHouseVersion(version);
  if (!parsed) return false;

  if (parsed.major !== minimumMajor) return parsed.major > minimumMajor;
  if (parsed.minor !== minimumMinor) return parsed.minor > minimumMinor;
  return parsed.patch >= minimumPatch;
}
