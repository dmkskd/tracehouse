import { isClickHouseVersionAtLeast } from '../../../utils/clickhouse-version.js';

/** ClickHouse Docker image used by all integration test containers. */
export const CH_IMAGE =
  process.env.CLICKHOUSE_IMAGE ?? 'clickhouse/clickhouse-server:latest';

/** Numeric ClickHouse version encoded in a pinned image tag, when available. */
export const CONFIGURED_CH_VERSION =
  CH_IMAGE.slice(CH_IMAGE.lastIndexOf(':') + 1).match(/^(\d+\.\d+(?:\.\d+)?)/)?.[1] ?? null;

/**
 * Return true only when a pinned CLICKHOUSE_IMAGE is older than the minimum.
 *
 * Unknown tags such as `latest` deliberately return false so ordinary local
 * runs still exercise the test. The compatibility matrix always pins a
 * numeric image tag.
 */
export function configuredClickHouseIsBefore(
  minimumMajor: number,
  minimumMinor: number,
  minimumPatch = 0,
): boolean {
  return CONFIGURED_CH_VERSION != null
    && !isClickHouseVersionAtLeast(
      CONFIGURED_CH_VERSION,
      minimumMajor,
      minimumMinor,
      minimumPatch,
    );
}
