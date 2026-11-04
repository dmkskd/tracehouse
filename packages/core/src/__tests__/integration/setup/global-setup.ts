/**
 * Vitest globalSetup for integration tests: checks once, before any suite
 * starts, that Testcontainers can reach a container runtime. Without this,
 * every suite fails separately with the same opaque
 * "Could not find a working container runtime strategy" stack trace.
 *
 * With CH_TEST_URL set, single-node suites use that instance and only the
 * cluster suites need Docker, so a missing runtime is a warning, not a stop.
 */

import { getContainerRuntimeClient } from 'testcontainers';

export default async function setup(): Promise<void> {
  try {
    await getContainerRuntimeClient();
    return;
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    const help = [
      'Docker is not reachable, so ClickHouse test containers cannot start.',
      `  Testcontainers: ${reason}`,
      '  Start Docker Desktop (or your runtime) and check with: docker info',
      '  A non-default socket can be set with DOCKER_HOST.',
    ];

    if (process.env.CH_TEST_URL) {
      console.warn([
        ...help,
        `  CH_TEST_URL=${process.env.CH_TEST_URL} is set: single-node suites will use it,`,
        '  but cluster suites still need Docker and will fail.',
      ].join('\n'));
      return;
    }

    throw new Error([
      ...help,
      '  Or run single-node suites against an existing server: CH_TEST_URL=http://localhost:8123',
    ].join('\n'));
  }
}
