/**
 * Start a container, replacing it when Docker never publishes its host ports.
 *
 * Docker Desktop's engine assigns host ports inside its VM (range 55000-65535 by
 * default) without knowing which ports are in use on the host. When a host
 * process already listens on the assigned port, Docker Desktop cannot forward it,
 * the container runs without published ports, and testcontainers fails after its
 * fixed 10 s wait with "Timed out after 10000ms while waiting for container ports
 * to be bound to the host". On macOS, rapportd listening on a port in that range
 * makes every full integration run fail this way. A new container receives the
 * next ports in the engine's range.
 *
 * testcontainers throws before it returns a handle, so each attempt carries a
 * unique label and a failed attempt's container is found by that label, stopped
 * and removed before the next attempt. Removing it matters for cluster setups:
 * left running, it would keep its network alias (`keeper`, a node name) next to
 * its replacement. Any other startup error is rethrown on the first attempt.
 *
 * `build` must return a new container definition on each call: `start()`
 * mutates the definition it is called on.
 */

import { randomUUID } from 'node:crypto';
import { getContainerRuntimeClient } from 'testcontainers';

const PORTS_NOT_BOUND = 'while waiting for container ports to be bound';
const ATTEMPT_LABEL = 'tracehouse.integration.start-attempt';

interface ContainerDefinition<T> {
  withLabels(labels: Record<string, string>): { start(): Promise<T> };
}

export interface FailedAttemptRemover {
  (label: string, value: string): Promise<void>;
}

async function removeByLabel(label: string, value: string): Promise<void> {
  const client = await getContainerRuntimeClient();
  const container = await client.container.fetchByLabel(label, value);
  if (!container) return;
  await client.container.stop(container, { timeout: 0 });
  await client.container.remove(container, { removeVolumes: true });
}

export async function startWithPortRetry<T>(
  build: () => ContainerDefinition<T>,
  attempts = 3,
  removeFailedAttempt: FailedAttemptRemover = removeByLabel,
): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    const attemptId = randomUUID();
    try {
      return await build().withLabels({ [ATTEMPT_LABEL]: attemptId }).start();
    } catch (error) {
      const portsNotBound = error instanceof Error && error.message.includes(PORTS_NOT_BOUND);
      if (!portsNotBound) throw error;
      await removeFailedAttempt(ATTEMPT_LABEL, attemptId);
      if (attempt >= attempts) throw error;
      console.warn(
        `[integration] container host ports were not published (attempt ${attempt} of ${attempts}); removed it and starting a new container`,
      );
    }
  }
}
