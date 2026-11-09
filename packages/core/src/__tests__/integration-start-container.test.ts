import { describe, expect, it, vi } from 'vitest';
import { startWithPortRetry } from './integration/setup/start-container.js';

const PORTS_NOT_BOUND = new Error('Timed out after 10000ms while waiting for container ports to be bound to the host');

/**
 * Container definitions whose start() returns the next outcome, one definition per
 * build() call. Records the labels each attempt was started with.
 */
function definitions(outcomes: Array<Error | string>) {
  const labels: Array<Record<string, string>> = [];
  const build = vi.fn(() => ({
    withLabels(attemptLabels: Record<string, string>) {
      labels.push(attemptLabels);
      const outcome = outcomes[labels.length - 1];
      return {
        start: () => outcome instanceof Error ? Promise.reject(outcome) : Promise.resolve(outcome),
      };
    },
  }));
  return { build, labels };
}

describe('startWithPortRetry', () => {
  it('removes the container whose ports were not published and starts a new one', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    const { build, labels } = definitions([PORTS_NOT_BOUND, 'started']);
    const remove = vi.fn(async () => {});

    await expect(startWithPortRetry(build, 3, remove)).resolves.toBe('started');

    expect(build).toHaveBeenCalledTimes(2);
    const [[label, value]] = remove.mock.calls as unknown as Array<[string, string]>;
    expect(remove).toHaveBeenCalledTimes(1);
    expect(labels[0]).toEqual({ [label]: value });
    expect(labels[1]![label]).not.toBe(value);
  });

  it('rethrows any other startup error without retrying or removing anything', async () => {
    const pullFailed = new Error('pull access denied for clickhouse/clickhouse-server');
    const { build } = definitions([pullFailed, 'started']);
    const remove = vi.fn(async () => {});

    await expect(startWithPortRetry(build, 3, remove)).rejects.toBe(pullFailed);

    expect(build).toHaveBeenCalledTimes(1);
    expect(remove).not.toHaveBeenCalled();
  });

  it('removes every failed attempt and rethrows the port error after the last one', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    const { build } = definitions([PORTS_NOT_BOUND, PORTS_NOT_BOUND, PORTS_NOT_BOUND, 'started']);
    const remove = vi.fn(async () => {});

    await expect(startWithPortRetry(build, 3, remove)).rejects.toBe(PORTS_NOT_BOUND);

    expect(build).toHaveBeenCalledTimes(3);
    expect(remove).toHaveBeenCalledTimes(3);
  });
});
