/**
 * Unit tests for the PollingService.
 *
 * Tests core polling behavior: start/stop, pause/resume, interval changes,
 * manual refresh, error handling, and concurrent subscriptions.
 */

import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  PollingService,
  isValidInterval,
  clampInterval,
  MIN_INTERVAL_MS,
  MAX_INTERVAL_MS,
  DEFAULT_INTERVAL_MS,
} from '../pollingService';

describe('PollingService', () => {
  let service: PollingService;

  beforeEach(() => {
    vi.useFakeTimers();
    service = new PollingService();
  });

  afterEach(() => {
    service.stopAll();
    vi.useRealTimers();
  });

  describe('interval validation', () => {
    test('validates interval bounds', () => {
      expect(isValidInterval(MIN_INTERVAL_MS)).toBe(true);
      expect(isValidInterval(MAX_INTERVAL_MS)).toBe(true);
      expect(isValidInterval(5000)).toBe(true);
      expect(isValidInterval(0)).toBe(false);
      expect(isValidInterval(999)).toBe(false);
      expect(isValidInterval(60001)).toBe(false);
      expect(isValidInterval(NaN)).toBe(false);
    });

    test('clamps values to valid range', () => {
      expect(clampInterval(500)).toBe(MIN_INTERVAL_MS);
      expect(clampInterval(100000)).toBe(MAX_INTERVAL_MS);
      expect(clampInterval(5000)).toBe(5000);
      expect(clampInterval(NaN)).toBe(DEFAULT_INTERVAL_MS);
    });
  });

  describe('start/stop', () => {
    test('starts polling and executes at interval', async () => {
      const callback = vi.fn().mockResolvedValue(undefined);
      service.start(callback, { interval: 5000, immediate: false });

      await vi.advanceTimersByTimeAsync(5000);
      expect(callback).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(5000);
      expect(callback).toHaveBeenCalledTimes(2);
    });

    test('executes immediately when immediate is true', async () => {
      const callback = vi.fn().mockResolvedValue(undefined);
      const id = service.start(callback, { interval: 5000 });
      await Promise.resolve();

      expect(callback).toHaveBeenCalledTimes(1);
      service.stop(id);
    });

    test('stop prevents further executions', async () => {
      const callback = vi.fn().mockResolvedValue(undefined);
      const id = service.start(callback, { interval: 5000, immediate: false });

      service.stop(id);
      await vi.advanceTimersByTimeAsync(10000);
      expect(callback).not.toHaveBeenCalled();
    });

    test('stopAll removes all subscriptions', () => {
      const cb = vi.fn().mockResolvedValue(undefined);
      service.start(cb, { interval: 5000, immediate: false });
      service.start(cb, { interval: 5000, immediate: false });
      expect(service.getSubscriptionCount()).toBe(2);

      service.stopAll();
      expect(service.getSubscriptionCount()).toBe(0);
    });

    test('throws for invalid interval', () => {
      const cb = vi.fn().mockResolvedValue(undefined);
      expect(() => service.start(cb, { interval: 500 })).toThrow(/Invalid polling interval/);
    });
  });

  describe('pause/resume', () => {
    test('pause stops polling, resume restarts with immediate fetch', async () => {
      const callback = vi.fn().mockResolvedValue(undefined);
      const id = service.start(callback, { interval: 5000, immediate: false });

      service.pause(id);
      expect(service.getStatus(id)).toBe('paused');

      await vi.advanceTimersByTimeAsync(10000);
      expect(callback).not.toHaveBeenCalled();

      service.resume(id);
      await Promise.resolve();
      expect(callback).toHaveBeenCalledTimes(1);
      expect(service.getStatus(id)).toBe('active');

      service.stop(id);
    });

    test('pause/resume all subscriptions when no ID provided', async () => {
      const cb1 = vi.fn().mockResolvedValue(undefined);
      const cb2 = vi.fn().mockResolvedValue(undefined);
      const id1 = service.start(cb1, { interval: 5000, immediate: false });
      const id2 = service.start(cb2, { interval: 5000, immediate: false });

      service.pause();
      expect(service.getStatus(id1)).toBe('paused');
      expect(service.getStatus(id2)).toBe('paused');

      service.resume();
      await Promise.resolve();
      expect(cb1).toHaveBeenCalledTimes(1);
      expect(cb2).toHaveBeenCalledTimes(1);

      service.stopAll();
    });
  });

  describe('setInterval', () => {
    test('updates polling interval', async () => {
      const callback = vi.fn().mockResolvedValue(undefined);
      const id = service.start(callback, { interval: 5000, immediate: false });

      service.setInterval(id, 2000);
      expect(service.getInterval(id)).toBe(2000);

      await vi.advanceTimersByTimeAsync(2000);
      expect(callback).toHaveBeenCalledTimes(1);
    });
  });

  describe('refresh', () => {
    test('executes callback immediately', async () => {
      const callback = vi.fn().mockResolvedValue(undefined);
      const id = service.start(callback, { interval: 5000, immediate: false });

      const result = await service.refresh(id);
      expect(result.success).toBe(true);
      expect(callback).toHaveBeenCalledTimes(1);
    });

    test('works on paused subscriptions', async () => {
      const callback = vi.fn().mockResolvedValue(undefined);
      const id = service.start(callback, { interval: 5000, immediate: false });
      service.pause(id);

      const result = await service.refresh(id);
      expect(result.success).toBe(true);
    });

    test('returns error for non-existent subscription', async () => {
      const result = await service.refresh('non-existent');
      expect(result.success).toBe(false);
    });
  });

  describe('error handling', () => {
    test('callback errors do not stop subsequent polls', async () => {
      const callback = vi.fn()
        .mockRejectedValueOnce(new Error('fail'))
        .mockResolvedValue(undefined);

      service.start(callback, { interval: 5000, immediate: false });

      await vi.advanceTimersByTimeAsync(5000);
      await vi.advanceTimersByTimeAsync(5000);
      expect(callback).toHaveBeenCalledTimes(2);
    });

    test('prevents concurrent callback executions', async () => {
      let resolveCallback: () => void;
      const callback = vi.fn().mockReturnValue(
        new Promise<void>((resolve) => { resolveCallback = resolve; })
      );
      const id = service.start(callback, { interval: 5000, immediate: true });
      await Promise.resolve();

      const result = await service.refresh(id);
      expect(result.success).toBe(false);
      expect(result.error?.message).toContain('already executing');

      resolveCallback!();
      await Promise.resolve();
      service.stop(id);
    });
  });

  describe('concurrent subscriptions', () => {
    test('multiple subscriptions run independently', async () => {
      const cb1 = vi.fn().mockResolvedValue(undefined);
      const cb2 = vi.fn().mockResolvedValue(undefined);

      service.start(cb1, { interval: 2000, immediate: false });
      service.start(cb2, { interval: 3000, immediate: false });

      await vi.advanceTimersByTimeAsync(2000);
      expect(cb1).toHaveBeenCalledTimes(1);
      expect(cb2).toHaveBeenCalledTimes(0);

      await vi.advanceTimersByTimeAsync(1000); // 3000ms total
      expect(cb2).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(1000); // 4000ms total
      expect(cb1).toHaveBeenCalledTimes(2);
    });
  });
});
