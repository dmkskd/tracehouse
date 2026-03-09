/**
 * Unit tests for the ConnectionRetryService.
 *
 * Tests core retry behavior: exponential backoff, start/stop, manual retry,
 * status callbacks, and maxAttempts.
 */

import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  ConnectionRetryService,
  calculateBackoffDelay,
  isValidRetryConfig,
  type RetryConfig,
} from '../connectionRetry';

describe('ConnectionRetryService', () => {
  let service: ConnectionRetryService;

  beforeEach(() => {
    vi.useFakeTimers();
    service = new ConnectionRetryService();
  });

  afterEach(() => {
    service.stop();
    vi.useRealTimers();
  });

  describe('calculateBackoffDelay', () => {
    test('follows exponential formula and caps at maxDelay', () => {
      const config: RetryConfig = { initialDelayMs: 1000, maxDelayMs: 30000 };

      expect(calculateBackoffDelay(0, config)).toBe(1000);
      expect(calculateBackoffDelay(1, config)).toBe(2000);
      expect(calculateBackoffDelay(2, config)).toBe(4000);
      expect(calculateBackoffDelay(3, config)).toBe(8000);
      expect(calculateBackoffDelay(4, config)).toBe(16000);
      expect(calculateBackoffDelay(5, config)).toBe(30000); // capped
      expect(calculateBackoffDelay(100, config)).toBe(30000); // still capped
    });

    test('treats negative attempts as 0', () => {
      const config: RetryConfig = { initialDelayMs: 1000, maxDelayMs: 30000 };
      expect(calculateBackoffDelay(-5, config)).toBe(1000);
    });
  });

  describe('isValidRetryConfig', () => {
    test('validates config correctly', () => {
      expect(isValidRetryConfig({ initialDelayMs: 1000, maxDelayMs: 30000 })).toBe(true);
      expect(isValidRetryConfig({ initialDelayMs: 500, maxDelayMs: 500 })).toBe(true);
      expect(isValidRetryConfig({ initialDelayMs: 0, maxDelayMs: 30000 })).toBe(false);
      expect(isValidRetryConfig({ initialDelayMs: 5000, maxDelayMs: 1000 })).toBe(false);
      expect(isValidRetryConfig({ initialDelayMs: 1000, maxDelayMs: 30000, maxAttempts: 0 })).toBe(false);
      expect(isValidRetryConfig(null as unknown as RetryConfig)).toBe(false);
    });
  });

  describe('start/stop lifecycle', () => {
    test('connects immediately and sets status on success', async () => {
      const connectFn = vi.fn().mockResolvedValue(true);
      service.start(connectFn);
      await Promise.resolve();

      expect(connectFn).toHaveBeenCalledTimes(1);
      expect(service.getStatus()).toBe('connected');
      expect(service.getAttemptCount()).toBe(0);
    });

    test('retries with exponential backoff on failure', async () => {
      const connectFn = vi.fn().mockResolvedValue(false);
      service.start(connectFn);

      await Promise.resolve();
      expect(service.getStatus()).toBe('reconnecting');
      expect(connectFn).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(2000); // attempt 1 delay
      expect(connectFn).toHaveBeenCalledTimes(2);

      await vi.advanceTimersByTimeAsync(4000); // attempt 2 delay
      expect(connectFn).toHaveBeenCalledTimes(3);
    });

    test('stops retrying when connection succeeds', async () => {
      const connectFn = vi.fn()
        .mockResolvedValueOnce(false)
        .mockResolvedValueOnce(true);

      service.start(connectFn);
      await Promise.resolve();
      await vi.advanceTimersByTimeAsync(2000);

      expect(service.getStatus()).toBe('connected');
      expect(service.getAttemptCount()).toBe(0);

      await vi.advanceTimersByTimeAsync(10000);
      expect(connectFn).toHaveBeenCalledTimes(2); // no more retries
    });

    test('respects maxAttempts', async () => {
      const connectFn = vi.fn().mockResolvedValue(false);
      service.start(connectFn, { initialDelayMs: 1000, maxDelayMs: 30000, maxAttempts: 2 });

      await Promise.resolve(); // attempt 1
      await vi.advanceTimersByTimeAsync(2000); // attempt 2

      await vi.advanceTimersByTimeAsync(30000);
      expect(connectFn).toHaveBeenCalledTimes(2);
      expect(service.getStatus()).toBe('disconnected');
    });

    test('stop cancels pending retries', async () => {
      const connectFn = vi.fn().mockResolvedValue(false);
      service.start(connectFn);
      await Promise.resolve();

      service.stop();
      expect(service.getStatus()).toBe('disconnected');

      await vi.advanceTimersByTimeAsync(10000);
      expect(connectFn).toHaveBeenCalledTimes(1);
    });
  });

  describe('manual retry', () => {
    test('retries immediately and returns result', async () => {
      const connectFn = vi.fn()
        .mockResolvedValueOnce(false)
        .mockResolvedValueOnce(true);

      service.start(connectFn);
      await Promise.resolve();

      const result = await service.retry();
      expect(result).toBe(true);
      expect(service.getStatus()).toBe('connected');
    });

    test('returns false when service not started', async () => {
      expect(await service.retry()).toBe(false);
    });
  });

  describe('status callbacks', () => {
    test('notifies on status changes and supports unsubscribe', async () => {
      const callback = vi.fn();
      const unsubscribe = service.onStatusChange(callback);

      const connectFn = vi.fn().mockResolvedValue(true);
      service.start(connectFn);
      await Promise.resolve();

      expect(callback).toHaveBeenCalledWith('connected');

      unsubscribe();
      callback.mockClear();

      service.stop();
      // After unsubscribe, callback should not be called
      expect(callback).not.toHaveBeenCalled();
    });
  });

  describe('notifyDisconnected / notifyConnected', () => {
    test('notifyDisconnected triggers reconnection when connected', async () => {
      const connectFn = vi.fn().mockResolvedValue(true);
      service.start(connectFn);
      await Promise.resolve();

      service.notifyDisconnected();
      expect(service.getStatus()).toBe('reconnecting');

      await vi.advanceTimersByTimeAsync(1000);
      expect(connectFn).toHaveBeenCalledTimes(2);
    });

    test('notifyConnected resets state and clears retries', async () => {
      const connectFn = vi.fn().mockResolvedValue(false);
      service.start(connectFn);
      await Promise.resolve();

      service.notifyConnected();
      expect(service.getStatus()).toBe('connected');
      expect(service.getAttemptCount()).toBe(0);

      await vi.advanceTimersByTimeAsync(10000);
      expect(connectFn).toHaveBeenCalledTimes(1); // no more retries
    });
  });
});
