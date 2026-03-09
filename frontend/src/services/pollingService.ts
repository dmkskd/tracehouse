/**
 * Polling Service for managing real-time data updates.
 *
 * This service provides configurable polling intervals with pause/resume
 * functionality for fetching data from the backend at regular intervals.
 *
 * - Configurable refresh intervals (1-60 seconds)
 * - Pause/resume data fetching
 * - Manual refresh option
 */

/**
 * Minimum allowed polling interval in milliseconds (1 second)
 */
export const MIN_INTERVAL_MS = 1000;

/**
 * Maximum allowed polling interval in milliseconds (60 seconds)
 */
export const MAX_INTERVAL_MS = 60000;

/**
 * Default polling interval in milliseconds (5 seconds)
 */
export const DEFAULT_INTERVAL_MS = 5000;

/**
 * Status of a polling subscription
 */
export type PollingStatus = 'active' | 'paused' | 'stopped';

/**
 * Options for configuring a polling subscription
 */
export interface PollingOptions {
  /**
   * Polling interval in milliseconds.
   * Must be between MIN_INTERVAL_MS (1000) and MAX_INTERVAL_MS (60000).
   */
  interval: number;

  /**
   * Whether to execute the callback immediately when starting.
   * Default: true
   */
  immediate?: boolean;
}

/**
 * Internal state for a polling subscription
 */
interface PollingSubscription {
  id: string;
  callback: () => Promise<void>;
  interval: number;
  status: PollingStatus;
  timerId: ReturnType<typeof setTimeout> | null;
  lastExecutionTime: number | null;
  isExecuting: boolean;
}

/**
 * Result of a polling operation
 */
export interface PollingResult {
  success: boolean;
  error?: Error;
  executionTime?: number;
}

/**
 * Validates that an interval is within the allowed range.
 *
 * @param interval - The interval in milliseconds to validate
 * @returns true if the interval is valid, false otherwise
 */
export function isValidInterval(interval: number): boolean {
  return (
    typeof interval === 'number' &&
    !isNaN(interval) &&
    isFinite(interval) &&
    interval >= MIN_INTERVAL_MS &&
    interval <= MAX_INTERVAL_MS
  );
}

/**
 * Clamps an interval to the valid range.
 *
 * @param interval - The interval in milliseconds to clamp
 * @returns The clamped interval value
 */
export function clampInterval(interval: number): number {
  if (typeof interval !== 'number' || isNaN(interval)) {
    return DEFAULT_INTERVAL_MS;
  }
  // Handle Infinity and -Infinity by clamping to bounds
  return Math.max(MIN_INTERVAL_MS, Math.min(MAX_INTERVAL_MS, interval));
}

/**
 * Generates a unique subscription ID.
 */
function generateSubscriptionId(): string {
  return `poll_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
}

/**
 * PollingService manages multiple polling subscriptions with configurable
 * intervals and pause/resume functionality.
 */
export class PollingService {
  private subscriptions: Map<string, PollingSubscription> = new Map();

  /**
   * Starts a new polling subscription.
   *
   * @param callback - Async function to execute on each poll
   * @param options - Polling configuration options
   * @returns The subscription ID for managing this polling instance
   * @throws Error if the interval is invalid
   */
  start(callback: () => Promise<void>, options: PollingOptions): string {
    const { interval, immediate = true } = options;

    // Validate interval
    if (!isValidInterval(interval)) {
      throw new Error(
        `Invalid polling interval: ${interval}ms. Must be between ${MIN_INTERVAL_MS}ms and ${MAX_INTERVAL_MS}ms.`
      );
    }

    const id = generateSubscriptionId();

    const subscription: PollingSubscription = {
      id,
      callback,
      interval,
      status: 'active',
      timerId: null,
      lastExecutionTime: null,
      isExecuting: false,
    };

    this.subscriptions.set(id, subscription);

    // Execute immediately if requested
    if (immediate) {
      this.executeCallback(subscription);
    }

    // Schedule the next poll
    this.scheduleNextPoll(subscription);

    return id;
  }

  /**
   * Stops a polling subscription and removes it.
   *
   * @param subscriptionId - The ID of the subscription to stop
   */
  stop(subscriptionId: string): void {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      return;
    }

    // Clear any pending timer
    if (subscription.timerId !== null) {
      clearTimeout(subscription.timerId);
      subscription.timerId = null;
    }

    subscription.status = 'stopped';
    this.subscriptions.delete(subscriptionId);
  }

  /**
   * Stops all polling subscriptions.
   */
  stopAll(): void {
    for (const id of this.subscriptions.keys()) {
      this.stop(id);
    }
  }

  /**
   * Pauses a specific subscription or all subscriptions.
   *
   * @param subscriptionId - Optional ID of the subscription to pause.
   *                         If not provided, pauses all subscriptions.
   */
  pause(subscriptionId?: string): void {
    if (subscriptionId !== undefined) {
      this.pauseSubscription(subscriptionId);
    } else {
      for (const id of this.subscriptions.keys()) {
        this.pauseSubscription(id);
      }
    }
  }

  /**
   * Resumes a specific subscription or all subscriptions.
   * When resumed, immediately fetches data and then continues polling.
   *
   * @param subscriptionId - Optional ID of the subscription to resume.
   *                         If not provided, resumes all subscriptions.
   */
  resume(subscriptionId?: string): void {
    if (subscriptionId !== undefined) {
      this.resumeSubscription(subscriptionId);
    } else {
      for (const id of this.subscriptions.keys()) {
        this.resumeSubscription(id);
      }
    }
  }

  /**
   * Updates the polling interval for a subscription.
   *
   * @param subscriptionId - The ID of the subscription to update
   * @param interval - The new interval in milliseconds
   * @throws Error if the interval is invalid
   */
  setInterval(subscriptionId: string, interval: number): void {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      return;
    }

    if (!isValidInterval(interval)) {
      throw new Error(
        `Invalid polling interval: ${interval}ms. Must be between ${MIN_INTERVAL_MS}ms and ${MAX_INTERVAL_MS}ms.`
      );
    }

    subscription.interval = interval;

    // If active, reschedule with new interval
    if (subscription.status === 'active' && subscription.timerId !== null) {
      clearTimeout(subscription.timerId);
      this.scheduleNextPoll(subscription);
    }
  }

  /**
   * Gets the status of a subscription.
   *
   * @param subscriptionId - The ID of the subscription
   * @returns The status of the subscription, or 'stopped' if not found
   */
  getStatus(subscriptionId: string): PollingStatus {
    const subscription = this.subscriptions.get(subscriptionId);
    return subscription?.status ?? 'stopped';
  }

  /**
   * Gets the current interval for a subscription.
   *
   * @param subscriptionId - The ID of the subscription
   * @returns The interval in milliseconds, or null if not found
   */
  getInterval(subscriptionId: string): number | null {
    const subscription = this.subscriptions.get(subscriptionId);
    return subscription?.interval ?? null;
  }

  /**
   * Gets the last execution time for a subscription.
   *
   * @param subscriptionId - The ID of the subscription
   * @returns The timestamp of the last execution, or null if never executed
   */
  getLastExecutionTime(subscriptionId: string): number | null {
    const subscription = this.subscriptions.get(subscriptionId);
    return subscription?.lastExecutionTime ?? null;
  }

  /**
   * Checks if a subscription exists.
   *
   * @param subscriptionId - The ID of the subscription
   * @returns true if the subscription exists, false otherwise
   */
  hasSubscription(subscriptionId: string): boolean {
    return this.subscriptions.has(subscriptionId);
  }

  /**
   * Gets the count of active subscriptions.
   *
   * @returns The number of subscriptions (including paused ones)
   */
  getSubscriptionCount(): number {
    return this.subscriptions.size;
  }

  /**
   * Gets all subscription IDs.
   *
   * @returns Array of subscription IDs
   */
  getSubscriptionIds(): string[] {
    return Array.from(this.subscriptions.keys());
  }

  /**
   * Manually triggers a refresh for a subscription.
   * This executes the callback immediately regardless of the polling schedule.
   *
   * @param subscriptionId - The ID of the subscription to refresh
   * @returns Promise that resolves with the result of the callback execution
   */
  async refresh(subscriptionId: string): Promise<PollingResult> {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      return {
        success: false,
        error: new Error(`Subscription ${subscriptionId} not found`),
      };
    }

    if (subscription.status === 'stopped') {
      return {
        success: false,
        error: new Error(`Subscription ${subscriptionId} is stopped`),
      };
    }

    return this.executeCallback(subscription);
  }

  /**
   * Pauses a single subscription.
   */
  private pauseSubscription(subscriptionId: string): void {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription || subscription.status !== 'active') {
      return;
    }

    // Clear pending timer
    if (subscription.timerId !== null) {
      clearTimeout(subscription.timerId);
      subscription.timerId = null;
    }

    subscription.status = 'paused';
  }

  /**
   * Resumes a single subscription.
   */
  private resumeSubscription(subscriptionId: string): void {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription || subscription.status !== 'paused') {
      return;
    }

    subscription.status = 'active';

    // Execute immediately on resume
    this.executeCallback(subscription);

    // Schedule next poll
    this.scheduleNextPoll(subscription);
  }

  /**
   * Executes the callback for a subscription.
   */
  private async executeCallback(
    subscription: PollingSubscription
  ): Promise<PollingResult> {
    // Prevent concurrent executions
    if (subscription.isExecuting) {
      return {
        success: false,
        error: new Error('Callback is already executing'),
      };
    }

    subscription.isExecuting = true;
    const startTime = Date.now();

    try {
      await subscription.callback();
      subscription.lastExecutionTime = Date.now();
      return {
        success: true,
        executionTime: Date.now() - startTime,
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error : new Error(String(error)),
        executionTime: Date.now() - startTime,
      };
    } finally {
      subscription.isExecuting = false;
    }
  }

  /**
   * Schedules the next poll for a subscription.
   */
  private scheduleNextPoll(subscription: PollingSubscription): void {
    // Don't schedule if not active
    if (subscription.status !== 'active') {
      return;
    }

    // Clear any existing timer
    if (subscription.timerId !== null) {
      clearTimeout(subscription.timerId);
    }

    subscription.timerId = setTimeout(async () => {
      // Check if still active before executing
      if (subscription.status === 'active') {
        await this.executeCallback(subscription);
        // Schedule next poll after execution
        this.scheduleNextPoll(subscription);
      }
    }, subscription.interval);
  }
}

/**
 * Default singleton instance of the polling service.
 * Use this for application-wide polling management.
 */
export const pollingService = new PollingService();

export default pollingService;
