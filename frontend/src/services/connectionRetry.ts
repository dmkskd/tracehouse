/**
 * Connection retry with exponential backoff (1s initial, 30s max).
 * Automatically reconnects on failure until the connection is restored or manually stopped.
 */

/**
 * Default initial delay in milliseconds (1 second)
 */
export const DEFAULT_INITIAL_DELAY_MS = 1000;

/**
 * Default maximum delay in milliseconds (30 seconds)
 */
export const DEFAULT_MAX_DELAY_MS = 30000;

/**
 * Connection status types
 */
export type ConnectionStatus = 'connected' | 'disconnected' | 'reconnecting';

/**
 * Configuration for retry behavior
 */
export interface RetryConfig {
  /**
   * Initial delay in milliseconds before first retry.
   * Default: 1000ms (1 second)
   */
  initialDelayMs: number;

  /**
   * Maximum delay in milliseconds between retries.
   * Default: 30000ms (30 seconds)
   */
  maxDelayMs: number;

  /**
   * Optional maximum number of retry attempts.
   * If not specified, retries indefinitely until stopped or connected.
   */
  maxAttempts?: number;
}

/**
 * Default retry configuration
 */
export const DEFAULT_RETRY_CONFIG: RetryConfig = {
  initialDelayMs: DEFAULT_INITIAL_DELAY_MS,
  maxDelayMs: DEFAULT_MAX_DELAY_MS,
};

/**
 * Status change callback type
 */
export type StatusChangeCallback = (status: ConnectionStatus) => void;

/**
 * Calculates the delay for a given attempt using exponential backoff.
 *
 * Formula: delay = min(initialDelay * 2^attempt, maxDelay)
 *
 * @param attempt - The current attempt number (0-indexed)
 * @param config - The retry configuration
 * @returns The delay in milliseconds
 */
export function calculateBackoffDelay(
  attempt: number,
  config: RetryConfig
): number {
  const { initialDelayMs, maxDelayMs } = config;

  // Ensure attempt is non-negative
  const safeAttempt = Math.max(0, attempt);

  // Calculate exponential delay: initialDelay * 2^attempt
  const exponentialDelay = initialDelayMs * Math.pow(2, safeAttempt);

  // Cap at maxDelay
  return Math.min(exponentialDelay, maxDelayMs);
}

/**
 * Validates a retry configuration.
 *
 * @param config - The configuration to validate
 * @returns true if valid, false otherwise
 */
export function isValidRetryConfig(config: RetryConfig): boolean {
  if (!config) return false;

  const { initialDelayMs, maxDelayMs, maxAttempts } = config;

  // initialDelayMs must be a positive number
  if (
    typeof initialDelayMs !== 'number' ||
    isNaN(initialDelayMs) ||
    initialDelayMs <= 0
  ) {
    return false;
  }

  // maxDelayMs must be a positive number >= initialDelayMs
  if (
    typeof maxDelayMs !== 'number' ||
    isNaN(maxDelayMs) ||
    maxDelayMs <= 0 ||
    maxDelayMs < initialDelayMs
  ) {
    return false;
  }

  // maxAttempts, if provided, must be a positive integer
  if (maxAttempts !== undefined) {
    if (
      typeof maxAttempts !== 'number' ||
      isNaN(maxAttempts) ||
      maxAttempts <= 0 ||
      !Number.isInteger(maxAttempts)
    ) {
      return false;
    }
  }

  return true;
}

/**
 * ConnectionRetryService manages automatic reconnection with exponential backoff.
 *
 * Usage:
 * ```typescript
 * const retryService = new ConnectionRetryService();
 *
 * // Subscribe to status changes
 * const unsubscribe = retryService.onStatusChange((status) => {
 *   console.log('Connection status:', status);
 * });
 *
 * // Start retry logic with a connect function
 * retryService.start(async () => {
 *   const result = await api.connect();
 *   return result.success;
 * });
 *
 * // Manual retry
 * await retryService.retry();
 *
 * // Stop retrying
 * retryService.stop();
 *
 * // Cleanup
 * unsubscribe();
 * ```
 */
export class ConnectionRetryService {
  private status: ConnectionStatus = 'disconnected';
  private attemptCount: number = 0;
  private config: RetryConfig = DEFAULT_RETRY_CONFIG;
  private connectFn: (() => Promise<boolean>) | null = null;
  private retryTimerId: ReturnType<typeof setTimeout> | null = null;
  private statusChangeCallbacks: Set<StatusChangeCallback> = new Set();
  private isRetrying: boolean = false;
  private isStopped: boolean = true;

  /**
   * Starts the connection retry service.
   *
   * @param connectFn - Async function that attempts to connect. Returns true on success, false on failure.
   * @param config - Optional retry configuration. Uses defaults if not provided.
   */
  start(
    connectFn: () => Promise<boolean>,
    config: RetryConfig = DEFAULT_RETRY_CONFIG
  ): void {
    // Validate config
    if (!isValidRetryConfig(config)) {
      throw new Error('Invalid retry configuration');
    }

    // Stop any existing retry process
    this.stop();

    this.connectFn = connectFn;
    this.config = { ...config };
    this.attemptCount = 0;
    this.isStopped = false;

    // Attempt initial connection
    this.attemptConnection();
  }

  /**
   * Stops the connection retry service.
   * Clears any pending retry timers and resets state.
   */
  stop(): void {
    this.isStopped = true;

    // Clear any pending retry timer
    if (this.retryTimerId !== null) {
      clearTimeout(this.retryTimerId);
      this.retryTimerId = null;
    }

    this.connectFn = null;
    this.isRetrying = false;

    // Update status to disconnected if we were reconnecting
    if (this.status === 'reconnecting') {
      this.setStatus('disconnected');
    }
  }

  /**
   * Manually triggers a retry attempt.
   * Resets the attempt count for fresh exponential backoff.
   *
   * @returns Promise that resolves to true if connection succeeded, false otherwise
   */
  async retry(): Promise<boolean> {
    if (!this.connectFn) {
      return false;
    }

    // Clear any pending retry timer
    if (this.retryTimerId !== null) {
      clearTimeout(this.retryTimerId);
      this.retryTimerId = null;
    }

    // Reset attempt count for manual retry
    this.attemptCount = 0;
    this.isStopped = false;

    return this.attemptConnection();
  }

  /**
   * Gets the current connection status.
   *
   * @returns The current connection status
   */
  getStatus(): ConnectionStatus {
    return this.status;
  }

  /**
   * Gets the current attempt count.
   *
   * @returns The number of connection attempts made
   */
  getAttemptCount(): number {
    return this.attemptCount;
  }

  /**
   * Gets the delay before the next retry attempt.
   *
   * @returns The delay in milliseconds, or 0 if not in retry mode
   */
  getNextRetryDelay(): number {
    if (this.status !== 'reconnecting') {
      return 0;
    }
    return calculateBackoffDelay(this.attemptCount, this.config);
  }

  /**
   * Gets the current retry configuration.
   *
   * @returns A copy of the current retry configuration
   */
  getConfig(): RetryConfig {
    return { ...this.config };
  }

  /**
   * Registers a callback to be notified of status changes.
   *
   * @param callback - Function to call when status changes
   * @returns Unsubscribe function to remove the callback
   */
  onStatusChange(callback: StatusChangeCallback): () => void {
    this.statusChangeCallbacks.add(callback);

    // Return unsubscribe function
    return () => {
      this.statusChangeCallbacks.delete(callback);
    };
  }

  /**
   * Notifies the service that the connection was lost.
   * This triggers the retry logic if the service is active.
   */
  notifyDisconnected(): void {
    if (this.isStopped || !this.connectFn) {
      this.setStatus('disconnected');
      return;
    }

    // Only start retry if we were connected
    if (this.status === 'connected') {
      this.setStatus('reconnecting');
      this.scheduleRetry();
    }
  }

  /**
   * Notifies the service that the connection was established.
   * This resets the retry state.
   */
  notifyConnected(): void {
    // Clear any pending retry timer
    if (this.retryTimerId !== null) {
      clearTimeout(this.retryTimerId);
      this.retryTimerId = null;
    }

    this.attemptCount = 0;
    this.isRetrying = false;
    this.setStatus('connected');
  }

  /**
   * Checks if the service is currently active (started and not stopped).
   *
   * @returns true if the service is active, false otherwise
   */
  isActive(): boolean {
    return !this.isStopped && this.connectFn !== null;
  }

  /**
   * Sets the status and notifies all callbacks.
   */
  private setStatus(newStatus: ConnectionStatus): void {
    if (this.status === newStatus) {
      return;
    }

    this.status = newStatus;

    // Notify all callbacks
    for (const callback of this.statusChangeCallbacks) {
      try {
        callback(newStatus);
      } catch (err) {
        console.warn('[ConnectionRetry] Status change callback error:', err);
      }
    }
  }

  /**
   * Attempts to establish a connection.
   *
   * @returns Promise that resolves to true if connection succeeded, false otherwise
   */
  private async attemptConnection(): Promise<boolean> {
    if (!this.connectFn || this.isStopped) {
      return false;
    }

    // Prevent concurrent connection attempts
    if (this.isRetrying) {
      return false;
    }

    this.isRetrying = true;
    this.setStatus('reconnecting');

    try {
      const success = await this.connectFn();

      if (success) {
        this.notifyConnected();
        return true;
      } else {
        // Connection failed, schedule retry
        this.attemptCount++;
        this.scheduleRetry();
        return false;
      }
    } catch {
      // Connection threw an error, schedule retry
      this.attemptCount++;
      this.scheduleRetry();
      return false;
    } finally {
      this.isRetrying = false;
    }
  }

  /**
   * Schedules the next retry attempt.
   */
  private scheduleRetry(): void {
    if (this.isStopped || !this.connectFn) {
      return;
    }

    // Check if we've exceeded max attempts
    if (
      this.config.maxAttempts !== undefined &&
      this.attemptCount >= this.config.maxAttempts
    ) {
      this.setStatus('disconnected');
      return;
    }

    // Calculate delay for this attempt
    const delay = calculateBackoffDelay(this.attemptCount, this.config);

    // Clear any existing timer
    if (this.retryTimerId !== null) {
      clearTimeout(this.retryTimerId);
    }

    // Schedule next attempt
    this.retryTimerId = setTimeout(() => {
      this.retryTimerId = null;
      this.attemptConnection();
    }, delay);
  }
}

/**
 * Default singleton instance of the connection retry service.
 * Use this for application-wide connection retry management.
 */
export const connectionRetryService = new ConnectionRetryService();

export default connectionRetryService;
