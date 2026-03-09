/** @deprecated Use @tracehouse/core services via ClickHouseProvider instead */
export { default as api } from './api';
export type { ApiResponse, PaginatedResponse } from './api';

export {
  PollingService,
  pollingService,
  isValidInterval,
  clampInterval,
  MIN_INTERVAL_MS,
  MAX_INTERVAL_MS,
  DEFAULT_INTERVAL_MS,
} from './pollingService';
export type {
  PollingOptions,
  PollingStatus,
  PollingResult,
} from './pollingService';

export {
  ConnectionRetryService,
  connectionRetryService,
  calculateBackoffDelay,
  isValidRetryConfig,
  DEFAULT_INITIAL_DELAY_MS,
  DEFAULT_MAX_DELAY_MS,
  DEFAULT_RETRY_CONFIG,
} from './connectionRetry';
export type {
  ConnectionStatus,
  RetryConfig,
  StatusChangeCallback,
} from './connectionRetry';
