/**
 * Connection Status Indicator Component
 *
 * Displays the current connection status with visual feedback and retry controls.
 * Integrates with the ConnectionRetryService to show reconnection progress.
 *
 * - Visual indicator for connection status (connected, disconnected, reconnecting)
 * - Shows retry attempt count and next retry delay
 * - Provides manual retry button
 */

import React, { useEffect, useState, useCallback } from 'react';
import {
  ConnectionRetryService,
  type ConnectionStatus,
} from '../../services/connectionRetry';

interface ConnectionStatusIndicatorProps {
  /**
   * The retry service instance to monitor.
   * If not provided, creates a display-only indicator.
   */
  retryService?: ConnectionRetryService;

  /**
   * Initial status to display when no retry service is provided.
   */
  initialStatus?: ConnectionStatus;

  /**
   * Callback when manual retry is triggered.
   */
  onRetry?: () => Promise<void>;

  /**
   * Whether to show the retry button.
   * Default: true
   */
  showRetryButton?: boolean;

  /**
   * Whether to show detailed retry information (attempt count, next delay).
   * Default: true
   */
  showDetails?: boolean;

  /**
   * Additional CSS classes for the container.
   */
  className?: string;
}

/**
 * Status configuration for visual display
 */
const STATUS_CONFIG: Record<
  ConnectionStatus,
  {
    label: string;
    color: string;
    bgColor: string;
    icon: string;
    pulse: boolean;
  }
> = {
  connected: {
    label: 'Connected',
    color: 'text-green-600',
    bgColor: 'bg-green-100',
    icon: '●',
    pulse: false,
  },
  disconnected: {
    label: 'Disconnected',
    color: 'text-red-600',
    bgColor: 'bg-red-100',
    icon: '●',
    pulse: false,
  },
  reconnecting: {
    label: 'Reconnecting',
    color: 'text-yellow-600',
    bgColor: 'bg-yellow-100',
    icon: '●',
    pulse: true,
  },
};

/**
 * Formats milliseconds into a human-readable string.
 */
function formatDelay(ms: number): string {
  if (ms < 1000) {
    return `${ms}ms`;
  }
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return remainingSeconds > 0 ? `${minutes}m ${remainingSeconds}s` : `${minutes}m`;
}

/**
 * ConnectionStatusIndicator displays the current connection status
 * with visual feedback and optional retry controls.
 */
export const ConnectionStatusIndicator: React.FC<ConnectionStatusIndicatorProps> = ({
  retryService,
  initialStatus = 'disconnected',
  onRetry,
  showRetryButton = true,
  showDetails = true,
  className = '',
}) => {
  const [status, setStatus] = useState<ConnectionStatus>(
    retryService?.getStatus() ?? initialStatus
  );
  const [attemptCount, setAttemptCount] = useState<number>(
    retryService?.getAttemptCount() ?? 0
  );
  const [nextDelay, setNextDelay] = useState<number>(
    retryService?.getNextRetryDelay() ?? 0
  );
  const [isRetrying, setIsRetrying] = useState(false);

  // Subscribe to status changes from the retry service
  useEffect(() => {
    if (!retryService) return;

    const unsubscribe = retryService.onStatusChange((newStatus) => {
      setStatus(newStatus);
      setAttemptCount(retryService.getAttemptCount());
      setNextDelay(retryService.getNextRetryDelay());
    });

    // Initial sync
    setStatus(retryService.getStatus());
    setAttemptCount(retryService.getAttemptCount());
    setNextDelay(retryService.getNextRetryDelay());

    return unsubscribe;
  }, [retryService]);

  // Update delay periodically when reconnecting
  useEffect(() => {
    if (status !== 'reconnecting' || !retryService) return;

    const intervalId = setInterval(() => {
      setNextDelay(retryService.getNextRetryDelay());
      setAttemptCount(retryService.getAttemptCount());
    }, 1000);

    return () => clearInterval(intervalId);
  }, [status, retryService]);

  const handleRetry = useCallback(async () => {
    if (isRetrying) return;

    setIsRetrying(true);
    try {
      if (onRetry) {
        await onRetry();
      } else if (retryService) {
        await retryService.retry();
      }
    } finally {
      setIsRetrying(false);
    }
  }, [onRetry, retryService, isRetrying]);

  const config = STATUS_CONFIG[status];

  return (
    <div
      className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-full ${config.bgColor} ${className}`}
      role="status"
      aria-live="polite"
    >
      {/* Status indicator dot */}
      <span
        className={`${config.color} ${config.pulse ? 'animate-pulse' : ''}`}
        aria-hidden="true"
      >
        {config.icon}
      </span>

      {/* Status label */}
      <span className={`text-sm font-medium ${config.color}`}>
        {config.label}
      </span>

      {/* Retry details */}
      {showDetails && status === 'reconnecting' && (
        <span className="text-xs text-gray-500">
          (Attempt {attemptCount}, next in {formatDelay(nextDelay)})
        </span>
      )}

      {/* Retry button */}
      {showRetryButton && status !== 'connected' && (
        <button
          onClick={handleRetry}
          disabled={isRetrying}
          className={`
            ml-2 px-2 py-0.5 text-xs font-medium rounded
            ${isRetrying
              ? 'bg-gray-200 text-gray-400 cursor-not-allowed'
              : 'bg-white text-gray-700 hover:bg-gray-50 border border-gray-300'
            }
            transition-colors duration-150
          `}
          aria-label={isRetrying ? 'Retrying connection...' : 'Retry connection'}
        >
          {isRetrying ? 'Retrying...' : 'Retry'}
        </button>
      )}
    </div>
  );
};

export default ConnectionStatusIndicator;
