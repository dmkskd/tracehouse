/**
 * MetricCard - Component for displaying individual server metrics
 * 
 * This component displays a single metric with its value, label, and optional
 * formatting. It supports different metric types, visual indicators, and
 * threshold warnings.
 * 
 */

import React from 'react';
import { MetricWarning, calculateSeverity } from './MetricWarning';
import type { WarningSeverity } from './MetricWarning';
import { formatBytes, formatDuration } from '../../utils/formatters';

export type MetricType = 'percentage' | 'bytes' | 'memory' | 'duration' | 'number';

export interface MetricCardProps {
  /** Label for the metric */
  label: string;
  /** Primary value to display */
  value: number | null;
  /** Secondary value (e.g., total for memory) */
  secondaryValue?: number | null;
  /** Type of metric for formatting */
  type: MetricType;
  /** Icon to display */
  icon?: string;
  /** Whether the metric is loading */
  isLoading?: boolean;
  /** Optional subtitle or description */
  subtitle?: string;
  /** Color theme for the card */
  color?: 'blue' | 'green' | 'yellow' | 'red' | 'purple' | 'gray';
  isWarning?: boolean;
  /** Threshold value for warning display */
  threshold?: number;
  /** Unit for threshold display (e.g., '%', 's') */
  thresholdUnit?: string;
  /** Callback when warning indicator is clicked */
  onWarningClick?: () => void;
}

/**
 * Format a number with appropriate precision for metric display
 */
function formatNumber(value: number): string {
  if (value >= 1000000) return `${(value / 1000000).toFixed(2)}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(2)}K`;
  return value.toFixed(0);
}

/**
 * Get formatted value based on metric type
 */
function getFormattedValue(
  value: number | null,
  secondaryValue: number | null | undefined,
  type: MetricType
): string {
  if (value === null) return '--';

  switch (type) {
    case 'percentage':
      return `${value.toFixed(1)}%`;
    
    case 'bytes':
      return formatBytes(value);
    
    case 'memory':
      if (secondaryValue !== null && secondaryValue !== undefined) {
        return `${formatBytes(value)} / ${formatBytes(secondaryValue)}`;
      }
      return formatBytes(value);
    
    case 'duration':
      return formatDuration(value);
    
    case 'number':
      return formatNumber(value);
    
    default:
      return value.toString();
  }
}

/**
 * Get color classes based on color theme and warning state
 */
function getColorClasses(
  color: MetricCardProps['color'],
  isWarning?: boolean
): {
  bg: string;
  text: string;
  icon: string;
  border: string;
} {
  // If in warning state, use warning colors
  if (isWarning) {
    return {
      bg: 'bg-yellow-50 dark:bg-yellow-900/20',
      text: 'text-yellow-600 dark:text-yellow-400',
      icon: 'text-yellow-500',
      border: 'border-yellow-300 dark:border-yellow-700',
    };
  }

  switch (color) {
    case 'blue':
      return {
        bg: 'bg-blue-50 dark:bg-blue-900/20',
        text: 'text-blue-600 dark:text-blue-400',
        icon: 'text-blue-500',
        border: 'border-transparent',
      };
    case 'green':
      return {
        bg: 'bg-green-50 dark:bg-green-900/20',
        text: 'text-green-600 dark:text-green-400',
        icon: 'text-green-500',
        border: 'border-transparent',
      };
    case 'yellow':
      return {
        bg: 'bg-yellow-50 dark:bg-yellow-900/20',
        text: 'text-yellow-600 dark:text-yellow-400',
        icon: 'text-yellow-500',
        border: 'border-transparent',
      };
    case 'red':
      return {
        bg: 'bg-red-50 dark:bg-red-900/20',
        text: 'text-red-600 dark:text-red-400',
        icon: 'text-red-500',
        border: 'border-transparent',
      };
    case 'purple':
      return {
        bg: 'bg-purple-50 dark:bg-purple-900/20',
        text: 'text-purple-600 dark:text-purple-400',
        icon: 'text-purple-500',
        border: 'border-transparent',
      };
    case 'gray':
    default:
      return {
        bg: 'bg-gray-50 dark:bg-gray-800',
        text: 'text-gray-600 dark:text-gray-400',
        icon: 'text-gray-500',
        border: 'border-transparent',
      };
  }
}

export const MetricCard: React.FC<MetricCardProps> = ({
  label,
  value,
  secondaryValue,
  type,
  icon,
  isLoading = false,
  subtitle,
  color = 'gray',
  isWarning = false,
  threshold,
  thresholdUnit,
  onWarningClick,
}) => {
  const formattedValue = getFormattedValue(value, secondaryValue, type);
  const colorClasses = getColorClasses(color, isWarning);
  
  // Calculate severity for warning display
  const severity: WarningSeverity = 
    value !== null && threshold !== undefined
      ? calculateSeverity(value, threshold)
      : 'warning';

  // Determine border style based on warning state
  const borderClass = isWarning 
    ? `border-2 ${colorClasses.border}` 
    : 'border border-transparent';

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-lg shadow-md p-6 transition-all hover:shadow-lg ${borderClass}`}>
      <div className="flex items-start justify-between">
        <div className="flex-1">
          {/* Label with warning indicator */}
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-gray-500 dark:text-gray-400">
              {label}
            </span>
            {isWarning && value !== null && threshold !== undefined && (
              <MetricWarning
                isWarning={isWarning}
                value={value}
                threshold={threshold}
                metricLabel={label}
                unit={thresholdUnit}
                severity={severity}
                mode="inline"
                onClick={onWarningClick}
              />
            )}
          </div>
          
          {/* Value */}
          <div className="flex items-baseline mt-1">
            {isLoading ? (
              <div className="animate-pulse">
                <div className="h-8 w-24 bg-gray-200 dark:bg-gray-700 rounded"></div>
              </div>
            ) : (
              <span className={`text-2xl font-bold ${isWarning ? 'text-yellow-600 dark:text-yellow-400' : 'text-gray-800 dark:text-white'}`}>
                {formattedValue}
              </span>
            )}
          </div>
          
          {/* Subtitle */}
          {subtitle && (
            <div className="text-xs text-gray-400 dark:text-gray-500 mt-1">
              {subtitle}
            </div>
          )}
          
          {/* Threshold info when in warning state */}
          {isWarning && threshold !== undefined && (
            <div className="text-xs text-yellow-600 dark:text-yellow-400 mt-1">
              Threshold: {threshold}{thresholdUnit || ''}
            </div>
          )}
        </div>
        
        {/* Icon */}
        {icon && (
          <div className={`p-3 rounded-full ${colorClasses.bg}`}>
            <span className={`text-xl ${colorClasses.icon}`}>{icon}</span>
          </div>
        )}
      </div>
      
      {/* Progress bar for percentage type */}
      {type === 'percentage' && value !== null && (
        <div className="mt-4">
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
            <div
              className={`h-2 rounded-full transition-all duration-300 ${
                isWarning ? 'bg-yellow-500' :
                value > 90 ? 'bg-red-500' :
                value > 70 ? 'bg-yellow-500' :
                'bg-green-500'
              }`}
              style={{ width: `${Math.min(value, 100)}%` }}
            />
          </div>
        </div>
      )}
      
      {/* Progress bar for memory type */}
      {type === 'memory' && value !== null && secondaryValue !== null && secondaryValue !== undefined && secondaryValue > 0 && (
        <div className="mt-4">
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
            <div
              className={`h-2 rounded-full transition-all duration-300 ${
                isWarning ? 'bg-yellow-500' :
                (value / secondaryValue) > 0.9 ? 'bg-red-500' :
                (value / secondaryValue) > 0.7 ? 'bg-yellow-500' :
                'bg-blue-500'
              }`}
              style={{ width: `${Math.min((value / secondaryValue) * 100, 100)}%` }}
            />
          </div>
          <div className="text-xs text-gray-400 dark:text-gray-500 mt-1">
            {((value / secondaryValue) * 100).toFixed(1)}% used
          </div>
        </div>
      )}
    </div>
  );
};

export default MetricCard;
