/**
 * MetricWarning - Component for displaying warning indicators when metrics exceed thresholds
 * 
 * This component provides visual warning indicators for metrics that exceed
 * their configured thresholds. It supports different severity levels and
 * can be used inline or as a standalone alert.
 */

import React from 'react';

export type WarningSeverity = 'warning' | 'critical';

export interface MetricWarningProps {
  /** Whether the warning is active */
  isWarning: boolean;
  /** Current value of the metric */
  value: number;
  /** Threshold value that triggers the warning */
  threshold: number;
  /** Label for the metric */
  metricLabel?: string;
  /** Unit for the metric value (e.g., '%', 's', 'parts') */
  unit?: string;
  /** Severity level of the warning */
  severity?: WarningSeverity;
  /** Display mode: 'inline' for small indicator, 'banner' for full alert */
  mode?: 'inline' | 'banner' | 'badge';
  /** Optional custom message */
  message?: string;
  /** Whether to show the threshold value */
  showThreshold?: boolean;
  /** Optional click handler */
  onClick?: () => void;
}

/**
 * Get severity based on how much the value exceeds the threshold
 */
export function calculateSeverity(value: number, threshold: number): WarningSeverity {
  // Critical if value exceeds threshold by more than 10%
  const exceedanceRatio = (value - threshold) / threshold;
  return exceedanceRatio > 0.1 ? 'critical' : 'warning';
}

/**
 * Get color classes based on severity
 */
function getSeverityClasses(severity: WarningSeverity): {
  bg: string;
  border: string;
  text: string;
  icon: string;
} {
  switch (severity) {
    case 'critical':
      return {
        bg: 'bg-red-50 dark:bg-red-900/30',
        border: 'border-red-300 dark:border-red-700',
        text: 'text-red-700 dark:text-red-300',
        icon: 'text-red-500',
      };
    case 'warning':
    default:
      return {
        bg: 'bg-yellow-50 dark:bg-yellow-900/30',
        border: 'border-yellow-300 dark:border-yellow-700',
        text: 'text-yellow-700 dark:text-yellow-300',
        icon: 'text-yellow-500',
      };
  }
}

/**
 * Get warning icon based on severity
 */
function getWarningIcon(severity: WarningSeverity): string {
  return severity === 'critical' ? '!' : '!';
}

/**
 * Format the warning message
 */
function formatWarningMessage(
  value: number,
  threshold: number,
  metricLabel?: string,
  unit?: string
): string {
  const formattedValue = unit === '%' ? value.toFixed(1) : value.toFixed(0);
  const formattedThreshold = unit === '%' ? threshold.toFixed(1) : threshold.toFixed(0);
  const unitStr = unit || '';
  
  if (metricLabel) {
    return `${metricLabel} (${formattedValue}${unitStr}) exceeds threshold (${formattedThreshold}${unitStr})`;
  }
  return `Value ${formattedValue}${unitStr} exceeds threshold ${formattedThreshold}${unitStr}`;
}

/**
 * Inline warning indicator - small icon/badge for use within cards
 */
const InlineWarning: React.FC<{
  severity: WarningSeverity;
  onClick?: () => void;
}> = ({ severity, onClick }) => {
  const classes = getSeverityClasses(severity);
  
  return (
    <span
      className={`inline-flex items-center justify-center w-6 h-6 rounded-full 
                  ${classes.bg} ${classes.border} border cursor-pointer
                  transition-transform hover:scale-110`}
      onClick={onClick}
      title={severity === 'critical' ? 'Critical threshold exceeded' : 'Warning threshold exceeded'}
    >
      <span className="text-sm">{getWarningIcon(severity)}</span>
    </span>
  );
};

/**
 * Badge warning indicator - compact label for use in headers
 */
const BadgeWarning: React.FC<{
  severity: WarningSeverity;
  label?: string;
  onClick?: () => void;
}> = ({ severity, label, onClick }) => {
  const classes = getSeverityClasses(severity);
  
  return (
    <span
      className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium
                  ${classes.bg} ${classes.text} ${classes.border} border cursor-pointer
                  transition-all hover:shadow-md`}
      onClick={onClick}
    >
      <span className="mr-1">{getWarningIcon(severity)}</span>
      {label || (severity === 'critical' ? 'Critical' : 'Warning')}
    </span>
  );
};

/**
 * Banner warning indicator - full alert banner for prominent display
 */
const BannerWarning: React.FC<{
  severity: WarningSeverity;
  message: string;
  showThreshold: boolean;
  value: number;
  threshold: number;
  unit?: string;
  onClick?: () => void;
}> = ({ severity, message, showThreshold, value, threshold, unit, onClick }) => {
  const classes = getSeverityClasses(severity);
  
  return (
    <div
      className={`flex items-start p-3 rounded-lg ${classes.bg} ${classes.border} border
                  ${onClick ? 'cursor-pointer hover:shadow-md' : ''} transition-all`}
      onClick={onClick}
    >
      <span className={`text-xl mr-3 ${classes.icon}`}>
        {getWarningIcon(severity)}
      </span>
      <div className="flex-1">
        <p className={`text-sm font-medium ${classes.text}`}>
          {message}
        </p>
        {showThreshold && (
          <p className={`text-xs mt-1 ${classes.text} opacity-80`}>
            Current: {value.toFixed(1)}{unit} | Threshold: {threshold.toFixed(1)}{unit}
          </p>
        )}
      </div>
    </div>
  );
};

/**
 * MetricWarning component - displays warning indicators for metrics exceeding thresholds
 */
export const MetricWarning: React.FC<MetricWarningProps> = ({
  isWarning,
  value,
  threshold,
  metricLabel,
  unit,
  severity: providedSeverity,
  mode = 'inline',
  message,
  showThreshold = false,
  onClick,
}) => {
  // Don't render if not in warning state
  if (!isWarning) {
    return null;
  }

  // Calculate severity if not provided
  const severity = providedSeverity || calculateSeverity(value, threshold);
  
  // Generate message if not provided
  const warningMessage = message || formatWarningMessage(value, threshold, metricLabel, unit);

  switch (mode) {
    case 'inline':
      return <InlineWarning severity={severity} onClick={onClick} />;
    
    case 'badge':
      return <BadgeWarning severity={severity} label={metricLabel} onClick={onClick} />;
    
    case 'banner':
      return (
        <BannerWarning
          severity={severity}
          message={warningMessage}
          showThreshold={showThreshold}
          value={value}
          threshold={threshold}
          unit={unit}
          onClick={onClick}
        />
      );
    
    default:
      return <InlineWarning severity={severity} onClick={onClick} />;
  }
};

/**
 * WarningsSummary - displays a summary of all active warnings
 */
export interface WarningsSummaryProps {
  warnings: Array<{
    isWarning: boolean;
    value: number;
    threshold: number;
    metricLabel: string;
    unit?: string;
  }>;
  onWarningClick?: (metricLabel: string) => void;
}

export const WarningsSummary: React.FC<WarningsSummaryProps> = ({
  warnings,
  onWarningClick,
}) => {
  const activeWarnings = warnings.filter(w => w.isWarning);
  
  if (activeWarnings.length === 0) {
    return null;
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center text-sm font-medium text-gray-700 dark:text-gray-300">
        <span className="mr-2 font-bold">!</span>
        {activeWarnings.length} Warning{activeWarnings.length > 1 ? 's' : ''}
      </div>
      <div className="space-y-2">
        {activeWarnings.map((warning, index) => (
          <MetricWarning
            key={index}
            isWarning={warning.isWarning}
            value={warning.value}
            threshold={warning.threshold}
            metricLabel={warning.metricLabel}
            unit={warning.unit}
            mode="banner"
            showThreshold={true}
            onClick={() => onWarningClick?.(warning.metricLabel)}
          />
        ))}
      </div>
    </div>
  );
};

export default MetricWarning;
