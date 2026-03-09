/**
 * AlertBanner - Critical alerts display
 */

import { OVERVIEW_COLORS } from '../../styles/overviewColors';
import type { AlertInfo } from '@tracehouse/core';

interface AlertBannerProps {
  alerts: AlertInfo[];
  className?: string;
}

export function AlertBanner({ alerts, className = '' }: AlertBannerProps) {
  // Don't render if no alerts
  if (alerts.length === 0) {
    return null;
  }

  // Sort by severity (crit first)
  const sortedAlerts = [...alerts].sort((a, b) => {
    if (a.severity === 'crit' && b.severity !== 'crit') return -1;
    if (a.severity !== 'crit' && b.severity === 'crit') return 1;
    return 0;
  });

  return (
    <div className={`space-y-2 ${className}`}>
      {sortedAlerts.map((alert, index) => {
        const isCrit = alert.severity === 'crit';
        const bgColor = isCrit ? 'rgba(239, 68, 68, 0.1)' : 'rgba(245, 158, 11, 0.1)';
        const borderColor = isCrit ? OVERVIEW_COLORS.crit : OVERVIEW_COLORS.warn;
        const textColor = isCrit ? OVERVIEW_COLORS.crit : OVERVIEW_COLORS.warn;

        return (
          <div
            key={`${alert.source}-${index}`}
            className="rounded-lg px-4 py-3 flex items-center gap-3"
            style={{
              backgroundColor: bgColor,
            }}
          >
            <div className="flex-1">
              <p className="text-sm" style={{ color: textColor }}>
                {alert.message}
              </p>
            </div>
            <span
              className="px-2 py-0.5 text-xs font-medium rounded uppercase"
              style={{
                backgroundColor: `${borderColor}20`,
                color: borderColor,
              }}
            >
              {alert.severity}
            </span>
          </div>
        );
      })}
    </div>
  );
}

export default AlertBanner;
