import React from 'react';
import type { OpenTelemetrySpan } from '@tracehouse/core';

/**
 * OpenTelemetry Spans Viewer component - Work in Progress
 */
export const SpansTab: React.FC<{
  spans: OpenTelemetrySpan[];
  isLoading: boolean;
  error: string | null;
  onRefresh: () => void;
}> = () => {
  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      height: '100%',
      minHeight: 300,
      padding: 40,
    }}>
      <div style={{ textAlign: 'center', maxWidth: 400 }}>
        <div style={{
          fontSize: 16,
          fontWeight: 500,
          color: 'var(--text-secondary)',
          marginBottom: 8,
        }}>
          Not Yet Available
        </div>
        <div style={{
          fontSize: 13,
          color: 'var(--text-muted)',
          lineHeight: 1.6,
        }}>
          OpenTelemetry span visualization is not yet implemented.
        </div>
      </div>
    </div>
  );
};
