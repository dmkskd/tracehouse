import type { ReactNode } from 'react';
import { useTheme2 } from '@grafana/ui';

/** Keep Grafana controls out of TraceHouse resets and inherited typography. */
export function GrafanaControl({ children }: { children: ReactNode }) {
  const theme = useTheme2();
  return (
    <div
      data-tracehouse-grafana=""
      style={{ display: 'contents', ...theme.typography.body, color: theme.colors.text.primary }}
    >
      {children}
    </div>
  );
}
