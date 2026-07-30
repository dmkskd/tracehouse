import React from 'react';
import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  inferDistributedTopology,
  type ProcessorProfileCompatibility,
  type SubQueryInfo,
} from '@tracehouse/core';
import { DistributedQueryTopology } from '../DistributedQueryTopology';

afterEach(cleanup);

const coordinator = {
  query_id: 'query-root',
  hostname: 'node-a',
  query_duration_ms: 20,
  query_start_time_microseconds: '2026-06-18 12:00:00.000000',
  memory_usage: 1024,
  read_rows: 10,
};

const subQueries = [{
  query_id: 'query-child',
  hostname: 'node-b',
  query_duration_ms: 10,
  query_start_time_microseconds: '2026-06-18 12:00:00.001000',
  memory_usage: 512,
  read_rows: 10,
  read_bytes: 100,
  query_preview: 'SELECT * FROM db.table',
  exception_code: 0,
}] as SubQueryInfo[];

function topologyWithCompatibility(
  processorProfileCompatibility: ProcessorProfileCompatibility,
) {
  return inferDistributedTopology({
    rootQueryId: coordinator.query_id,
    processorProfileCompatibility,
    capabilities: {
      processorsProfileLog: processorProfileCompatibility.mode !== 'unavailable',
    },
    executions: [
      {
        queryId: coordinator.query_id,
        initialQueryId: coordinator.query_id,
        isInitialQuery: true,
        hostname: coordinator.hostname,
        queryKind: 'Select',
        queryStartTimeMicroseconds: coordinator.query_start_time_microseconds,
        queryDurationMs: coordinator.query_duration_ms,
      },
      {
        queryId: 'query-child',
        initialQueryId: coordinator.query_id,
        isInitialQuery: false,
        hostname: 'node-b',
        queryKind: 'Select',
        queryStartTimeMicroseconds: '2026-06-18 12:00:00.001000',
        queryDurationMs: 10,
      },
    ],
  });
}

describe('DistributedQueryTopology processor compatibility', () => {
  it('shows why plan-step enrichment is degraded on a legacy schema', () => {
    const message = 'Processor plan-step metadata is unavailable on this ClickHouse schema; topology uses processor names and query_log/ProfileEvents.';

    render(
      <DistributedQueryTopology
        coordinator={coordinator}
        subQueries={subQueries}
        inferredTopology={topologyWithCompatibility({
          mode: 'legacy',
          reason: 'legacy_schema',
          message,
        })}
        activeQueryId={coordinator.query_id}
        onNavigate={vi.fn()}
      />,
    );

    expect(screen.getByTestId('processor-profile-compatibility')).toHaveTextContent(message);
  });

  it('does not show a compatibility notice for the full schema', () => {
    render(
      <DistributedQueryTopology
        coordinator={coordinator}
        subQueries={subQueries}
        inferredTopology={topologyWithCompatibility({
          mode: 'full',
          reason: 'full_schema',
          message: 'Processor names and plan-step metadata are available on every queried host.',
        })}
        activeQueryId={coordinator.query_id}
        onNavigate={vi.fn()}
      />,
    );

    expect(screen.queryByTestId('processor-profile-compatibility')).toBeNull();
  });
});
