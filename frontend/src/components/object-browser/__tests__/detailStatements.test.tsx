import { act, cleanup, renderHook, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ClickHouseContext } from '@tracehouse/ui-shared';
import type { ClickHouseServices } from '@tracehouse/ui-shared';
import type { DetailSection } from '@tracehouse/core/services/object-browser';
import { useDetailStatements } from '../useDetailStatements';

afterEach(() => cleanup());

describe('detail statements', () => {
  it('loads a multi-line statement whole and stores its result under that statement', async () => {
    const executeQuery = vi.fn(async () => [{ statement: 'CREATE TABLE demo.trips (id UInt64) ENGINE = MergeTree ORDER BY id' }]);
    const service = { adapter: { executeQuery } } as unknown as ClickHouseServices;
    const wrapper = ({ children }: { children: ReactNode }) => <ClickHouseContext.Provider value={service}>{children}</ClickHouseContext.Provider>;
    // Same shape as OBJECT_TABLE_DDL: the query spans several lines.
    const sql = "\nSELECT create_table_query AS statement\nFROM system.tables WHERE database = 'demo' AND name = 'trips'";
    const { result } = renderHook(() => useDetailStatements([{ kind: 'statement', statement: { title: 'CREATE statement', sql } }]), { wrapper });
    await waitFor(() => expect(result.current.results[sql]).toEqual({ text: 'CREATE TABLE demo.trips (id UInt64) ENGINE = MergeTree ORDER BY id' }));
    expect(executeQuery).toHaveBeenCalledTimes(1);
    expect(String((executeQuery.mock.calls[0] as unknown[])[0])).toContain("FROM system.tables WHERE database = 'demo'");
  });
  it('runs an external statement only after it is requested', async () => {
    const executeQuery = vi.fn(async () => [{ snapshot: '2026-11-18 APPEND' }]);
    const service = { adapter: { executeQuery } } as unknown as ClickHouseServices;
    const wrapper = ({ children }: { children: ReactNode }) => <ClickHouseContext.Provider value={service}>{children}</ClickHouseContext.Provider>;
    const statements: DetailSection[] = [{ kind: 'statement', statement: { title: 'Snapshots', sql: 'SELECT 1', external: 'Reads Iceberg metadata from storage' } }];
    const { result } = renderHook(() => useDetailStatements(statements), { wrapper });
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(executeQuery).not.toHaveBeenCalled();
    act(() => result.current.load('SELECT 1'));
    await waitFor(() => expect(result.current.results['SELECT 1']).toEqual({ text: '2026-11-18 APPEND' }));
    expect(executeQuery).toHaveBeenCalledTimes(1);
  });
});
