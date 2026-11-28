import type { ReactElement } from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { OperationalEvent, RecentActivity } from '@tracehouse/core';
import type { BlockEntry } from '../arena-types';

// Inspect the scene inputs without requiring a WebGL context in jsdom.
vi.mock('@react-three/fiber', () => ({
  Canvas: ({ children }: { children: ReactElement<{ entries: BlockEntry[]; hostNames: string[] }> }) =>
    <output data-testid="scene-layout">{JSON.stringify({ entries: children.props.entries.map(e => ({ id: e.id, lane: e.lane, deck: e.deck,
      cpu: e.cpu, mem: e.mem, startTime: e.startTime, endTime: e.endTime, failureCode: e.failureCode })), hosts: children.props.hostNames })}</output>,
  useFrame: vi.fn(), useThree: vi.fn(),
}));
vi.mock('../../../providers/ClickHouseProvider', () => ({ useClickHouseServices: () => null }));
vi.mock('../useArenaEvents', () => ({ useArenaEvents: () => ({ events: [], coverage: [], arrivals: new Map(), error: null }), useReducedArenaMotion: () => true }));
import { ResourceArena3D } from '../ResourceArena3D';

afterEach(cleanup);
describe('event visibility and operation layout', () => {
  it('renders host-provided events without services in the scene bundle', () => {
    const onVisibilityChange = vi.fn();
    const data = { events: [{ id: 'oom', occurred_at: new Date().toISOString(), hostname: 'node-01',
      kind: 'query_oom', category: 'queries', severity: 'error', precision: 'exact', title: 'Query OOM',
      source: 'system.query_log', capability: 'query_log' } as OperationalEvent],
      coverage: [], arrivals: new Map<string, number>(), error: null };
    render(<MemoryRouter><ResourceArena3D queries={[]} merges={[]} cpuUsage={1} memoryPct={1}
      hosts={['node-01']} eventState={{ data, visible: true, onVisibilityChange }} /></MemoryRouter>);
    expect(screen.getByLabelText('Events in the arena').textContent).toContain('Query OOM');
    fireEvent.click(screen.getByRole('switch', { name: 'Show operational events' }));
    expect(onVisibilityChange).toHaveBeenCalledWith(false);
  });
  it.each([false, true])('does not repack operations when toggling events with Split=%s', splitActive => {
    const history = { queries: ['node-01', 'node-02'].map((hostname, i) => ({
      queryId: `q-${i}`, hostname, user: 'default', elapsed: 10, endedAgoSec: 5,
      cpuCores: .5, memoryUsage: 1000, ioReadRate: 0, rowsRead: 0, bytesRead: 0, progress: 100,
      queryKind: 'SELECT', query: 'SELECT 1',
      exceptionCode: i === 0 ? 241 : 0,
    })), merges: [] } as RecentActivity;
    render(<MemoryRouter><ResourceArena3D queries={[]} merges={[]} cpuUsage={1} memoryPct={1}
      history={history} hosts={['node-01', 'node-02']} splitActive={splitActive} /></MemoryRouter>);
    const before = screen.getByTestId('scene-layout').textContent;
    expect(JSON.parse(before!).entries[0].failureCode).toBe(241);
    const toggle = screen.getByRole('switch', { name: 'Show operational events' });
    fireEvent.click(toggle);
    expect(screen.getByTestId('scene-layout').textContent).toBe(before);
    fireEvent.click(toggle);
    expect(screen.getByTestId('scene-layout').textContent).toBe(before);
  });
});
