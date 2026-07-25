import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ConnectionSelector } from '../ConnectionSelector';

const connectionState = vi.hoisted(() => ({
  profiles: [],
  activeProfileId: null,
  isLoading: false,
  error: null,
  isConnectionFormOpen: true,
  fetchProfiles: vi.fn().mockResolvedValue(undefined),
  deleteProfile: vi.fn(),
  setActiveProfile: vi.fn(),
  setConnectionFormOpen: vi.fn(),
  clearError: vi.fn(),
}));

vi.mock('../../../stores/connectionStore', () => ({
  useConnectionStore: () => connectionState,
}));

vi.mock('../../../stores/clusterStore', () => ({
  useClusterStore: (
    selector: (state: { clusterName: null; replicaCount: number }) => unknown,
  ) => selector({ clusterName: null, replicaCount: 1 }),
}));

vi.mock('../ConnectionForm', () => ({
  ConnectionForm: () => <div data-testid="connection-form" />,
}));

describe('ConnectionSelector', () => {
  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it('renders the connection editor above query-detail modals', () => {
    render(<ConnectionSelector />);

    const overlay = screen.getByTestId('connection-form').parentElement;
    expect(overlay).toHaveStyle('z-index: 100010');
  });
});
