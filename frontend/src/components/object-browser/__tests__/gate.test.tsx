import { render, screen, waitFor } from '@testing-library/react';
import { lazy } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup } from '@testing-library/react';
import { ObjectBrowserGate } from '../index';
import { FeatureAvailabilityContext } from '../../../providers/FeatureAvailability';
import { useUserPreferenceStore } from '../../../stores/userPreferenceStore';

const memory = new Map<string, string>();
vi.stubGlobal('localStorage', { getItem: (k: string) => memory.get(k) ?? null, setItem: (k: string, v: string) => memory.set(k, v), removeItem: (k: string) => memory.delete(k) });
afterEach(cleanup);
describe('object browser gate before lazy runtime import', () => {
  it('does not load on direct routes with absent availability', () => {
    const load = vi.fn(async () => ({ default: () => <div>Feature runtime</div> }));
    render(<ObjectBrowserGate page={lazy(load)} />);
    expect(screen.getByText(/disabled for this deployment/)).toBeInTheDocument();
    expect(load).not.toHaveBeenCalled();
  });
  it('loads with deployment availability, whatever the experimental preference, and unloads without it', async () => {
    useUserPreferenceStore.setState({ experimentalEnabled: false });
    const dispose = vi.fn();
    const React = await import('react');
    const Page = () => { React.useEffect(() => dispose, []);return <div>Feature runtime</div>; };
    const load = vi.fn(async () => ({ default: Page }));
    const page = lazy(load);
    const view = render(<FeatureAvailabilityContext.Provider value={{ objectBrowser: true }}><ObjectBrowserGate page={page} /></FeatureAvailabilityContext.Provider>);
    await screen.findByText('Feature runtime');expect(load).toHaveBeenCalledTimes(1);
    view.rerender(<FeatureAvailabilityContext.Provider value={{ objectBrowser: false }}><ObjectBrowserGate page={page} /></FeatureAvailabilityContext.Provider>);
    await waitFor(() => expect(dispose).toHaveBeenCalledTimes(1));
    expect(screen.getByText(/disabled for this deployment/)).toBeInTheDocument();
  });
});
