/**
 * useAppLocation - Router-agnostic location hook
 * 
 * This hook provides location information that works in both:
 * - Standalone frontend (uses react-router-dom)
 * - Grafana plugin (uses stub/Grafana's location service)
 * 
 * Pages should use this instead of importing useLocation directly
 * from react-router-dom to ensure compatibility with both contexts.
 */

import { useLocation, useNavigate, useParams, useSearchParams } from 'react-router-dom';

export interface AppLocation {
  pathname: string;
  search: string;
  hash: string;
  state: unknown;
}

export function useAppLocation(): AppLocation {
  const location = useLocation();
  return {
    pathname: location.pathname,
    search: location.search,
    hash: location.hash,
    state: location.state,
  };
}

// Re-export router hooks for convenience
export { useNavigate, useParams, useSearchParams };
