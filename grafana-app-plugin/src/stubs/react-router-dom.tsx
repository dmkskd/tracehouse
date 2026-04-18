/**
 * Stub for react-router-dom in Grafana plugin context.
 *
 * Frontend components import Link/NavLink from react-router-dom.
 * In Grafana there's no Router context, so we provide simple <a> tag replacements
 * that navigate via Grafana's location service.
 *
 * When `state` is passed (e.g. for back-navigation), Link uses the
 * LocationContext.navigate so the state is preserved in-memory.
 */
import React, { useContext, useState, useCallback } from 'react';
import { LocationContext } from '../hooks/useAppLocation';

// Link → plain <a> that uses Grafana's page navigation
// When state is provided, uses LocationContext.navigate to preserve it
export const Link = React.forwardRef<
  HTMLAnchorElement,
  React.AnchorHTMLAttributes<HTMLAnchorElement> & { to: string; replace?: boolean; state?: unknown }
>(({ to, replace: _replace, state, children, onClick, ...rest }, ref) => {
  const ctx = useContext(LocationContext);
  const href = to.startsWith('/') ? `/a/dmkskd-tracehouse-app${to}` : to;

  if (state && ctx) {
    return (
      <a
        ref={ref}
        href={href}
        onClick={(e) => {
          onClick?.(e);
          // Store state in context before Grafana navigates
          ctx.navigate(to, { state });
          // Let the default <a href> navigation proceed so Grafana actually changes the page
        }}
        {...rest}
      >
        {children}
      </a>
    );
  }

  return <a ref={ref} href={href} onClick={onClick} {...rest}>{children}</a>;
});
Link.displayName = 'Link';

// NavLink → same as Link with an isActive-style className stub
export const NavLink = React.forwardRef<
  HTMLAnchorElement,
  React.AnchorHTMLAttributes<HTMLAnchorElement> & { to: string; end?: boolean }
>(({ to, end: _end, children, ...rest }, ref) => {
  const href = to.startsWith('/') ? `/a/dmkskd-tracehouse-app${to}` : to;
  return <a ref={ref} href={href} {...rest}>{children}</a>;
});
NavLink.displayName = 'NavLink';

// Navigate component → no-op in plugin context
export function Navigate(_props: { to: string; replace?: boolean }) {
  return null;
}

// Hooks → safe no-ops
export function useLocation() {
  return { pathname: '/', search: '', hash: '', state: null, key: 'default' };
}

export function useNavigate() {
  return (_to: string | number, _opts?: { replace?: boolean; state?: unknown }) => {};
}

export function useParams<T extends Record<string, string | undefined> = Record<string, string | undefined>>(): T {
  return {} as T;
}

export function useSearchParams(): [URLSearchParams, (updater: URLSearchParams | ((prev: URLSearchParams) => URLSearchParams), opts?: { replace?: boolean }) => void] {
  const readSearch = () => (typeof window !== 'undefined' ? window.location : { search: '' }).search;
  const [params, setParamsState] = useState(() => new URLSearchParams(readSearch()));

  const setParams = useCallback((updater: URLSearchParams | ((prev: URLSearchParams) => URLSearchParams), opts?: { replace?: boolean }) => {
    const next = typeof updater === 'function'
      ? updater(new URLSearchParams(readSearch()))
      : updater;
    const qs = next.toString();
    const url = qs ? `${window.location.pathname}?${qs}` : window.location.pathname;
    if (opts?.replace) {
      window.history.replaceState(null, '', url);
    } else {
      window.history.pushState(null, '', url);
    }
    setParamsState(next);
  }, []);

  return [params, setParams];
}

// Router components → pass-through
export function BrowserRouter({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}
export function HashRouter({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}
export function MemoryRouter({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}
export function Routes({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}
export function Route(_props: { path?: string; element?: React.ReactNode; children?: React.ReactNode }) {
  return null;
}
