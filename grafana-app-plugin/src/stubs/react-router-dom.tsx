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
import React, { useContext } from 'react';
import { config } from '@grafana/runtime';
import {
  LocationContext,
  useAppLocation,
  useNavigate as useAppNavigate,
  useParams as useAppParams,
  useSearchParams as useAppSearchParams,
} from '../hooks/useAppLocation';

const PLUGIN_BASE_PATH = '/a/dmkskd-tracehouse-app';
const pluginHref = (to: string) => {
  if (!to.startsWith('/') || to.startsWith(PLUGIN_BASE_PATH)) return to;
  return `${config.appSubUrl ?? ''}${PLUGIN_BASE_PATH}${to}`;
};

// Link → plain <a> that uses Grafana's page navigation
// When state is provided, uses LocationContext.navigate to preserve it
export const Link = React.forwardRef<
  HTMLAnchorElement,
  React.AnchorHTMLAttributes<HTMLAnchorElement> & { to: string; replace?: boolean; state?: unknown }
>(({ to, replace: _replace, state, children, onClick, ...rest }, ref) => {
  const ctx = useContext(LocationContext);
  const href = pluginHref(to);

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
  const href = pluginHref(to);
  return <a ref={ref} href={href} {...rest}>{children}</a>;
});
NavLink.displayName = 'NavLink';

// Navigate component → no-op in plugin context
export function Navigate(_props: { to: string; replace?: boolean }) {
  return null;
}

// Hooks → safe no-ops
export const useLocation = useAppLocation;
export const useNavigate = useAppNavigate;
export const useParams = useAppParams;
export const useSearchParams = useAppSearchParams;

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
