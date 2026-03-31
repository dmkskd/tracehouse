/**
 * Docusaurus client module — wires up the Live Demo navbar link and
 * pings the demo instance to show a green status dot when it's up.
 *
 * Configuration: set `customFields.demoUrl` in docusaurus.config.ts.
 * Set it to '' to hide the demo link entirely.
 */
import siteConfig from '@generated/docusaurus.config';

const DEMO_URL = (siteConfig.customFields?.demoUrl as string) || '';

function setup() {
  if (!DEMO_URL) {
    // No demo configured — hide the navbar link
    const link = document.getElementById('navbar-demo-link');
    if (link) link.style.display = 'none';
    return;
  }

  // Wire up the navbar link href
  const link = document.getElementById('navbar-demo-link');
  if (link) {
    (link as HTMLAnchorElement).href = DEMO_URL;
  }

  // Ping the demo — show green dot on success, do nothing on failure
  const pingUrl = DEMO_URL.replace(/\/$/, '') + '/proxy/ping';
  fetch(pingUrl, { mode: 'cors', cache: 'no-store' })
    .then((r) => {
      if (!r.ok) return;
      // Show the navbar dot
      const dot = document.getElementById('navbar-demo-dot');
      if (dot) dot.hidden = false;
      // Show dots on hero buttons (if on homepage)
      document.querySelectorAll<HTMLElement>('.hero-demo-dot').forEach((el) => {
        el.hidden = false;
      });
    })
    .catch(() => {
      // Silently ignore — the link still works, just no status dot
    });
}

// Run on every page load / client-side navigation
if (typeof window !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', setup);
  } else {
    // Defer slightly — Docusaurus may not have rendered the navbar yet
    setTimeout(setup, 0);
  }
}

export function onRouteDidUpdate() {
  // Re-run on client-side navigation (navbar may re-render)
  setTimeout(setup, 0);
}
