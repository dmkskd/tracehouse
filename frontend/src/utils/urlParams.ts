/**
 * Read a URL param from the current location.
 *
 * Standalone (Vite) build: checks the HashRouter fragment first
 * (e.g. `/#/path?key=val`), falls back to `window.location.search`.
 *
 * The Grafana plugin build aliases this module to a locationService-backed
 * version (see grafana-app-plugin/.config/webpack/webpack.config.cjs).
 */
export function getUrlParam(key: string): string | null {
  const hash = window.location.hash;
  const qIdx = hash.indexOf('?');
  if (qIdx !== -1) {
    const val = new URLSearchParams(hash.slice(qIdx + 1)).get(key);
    if (val) return val;
  }
  return new URLSearchParams(window.location.search).get(key);
}
