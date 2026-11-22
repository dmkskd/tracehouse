/**
 * The dev server the e2e tests run against. The tests always start their own, on a port of
 * their own and with their own Vite dependency cache, so a dev server someone else runs (on
 * 5173 or anywhere) is never reused and the two never share or invalidate a cache.
 */
export const E2E_FRONTEND_PORT = Number(process.env.E2E_FRONTEND_PORT ?? 5191);
export const E2E_FRONTEND_ORIGIN = `http://localhost:${E2E_FRONTEND_PORT}`;
/** Vite cacheDir, relative to the frontend workspace (vite.config.ts reads VITE_CACHE_DIR). */
export const E2E_VITE_CACHE_DIR = 'node_modules/.vite-e2e';
