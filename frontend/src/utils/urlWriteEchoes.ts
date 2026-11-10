/**
 * Recognizes a page's own URL writes when the router reports them back.
 *
 * A page that keeps its state in the URL both writes the URL from its state and
 * applies URL changes it did not make (a pasted link, back/forward) to its state.
 * It must not apply its own writes. Remembering only the last write is not enough
 * when the router reports every write separately, as the Grafana plugin's
 * locationService shim does: after two quick writes A and B, the echo of A arrives
 * while B is the remembered write, A is applied as an outside change, and the page
 * writes again, alternating between the two states indefinitely.
 *
 * Each write is recorded in order. An incoming URL that matches a pending write is
 * an echo: it and every older pending write are dropped, since a router that
 * coalesces writes (React Router) reports only the latest one.
 *
 * URLs are compared as parameters, not as strings. Grafana's locationService
 * updates existing keys in place, while a page that deletes and re-appends a
 * multi-value parameter moves it to the end, so the echo of a write can list the
 * same parameters in a different order.
 */

const MAX_PENDING = 20;

/** The parameters of `search` sorted by key; values of a repeated key keep their order. */
function canonical(search: string): string {
  const params = new URLSearchParams(search);
  params.sort();
  return params.toString();
}

export interface UrlWriteEchoes {
  /** Record a URL the page has just written. */
  recordWrite(search: string): void;
  /** Whether `search` is the echo of a recorded write; consumes it and older writes. */
  consumeEcho(search: string): boolean;
}

export function createUrlWriteEchoes(): UrlWriteEchoes {
  let pending: string[] = [];
  return {
    recordWrite(search) {
      pending.push(canonical(search));
      if (pending.length > MAX_PENDING) pending = pending.slice(-MAX_PENDING);
    },
    consumeEcho(search) {
      const index = pending.lastIndexOf(canonical(search));
      if (index < 0) return false;
      pending = pending.slice(index + 1);
      return true;
    },
  };
}
