import { locationService } from '@grafana/runtime';

/** Read a URL param from Grafana's locationService (no hash routing in plugin context). */
export function getUrlParam(key: string): string | null {
  return new URLSearchParams(locationService.getLocation().search).get(key);
}
