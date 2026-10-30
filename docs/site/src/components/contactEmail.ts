/**
 * Contact address, assembled at runtime only (after a user click) so it never
 * appears in the static HTML that scrapers read.
 */
export function contactEmail(): string {
  return ['ddmmkk', 'proton', 'me'].join('@').replace('proton@', 'proton.');
}
