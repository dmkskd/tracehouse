/**
 * Generate a random UUID v4 string.
 *
 * `crypto.randomUUID()` is only available in secure contexts (HTTPS / localhost).
 * When served over plain HTTP (e.g. demo env on an IP) we fall back to a
 * Math.random-based generator which is fine for non-cryptographic IDs.
 */
export function randomUUID(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  // RFC 4122 v4 layout: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
