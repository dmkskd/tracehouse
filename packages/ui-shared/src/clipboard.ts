/**
 * copyToClipboard — clipboard write that also works on insecure origins.
 *
 * `navigator.clipboard` is only defined in a secure context (HTTPS or
 * localhost). TraceHouse is routinely served from a plain-HTTP host over an
 * IP address, where `navigator.clipboard` is `undefined` and every copy button
 * throws a TypeError. The `document.execCommand('copy')` path is deprecated
 * but is still the only clipboard write available on such an origin.
 *
 * Returns whether the text was copied, so callers can report the failure
 * instead of silently doing nothing.
 */

function execCommandCopy(text: string): boolean {
  if (typeof document === 'undefined') return false;

  const textarea = document.createElement('textarea');
  textarea.value = text;
  // Keep it off-screen and non-focusable-looking, but still selectable:
  // display:none or visibility:hidden would make the selection empty.
  textarea.setAttribute('readonly', '');
  textarea.style.position = 'fixed';
  textarea.style.top = '-9999px';
  textarea.style.opacity = '0';
  document.body.appendChild(textarea);

  const previousSelection = document.getSelection()?.rangeCount
    ? document.getSelection()!.getRangeAt(0)
    : null;

  try {
    textarea.select();
    textarea.setSelectionRange(0, textarea.value.length);
    return document.execCommand('copy');
  } catch {
    return false;
  } finally {
    document.body.removeChild(textarea);
    if (previousSelection) {
      const selection = document.getSelection();
      selection?.removeAllRanges();
      selection?.addRange(previousSelection);
    }
  }
}

export async function copyToClipboard(text: string): Promise<boolean> {
  if (typeof navigator !== 'undefined' && navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // Permission denied, or a document that isn't focused. Fall through to
      // the legacy path rather than reporting a failure the user can't act on.
    }
  }
  return execCommandCopy(text);
}
