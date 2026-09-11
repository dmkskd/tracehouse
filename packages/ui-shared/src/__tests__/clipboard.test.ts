/**
 * @vitest-environment jsdom
 *
 * copyToClipboard must work on the plain-HTTP hosts TraceHouse is deployed to,
 * where `navigator.clipboard` is undefined because the origin is not secure.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';
import { copyToClipboard } from '../clipboard.js';

const originalClipboard = Object.getOwnPropertyDescriptor(navigator, 'clipboard');

function setClipboard(value: unknown) {
  Object.defineProperty(navigator, 'clipboard', {
    value,
    configurable: true,
    writable: true,
  });
}

afterEach(() => {
  if (originalClipboard) {
    Object.defineProperty(navigator, 'clipboard', originalClipboard);
  } else {
    delete (navigator as { clipboard?: unknown }).clipboard;
  }
  vi.restoreAllMocks();
});

describe('copyToClipboard', () => {
  it('uses the async clipboard API when the context is secure', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    setClipboard({ writeText });

    await expect(copyToClipboard('hello')).resolves.toBe(true);
    expect(writeText).toHaveBeenCalledWith('hello');
  });

  it('falls back to execCommand when navigator.clipboard is undefined', async () => {
    setClipboard(undefined);
    const execCommand = vi.fn().mockReturnValue(true);
    (document as unknown as { execCommand: unknown }).execCommand = execCommand;

    await expect(copyToClipboard('insecure origin')).resolves.toBe(true);
    expect(execCommand).toHaveBeenCalledWith('copy');
  });

  it('falls back to execCommand when the async write rejects', async () => {
    setClipboard({ writeText: vi.fn().mockRejectedValue(new Error('denied')) });
    const execCommand = vi.fn().mockReturnValue(true);
    (document as unknown as { execCommand: unknown }).execCommand = execCommand;

    await expect(copyToClipboard('denied')).resolves.toBe(true);
    expect(execCommand).toHaveBeenCalledWith('copy');
  });

  it('reports failure when neither path can copy', async () => {
    setClipboard(undefined);
    (document as unknown as { execCommand: unknown }).execCommand = vi.fn().mockReturnValue(false);

    await expect(copyToClipboard('nope')).resolves.toBe(false);
  });

  it('leaves no textarea behind after the fallback runs', async () => {
    setClipboard(undefined);
    (document as unknown as { execCommand: unknown }).execCommand = vi.fn().mockReturnValue(true);

    await copyToClipboard('cleanup');

    expect(document.querySelectorAll('textarea')).toHaveLength(0);
  });
});
