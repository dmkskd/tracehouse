export const QUERY_ACTIVITY_PREVIEW_STORAGE_KEY = 'tracehouse.queryHistory.showHoverPreview';
export const MERGE_ACTIVITY_PREVIEW_STORAGE_KEY = 'tracehouse.mergeActivity.showHoverPreview';

export function loadPreviewPreference(storageKey: string): boolean {
  if (typeof window === 'undefined') return false;
  try {
    return window.localStorage.getItem(storageKey) === 'true';
  } catch {
    return false;
  }
}

export function savePreviewPreference(storageKey: string, value: boolean): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(storageKey, String(value));
  } catch {
    // Keep the in-memory preference when storage is unavailable.
  }
}
