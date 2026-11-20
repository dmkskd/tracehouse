/** Browser storage owned by TraceHouse, shared by standalone and Grafana. */
export const TRACEHOUSE_STORAGE_PREFIX = 'dmkskd-tracehouse-app:';

export function storageKey(key: string): string {
  return key.startsWith(TRACEHOUSE_STORAGE_PREFIX) ? key : `${TRACEHOUSE_STORAGE_PREFIX}${key}`;
}

type StorageAccess = Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;

/** Copy exact legacy keys on read, retaining the old value for version rollback. */
export function createTracehouseStorage(resolveStorage: () => StorageAccess): StorageAccess {
  return {
    getItem(key) {
      const storage = resolveStorage();
      const namespacedKey = storageKey(key);
      const current = storage.getItem(namespacedKey);
      if (current !== null) return current;
      const legacyKey = namespacedKey.slice(TRACEHOUSE_STORAGE_PREFIX.length);
      const legacy = storage.getItem(legacyKey);
      if (legacy !== null) {
        try {
          storage.setItem(namespacedKey, legacy);
        } catch (error) {
          console.warn(`[TraceHouse] Could not migrate storage key "${namespacedKey}"; using its legacy value.`, error);
        }
      }
      return legacy;
    },
    setItem(key, value) {
      const storage = resolveStorage();
      const namespacedKey = storageKey(key);
      storage.setItem(namespacedKey, value);
      // Preserve any pre-upgrade value; new writes belong only to our namespace.
    },
    removeItem(key) {
      const storage = resolveStorage();
      const namespacedKey = storageKey(key);
      storage.removeItem(namespacedKey);
      storage.removeItem(namespacedKey.slice(TRACEHOUSE_STORAGE_PREFIX.length));
    },
  };
}

export const tracehouseLocalStorage = createTracehouseStorage(() => localStorage);
export const tracehouseSessionStorage = createTracehouseStorage(() => sessionStorage);
