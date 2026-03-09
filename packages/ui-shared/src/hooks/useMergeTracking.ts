/**
 * Headless hook for merge tracking functionality.
 * Fetches active merges, tracks completed merges with configurable
 * fade timers, and provides merge statistics.
 */
import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import type { IClickHouseAdapter, MergeInfo } from '@tracehouse/core';
import { MergeTracker } from '@tracehouse/core';

export interface MergeTrackingOptions {
  /** Polling interval in milliseconds (default: 5000) */
  pollInterval?: number;
  /** How long completed merges remain visible in milliseconds (default: 10000) */
  fadeDuration?: number;
}

export interface CompletedMerge {
  merge: MergeInfo;
  completedAt: number;
}

export interface MergeStatistics {
  activeMergeCount: number;
  completedMergeCount: number;
  totalBytesProcessing: number;
  averageProgress: number;
}

export interface UseMergeTrackingResult {
  activeMerges: MergeInfo[];
  completedMerges: CompletedMerge[];
  statistics: MergeStatistics;
  isLoading: boolean;
  error: string | null;
  refresh: () => void;
}

export function useMergeTracking(
  adapter: IClickHouseAdapter,
  database: string,
  table: string,
  options: MergeTrackingOptions = {}
): UseMergeTrackingResult {
  const { pollInterval = 5000, fadeDuration = 10000 } = options;

  const [activeMerges, setActiveMerges] = useState<MergeInfo[]>([]);
  const [completedMerges, setCompletedMerges] = useState<CompletedMerge[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const previousMergeKeysRef = useRef<Set<string>>(new Set());
  const cancelledRef = useRef(false);

  const getMergeKey = useCallback(
    (m: MergeInfo) => `${m.database}.${m.table}.${m.result_part_name}`,
    []
  );

  const fetchMerges = useCallback(() => {
    if (!database || !table) return;

    cancelledRef.current = false;
    setIsLoading(true);
    setError(null);

    const tracker = new MergeTracker(adapter);
    tracker
      .getActiveMerges()
      .then(merges => {
        if (cancelledRef.current) return;

        // Filter to the specific database/table
        const filtered = merges.filter(
          m => m.database === database && m.table === table
        );

        const currentKeys = new Set(filtered.map(getMergeKey));
        const prevKeys = previousMergeKeysRef.current;

        // Detect completed merges: were in previous set but not in current
        const now = Date.now();
        const newlyCompleted: CompletedMerge[] = [];
        for (const key of prevKeys) {
          if (!currentKeys.has(key)) {
            // Find the merge info from the previous active set — we stored it
            // We don't have the old merge objects here, so we track by key only.
            // Instead, we'll track completed merges from the previous activeMerges state.
          }
        }

        setActiveMerges(prev => {
          // Detect merges that disappeared (completed)
          for (const prevMerge of prev) {
            const key = getMergeKey(prevMerge);
            if (!currentKeys.has(key)) {
              newlyCompleted.push({ merge: prevMerge, completedAt: now });
            }
          }

          if (newlyCompleted.length > 0) {
            setCompletedMerges(existing => [...existing, ...newlyCompleted]);
          }

          return filtered;
        });

        previousMergeKeysRef.current = currentKeys;
        setIsLoading(false);
      })
      .catch(err => {
        if (!cancelledRef.current) {
          setError(err instanceof Error ? err.message : String(err));
          setIsLoading(false);
        }
      });
  }, [adapter, database, table, getMergeKey]);

  // Polling effect
  useEffect(() => {
    if (!database || !table) return;

    fetchMerges();
    const interval = setInterval(fetchMerges, pollInterval);

    return () => {
      cancelledRef.current = true;
      clearInterval(interval);
    };
  }, [fetchMerges, pollInterval, database, table]);

  // Fade timer: remove completed merges after fadeDuration
  useEffect(() => {
    if (completedMerges.length === 0) return;

    const timer = setInterval(() => {
      const now = Date.now();
      setCompletedMerges(prev =>
        prev.filter(cm => now - cm.completedAt < fadeDuration)
      );
    }, 1000);

    return () => clearInterval(timer);
  }, [completedMerges.length, fadeDuration]);

  const statistics = useMemo<MergeStatistics>(() => {
    const activeMergeCount = activeMerges.length;
    const completedMergeCount = completedMerges.length;
    const totalBytesProcessing = activeMerges.reduce(
      (sum, m) => sum + m.total_size_bytes_compressed,
      0
    );
    const averageProgress =
      activeMergeCount > 0
        ? activeMerges.reduce((sum, m) => sum + m.progress, 0) / activeMergeCount
        : 0;

    return { activeMergeCount, completedMergeCount, totalBytesProcessing, averageProgress };
  }, [activeMerges, completedMerges]);

  return {
    activeMerges,
    completedMerges,
    statistics,
    isLoading,
    error,
    refresh: fetchMerges,
  };
}
