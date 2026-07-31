import { useCallback, useMemo, useState } from 'react';

export interface ErrorCodeSuggestion {
  code: number;
  label: string;
}

export interface ErrorCodeChoice extends ErrorCodeSuggestion {
  selected: boolean;
}

export const ERROR_CODE_GROUP_LABEL = 'Error codes in results · ⌘/Ctrl-click for multiple';

/** Build stable code/name/count suggestions from result records. */
export function buildErrorCodeSuggestions<T>(
  records: T[],
  getCode: (record: T) => number | undefined,
  getException: (record: T) => string | null | undefined,
): ErrorCodeSuggestion[] {
  const byCode = new Map<number, { count: number; name?: string }>();
  for (const record of records) {
    const code = getCode(record) ?? 0;
    if (!Number.isInteger(code) || code <= 0) continue;
    const names = [...(getException(record) ?? '').matchAll(/\(([A-Z][A-Z0-9_]+)\)/g)];
    const name = names.at(-1)?.[1];
    const existing = byCode.get(code);
    if (existing) {
      existing.count += 1;
      if (!existing.name && name) existing.name = name;
    } else {
      byCode.set(code, { count: 1, name });
    }
  }
  return [...byCode.entries()]
    .sort((a, b) => b[1].count - a[1].count || a[0] - b[0])
    .map(([code, detail]) => ({
      code,
      label: `Code ${code}${detail.name ? ` · ${detail.name}` : ''} (${detail.count})`,
    }));
}

/**
 * Keep the original choices available after selecting the first code, because
 * the resulting server-side filter narrows the refreshed result set.
 */
export function useErrorCodeChoices(
  suggestions: ErrorCodeSuggestion[],
  selectedCodes: number[] | undefined,
  query: string,
): {
  choices: ErrorCodeChoice[];
  rememberCurrentChoices: () => void;
} {
  const [unfilteredSuggestions, setUnfilteredSuggestions] = useState(suggestions);
  const choices = useMemo(() => {
    const source = selectedCodes?.length
      ? [...new Map(
          [...unfilteredSuggestions, ...suggestions]
            .map(suggestion => [suggestion.code, suggestion]),
        ).values()]
      : suggestions;
    const normalizedQuery = query.toLowerCase();
    const selected = new Set(selectedCodes ?? []);
    return source
      .filter(suggestion =>
        !normalizedQuery
        || String(suggestion.code).includes(normalizedQuery)
        || suggestion.label.toLowerCase().includes(normalizedQuery)
      )
      .map(suggestion => ({
        ...suggestion,
        selected: selected.has(suggestion.code),
      }));
  }, [query, selectedCodes, suggestions, unfilteredSuggestions]);

  const rememberCurrentChoices = useCallback(() => {
    setUnfilteredSuggestions(suggestions);
  }, [suggestions]);

  return { choices, rememberCurrentChoices };
}
