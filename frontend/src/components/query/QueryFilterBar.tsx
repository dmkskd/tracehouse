/**
 * QueryFilterBar - Shared filter bar with tag-based search input.
 *
 * Layout:  Start Time | End Time | [chip search input] | Limit | ☐ Hide tracehouse queries
 *
 * The chip search input supports field:value pairs. Typing shows autocomplete
 * for field names; after selecting a field, User/Server show dropdown hints
 * from queryAnalyzer while other fields accept freeform text. Confirmed
 * entries become removable chips. Values within a field are ORed; fields are ANDed.
 */

import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import type { QueryAnalyzer } from '@tracehouse/core';
import {
  TrackerFilterBarShell,
  TrackerLimitInput,
  TrackerRefreshButton,
} from '../common/TrackerFilterBarShell';
import {
  trackerFilterLabelStyle,
  trackerScopeOptionStyle,
} from '../common/trackerFilterStyles';
import { TimeRangePicker } from '../common/TimeRangePicker';
import {
  TRACKER_TIME_PRESETS,
  trackerTimeRangeHours,
} from '../../utils/trackerTimeRange';

/* ------------------------------------------------------------------ */
/*  Public types                                                       */
/* ------------------------------------------------------------------ */

export interface QueryFilterState {
  quickFilter?: QueryQuickFilter;
  timeRange?: string;
  queryId?: string[];
  user?: string[];
  hostname?: string[];
  queryText?: string;
  startTime?: string;
  endTime?: string;
  minDurationMs?: number;
  minMemoryBytes?: number;
  limit?: number;
  excludeAppQueries?: boolean;
  queryKind?: string[];
  status?: string[];
  database?: string[];
  table?: string[];
}

export type QueryQuickFilter = 'running' | 'recent' | 'failed' | 'slow';

interface QueryFilterBarProps {
  filter: QueryFilterState;
  onFilterChange: (patch: Partial<QueryFilterState>) => void;
  queryAnalyzer?: QueryAnalyzer;
  onRefresh?: () => void;
  isLoading?: boolean;
}

/* ------------------------------------------------------------------ */
/*  Filter field definitions                                           */
/* ------------------------------------------------------------------ */

interface FilterFieldDef {
  key: string;
  label: string;
  /** placeholder shown after the field is selected */
  placeholder: string;
  /** map typed value → QueryFilterState patch */
  toFilter: (value: string | string[]) => Partial<QueryFilterState>;
  /** extract display value from current filter state (for chip) */
  fromFilter: (f: QueryFilterState) => string | string[] | undefined;
  /** clear this field */
  clear: () => Partial<QueryFilterState>;
  /** whether queryAnalyzer provides suggestions */
  hasSuggestions?: boolean;
  /** key used with queryAnalyzer.getDistinctFilterValues */
  suggestionKey?: string;
  /** static suggestion values (no queryAnalyzer needed) */
  hasStaticSuggestions?: boolean;
  staticSuggestions?: string[];
  /** Multiple values are ORed together; different fields remain ANDed. */
  multi?: boolean;
}

const valuesOf = (value: string | string[] | undefined): string[] =>
  Array.isArray(value) ? value : value ? [value] : [];

const singleValue = (value: string | string[]): string =>
  Array.isArray(value) ? value[0] ?? '' : value;

const multiValue = (value: string | string[]): string[] =>
  Array.isArray(value) ? value : [value];

const FILTER_FIELDS: FilterFieldDef[] = [
  {
    key: 'user', label: 'User', placeholder: 'e.g. default',
    toFilter: v => ({ user: multiValue(v) }),
    fromFilter: f => f.user,
    clear: () => ({ user: undefined }),
    hasSuggestions: true, suggestionKey: 'user',
    multi: true,
  },
  {
    key: 'server', label: 'Server', placeholder: 'e.g. chi-clickhouse-0-0',
    toFilter: v => ({ hostname: multiValue(v) }),
    fromFilter: f => f.hostname,
    clear: () => ({ hostname: undefined }),
    hasSuggestions: true, suggestionKey: 'hostname',
    multi: true,
  },
  {
    key: 'query_id', label: 'Query ID', placeholder: 'enter a query ID',
    toFilter: v => ({ queryId: multiValue(v) }),
    fromFilter: f => f.queryId,
    clear: () => ({ queryId: undefined }),
    multi: true,
  },
  {
    key: 'query', label: 'Query Contains', placeholder: 'e.g. SELECT, s3(…',
    toFilter: v => ({ queryText: singleValue(v) || undefined }),
    fromFilter: f => f.queryText,
    clear: () => ({ queryText: undefined }),
  },
  {
    key: 'min_duration', label: 'Min Duration (ms)', placeholder: 'e.g. 1000',
    toFilter: v => {
      const value = singleValue(v);
      return { minDurationMs: value ? parseInt(value, 10) : undefined };
    },
    fromFilter: f => f.minDurationMs != null ? String(f.minDurationMs) : undefined,
    clear: () => ({ minDurationMs: undefined }),
  },
  {
    key: 'min_memory', label: 'Min Memory (MB)', placeholder: 'e.g. 100',
    toFilter: v => {
      const value = singleValue(v);
      return { minMemoryBytes: value ? parseInt(value, 10) * 1024 * 1024 : undefined };
    },
    fromFilter: f => f.minMemoryBytes ? String(Math.round(f.minMemoryBytes / 1024 / 1024)) : undefined,
    clear: () => ({ minMemoryBytes: undefined }),
  },
  {
    key: 'query_kind', label: 'Type', placeholder: 'e.g. SELECT, INSERT…',
    toFilter: v => ({ queryKind: multiValue(v) }),
    fromFilter: f => f.queryKind,
    clear: () => ({ queryKind: undefined }),
    hasSuggestions: true, suggestionKey: 'query_kind',
    multi: true,
  },
  {
    key: 'status', label: 'Status', placeholder: 'running, success, or error',
    toFilter: v => ({ status: multiValue(v) }),
    fromFilter: f => f.status,
    clear: () => ({ status: undefined }),
    hasStaticSuggestions: true,
    staticSuggestions: ['running', 'success', 'error'],
    multi: true,
  },
  {
    key: 'database', label: 'Database', placeholder: 'e.g. default',
    toFilter: v => ({ database: multiValue(v) }),
    fromFilter: f => f.database,
    clear: () => ({ database: undefined }),
    multi: true,
  },
  {
    key: 'table', label: 'Table', placeholder: 'e.g. my_table',
    toFilter: v => ({ table: multiValue(v) }),
    fromFilter: f => f.table,
    clear: () => ({ table: undefined }),
    multi: true,
  },
];

interface QuickFilterDef {
  key: QueryQuickFilter;
  label: string;
  description: string;
  patch: Partial<QueryFilterState>;
  matches: (filter: QueryFilterState) => boolean;
}

const hasOnlyStatus = (filter: QueryFilterState, status: string): boolean =>
  filter.status?.length === 1
  && filter.status[0]?.toLowerCase() === status
  && filter.minDurationMs == null;

const hasOnlyStatuses = (filter: QueryFilterState, statuses: string[]): boolean => {
  const selected = new Set(filter.status?.map(status => status.toLowerCase()) ?? []);
  return selected.size === statuses.length
    && statuses.every(status => selected.has(status))
    && filter.minDurationMs == null;
};

const QUICK_FILTERS: QuickFilterDef[] = [
  {
    key: 'running',
    label: 'Running now',
    description: 'Currently executing',
    patch: { status: ['running'], minDurationMs: undefined },
    matches: filter => hasOnlyStatus(filter, 'running'),
  },
  {
    key: 'recent',
    label: 'Recent',
    description: 'Completed in the selected time range',
    patch: { status: ['success', 'error'], minDurationMs: undefined },
    matches: filter => hasOnlyStatuses(filter, ['success', 'error']),
  },
  {
    key: 'failed',
    label: 'Failed queries',
    description: 'Errors in the selected time range',
    patch: { status: ['error'], minDurationMs: undefined },
    matches: filter => hasOnlyStatus(filter, 'error'),
  },
  {
    key: 'slow',
    label: 'Slow queries',
    description: 'Duration ≥ 1 second',
    patch: { status: undefined, minDurationMs: 1_000 },
    matches: filter => !filter.status?.length && filter.minDurationMs === 1_000,
  },
];

const isPresetConstraintField = (field: FilterFieldDef): boolean =>
  field.key === 'status' || field.key === 'min_duration';

/* ------------------------------------------------------------------ */
/*  Styles                                                             */
/* ------------------------------------------------------------------ */

const chipStyle: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 4,
  padding: '3px 8px', fontSize: 11, borderRadius: 12,
  background: 'rgba(88,166,255,0.12)', color: '#58a6ff',
  border: '1px solid rgba(88,166,255,0.25)', whiteSpace: 'nowrap',
};

const chipRemoveStyle: React.CSSProperties = {
  cursor: 'pointer', fontSize: 13, lineHeight: 1, marginLeft: 2,
  color: '#58a6ff', opacity: 0.7,
};

const quickChipStyle: React.CSSProperties = {
  ...chipStyle,
  background: 'rgba(163,113,247,0.12)',
  color: '#a371f7',
  border: '1px solid rgba(163,113,247,0.3)',
};

const dropdownContainerStyle: React.CSSProperties = {
  position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 20,
  maxHeight: 200, overflowY: 'auto',
  background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)',
  borderRadius: 6, marginTop: 2, boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
};

const dropdownItemStyle: React.CSSProperties = {
  padding: '6px 10px', fontSize: 12, cursor: 'pointer',
  color: 'var(--text-secondary)',
};

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

type Phase = 'idle' | 'picking_field' | 'entering_value';

type DropdownItem =
  | { id: string; label: string; group: 'quick'; quickFilter: QuickFilterDef }
  | { id: string; label: string; group: 'field'; field: FilterFieldDef }
  | { id: string; label: string; group: 'value'; field: FilterFieldDef };

export const QueryFilterBar: React.FC<QueryFilterBarProps> = ({
  filter, onFilterChange, queryAnalyzer, onRefresh, isLoading,
}) => {
  /* --- local state for the chip search input --- */
  const [phase, setPhase] = useState<Phase>('idle');
  const [activeField, setActiveField] = useState<FilterFieldDef | null>(null);
  const [inputValue, setInputValue] = useState('');
  const [highlightIdx, setHighlightIdx] = useState(-1);
  const [showDropdown, setShowDropdown] = useState(false);

  /* --- suggestions from queryAnalyzer --- */
  const [suggestionCache, setSuggestionCache] = useState<Record<string, string[]>>({});
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!queryAnalyzer) return;
    FILTER_FIELDS.filter(f => f.hasSuggestions && f.suggestionKey).forEach(f => {
      queryAnalyzer.getDistinctFilterValues(f.suggestionKey as 'user' | 'hostname' | 'query_kind').then(vals => {
        setSuggestionCache(prev => ({ ...prev, [f.key]: vals }));
      }).catch(() => {});
    });
  }, [queryAnalyzer]);

  const activeQuickFilter = useMemo(
    () => QUICK_FILTERS.find(preset =>
      preset.key === filter.quickFilter && preset.matches(filter)
    ) ?? null,
    [filter],
  );

  /* --- active chips derived from filter state --- */
  const activeChips = useMemo(() => {
    const chips: { field: FilterFieldDef; displayValues: string[] }[] = [];
    for (const f of FILTER_FIELDS) {
      if (activeQuickFilter && isPresetConstraintField(f)) continue;
      const displayValues = valuesOf(f.fromFilter(filter));
      if (displayValues.length > 0) chips.push({ field: f, displayValues });
    }
    return chips;
  }, [activeQuickFilter, filter]);

  /* --- which fields are still available (not yet used) --- */
  const availableFields = useMemo(() => {
    const usedKeys = new Set(activeChips.map(c => c.field.key));
    return FILTER_FIELDS.filter(f => f.multi || !usedKeys.has(f.key));
  }, [activeChips]);

  /* --- dropdown items based on phase --- */
  const dropdownItems = useMemo(() => {
    if (phase === 'idle' || phase === 'picking_field') {
      const q = inputValue.toLowerCase();
      const quickItems: DropdownItem[] = QUICK_FILTERS
        .filter(preset => preset.key !== activeQuickFilter?.key)
        .filter(preset =>
          !q
          || preset.label.toLowerCase().includes(q)
          || preset.description.toLowerCase().includes(q)
        )
        .map(preset => ({
          id: `quick:${preset.key}`,
          label: preset.label,
          group: 'quick',
          quickFilter: preset,
        }));
      const fieldItems: DropdownItem[] = availableFields
        .filter(f => !q || f.label.toLowerCase().includes(q) || f.key.toLowerCase().includes(q))
        .map(f => ({ id: `field:${f.key}`, label: f.label, group: 'field', field: f }));
      return [...quickItems, ...fieldItems];
    }
    if (phase === 'entering_value' && activeField?.hasSuggestions) {
      const vals = suggestionCache[activeField.key] || [];
      const selected = new Set(valuesOf(activeField.fromFilter(filter)).map(v => v.toLowerCase()));
      const q = inputValue.toLowerCase();
      return vals
        .filter(v => !selected.has(v.toLowerCase()))
        .filter(v => !q || v.toLowerCase().includes(q))
        .map(v => ({ id: v, label: v, group: 'value', field: activeField }) satisfies DropdownItem);
    }
    if (phase === 'entering_value' && activeField?.hasStaticSuggestions) {
      const vals = activeField.staticSuggestions || [];
      const selected = new Set(valuesOf(activeField.fromFilter(filter)).map(v => v.toLowerCase()));
      const q = inputValue.toLowerCase();
      return vals
        .filter(v => !selected.has(v.toLowerCase()))
        .filter(v => !q || v.toLowerCase().includes(q))
        .map(v => ({ id: v, label: v, group: 'value', field: activeField }) satisfies DropdownItem);
    }
    return [];
  }, [phase, inputValue, availableFields, activeField, suggestionCache, filter, activeQuickFilter]);

  /* --- handlers --- */
  const finishValueEntry = useCallback(() => {
    setActiveField(null);
    setInputValue('');
    setPhase('idle');
    setShowDropdown(false);
    setHighlightIdx(-1);
  }, []);

  const commitValue = useCallback((value: string, continueMultiEntry = true) => {
    if (!activeField || !value.trim()) return;
    const additions = activeField.key === 'query_id'
      ? value.split(/[\s,]+/).map(item => item.trim()).filter(Boolean)
      : [value.trim()];
    if (activeField.multi) {
      const current = valuesOf(activeField.fromFilter(filter));
      const seen = new Set(current.map(item => item.toLowerCase()));
      const next = [...current];
      additions.forEach(item => {
        if (!seen.has(item.toLowerCase())) {
          seen.add(item.toLowerCase());
          next.push(item);
        }
      });
      onFilterChange({
        ...activeField.toFilter(next),
        ...(isPresetConstraintField(activeField) && filter.quickFilter ? { quickFilter: undefined } : {}),
      });
      if (continueMultiEntry) {
        setInputValue('');
        setShowDropdown(activeField.hasSuggestions === true || activeField.hasStaticSuggestions === true);
        setHighlightIdx(-1);
        return;
      }
    } else {
      onFilterChange({
        ...activeField.toFilter(additions[0]!),
        ...(isPresetConstraintField(activeField) && filter.quickFilter ? { quickFilter: undefined } : {}),
      });
    }
    finishValueEntry();
  }, [activeField, filter, onFilterChange, finishValueEntry]);

  const selectField = useCallback((field: FilterFieldDef) => {
    setActiveField(field);
    setInputValue('');
    setPhase('entering_value');
    setHighlightIdx(-1);
    // keep dropdown open for value suggestions
    setShowDropdown(field.hasSuggestions === true || field.hasStaticSuggestions === true);
    setTimeout(() => inputRef.current?.focus(), 0);
  }, []);

  const removeValue = useCallback((field: FilterFieldDef, value: string) => {
    const quickFilterPatch = isPresetConstraintField(field) && filter.quickFilter
      ? { quickFilter: undefined }
      : {};
    if (!field.multi) {
      onFilterChange({ ...field.clear(), ...quickFilterPatch });
      return;
    }
    const remaining = valuesOf(field.fromFilter(filter))
      .filter(item => item.toLowerCase() !== value.toLowerCase());
    onFilterChange({
      ...(remaining.length > 0 ? field.toFilter(remaining) : field.clear()),
      ...quickFilterPatch,
    });
  }, [filter, onFilterChange]);

  const applyQuickFilter = useCallback((preset: QuickFilterDef) => {
    onFilterChange({
      quickFilter: preset.key,
      ...preset.patch,
    });
    setActiveField(null);
    setInputValue('');
    setPhase('idle');
    setShowDropdown(false);
    setHighlightIdx(-1);
  }, [onFilterChange]);

  const removeQuickFilter = useCallback(() => {
    onFilterChange({
      quickFilter: undefined,
      status: undefined,
      minDurationMs: undefined,
    });
  }, [onFilterChange]);

  /** Reopen a field without clearing its selected values so more can be added. */
  const editChip = useCallback((field: FilterFieldDef) => {
    setActiveField(field);
    setInputValue('');
    setPhase('entering_value');
    setHighlightIdx(-1);
    setShowDropdown(field.hasSuggestions === true || field.hasStaticSuggestions === true);
    setTimeout(() => {
      inputRef.current?.focus();
    }, 0);
  }, []);

  const handleInputFocus = useCallback(() => {
    if (phase === 'idle') setPhase('picking_field');
    setShowDropdown(true);
  }, [phase]);

  const handleInputBlur = useCallback(() => {
    // delay to allow click on dropdown item or chip
    setTimeout(() => {
      setShowDropdown(false);
      // If we were entering a value and there's text, commit it on blur
      if (phase === 'entering_value' && activeField && inputValue.trim()) {
        commitValue(inputValue, false);
      } else if (phase === 'picking_field') {
        setPhase('idle');
        setInputValue('');
      } else if (phase === 'entering_value' && activeField && !inputValue.trim()) {
        // abandoned edit with no value — just reset
        finishValueEntry();
      }
    }, 200);
  }, [phase, activeField, inputValue, commitValue, finishValueEntry]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Escape') {
      setPhase('idle'); setActiveField(null); setInputValue('');
      setShowDropdown(false); setHighlightIdx(-1);
      inputRef.current?.blur();
      return;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setHighlightIdx(i => Math.min(i + 1, dropdownItems.length - 1));
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      setHighlightIdx(i => Math.max(i - 1, 0));
      return;
    }
    if (e.key === 'Enter') {
      e.preventDefault();
      if (highlightIdx >= 0 && highlightIdx < dropdownItems.length) {
        const item = dropdownItems[highlightIdx];
        if (item.group === 'quick') applyQuickFilter(item.quickFilter);
        else if (item.group === 'field') selectField(item.field);
        else commitValue(item.label);
      } else if (phase === 'entering_value' && inputValue.trim()) {
        commitValue(inputValue);
      } else if (phase === 'entering_value') {
        finishValueEntry();
      }
      return;
    }
    if (e.key === 'Backspace' && !inputValue && phase === 'entering_value') {
      // go back to field picking
      setActiveField(null); setPhase('picking_field'); setShowDropdown(true);
      return;
    }
    if (e.key === 'Backspace' && !inputValue && phase === 'picking_field' && activeChips.length > 0) {
      // Match tag-picker behavior: remove the last selected value.
      const last = activeChips[activeChips.length - 1];
      const lastValue = last.displayValues[last.displayValues.length - 1];
      if (lastValue) removeValue(last.field, lastValue);
      return;
    }
    if (e.key === 'Backspace' && !inputValue && phase === 'picking_field' && activeQuickFilter) {
      removeQuickFilter();
    }
  }, [dropdownItems, highlightIdx, phase, inputValue, activeChips, activeQuickFilter, selectField, commitValue, finishValueEntry, removeValue, applyQuickFilter, removeQuickFilter]);

  const handleInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setInputValue(e.target.value);
    setHighlightIdx(-1);
    if (phase === 'idle') setPhase('picking_field');
    setShowDropdown(true);
  }, [phase]);

  const handleDropdownClick = useCallback((item: DropdownItem) => {
    if (item.group === 'quick') {
      applyQuickFilter(item.quickFilter);
    } else if (item.group === 'field') {
      selectField(item.field);
    } else {
      commitValue(item.label);
    }
  }, [applyQuickFilter, selectField, commitValue]);

  /* --- lookback warning --- */
  const lookbackHours = trackerTimeRangeHours(
    filter.timeRange,
    filter.startTime,
    filter.endTime,
  );
  const fmtLookback = lookbackHours < 24
    ? `${Math.round(lookbackHours)}h`
    : lookbackHours < 168
      ? `${(lookbackHours / 24).toFixed(1)}d`
      : `${(lookbackHours / 168).toFixed(1)}w`;

  /* --- render --- */
  return (
    <TrackerFilterBarShell
      footer={lookbackHours > 1 ? (
        <div style={{
          marginTop: 8,
          padding: '5px 10px',
          fontSize: 10,
          color: lookbackHours > 24 ? '#d29922' : 'var(--text-muted)',
          background: lookbackHours > 24 ? 'rgba(210, 153, 34, 0.06)' : 'transparent',
          borderRadius: 4,
          letterSpacing: '0.3px',
        }}>
          Lookback window: {fmtLookback} — wider windows scan more data from system.query_log{lookbackHours > 24 ? ' and consume more server resources' : ''}
        </div>
      ) : undefined}
    >
        {/* Time range */}
        <div>
          <label style={trackerFilterLabelStyle}>Time Range</label>
          <TimeRangePicker
            value={filter.timeRange ?? '1 HOUR'}
            onChange={value => onFilterChange({
              timeRange: value ?? '1 HOUR',
              startTime: undefined,
              endTime: undefined,
            })}
            presets={[...TRACKER_TIME_PRESETS]}
            popoverAlign="left"
          />
        </div>

        {/* Chip search input */}
        <div style={{ flex: 1, minWidth: 260, position: 'relative' }}>
          <label style={trackerFilterLabelStyle}>Filters</label>
          <div
            style={{
              display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 4,
              padding: '4px 8px', minHeight: 32,
              background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)',
              borderRadius: 6, cursor: 'text',
            }}
            onClick={() => inputRef.current?.focus()}
          >
            {activeQuickFilter && (
              <span style={quickChipStyle}>
                <span aria-hidden="true" style={{ fontSize: 10 }}>⚡</span>
                <span style={{ fontWeight: 600 }}>{activeQuickFilter.label}</span>
                <span
                  role="button"
                  aria-label={`Remove quick filter ${activeQuickFilter.label}`}
                  style={{ ...chipRemoveStyle, color: '#a371f7' }}
                  onClick={e => { e.stopPropagation(); removeQuickFilter(); }}
                >
                  ×
                </span>
              </span>
            )}
            {/* Existing chips */}
            {activeChips.map(c => (
              <span key={c.field.key} style={{ ...chipStyle, cursor: 'pointer' }}
                onClick={e => { e.stopPropagation(); editChip(c.field); }}>
                <span style={{ fontWeight: 600, fontSize: 10 }}>{c.field.label}:</span>
                {c.displayValues.map(displayValue => (
                  <span
                    key={displayValue}
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: 3,
                      padding: '0 4px',
                      borderRadius: 999,
                      background: 'rgba(88,166,255,0.08)',
                    }}
                  >
                    {displayValue}
                    <span
                      role="button"
                      aria-label={`Remove ${c.field.label} ${displayValue}`}
                      style={chipRemoveStyle}
                      onClick={e => { e.stopPropagation(); removeValue(c.field, displayValue); }}
                    >
                      ×
                    </span>
                  </span>
                ))}
              </span>
            ))}
            {/* Active field label (while entering value) */}
            {phase === 'entering_value' && activeField && valuesOf(activeField.fromFilter(filter)).length === 0 && (
              <span style={{ ...chipStyle, background: 'rgba(88,166,255,0.06)', borderStyle: 'dashed' }}>
                <span style={{ fontWeight: 600, fontSize: 10 }}>{activeField.label}:</span>
              </span>
            )}
            {/* The actual input */}
            <input
              ref={inputRef}
              value={inputValue}
              onChange={handleInputChange}
              onFocus={handleInputFocus}
              onBlur={handleInputBlur}
              onKeyDown={handleKeyDown}
              placeholder={
                phase === 'entering_value' && activeField
                  ? activeField.multi && valuesOf(activeField.fromFilter(filter)).length > 0
                    ? `Add another ${activeField.label.toLowerCase()}…`
                    : activeField.placeholder
                  : activeQuickFilter || activeChips.length > 0
                    ? 'Add filter…'
                    : 'Type to filter (user, server, query…)'
              }
              style={{
                flex: 1, minWidth: 120, border: 'none', outline: 'none',
                background: 'transparent', color: 'var(--text-primary)',
                fontSize: 12, fontFamily: 'inherit', padding: '2px 0',
              }}
            />
          </div>
          {/* Dropdown */}
          {showDropdown && dropdownItems.length > 0 && (
            <div style={dropdownContainerStyle}>
              {dropdownItems.map((item, idx) => {
                const showGroupLabel = item.group !== 'value'
                  && dropdownItems[idx - 1]?.group !== item.group;
                return (
                  <React.Fragment key={item.id}>
                    {showGroupLabel && (
                      <div style={{
                        padding: '8px 10px 4px',
                        color: 'var(--text-muted)',
                        fontSize: 9,
                        fontWeight: 700,
                        letterSpacing: '0.8px',
                        textTransform: 'uppercase',
                      }}>
                        {item.group === 'quick' ? 'Quick filters' : 'Add a filter'}
                      </div>
                    )}
                    <div
                      onMouseDown={e => { e.preventDefault(); handleDropdownClick(item); }}
                      onMouseEnter={() => setHighlightIdx(idx)}
                      style={{
                        ...dropdownItemStyle,
                        background: idx === highlightIdx ? 'var(--bg-secondary)' : 'transparent',
                        fontWeight: item.group === 'value' ? 400 : 500,
                        display: 'flex',
                        alignItems: 'center',
                        gap: 7,
                      }}
                    >
                      <span style={{
                        color: item.group === 'quick' ? '#a371f7' : 'var(--text-muted)',
                        fontSize: 10,
                        width: 11,
                        flex: '0 0 11px',
                      }}>
                        {item.group === 'quick' ? '⚡' : item.group === 'field' ? '⊕' : ''}
                      </span>
                      <span style={{ minWidth: 0 }}>
                        <span>{item.label}</span>
                        {item.group === 'quick' && (
                          <span style={{ marginLeft: 8, color: 'var(--text-muted)', fontWeight: 400, fontSize: 10 }}>
                            {item.quickFilter.description}
                          </span>
                        )}
                      </span>
                    </div>
                  </React.Fragment>
                );
              })}
            </div>
          )}
        </div>

        <TrackerLimitInput
          value={filter.limit}
          onChange={limit => onFilterChange({ limit })}
        />

        <label style={{
          ...trackerScopeOptionStyle,
          width: 92,
          whiteSpace: 'normal',
        }}>
          <input type="checkbox"
            checked={filter.excludeAppQueries ?? false}
            onChange={e => onFilterChange({ excludeAppQueries: e.target.checked })}
            style={{ margin: 0 }} />
          <span>Hide TraceHouse<br />queries</span>
        </label>

      {onRefresh && (
        <div style={{ marginLeft: 'auto' }}>
          <TrackerRefreshButton onRefresh={onRefresh} isLoading={isLoading} />
        </div>
      )}
    </TrackerFilterBarShell>
  );
};

export default QueryFilterBar;
