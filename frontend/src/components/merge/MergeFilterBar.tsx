/**
 * MergeFilterBar - Tag-based filter bar for merge/mutation tabs.
 *
 * Layout:  [chip search input] | Limit | count | Refresh
 *
 * Mirrors the QueryFilterBar pattern: typing shows autocomplete for field
 * names; after selecting a field, shows dropdown suggestions from props.
 * Confirmed entries become removable chips. Values within a field are ORed; fields are ANDed.
 */

import React, { useState, useRef, useCallback, useMemo } from 'react';
import type { MergeHistoryFilter } from '../../stores/mergeStore';
import { TimeRangePicker } from '../common/TimeRangePicker';
import {
  TrackerFilterBarShell,
  TrackerLimitInput,
  TrackerRefreshButton,
} from '../common/TrackerFilterBarShell';
import {
  trackerFilterLabelStyle,
  trackerScopeOptionStyle,
} from '../common/trackerFilterStyles';
import { TRACKER_TIME_PRESETS } from '../../utils/trackerTimeRange';

export type MergeTab = 'merges' | 'mutations' | 'health';

/* ------------------------------------------------------------------ */
/*  Public types                                                       */
/* ------------------------------------------------------------------ */

interface MergeFilterBarProps {
  tab: MergeTab;
  filter: MergeHistoryFilter;
  onFilterChange: (patch: Partial<MergeHistoryFilter>) => void;
  availableDatabases: string[];
  availableTables: string[];
  /** Toggle to exclude system/information_schema databases */
  excludeSystemDatabases?: boolean;
  onExcludeSystemChange?: (v: boolean) => void;
  /** Toggle to hide replica merges (same merge on multiple replicas) */
  hideReplicaMerges?: boolean;
  onHideReplicaMergesChange?: (v: boolean) => void;
  /** For Merge History: distinct merge_reason values */
  mergeReasons?: string[];
  selectedMergeReason?: string[];
  onMergeReasonChange?: (v: string[] | undefined) => void;
  /** Host filter (client-side) */
  availableHosts?: string[];
  selectedHost?: string[];
  onHostChange?: (v: string[] | undefined) => void;
  /** Status filter (client-side): OK or Error */
  availableStatuses?: string[];
  selectedStatus?: string[];
  onStatusChange?: (v: string[] | undefined) => void;
  /** Part name filter (client-side, substring match) */
  selectedPartName?: string;
  onPartNameChange?: (v: string | undefined) => void;
  onRefresh?: () => void;
  isLoading?: boolean;
}

/* ------------------------------------------------------------------ */
/*  Filter field definitions                                           */
/* ------------------------------------------------------------------ */

interface FilterFieldDef {
  key: string;
  label: string;
  placeholder: string;
  /** Which tabs this field is visible on (undefined = all) */
  tabs?: MergeTab[];
  /** Get suggestions from props */
  getSuggestions: (props: MergeFilterBarProps) => string[];
  /** Extract current display value */
  fromProps: (props: MergeFilterBarProps) => string | string[] | undefined;
  /** Apply a value */
  apply: (value: string | string[], props: MergeFilterBarProps) => void;
  /** Clear this field */
  clear: (props: MergeFilterBarProps) => void;
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
    key: 'database', label: 'Database', placeholder: 'e.g. default',
    getSuggestions: p => p.availableDatabases,
    fromProps: p => p.filter.database,
    apply: (v, p) => p.onFilterChange({ database: multiValue(v) }),
    clear: p => p.onFilterChange({ database: undefined, table: undefined }),
    multi: true,
  },
  {
    key: 'table', label: 'Table', placeholder: 'e.g. my_table',
    getSuggestions: p => p.availableTables,
    fromProps: p => p.filter.table,
    apply: (v, p) => p.onFilterChange({ table: multiValue(v) }),
    clear: p => p.onFilterChange({ table: undefined }),
    multi: true,
  },
  {
    key: 'merge_reason', label: 'Category', placeholder: 'e.g. Regular, TTLDelete, Mutation',
    tabs: ['merges'],
    getSuggestions: p => p.mergeReasons || [],
    fromProps: p => p.selectedMergeReason,
    apply: (v, p) => p.onMergeReasonChange?.(multiValue(v)),
    clear: p => p.onMergeReasonChange?.(undefined),
    multi: true,
  },
  {
    key: 'status', label: 'Status', placeholder: 'OK or Error',
    tabs: ['merges'],
    getSuggestions: p => p.availableStatuses || [],
    fromProps: p => p.selectedStatus,
    apply: (v, p) => p.onStatusChange?.(multiValue(v)),
    clear: p => p.onStatusChange?.(undefined),
    multi: true,
  },
  {
    key: 'host', label: 'Host', placeholder: 'e.g. chi-clickhouse-0-0',
    tabs: ['merges'],
    getSuggestions: p => p.availableHosts || [],
    fromProps: p => p.selectedHost,
    apply: (v, p) => p.onHostChange?.(multiValue(v)),
    clear: p => p.onHostChange?.(undefined),
    multi: true,
  },
  {
    key: 'part', label: 'Part', placeholder: 'e.g. all_1_3_1',
    tabs: ['merges'],
    getSuggestions: () => [],
    fromProps: p => p.selectedPartName,
    apply: (v, p) => p.onPartNameChange?.(singleValue(v) || undefined),
    clear: p => p.onPartNameChange?.(undefined),
  },
  {
    key: 'min_duration', label: 'Min Duration (s)', placeholder: 'e.g. 5',
    tabs: ['merges'],
    getSuggestions: () => ['1', '5', '10', '30', '60'],
    fromProps: p => p.filter.minDurationMs != null ? String(p.filter.minDurationMs / 1000) : undefined,
    apply: (v, p) => {
      const secs = parseFloat(singleValue(v));
      p.onFilterChange({ minDurationMs: secs > 0 ? Math.round(secs * 1000) : undefined });
    },
    clear: p => p.onFilterChange({ minDurationMs: undefined }),
  },
  {
    key: 'min_size', label: 'Min Size (MB)', placeholder: 'e.g. 100',
    tabs: ['merges'],
    getSuggestions: () => ['10', '100', '500', '1000'],
    fromProps: p => p.filter.minSizeBytes != null ? String(Math.round(p.filter.minSizeBytes / (1024 * 1024))) : undefined,
    apply: (v, p) => {
      const mb = parseFloat(singleValue(v));
      p.onFilterChange({ minSizeBytes: mb > 0 ? Math.round(mb * 1024 * 1024) : undefined });
    },
    clear: p => p.onFilterChange({ minSizeBytes: undefined }),
  },
];

/* ------------------------------------------------------------------ */
/*  Styles                                                             */
/* ------------------------------------------------------------------ */

const chipStyle: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 4,
  padding: '3px 8px', fontSize: 11, borderRadius: 12,
  background: 'rgba(240,136,62,0.12)', color: '#f0883e',
  border: '1px solid rgba(240,136,62,0.25)', whiteSpace: 'nowrap',
};

const chipRemoveStyle: React.CSSProperties = {
  cursor: 'pointer', fontSize: 13, lineHeight: 1, marginLeft: 2,
  color: '#f0883e', opacity: 0.7,
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

export const MergeFilterBar: React.FC<MergeFilterBarProps> = (props) => {
  const {
    tab, filter, onFilterChange, onRefresh, isLoading,
  } = props;

  const showLimit = tab === 'merges' || tab === 'mutations';

  /* --- fields visible for current tab --- */
  const visibleFields = useMemo(
    () => FILTER_FIELDS.filter(f => !f.tabs || f.tabs.includes(tab)),
    [tab],
  );

  /* --- local state for chip search input --- */
  const [phase, setPhase] = useState<Phase>('idle');
  const [activeField, setActiveField] = useState<FilterFieldDef | null>(null);
  const [inputValue, setInputValue] = useState('');
  const [highlightIdx, setHighlightIdx] = useState(-1);
  const [showDropdown, setShowDropdown] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  /* --- active chips derived from props --- */
  const activeChips = useMemo(() => {
    const chips: { field: FilterFieldDef; displayValues: string[] }[] = [];
    for (const f of visibleFields) {
      const displayValues = valuesOf(f.fromProps(props));
      if (displayValues.length > 0) chips.push({ field: f, displayValues });
    }
    return chips;
  }, [visibleFields, props]);

  /* --- available fields (not yet used) --- */
  const availableFields = useMemo(() => {
    const usedKeys = new Set(activeChips.map(c => c.field.key));
    return visibleFields.filter(f => f.multi || !usedKeys.has(f.key));
  }, [activeChips, visibleFields]);

  /* --- dropdown items based on phase --- */
  const dropdownItems = useMemo(() => {
    if (phase === 'idle' || phase === 'picking_field') {
      const q = inputValue.toLowerCase();
      return availableFields
        .filter(f => !q || f.label.toLowerCase().includes(q) || f.key.toLowerCase().includes(q))
        .map(f => ({ id: f.key, label: f.label, field: f }));
    }
    if (phase === 'entering_value' && activeField) {
      const vals = activeField.getSuggestions(props);
      const selected = new Set(valuesOf(activeField.fromProps(props)).map(v => v.toLowerCase()));
      const q = inputValue.toLowerCase();
      return vals
        .filter(v => !selected.has(v.toLowerCase()))
        .filter(v => !q || v.toLowerCase().includes(q))
        .map(v => ({ id: v, label: v, field: activeField }));
    }
    return [];
  }, [phase, inputValue, availableFields, activeField, props]);

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
    if (activeField.multi) {
      const current = valuesOf(activeField.fromProps(props));
      const seen = new Set(current.map(item => item.toLowerCase()));
      const next = [...current];
      if (!seen.has(value.trim().toLowerCase())) next.push(value.trim());
      activeField.apply(next, props);
      if (continueMultiEntry) {
        setInputValue('');
        setShowDropdown(activeField.getSuggestions(props).length > 0);
        setHighlightIdx(-1);
        return;
      }
    } else {
      activeField.apply(value.trim(), props);
    }
    finishValueEntry();
  }, [activeField, props, finishValueEntry]);

  const selectField = useCallback((field: FilterFieldDef) => {
    setActiveField(field);
    setInputValue('');
    setPhase('entering_value');
    setHighlightIdx(-1);
    const hasSuggestions = field.getSuggestions(props).length > 0;
    setShowDropdown(hasSuggestions);
    setTimeout(() => inputRef.current?.focus(), 0);
  }, [props]);

  const removeValue = useCallback((field: FilterFieldDef, value: string) => {
    if (!field.multi) {
      field.clear(props);
      return;
    }
    const remaining = valuesOf(field.fromProps(props))
      .filter(item => item.toLowerCase() !== value.toLowerCase());
    if (remaining.length > 0) field.apply(remaining, props);
    else field.clear(props);
  }, [props]);

  const editChip = useCallback((field: FilterFieldDef) => {
    setActiveField(field);
    setInputValue('');
    setPhase('entering_value');
    setHighlightIdx(-1);
    setShowDropdown(field.getSuggestions(props).length > 0);
    setTimeout(() => {
      inputRef.current?.focus();
    }, 0);
  }, [props]);

  const handleInputFocus = useCallback(() => {
    if (phase === 'idle') setPhase('picking_field');
    setShowDropdown(true);
  }, [phase]);

  const handleInputBlur = useCallback(() => {
    setTimeout(() => {
      setShowDropdown(false);
      if (phase === 'entering_value' && activeField && inputValue.trim()) {
        commitValue(inputValue, false);
      } else if (phase === 'picking_field') {
        setPhase('idle');
        setInputValue('');
      } else if (phase === 'entering_value' && activeField && !inputValue.trim()) {
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
        if (phase === 'picking_field') selectField(item.field);
        else if (phase === 'entering_value') commitValue(item.label);
      } else if (phase === 'entering_value' && inputValue.trim()) {
        commitValue(inputValue);
      } else if (phase === 'entering_value') {
        finishValueEntry();
      }
      return;
    }
    if (e.key === 'Backspace' && !inputValue && phase === 'entering_value') {
      setActiveField(null); setPhase('picking_field'); setShowDropdown(true);
      return;
    }
    if (e.key === 'Backspace' && !inputValue && phase === 'picking_field' && activeChips.length > 0) {
      const last = activeChips[activeChips.length - 1];
      const lastValue = last.displayValues[last.displayValues.length - 1];
      if (lastValue) removeValue(last.field, lastValue);
    }
  }, [dropdownItems, highlightIdx, phase, inputValue, activeChips, selectField, commitValue, finishValueEntry, removeValue]);

  const handleInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    setInputValue(e.target.value);
    setHighlightIdx(-1);
    if (phase === 'idle') setPhase('picking_field');
    setShowDropdown(true);
  }, [phase]);

  const handleDropdownClick = useCallback((item: typeof dropdownItems[0]) => {
    if (phase === 'picking_field') selectField(item.field);
    else if (phase === 'entering_value') commitValue(item.label);
  }, [phase, selectField, commitValue]);

  /* --- reset chips when tab changes (clear tab-specific filters) --- */
  const prevTabRef = useRef(tab);
  React.useEffect(() => {
    if (prevTabRef.current !== tab) {
      prevTabRef.current = tab;
      // Reset phase when switching tabs
      setPhase('idle');
      setActiveField(null);
      setInputValue('');
      setShowDropdown(false);
      setHighlightIdx(-1);
    }
  }, [tab]);

  /* --- render --- */
  return (
    <TrackerFilterBarShell>
        {/* Time range */}
        <div>
          <label style={trackerFilterLabelStyle}>Time Range</label>
          <TimeRangePicker
            value={filter.timeRange ?? '1 HOUR'}
            onChange={v => onFilterChange({ timeRange: v })}
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
                      background: 'rgba(240,136,62,0.08)',
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
            {phase === 'entering_value' && activeField && valuesOf(activeField.fromProps(props)).length === 0 && (
              <span style={{ ...chipStyle, background: 'rgba(240,136,62,0.06)', borderStyle: 'dashed' }}>
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
                  ? activeField.multi && valuesOf(activeField.fromProps(props)).length > 0
                    ? `Add another ${activeField.label.toLowerCase()}…`
                    : activeField.placeholder
                  : activeChips.length > 0
                    ? 'Add filter…'
                    : 'Type to filter (database, table…)'
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
              {dropdownItems.map((item, idx) => (
                <div
                  key={item.id}
                  onMouseDown={e => { e.preventDefault(); handleDropdownClick(item); }}
                  onMouseEnter={() => setHighlightIdx(idx)}
                  style={{
                    ...dropdownItemStyle,
                    background: idx === highlightIdx ? 'var(--bg-secondary)' : 'transparent',
                    fontWeight: phase === 'picking_field' ? 500 : 400,
                  }}
                >
                  {phase === 'picking_field' && (
                    <span style={{ color: 'var(--text-muted)', fontSize: 10, marginRight: 6 }}>⊕</span>
                  )}
                  {item.label}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Limit */}
        {showLimit && (
          <TrackerLimitInput
            value={filter.limit}
            onChange={limit => onFilterChange({ limit })}
          />
        )}

        {/* Exclude system databases toggle */}
        {props.onExcludeSystemChange && (
          <label style={trackerScopeOptionStyle}>
            <input
              type="checkbox"
              checked={props.excludeSystemDatabases ?? false}
              onChange={e => props.onExcludeSystemChange!(e.target.checked)}
              style={{ margin: 0 }}
            />
            Hide system
          </label>
        )}

        {/* Hide replica merges toggle */}
        {props.onHideReplicaMergesChange && (
          <label style={trackerScopeOptionStyle}>
            <input
              type="checkbox"
              checked={props.hideReplicaMerges ?? false}
              onChange={e => props.onHideReplicaMergesChange!(e.target.checked)}
              style={{ margin: 0 }}
            />
            Hide replicas
          </label>
        )}

        {/* Refresh */}
        <div style={{ marginLeft: 'auto' }}>
          {onRefresh && (
            <TrackerRefreshButton onRefresh={onRefresh} isLoading={isLoading} />
          )}
        </div>
    </TrackerFilterBarShell>
  );
};

export default MergeFilterBar;
