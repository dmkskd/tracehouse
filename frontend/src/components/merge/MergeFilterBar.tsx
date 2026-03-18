/**
 * MergeFilterBar - Tag-based filter bar for merge/mutation tabs.
 *
 * Layout:  [chip search input] | Limit | count | Refresh
 *
 * Mirrors the QueryFilterBar pattern: typing shows autocomplete for field
 * names; after selecting a field, shows dropdown suggestions from props.
 * Confirmed entries become removable chips. All filters are ANDed.
 */

import React, { useState, useRef, useCallback, useMemo } from 'react';
import type { MergeHistoryFilter } from '../../stores/mergeStore';

export type MergeTab = 'active' | 'mutations' | 'mutationHistory' | 'history';

/* ------------------------------------------------------------------ */
/*  Public types                                                       */
/* ------------------------------------------------------------------ */

interface MergeFilterBarProps {
  tab: MergeTab;
  filter: MergeHistoryFilter;
  onFilterChange: (patch: Partial<MergeHistoryFilter>) => void;
  availableDatabases: string[];
  availableTables: string[];
  /** For Active Merges: distinct merge_type values */
  mergeTypes?: string[];
  selectedMergeType?: string;
  onMergeTypeChange?: (v: string | undefined) => void;
  /** For Merge History: distinct merge_reason values */
  mergeReasons?: string[];
  selectedMergeReason?: string;
  onMergeReasonChange?: (v: string | undefined) => void;
  /** Host filter (client-side) */
  availableHosts?: string[];
  selectedHost?: string;
  onHostChange?: (v: string | undefined) => void;
  /** Status filter (client-side): OK or Error */
  availableStatuses?: string[];
  selectedStatus?: string;
  onStatusChange?: (v: string | undefined) => void;
  /** Part name filter (client-side, substring match) */
  selectedPartName?: string;
  onPartNameChange?: (v: string | undefined) => void;
  onRefresh?: () => void;
  isLoading?: boolean;
  resultCount?: number;
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
  fromProps: (props: MergeFilterBarProps) => string | undefined;
  /** Apply a value */
  apply: (value: string, props: MergeFilterBarProps) => void;
  /** Clear this field */
  clear: (props: MergeFilterBarProps) => void;
}

const FILTER_FIELDS: FilterFieldDef[] = [
  {
    key: 'database', label: 'Database', placeholder: 'e.g. default',
    getSuggestions: p => p.availableDatabases,
    fromProps: p => p.filter.database,
    apply: (v, p) => p.onFilterChange({ database: v || undefined, table: undefined }),
    clear: p => p.onFilterChange({ database: undefined, table: undefined }),
  },
  {
    key: 'table', label: 'Table', placeholder: 'e.g. my_table',
    getSuggestions: p => p.availableTables,
    fromProps: p => p.filter.table,
    apply: (v, p) => p.onFilterChange({ table: v || undefined }),
    clear: p => p.onFilterChange({ table: undefined }),
  },
  {
    key: 'merge_type', label: 'Merge Type', placeholder: 'e.g. Normal, Mutation',
    tabs: ['active'],
    getSuggestions: p => p.mergeTypes || [],
    fromProps: p => p.selectedMergeType,
    apply: (v, p) => p.onMergeTypeChange?.(v || undefined),
    clear: p => p.onMergeTypeChange?.(undefined),
  },
  {
    key: 'merge_reason', label: 'Category', placeholder: 'e.g. RegularMerge, TTLDelete',
    tabs: ['history'],
    getSuggestions: p => p.mergeReasons || [],
    fromProps: p => p.selectedMergeReason,
    apply: (v, p) => p.onMergeReasonChange?.(v || undefined),
    clear: p => p.onMergeReasonChange?.(undefined),
  },
  {
    key: 'status', label: 'Status', placeholder: 'OK or Error',
    tabs: ['history'],
    getSuggestions: p => p.availableStatuses || [],
    fromProps: p => p.selectedStatus,
    apply: (v, p) => p.onStatusChange?.(v || undefined),
    clear: p => p.onStatusChange?.(undefined),
  },
  {
    key: 'host', label: 'Host', placeholder: 'e.g. chi-clickhouse-0-0',
    tabs: ['active', 'history'],
    getSuggestions: p => p.availableHosts || [],
    fromProps: p => p.selectedHost,
    apply: (v, p) => p.onHostChange?.(v || undefined),
    clear: p => p.onHostChange?.(undefined),
  },
  {
    key: 'part', label: 'Part', placeholder: 'e.g. all_1_3_1',
    getSuggestions: () => [],
    fromProps: p => p.selectedPartName,
    apply: (v, p) => p.onPartNameChange?.(v || undefined),
    clear: p => p.onPartNameChange?.(undefined),
  },
  {
    key: 'min_duration', label: 'Min Duration (s)', placeholder: 'e.g. 5',
    tabs: ['history'],
    getSuggestions: () => ['1', '5', '10', '30', '60'],
    fromProps: p => p.filter.minDurationMs != null ? String(p.filter.minDurationMs / 1000) : undefined,
    apply: (v, p) => {
      const secs = parseFloat(v);
      p.onFilterChange({ minDurationMs: secs > 0 ? Math.round(secs * 1000) : undefined });
    },
    clear: p => p.onFilterChange({ minDurationMs: undefined }),
  },
  {
    key: 'min_size', label: 'Min Size (MB)', placeholder: 'e.g. 100',
    tabs: ['history'],
    getSuggestions: () => ['10', '100', '500', '1000'],
    fromProps: p => p.filter.minSizeBytes != null ? String(Math.round(p.filter.minSizeBytes / (1024 * 1024))) : undefined,
    apply: (v, p) => {
      const mb = parseFloat(v);
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

const lblStyle: React.CSSProperties = {
  display: 'block', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)',
  marginBottom: 4, textTransform: 'uppercase', letterSpacing: '0.3px',
};

const inputStyle: React.CSSProperties = {
  padding: '6px 10px', fontSize: 12, fontFamily: 'inherit',
  background: 'var(--bg-tertiary)', color: 'var(--text-primary)',
  border: '1px solid var(--border-primary)', borderRadius: 6, outline: 'none',
};

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

type Phase = 'idle' | 'picking_field' | 'entering_value';

export const MergeFilterBar: React.FC<MergeFilterBarProps> = (props) => {
  const {
    tab, filter, onFilterChange, onRefresh, isLoading, resultCount,
  } = props;

  const showLimit = tab === 'history' || tab === 'mutationHistory';

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

  /* --- limit local state with debounce --- */
  const [localLimit, setLocalLimit] = useState(String(filter.limit || 100));
  const limitTimerRef = useRef<ReturnType<typeof setTimeout>>();
  React.useEffect(() => {
    clearTimeout(limitTimerRef.current);
    limitTimerRef.current = setTimeout(() => {
      const v = parseInt(localLimit, 10);
      onFilterChange({ limit: v > 0 ? v : 100 });
    }, 500);
  }, [localLimit]); // eslint-disable-line react-hooks/exhaustive-deps
  React.useEffect(() => { setLocalLimit(String(filter.limit || 100)); }, [filter.limit]);

  /* --- active chips derived from props --- */
  const activeChips = useMemo(() => {
    const chips: { field: FilterFieldDef; displayValue: string }[] = [];
    for (const f of visibleFields) {
      const v = f.fromProps(props);
      if (v) chips.push({ field: f, displayValue: v });
    }
    return chips;
  }, [visibleFields, props]);

  /* --- available fields (not yet used) --- */
  const availableFields = useMemo(() => {
    const usedKeys = new Set(activeChips.map(c => c.field.key));
    return visibleFields.filter(f => !usedKeys.has(f.key));
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
      const q = inputValue.toLowerCase();
      return vals
        .filter(v => !q || v.toLowerCase().includes(q))
        .map(v => ({ id: v, label: v, field: activeField }));
    }
    return [];
  }, [phase, inputValue, availableFields, activeField, props]);

  /* --- handlers --- */
  const commitValue = useCallback((value: string) => {
    if (!activeField || !value.trim()) return;
    activeField.apply(value.trim(), props);
    setActiveField(null);
    setInputValue('');
    setPhase('idle');
    setShowDropdown(false);
    setHighlightIdx(-1);
  }, [activeField, props]);

  const selectField = useCallback((field: FilterFieldDef) => {
    setActiveField(field);
    setInputValue('');
    setPhase('entering_value');
    setHighlightIdx(-1);
    const hasSuggestions = field.getSuggestions(props).length > 0;
    setShowDropdown(hasSuggestions);
    setTimeout(() => inputRef.current?.focus(), 0);
  }, [props]);

  const removeChip = useCallback((field: FilterFieldDef) => {
    field.clear(props);
  }, [props]);

  const editChip = useCallback((field: FilterFieldDef, currentValue: string) => {
    field.clear(props);
    setActiveField(field);
    setInputValue(currentValue);
    setPhase('entering_value');
    setHighlightIdx(-1);
    setShowDropdown(field.getSuggestions(props).length > 0);
    setTimeout(() => {
      inputRef.current?.focus();
      inputRef.current?.select();
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
        activeField.apply(inputValue.trim(), props);
        setActiveField(null);
        setInputValue('');
        setPhase('idle');
        setHighlightIdx(-1);
      } else if (phase === 'picking_field') {
        setPhase('idle');
        setInputValue('');
      } else if (phase === 'entering_value' && activeField && !inputValue.trim()) {
        setActiveField(null);
        setInputValue('');
        setPhase('idle');
        setHighlightIdx(-1);
      }
    }, 200);
  }, [phase, activeField, inputValue, props]);

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
      }
      return;
    }
    if (e.key === 'Backspace' && !inputValue && phase === 'entering_value') {
      setActiveField(null); setPhase('picking_field'); setShowDropdown(true);
      return;
    }
    if (e.key === 'Backspace' && !inputValue && phase === 'picking_field' && activeChips.length > 0) {
      const last = activeChips[activeChips.length - 1];
      removeChip(last.field);
    }
  }, [dropdownItems, highlightIdx, phase, inputValue, activeChips, selectField, commitValue, removeChip]);

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
    <div style={{
      background: 'var(--bg-secondary)', borderRadius: 8,
      padding: '12px 16px', marginBottom: 10,
      border: '1px solid var(--border-primary)',
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-end', gap: 10, flexWrap: 'wrap' }}>
        {/* Chip search input */}
        <div style={{ flex: 1, minWidth: 260, position: 'relative' }}>
          <label style={lblStyle}>Filters</label>
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
                onClick={e => { e.stopPropagation(); editChip(c.field, c.displayValue); }}>
                <span style={{ fontWeight: 600, fontSize: 10 }}>{c.field.label}:</span>
                {c.displayValue}
                <span style={chipRemoveStyle} onClick={e => { e.stopPropagation(); removeChip(c.field); }}>×</span>
              </span>
            ))}
            {/* Active field label (while entering value) */}
            {phase === 'entering_value' && activeField && (
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
                  ? activeField.placeholder
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

        {/* Limit (history tabs) */}
        {showLimit && (
          <div style={{ width: 70 }}>
            <label style={lblStyle}>Limit</label>
            <input type="number" value={localLimit} min="1" max="10000" step="50"
              onChange={e => setLocalLimit(e.target.value)}
              style={{ ...inputStyle, width: '100%' }} />
          </div>
        )}

        {/* Count + Refresh */}
        <div style={{ display: 'flex', alignItems: 'flex-end', gap: 8, flexShrink: 0, marginLeft: 'auto' }}>
          {resultCount !== undefined && (
            <span style={{ fontSize: 11, color: 'var(--text-muted)', whiteSpace: 'nowrap', paddingBottom: 6 }}>
              {isLoading ? 'Loading…' : `${resultCount}`}
            </span>
          )}
          {onRefresh && (
            <button onClick={onRefresh} disabled={isLoading} style={{
              padding: '6px 12px', fontSize: 11, fontWeight: 500, borderRadius: 6,
              background: 'var(--bg-tertiary)',
              color: isLoading ? 'var(--text-muted)' : 'var(--text-secondary)',
              border: '1px solid var(--border-primary)',
              cursor: isLoading ? 'not-allowed' : 'pointer', whiteSpace: 'nowrap',
            }}>
              {isLoading ? 'Loading…' : 'Refresh'}
            </button>
          )}
        </div>
      </div>
    </div>
  );
};

export default MergeFilterBar;
