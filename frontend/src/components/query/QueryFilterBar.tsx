/**
 * QueryFilterBar - Shared filter bar with tag-based search input.
 *
 * Layout:  Start Time | End Time | [chip search input] | Limit | ☐ Hide tracehouse queries
 *
 * The chip search input supports field:value pairs. Typing shows autocomplete
 * for field names; after selecting a field, User/Server show dropdown hints
 * from queryAnalyzer while other fields accept freeform text. Confirmed
 * entries become removable chips. All filters are ANDed.
 */

import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import type { QueryAnalyzer } from '@tracehouse/core';

/* ------------------------------------------------------------------ */
/*  Public types                                                       */
/* ------------------------------------------------------------------ */

export interface QueryFilterState {
  queryId?: string;
  user?: string;
  hostname?: string;
  queryText?: string;
  startTime?: string;
  endTime?: string;
  minDurationMs?: number;
  minMemoryBytes?: number;
  limit?: number;
  excludeAppQueries?: boolean;
  queryKind?: string;
  status?: string;
  database?: string;
  table?: string;
}

interface QueryFilterBarProps {
  filter: QueryFilterState;
  onFilterChange: (patch: Partial<QueryFilterState>) => void;
  queryAnalyzer?: QueryAnalyzer;
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
  toFilter: (value: string) => Partial<QueryFilterState>;
  /** extract display value from current filter state (for chip) */
  fromFilter: (f: QueryFilterState) => string | undefined;
  /** clear this field */
  clear: () => Partial<QueryFilterState>;
  /** whether queryAnalyzer provides suggestions */
  hasSuggestions?: boolean;
  /** key used with queryAnalyzer.getDistinctFilterValues */
  suggestionKey?: string;
  /** static suggestion values (no queryAnalyzer needed) */
  hasStaticSuggestions?: boolean;
  staticSuggestions?: string[];
}

const FILTER_FIELDS: FilterFieldDef[] = [
  {
    key: 'user', label: 'User', placeholder: 'e.g. default',
    toFilter: v => ({ user: v || undefined }),
    fromFilter: f => f.user,
    clear: () => ({ user: undefined }),
    hasSuggestions: true, suggestionKey: 'user',
  },
  {
    key: 'server', label: 'Server', placeholder: 'e.g. chi-clickhouse-0-0',
    toFilter: v => ({ hostname: v || undefined }),
    fromFilter: f => f.hostname,
    clear: () => ({ hostname: undefined }),
    hasSuggestions: true, suggestionKey: 'hostname',
  },
  {
    key: 'query_id', label: 'Query ID', placeholder: 'single or space/comma-separated',
    toFilter: v => ({ queryId: v || undefined }),
    fromFilter: f => f.queryId,
    clear: () => ({ queryId: undefined }),
  },
  {
    key: 'query', label: 'Query Contains', placeholder: 'e.g. SELECT, s3(…',
    toFilter: v => ({ queryText: v || undefined }),
    fromFilter: f => f.queryText,
    clear: () => ({ queryText: undefined }),
  },
  {
    key: 'min_duration', label: 'Min Duration (ms)', placeholder: 'e.g. 1000',
    toFilter: v => ({ minDurationMs: v ? parseInt(v, 10) : undefined }),
    fromFilter: f => f.minDurationMs != null ? String(f.minDurationMs) : undefined,
    clear: () => ({ minDurationMs: undefined }),
  },
  {
    key: 'min_memory', label: 'Min Memory (MB)', placeholder: 'e.g. 100',
    toFilter: v => ({ minMemoryBytes: v ? parseInt(v, 10) * 1024 * 1024 : undefined }),
    fromFilter: f => f.minMemoryBytes ? String(Math.round(f.minMemoryBytes / 1024 / 1024)) : undefined,
    clear: () => ({ minMemoryBytes: undefined }),
  },
  {
    key: 'query_kind', label: 'Type', placeholder: 'e.g. SELECT, INSERT…',
    toFilter: v => ({ queryKind: v || undefined }),
    fromFilter: f => f.queryKind,
    clear: () => ({ queryKind: undefined }),
    hasSuggestions: true, suggestionKey: 'query_kind',
  },
  {
    key: 'status', label: 'Status', placeholder: 'success or error',
    toFilter: v => ({ status: v || undefined }),
    fromFilter: f => f.status,
    clear: () => ({ status: undefined }),
    hasStaticSuggestions: true,
    staticSuggestions: ['success', 'error'],
  },
  {
    key: 'database', label: 'Database', placeholder: 'e.g. default',
    toFilter: v => ({ database: v || undefined }),
    fromFilter: f => f.database,
    clear: () => ({ database: undefined }),
  },
  {
    key: 'table', label: 'Table', placeholder: 'e.g. my_table',
    toFilter: v => ({ table: v || undefined }),
    fromFilter: f => f.table,
    clear: () => ({ table: undefined }),
  },
];

/* ------------------------------------------------------------------ */
/*  Styles                                                             */
/* ------------------------------------------------------------------ */

const inputStyle: React.CSSProperties = {
  padding: '6px 10px', fontSize: 12, fontFamily: 'inherit',
  background: 'var(--bg-tertiary)', color: 'var(--text-primary)',
  border: '1px solid var(--border-primary)', borderRadius: 6, outline: 'none',
};

const lblStyle: React.CSSProperties = {
  display: 'block', fontSize: 10, fontWeight: 500, color: 'var(--text-muted)',
  marginBottom: 4, textTransform: 'uppercase', letterSpacing: '0.3px',
};

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

export const QueryFilterBar: React.FC<QueryFilterBarProps> = ({
  filter, onFilterChange, queryAnalyzer,
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

  /* --- limit local state --- */
  const [localLimit, setLocalLimit] = useState(String(filter.limit || 100));
  const limitTimerRef = useRef<ReturnType<typeof setTimeout>>();
  useEffect(() => {
    clearTimeout(limitTimerRef.current);
    limitTimerRef.current = setTimeout(() => {
      const v = parseInt(localLimit, 10);
      onFilterChange({ limit: v > 0 ? v : 100 });
    }, 500);
  }, [localLimit]);
  // sync from props
  useEffect(() => { setLocalLimit(String(filter.limit || 100)); }, [filter.limit]);

  /* --- active chips derived from filter state --- */
  const activeChips = useMemo(() => {
    const chips: { field: FilterFieldDef; displayValue: string }[] = [];
    for (const f of FILTER_FIELDS) {
      const v = f.fromFilter(filter);
      if (v) chips.push({ field: f, displayValue: v });
    }
    return chips;
  }, [filter]);

  /* --- which fields are still available (not yet used) --- */
  const availableFields = useMemo(() => {
    const usedKeys = new Set(activeChips.map(c => c.field.key));
    return FILTER_FIELDS.filter(f => !usedKeys.has(f.key));
  }, [activeChips]);

  /* --- dropdown items based on phase --- */
  const dropdownItems = useMemo(() => {
    if (phase === 'idle' || phase === 'picking_field') {
      // show field names filtered by input
      const q = inputValue.toLowerCase();
      return availableFields
        .filter(f => !q || f.label.toLowerCase().includes(q) || f.key.toLowerCase().includes(q))
        .map(f => ({ id: f.key, label: f.label, field: f }));
    }
    if (phase === 'entering_value' && activeField?.hasSuggestions) {
      const vals = suggestionCache[activeField.key] || [];
      const q = inputValue.toLowerCase();
      return vals
        .filter(v => !q || v.toLowerCase().includes(q))
        .map(v => ({ id: v, label: v, field: activeField }));
    }
    if (phase === 'entering_value' && activeField?.hasStaticSuggestions) {
      const vals = activeField.staticSuggestions || [];
      const q = inputValue.toLowerCase();
      return vals
        .filter(v => !q || v.toLowerCase().includes(q))
        .map(v => ({ id: v, label: v, field: activeField }));
    }
    return [];
  }, [phase, inputValue, availableFields, activeField, suggestionCache]);

  /* --- handlers --- */
  const commitValue = useCallback((value: string) => {
    if (!activeField || !value.trim()) return;
    onFilterChange(activeField.toFilter(value.trim()));
    setActiveField(null);
    setInputValue('');
    setPhase('idle');
    setShowDropdown(false);
    setHighlightIdx(-1);
  }, [activeField, onFilterChange]);

  const selectField = useCallback((field: FilterFieldDef) => {
    setActiveField(field);
    setInputValue('');
    setPhase('entering_value');
    setHighlightIdx(-1);
    // keep dropdown open for value suggestions
    setShowDropdown(field.hasSuggestions === true || field.hasStaticSuggestions === true);
    setTimeout(() => inputRef.current?.focus(), 0);
  }, []);

  const removeChip = useCallback((field: FilterFieldDef) => {
    onFilterChange(field.clear());
  }, [onFilterChange]);

  /** Click a chip to edit it: clear the filter, pre-fill the input with the old value */
  const editChip = useCallback((field: FilterFieldDef, currentValue: string) => {
    onFilterChange(field.clear());
    setActiveField(field);
    setInputValue(currentValue);
    setPhase('entering_value');
    setHighlightIdx(-1);
    setShowDropdown(field.hasSuggestions === true);
    setTimeout(() => {
      inputRef.current?.focus();
      // select all text so user can easily replace
      inputRef.current?.select();
    }, 0);
  }, [onFilterChange]);

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
        onFilterChange(activeField.toFilter(inputValue.trim()));
        setActiveField(null);
        setInputValue('');
        setPhase('idle');
        setHighlightIdx(-1);
      } else if (phase === 'picking_field') {
        setPhase('idle');
        setInputValue('');
      } else if (phase === 'entering_value' && activeField && !inputValue.trim()) {
        // abandoned edit with no value — just reset
        setActiveField(null);
        setInputValue('');
        setPhase('idle');
        setHighlightIdx(-1);
      }
    }, 200);
  }, [phase, activeField, inputValue, onFilterChange]);

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
      // go back to field picking
      setActiveField(null); setPhase('picking_field'); setShowDropdown(true);
      return;
    }
    if (e.key === 'Backspace' && !inputValue && phase === 'picking_field' && activeChips.length > 0) {
      // remove last chip
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
    if (phase === 'picking_field') {
      selectField(item.field);
    } else if (phase === 'entering_value') {
      commitValue(item.label);
    }
  }, [phase, selectField, commitValue]);

  /* --- time helpers --- */
  const getDefaultStartTime = () => {
    const d = new Date(); d.setHours(d.getHours() - 1);
    return d.toISOString().slice(0, 16);
  };
  const getDefaultEndTime = () => new Date().toISOString().slice(0, 16);

  /* --- lookback warning --- */
  const startMs = filter.startTime
    ? new Date(filter.startTime).getTime()
    : new Date(getDefaultStartTime()).getTime();
  const endMs = filter.endTime
    ? new Date(filter.endTime).getTime()
    : Date.now();
  const lookbackHours = (endMs - startMs) / (60 * 60 * 1000);
  const fmtLookback = lookbackHours < 24
    ? `${Math.round(lookbackHours)}h`
    : lookbackHours < 168
      ? `${(lookbackHours / 24).toFixed(1)}d`
      : `${(lookbackHours / 168).toFixed(1)}w`;

  /* --- render --- */
  return (
    <div style={{
      background: 'var(--bg-secondary)', borderRadius: 8,
      padding: '12px 16px', marginBottom: 12,
      border: '1px solid var(--border-primary)',
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-end', gap: 10, flexWrap: 'wrap' }}>
        {/* Start Time */}
        <div style={{ width: 150 }}>
          <label style={lblStyle}>Start Time</label>
          <input type="datetime-local"
            value={filter.startTime?.slice(0, 16) || getDefaultStartTime()}
            onChange={e => onFilterChange({ startTime: e.target.value ? new Date(e.target.value).toISOString() : undefined })}
            style={{ ...inputStyle, width: '100%' }} />
        </div>
        {/* End Time */}
        <div style={{ width: 150 }}>
          <label style={lblStyle}>End Time</label>
          <input type="datetime-local"
            value={filter.endTime?.slice(0, 16) || getDefaultEndTime()}
            onChange={e => onFilterChange({ endTime: e.target.value ? new Date(e.target.value).toISOString() : undefined })}
            style={{ ...inputStyle, width: '100%' }} />
        </div>

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
                  ? activeField.placeholder
                  : activeChips.length > 0
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
        <div style={{ width: 70 }}>
          <label style={lblStyle}>Limit</label>
          <input type="number" value={localLimit} min="1" max="10000" step="50"
            onChange={e => setLocalLimit(e.target.value)}
            style={{ ...inputStyle, width: '100%' }} />
        </div>

        {/* Exclude internal + count */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, paddingBottom: 2 }}>
          <label style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 11, color: 'var(--text-muted)', cursor: 'pointer', userSelect: 'none', lineHeight: 1.3 }}>
            <input type="checkbox"
              checked={filter.excludeAppQueries ?? false}
              onChange={e => onFilterChange({ excludeAppQueries: e.target.checked })}
              style={{ margin: 0 }} />
            <span>Hide tracehouse<br />queries</span>
          </label>
        </div>
      </div>
      {lookbackHours > 1 && (
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
      )}
    </div>
  );
};

export default QueryFilterBar;
