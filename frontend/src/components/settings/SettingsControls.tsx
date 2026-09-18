import type React from 'react';

/**
 * One layout for every control in the settings popover. Each section was
 * previously styled inline at its call site, so they drifted apart in font,
 * padding and wrapping behaviour. The popover also sized itself to its widest
 * child, which made segmented rows wrap differently from one another.
 */
export const SETTINGS_POPOVER_WIDTH = 264;

export function SettingsSection({ label, hint, children }: {
  label: string;
  /** Shown on hover, for scope or caveats that do not belong inline. */
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <div style={{ padding: '8px 12px' }}>
      <div
        title={hint}
        style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 4,
          cursor: hint ? 'help' : 'default',
          fontSize: 9, fontWeight: 600, color: 'var(--text-muted)',
          textTransform: 'uppercase', letterSpacing: '1.5px', marginBottom: 6,
          whiteSpace: 'nowrap',
        }}
      >
        {label}
        {hint ? <span aria-hidden style={{ textTransform: 'none', letterSpacing: 0, fontSize: 10 }}>ⓘ</span> : null}
      </div>
      {children}
    </div>
  );
}

export interface SegmentedOption<T> {
  value: T;
  label: React.ReactNode;
  title?: string;
}

export function SegmentedControl<T extends string | number>({ ariaLabel, options, value, columns, onSelect, disabled, title }: {
  ariaLabel: string;
  options: readonly SegmentedOption<T>[];
  value: T;
  /** Fixed column count, so segments are equal width and wrap in whole rows. */
  columns: number;
  onSelect: (value: T) => void;
  disabled?: boolean;
  title?: string;
}) {
  return (
    <div
      role="group"
      aria-label={ariaLabel}
      title={title}
      style={{
        display: 'grid',
        gridTemplateColumns: `repeat(${columns}, 1fr)`,
        gap: 2,
        background: 'var(--bg-primary)',
        borderRadius: 6,
        border: '1px solid var(--border-primary)',
        padding: 2,
      }}
    >
      {options.map(option => {
        const active = option.value === value;
        return (
          <button
            key={String(option.value)}
            type="button"
            title={option.title}
            disabled={disabled}
            aria-pressed={active}
            onClick={() => onSelect(option.value)}
            style={{
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 4,
              padding: '4px 2px', border: 'none',
              cursor: disabled ? 'default' : 'pointer',
              borderRadius: 4,
              fontSize: 10, fontWeight: 600,
              fontFamily: "'Share Tech Mono', monospace",
              whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
              transition: 'all 0.15s ease',
              opacity: disabled ? 0.5 : 1,
              ...(active
                ? { background: 'var(--bg-card-hover)', color: 'var(--text-primary)', boxShadow: '0 1px 3px rgba(0,0,0,0.1)' }
                : { background: 'transparent', color: 'var(--text-muted)' }),
            }}
          >
            {option.label}
          </button>
        );
      })}
    </div>
  );
}
