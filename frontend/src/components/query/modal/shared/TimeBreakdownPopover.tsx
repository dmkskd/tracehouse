/**
 * TimeBreakdownPopover — the structured explanation behind the breakdown bar.
 *
 * Replaces a native `title` tooltip, which could not be styled and turned the
 * three-layer explanation into a wall of wrapped text. The content is
 * hierarchical — segments, then two independent layers explaining the parked
 * one, then caveats — and needs the structure to be readable.
 *
 * Rendered through a portal because the host card clips its content
 * (ExploreDestinationCard caps its slot height), and positioned against the
 * anchor's viewport rect. Colors are resolved explicitly from the detected
 * theme rather than CSS variables, which do not reach a body-level portal.
 */

import React from 'react';
import { createPortal } from 'react-dom';
import { useThemeDetection } from '../../../../hooks/useThemeDetection';

export interface PopoverSegment {
  label: string;
  color: string;
  pct: string;
  hint: string;
  source: string;
}

export interface PopoverLayer {
  /** e.g. "processors_profile_log" — the table the claim rests on. */
  source: string;
  /** What question this layer answers. */
  question: string;
  /** Present rows, or an explanation of why the layer is empty. */
  rows: { label: string; pct?: string; hint: string }[];
  unavailable?: string;
  /** Caveat about how this layer's numbers relate to the segment above. */
  note?: string;
}

const THEME = {
  dark: {
    bg: '#161b22', veil: 'rgba(22, 27, 34, 0.82)', border: '#30363d', text: '#e6edf3',
    muted: '#8b949e', faint: '#6e7681', warn: '#FFA15A',
  },
  light: {
    bg: '#ffffff', veil: 'rgba(255, 255, 255, 0.82)', border: 'rgba(0,0,0,0.14)', text: '#1f2328',
    muted: '#57606a', faint: '#8c959f', warn: '#bc4c00',
  },
} as const;

export const TimeBreakdownPopover: React.FC<{
  /** Viewport rect of the element being hovered, captured on mouse enter. */
  anchor: DOMRect | null;
  title: string;
  /** Identity/metric rows shown above the composition, e.g. host and query_id. */
  facts?: { label: string; value: string }[];
  segments: PopoverSegment[];
  layers: PopoverLayer[];
  /** Heading tying the layers to the segment they explain, e.g. "Unaccounted 23%". */
  layersHeading?: string;
  caveats: string[];
  /** Keeps the panel open while the pointer is over it. */
  onPointerEnter?: () => void;
  onPointerLeave?: () => void;
  /**
   * Whether the panel accepts the pointer.
   *
   * True where the content is long enough to want reading or selecting. False
   * where the user is scanning a list and the panel would sit over the rows
   * they are comparing — there it must be transparent to the pointer so moving
   * on is never blocked by the explanation of what you just left.
   */
  interactive?: boolean;
  /**
   * Widest the panel may get. The default suits the three-layer explanation;
   * callers with only facts and segments pass something narrower so the panel
   * does not span half the screen to describe one node.
   */
  maxWidth?: number;
  /**
   * How the panel carries itself.
   *
   * 'panel' is the full explanation: solid, wide, every counter named. It is
   * what the three-layer breakdown needs.
   *
   * 'overlay' is for panels that open over the thing being read — a diagram, a
   * list of rows being compared. Frosted so what is underneath stays legible,
   * narrow, and without the ProfileEvent column, which is the widest thing in
   * the panel and the least useful when you are scanning.
   */
  variant?: 'panel' | 'overlay';
}> = ({
  anchor, title, facts = [], segments, layers, layersHeading, caveats,
  onPointerEnter, onPointerLeave, interactive = true, variant = 'panel',
  maxWidth = variant === 'overlay' ? 360 : 680,
}) => {
  const overlay = variant === 'overlay';
  const theme = useThemeDetection();
  const c = THEME[theme];

  if (!anchor) return null;

  // Placed from the anchor's rect alone — no measuring of this element, so
  // there is no hidden-then-revealed step that can strand it off-screen.
  // Flips above the anchor in the lower half of the viewport, and clamps
  // horizontally so a card near the right edge still shows its explanation.
  const flipUp = anchor.bottom > window.innerHeight * 0.55;
  const placement: React.CSSProperties = flipUp
    ? { bottom: Math.max(8, window.innerHeight - anchor.top + 8) }
    : { top: anchor.bottom + 8 };
  const left = Math.min(anchor.left, Math.max(8, window.innerWidth - (maxWidth + 20)));

  return createPortal(
    <div
      role="tooltip"
      onMouseEnter={interactive ? onPointerEnter : undefined}
      onMouseLeave={interactive ? onPointerLeave : undefined}
      style={{
        position: 'fixed',
        ...placement,
        left,
        // Must clear ModalWrapper's 100000: this portals to body, so without
        // that it renders behind the very modal it belongs to.
        zIndex: 100_001,
        maxWidth,
        background: overlay ? c.veil : c.bg,
        backdropFilter: overlay ? 'blur(14px) saturate(1.4)' : undefined,
        WebkitBackdropFilter: overlay ? 'blur(14px) saturate(1.4)' : undefined,
        border: `1px solid ${c.border}`,
        borderRadius: 8,
        boxShadow: overlay ? '0 6px 20px rgba(0,0,0,0.16)' : '0 8px 28px rgba(0,0,0,0.28)',
        padding: '10px 12px',
        pointerEvents: interactive ? 'auto' : 'none',
        fontFamily: 'var(--font-mono, monospace)',
        fontSize: 11,
        lineHeight: 1.5,
        color: c.text,
      }}
    >
      <div style={{ color: c.muted, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.7px', marginBottom: 7 }}>
        {title}
      </div>

      {facts.length > 0 && (
        <div style={{ marginBottom: segments.length > 0 ? 8 : 0 }}>
          {facts.map(fact => (
            <div key={fact.label} style={{ display: 'grid', gridTemplateColumns: '78px 1fr', gap: 7, whiteSpace: 'nowrap' }}>
              <span style={{ color: c.faint }}>{fact.label}</span>
              <span style={{ color: c.text, overflow: 'hidden', textOverflow: 'ellipsis' }}>{fact.value}</span>
            </div>
          ))}
        </div>
      )}

      {segments.length > 0 && facts.length > 0 && (
        <div style={{ borderTop: `1px solid ${c.border}`, paddingTop: 8, marginBottom: 4, color: c.muted, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.7px' }}>
          time spent
        </div>
      )}

      {/* The composition as a bar lives here rather than on any timeline: this
          panel has no time axis, so left-to-right can safely mean proportion. */}
      {segments.length > 1 && (
        <div style={{ display: 'flex', height: 6, borderRadius: 999, overflow: 'hidden', marginBottom: 8 }}>
          {segments.map(s => (
            <div key={s.label} style={{ width: s.pct, background: s.color }} />
          ))}
        </div>
      )}

      {/* One line per segment. Aligned columns rather than run-on text: the
          percentage is the thing being scanned, so it gets its own column and
          tabular figures instead of floating at the end of a sentence. */}
      {segments.map(s => (
        <div
          key={s.label}
          style={{
            display: 'grid',
            gridTemplateColumns: overlay ? '8px 62px 44px auto' : '8px 62px 44px 1fr',
            gap: 7,
            alignItems: 'baseline',
            marginBottom: 3,
            whiteSpace: 'nowrap',
          }}
        >
          <span style={{ width: 8, height: 8, borderRadius: 2, background: s.color, transform: 'translateY(1px)' }} />
          <span style={{ color: c.text }}>{s.label}</span>
          <span style={{ color: c.text, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{s.pct}</span>
          <span style={{ minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis' }}>
            <span style={{ color: c.muted }}>{s.hint}</span>
            {!overlay && <span style={{ color: c.faint }}> · {s.source}</span>}
          </span>
        </div>
      ))}

      {/* The layers explain one segment, so they sit under a heading naming it.
          Without that they read as unrelated facts floating below the bar. */}
      {layers.length > 0 && layersHeading && (
        <div style={{ marginTop: 10, paddingTop: 8, borderTop: `1px solid ${c.border}`, color: c.muted, fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.7px' }}>
          {layersHeading}
        </div>
      )}

      {layers.map(layer => (
        <div key={layer.source} style={{ marginTop: 8, paddingLeft: 10, borderLeft: `2px solid ${c.border}` }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, marginBottom: 4 }}>
            <span style={{ color: c.text, fontSize: 10 }}>{layer.source}</span>
            <span style={{ color: c.faint, fontSize: 10 }}>{layer.question}</span>
          </div>
          {layer.note && (
            <div style={{ color: c.faint, fontSize: 9, marginBottom: 4 }}>{layer.note}</div>
          )}
          {layer.unavailable ? (
            <div style={{ color: c.faint, fontSize: 10 }}>{layer.unavailable}</div>
          ) : (
            layer.rows.map(row => (
              <div
                key={row.label}
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'minmax(150px, max-content) 44px 1fr',
                  gap: 7,
                  alignItems: 'baseline',
                  marginBottom: 3,
                  whiteSpace: 'nowrap',
                }}
              >
                <span style={{ color: c.text, overflow: 'hidden', textOverflow: 'ellipsis' }}>{row.label}</span>
                <span style={{ color: c.text, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{row.pct ?? ''}</span>
                <span style={{ color: c.muted, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis' }}>{row.hint}</span>
              </div>
            ))
          )}
        </div>
      ))}

      {caveats.length > 0 && (
        <div style={{ marginTop: 9, paddingTop: 8, borderTop: `1px solid ${c.border}`, color: c.warn, fontSize: 10 }}>
          {caveats.map(caveat => <div key={caveat}>{caveat}</div>)}
        </div>
      )}
    </div>,
    document.body,
  );
};
