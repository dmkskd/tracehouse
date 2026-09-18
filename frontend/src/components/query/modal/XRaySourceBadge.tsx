import { useQueryXRayPreference } from '../query-xray-preference';
/**
 * Source badge for the Query X-Ray.
 *
 * Shows which table the samples came from and lets the user pin it. All the
 * decision logic lives in selectQueryXRaySource() in core — this component only
 * renders the outcome and writes the preference back.
 *
 * Styled to sit inside the X-Ray summary bar: same 11px monospace as its
 * siblings, no control chrome of its own. A native select with default styling
 * is taller than the bar's text and drags the whole row out of alignment, so
 * the select is stripped (`appearance: none`) and given a text-sized caret.
 */

import React from 'react';
import { xraySourceLabel, type QueryXRaySourcePreference, type QueryXRaySourceSelection } from '@tracehouse/core';

const REASON_TITLE: Record<QueryXRaySourceSelection['reason'], string> = {
  auto: 'Chosen automatically',
  override: 'Pinned by you',
  fallback: 'Requested source could not serve this query',
  only_available: 'The only source available on this connection',
};

/**
 * The select is the whole control: showing the active table next to it repeated
 * the same word twice. Under 'auto' the chosen table is named in the option
 * itself, so the effective source is always visible without a second label.
 */
function preferenceLabels(active: string): Record<QueryXRaySourcePreference, string> {
  return {
    auto: `automatic (${active})`,
    processes_history: 'processes_history',
    query_metric_log: 'query_metric_log',
  };
}

/**
 * A native select sizes itself to its WIDEST option, which left a block of
 * empty space after short values like "auto". So the visible label is plain
 * text sized to the current value, and the select is stretched transparently
 * over it purely to capture the click and render the native menu.
 */
const overlaySelectStyle: React.CSSProperties = {
  position: 'absolute',
  inset: 0,
  width: '100%',
  height: '100%',
  opacity: 0,
  appearance: 'none',
  WebkitAppearance: 'none',
  MozAppearance: 'none',
  border: 'none',
  margin: 0,
  padding: 0,
  cursor: 'pointer',
  font: 'inherit',
};

export interface XRaySourceBadgeProps {
  meta: QueryXRaySourceSelection;
}

export const XRaySourceBadge: React.FC<XRaySourceBadgeProps> = ({ meta }) => {
  const { preference, setPreference, override, defaultSource, connectionId } = useQueryXRayPreference();
  // The select is transparent, so its native focus ring is invisible. Track
  // focus and outline the visible label instead, or the control disappears for
  // keyboard users.
  const [focused, setFocused] = React.useState(false);

  const degraded = meta.reason === 'fallback' || meta.missing.length > 0;
  // Inheriting 'auto' and pinning 'auto' resolve identically, so offering both
  // shows two options that differ only in whether an entry is stored. The
  // explicit pin is listed only when it can override an admin-pinned table.
  const inheritsAuto = defaultSource === 'auto';
  const labels = preferenceLabels(xraySourceLabel(meta.source));
  const title = [
    meta.reason === 'override' && override === undefined ? 'Configured default' : REASON_TITLE[meta.reason],
    meta.note,
    meta.missing.length > 0 ? `Not available from this source: ${meta.missing.join(', ')}` : null,
    meta.lagMs > 0 ? `Buffered log: up to ${(meta.lagMs / 1000).toFixed(1)}s behind` : null,
  ].filter(Boolean).join(' · ');

  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }} title={title}>
      <span style={{ color: '#555' }}>source</span>
      <span style={{
        position: 'relative',
        display: 'inline-flex',
        alignItems: 'center',
        gap: 4,
        color: degraded ? '#FECB52' : '#888',
        cursor: 'pointer',
        outline: focused ? '1px solid #58a6ff' : 'none',
        outlineOffset: 2,
        borderRadius: 2,
      }}>
        {override === undefined && !inheritsAuto ? `default (${xraySourceLabel(meta.source)})` : labels[preference]}
        <span aria-hidden style={{ fontSize: 8, color: '#555' }}>▾</span>
        <select
          aria-label="Query X-Ray source"
          disabled={!connectionId}
          value={override ?? 'inherit'}
          onChange={e => setPreference(e.target.value === 'inherit' ? undefined : e.target.value as QueryXRaySourcePreference)}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          style={overlaySelectStyle}
        >
          <option value="inherit">{inheritsAuto ? `automatic (${xraySourceLabel(meta.source)})` : `default (${defaultSource})`}</option>
          {(Object.keys(labels) as QueryXRaySourcePreference[])
            .filter(key => !(inheritsAuto && key === 'auto'))
            .map(key => (
              <option key={key} value={key}>{labels[key]}</option>
            ))}
        </select>
      </span>
    </span>
  );
};
