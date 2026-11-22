import { css } from '@emotion/css';
import { describeTracking, type TimelineTracking, type TrackingDimension } from '@tracehouse/core';

const styles = css`
  margin: 12px 16px 0; padding: 8px 14px; border-radius: 8px; font-size: 12px;
  display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap;
  color: #58a6ff; background: rgba(88, 166, 255, 0.08); border: 1px solid rgba(88, 166, 255, 0.2);
  .tracked { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  .chip { display: inline-flex; align-items: center; gap: 6px; padding: 2px 4px 2px 10px; border-radius: 999px; background: rgba(88, 166, 255, 0.15); }
  .chip .dimension { opacity: 0.75; }
  .chip .value { font-family: monospace; color: var(--text-primary); }
  .chip button { border: none; background: transparent; color: #58a6ff; cursor: pointer; font-size: 13px; line-height: 1; padding: 0 4px; }
  .matches { opacity: 0.8; }
  .mode { display: flex; gap: 2px; padding: 2px; border-radius: 6px; background: rgba(88, 166, 255, 0.1); border: 1px solid rgba(88, 166, 255, 0.2); }
  .mode button { padding: 3px 10px; font-size: 11px; font-weight: 600; border-radius: 4px; border: none; cursor: pointer; color: #58a6ff; background: transparent; }
  .mode button[aria-pressed="true"] { background: rgba(88, 166, 255, 0.25); }
`;

const shown = (dimension: TrackingDimension, value: string) => dimension === 'queryHash' && value.length > 12 ? `${value.slice(0, 12)}…` : value;

/** What Time Travel tracks, one removable chip per dimension, and whether untracked operations stay visible. */
export function TrackingStrip({ tracking, matches, trackedOnly, onTrackedOnly, onClear }: {
  tracking: TimelineTracking; matches: number | null; trackedOnly: boolean;
  onTrackedOnly: (only: boolean) => void; onClear: (dimension: TrackingDimension) => void;
}) {
  return <div className={styles} role="region" aria-label="Tracking">
    <div className="tracked">
      <span>Tracking</span>
      {describeTracking(tracking).map(({ dimension, label, value }) => <span className="chip" key={dimension} title={`${label} ${value}`}>
        <span className="dimension">{label}</span><span className="value">{shown(dimension, value)}</span>
        <button type="button" aria-label={`Stop tracking ${label}`} onClick={() => onClear(dimension)}>×</button>
      </span>)}
      {matches != null && <span className="matches">{matches} {matches === 1 ? 'match' : 'matches'} in window</span>}
    </div>
    <div className="mode">
      <button type="button" aria-pressed={!trackedOnly} onClick={() => onTrackedOnly(false)}>All + highlighted</button>
      <button type="button" aria-pressed={trackedOnly} onClick={() => onTrackedOnly(true)}>Matched only</button>
    </div>
  </div>;
}
