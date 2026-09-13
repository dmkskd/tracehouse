import { css } from "@emotion/css";

export const overviewStyles = css`
& {
  display: flex;
  flex-direction: column;
  gap: 20px;
  color: var(--text-primary);
}
& .overview-identity {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px 20px;
  font-size: 11px;
}
& .overview-status {
  padding: 4px 8px;
  border-radius: 6px;
  font-size: 10px;
  font-weight: 600;
}
& .overview-fact {
  display: flex;
  gap: 7px;
  align-items: baseline;
  min-width: 0;
}
& .overview-fact > span {
  color: var(--text-muted);
  font-size: 9px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
& .overview-fact strong,
& .overview-fact button {
  font: 500 11px var(--font-mono, monospace);
  overflow-wrap: anywhere;
}
& .overview-fact button,
& .overview-sql-heading button {
  border: 0;
  background: transparent;
  color: var(--accent-blue);
  padding: 0;
  cursor: pointer;
}
& .overview-sql {
  border: 1px solid var(--border-secondary);
  border-radius: 8px;
  padding: 13px 15px;
  background: var(--bg-card);
}
& .overview-sql-heading {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}
& .overview-sql-heading strong {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text-secondary);
}
& .overview-sql-heading button {
  font-size: 11px;
}
& .overview-summary {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  padding: 17px 0;
  border: 1px solid var(--border-secondary);
  border-radius: 8px;
  background: var(--bg-tertiary);
}
& .overview-summary > div {
  padding: 0 20px;
  border-right: 1px solid var(--border-secondary);
  min-width: 0;
}
& .overview-summary > div:last-child {
  border: 0;
}
& .overview-summary small {
  display: block;
  color: var(--text-secondary);
  font-size: 10px;
  margin-bottom: 7px;
}
& .overview-summary strong {
  display: block;
  font: 600 21px var(--font-mono, monospace);
  letter-spacing: -0.6px;
}
& .overview-summary span {
  display: block;
  color: var(--text-muted);
  font-size: 10px;
  line-height: 1.5;
  margin-top: 7px;
}
& .overview-explore-heading {
  display: flex;
  gap: 14px;
  align-items: baseline;
  margin: 4px 0 14px;
}
& .overview-explore-heading h3 {
  margin: 0;
  font-size: 14px;
  font-weight: 600;
}
& .overview-explore-heading > span {
  font-size: 11px;
  color: var(--text-muted);
}
& .overview-destinations {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
}
& .overview-destination {
  border: 1px solid var(--border-primary);
  border-radius: 8px;
  background: var(--bg-card);
  padding: 15px;
  color: inherit;
  text-align: left;
  font: inherit;
  cursor: pointer;
  min-width: 0;
  display: flex;
  flex-direction: column;
  transition:
    border-color 0.15s,
    background 0.15s;
}
& .overview-destination:hover {
  border-color: var(--accent-blue);
  background: var(--bg-card-hover);
}
& .overview-destination:focus-visible,
& .overview-mini:focus-visible {
  outline: 2px solid var(--accent-blue);
  outline-offset: 3px;
}
& .overview-destination-title {
  display: flex;
  justify-content: space-between;
  color: var(--accent-blue);
  font-size: 12px;
}
& .overview-destination-title strong {
  font-weight: 600;
}
& .overview-destination h4 {
  font-size: 13px;
  font-weight: 600;
  margin: 13px 0 6px;
  line-height: 1.4;
}
& .overview-destination p {
  font-size: 11px;
  line-height: 1.5;
  color: var(--text-secondary);
  margin: 0 0 17px;
  flex-grow: 1;
}
& .overview-signal {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
  margin-top: auto;
  min-height: 20px;
}
& .overview-signal small {
  font: 10px var(--font-mono, monospace);
  color: var(--text-muted);
  line-height: 1.5;
}
& .overview-mini {
  position: relative;
  display: inline-flex;
  flex-shrink: 0;
  cursor: help;
}
& .overview-mini svg {
  width: 78px;
  height: 20px;
  fill: none;
  stroke: #b6cbe5;
  stroke-width: 1.4;
  stroke-linecap: round;
  stroke-linejoin: round;
}
& .overview-mini rect {
  fill: #c1d3e9;
  stroke: none;
}
& .overview-mini-distributed rect,
& .overview-mini-flamegraph rect,
& .overview-mini-pipeline rect {
  fill: #decba1;
}
& .overview-mini-pipeline svg {
  stroke: #d1c1a4;
}
& .overview-mini-analytics rect {
  fill: #b7d0c0;
}
& .overview-mini-tooltip {
  position: absolute;
  right: -3px;
  bottom: calc(100% + 10px);
  width: 250px;
  max-width: 75vw;
  padding: 11px 13px;
  border-radius: 7px;
  background: var(--bg-primary);
  color: var(--text-primary);
  border: 1px solid var(--border-primary);
  box-shadow: var(--shadow-sm);
  font: 11px/1.6 var(--font-mono, monospace);
  opacity: 0;
  visibility: hidden;
  z-index: 10;
  pointer-events: none;
  transition: opacity 0.12s;
}
& .overview-mini:hover .overview-mini-tooltip,
& .overview-mini:focus-visible .overview-mini-tooltip {
  opacity: 1;
  visibility: visible;
}
& .overview-error {
  padding: 12px;
  border: 1px solid rgba(var(--color-error-rgb), 0.2);
  background: rgba(var(--color-error-rgb), 0.08);
  border-radius: 6px;
  color: var(--color-error);
  font-size: 11px;
}
& .overview-error > div {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}
& .overview-error pre {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  margin: 8px 0 0;
}
@media (max-width: 1100px) {
  & .overview-destinations {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
  & .overview-summary > div {
    padding: 0 13px;
  }
}
@media (max-width: 800px) {
  & .overview-destinations,
  & .overview-summary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  & .overview-summary {
    gap: 18px 0;
  }
  & .overview-summary > div:nth-child(2) {
    border: 0;
  }
  & .overview-explore-heading {
    flex-wrap: wrap;
    gap: 5px 12px;
  }
}
@media (max-width: 480px) {
  & .overview-destinations {
    grid-template-columns: 1fr;
  }
  & .overview-summary strong {
    font-size: 17px;
  }
}

/* Match the proposal's white cards in the light theme. */
[data-theme="light"] & .overview-destination,
.theme-light & .overview-destination {
  background: #ffffff;
}
[data-theme="light"] & .overview-destination:hover,
.theme-light & .overview-destination:hover {
  background: #f8fbff;
}
`;
