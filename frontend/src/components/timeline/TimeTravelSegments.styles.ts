import { css } from '@emotion/css';

export const segmentsStyle = css`
& {
  display: flex;
  align-items: stretch;
  gap: 2px;
  min-height: 26px;
  min-width: 0;
  box-sizing: border-box;
}
& button {
  min-height: 26px;
  padding: 0 10px;
  border: none;
  border-radius: 5px;
  background: transparent;
  color: var(--text-muted);
  font-family: inherit;
  font-size: 13px;
  font-weight: 500;
  white-space: nowrap;
  cursor: pointer;
  transition: background 0.15s ease, color 0.15s ease;
}
& button[aria-pressed="true"] {
  background: var(--bg-primary);
  color: var(--text-primary);
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}
& button:disabled { opacity: 0.4; cursor: not-allowed; }
`;
