import { css } from '@emotion/css';

export const controlsStyle = css`
& {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 4px;
  min-width: 0;
  max-width: 100%;
  padding: 3px;
  border: 1px solid var(--border-primary);
  border-radius: 8px;
  background: var(--bg-card);
  box-sizing: border-box;
}
& .tt-controls-divider {
  width: 1px;
  height: 20px;
  margin: 0 4px;
  flex-shrink: 0;
  background: var(--border-primary);
}
`;
