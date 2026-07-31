import React from 'react';
import type { ErrorCodeChoice } from './errorCodeFilterModel';

export const ErrorCodeRefineButton: React.FC<{ onOpen: () => void }> = ({ onOpen }) => (
  <button
    type="button"
    style={{
      padding: '2px 7px',
      border: '1px dashed var(--border-primary)',
      borderRadius: 10,
      background: 'transparent',
      color: 'var(--text-secondary)',
      fontSize: 10,
      fontFamily: 'inherit',
      cursor: 'pointer',
      whiteSpace: 'nowrap',
    }}
    onMouseDown={event => {
      event.preventDefault();
      event.stopPropagation();
    }}
    onClick={event => {
      event.stopPropagation();
      onOpen();
    }}
  >
    + Error code
  </button>
);

export const ErrorCodeDropdownRow: React.FC<{
  choice: ErrorCodeChoice;
  accentColor: string;
  highlighted: boolean;
  onMouseDown: (event: React.MouseEvent<HTMLDivElement>) => void;
  onMouseEnter: () => void;
}> = ({ choice, accentColor, highlighted, onMouseDown, onMouseEnter }) => (
  <div
    onMouseDown={onMouseDown}
    onMouseEnter={onMouseEnter}
    style={{
      padding: '6px 10px',
      fontSize: 12,
      cursor: 'pointer',
      color: 'var(--text-secondary)',
      background: highlighted ? 'var(--bg-secondary)' : 'transparent',
      fontWeight: 400,
      display: 'flex',
      alignItems: 'center',
      gap: 7,
    }}
  >
    <span style={{
      fontSize: 11,
      width: 14,
      height: 14,
      flex: '0 0 14px',
      border: `1px solid ${choice.selected ? accentColor : 'var(--border-primary)'}`,
      borderRadius: 3,
      background: choice.selected ? accentColor : 'transparent',
      color: choice.selected ? '#fff' : 'var(--text-muted)',
      display: 'inline-flex',
      alignItems: 'center',
      justifyContent: 'center',
    }}>
      {choice.selected ? '✓' : ''}
    </span>
    <span style={{ minWidth: 0 }}>{choice.label}</span>
  </div>
);
