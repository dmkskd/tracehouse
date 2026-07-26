import React from 'react';

interface PreviewToggleButtonProps {
  label: string;
  visible: boolean;
  onToggle: () => void;
}

export const PreviewToggleButton: React.FC<PreviewToggleButtonProps> = ({
  label,
  visible,
  onToggle,
}) => (
  <button
    type="button"
    onClick={onToggle}
    style={{
      padding: '5px 12px',
      fontSize: 11,
      borderRadius: 5,
      border: visible
        ? '1px solid rgba(88, 166, 255, 0.35)'
        : '1px solid var(--border-primary)',
      background: visible ? 'rgba(88, 166, 255, 0.12)' : 'transparent',
      color: visible ? '#58a6ff' : 'var(--text-muted)',
      cursor: 'pointer',
      fontWeight: visible ? 600 : 400,
      transition: 'all 0.15s',
    }}
  >
    {visible ? `Hide ${label}` : `Show ${label}`}
  </button>
);
