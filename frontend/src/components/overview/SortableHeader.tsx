import type { SortDirection } from '../../hooks/useSortState';

interface SortableHeaderProps {
  label: string;
  sortKey: string;
  activeSortKey: string;
  direction: SortDirection;
  onSort: (key: string) => void;
  align?: 'left' | 'right' | 'center';
  width?: number;
}

export function SortableHeader({ label, sortKey, activeSortKey, direction, onSort, align = 'left', width }: SortableHeaderProps) {
  const isActive = activeSortKey === sortKey;
  const arrow = isActive ? (direction === 'desc' ? ' ▾' : ' ▴') : '';

  return (
    <th
      onClick={() => onSort(sortKey)}
      style={{
        padding: '6px 8px',
        textAlign: align,
        color: isActive ? 'var(--text-secondary)' : 'var(--text-muted)',
        fontWeight: 500,
        fontSize: 10,
        cursor: 'pointer',
        userSelect: 'none',
        whiteSpace: 'nowrap',
        width: width ?? undefined,
      }}
    >
      {label}{arrow}
    </th>
  );
}
