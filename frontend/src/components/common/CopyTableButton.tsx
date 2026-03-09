/**
 * CopyTableButton — consistent "copy table as TSV" button for all data tables.
 *
 * Usage:
 *   <CopyTableButton
 *     headers={['Table', 'Size', 'Rows']}
 *     rows={data.map(d => [d.table, d.size, d.rows])}
 *   />
 */

import React, { useState, useCallback } from 'react';

interface CopyTableButtonProps {
  /** Column headers */
  headers: string[];
  /** Row data — each inner array matches headers order. Values are stringified. */
  rows: (string | number | boolean | null | undefined)[][];
  /** Optional tooltip override (default: "Copy table as TSV") */
  title?: string;
  /** Optional size override */
  size?: number;
}

export const CopyTableButton: React.FC<CopyTableButtonProps> = ({
  headers,
  rows,
  title = 'Copy table as TSV',
  size = 14,
}) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation();
    const tsv = [
      headers.join('\t'),
      ...rows.map(row => row.map(cell => (cell == null ? '' : String(cell))).join('\t')),
    ].join('\n');
    try {
      await navigator.clipboard.writeText(tsv);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.warn('[CopyTableButton] Clipboard write failed:', err);
    }
  }, [headers, rows]);

  return (
    <button
      onClick={handleCopy}
      title={title}
      style={{
        background: 'none',
        border: 'none',
        cursor: 'pointer',
        color: copied ? 'var(--accent-green, #3fb950)' : 'var(--text-muted)',
        fontSize: size,
        padding: '2px 6px',
        borderRadius: 4,
        lineHeight: 1,
        transition: 'color 0.15s',
      }}
    >
      {copied ? '✓' : '⧉'}
    </button>
  );
};

export default CopyTableButton;
