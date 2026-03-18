/**
 * ModalWrapper — Shared modal chrome used by query, merge, and mutation detail modals.
 *
 * Provides: backdrop overlay, centered container, escape-to-close, body scroll lock.
 */

import React, { useEffect, useCallback } from 'react';

/**
 * Modal wrapper component for consistent styling
 */
export const ModalWrapper: React.FC<{
  isOpen: boolean;
  onClose: () => void;
  children: React.ReactNode;
  maxWidth?: number;
}> = ({ isOpen, onClose, children, maxWidth = 1200 }) => {
  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (e.key === 'Escape') onClose();
  }, [onClose]);

  useEffect(() => {
    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown);
      document.body.style.overflow = 'hidden';
      return () => {
        document.removeEventListener('keydown', handleKeyDown);
        document.body.style.overflow = '';
      };
    }
  }, [isOpen, handleKeyDown]);

  if (!isOpen) return null;

  const isDark = document.documentElement.getAttribute('data-theme') !== 'light' 
    && !document.documentElement.classList.contains('theme-light');
  const modalBg = isDark ? '#0a0a1a' : '#f8fafc';
  const borderColor = isDark ? '#2a2a4a' : '#e2e8f0';

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 100000,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: 16,
      }}
    >
      <div
        style={{
          position: 'absolute',
          inset: 0,
          background: 'rgba(0, 0, 0, 0.8)',
          backdropFilter: 'blur(4px)',
        }}
        onClick={onClose}
      />
      <div
        style={{
          position: 'relative',
          width: '100%',
          maxWidth,
          height: '85vh',
          maxHeight: 960,
          background: modalBg,
          borderRadius: 16,
          border: `1px solid ${borderColor}`,
          boxShadow: '0 25px 50px rgba(0,0,0,0.5)',
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {children}
      </div>
    </div>
  );
};

/**
 * Metric item card — label + monospace value.
 */
export const MetricItem: React.FC<{ label: string; value: React.ReactNode; color?: string }> = ({ label, value, color }) => (
  <div style={{ 
    background: 'var(--bg-card)', 
    border: '1px solid var(--border-secondary)',
    borderRadius: 6, 
    padding: '8px 12px',
  }}>
    <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 4 }}>{label}</div>
    <div style={{ fontSize: 13, fontWeight: 400, color: color || 'var(--text-primary)', fontFamily: 'monospace' }}>{value}</div>
  </div>
);
