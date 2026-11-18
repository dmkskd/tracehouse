/**
 * ModalWrapper — Shared modal chrome used by query, merge, and mutation detail modals.
 *
 * Provides: backdrop overlay, centered container, escape-to-close, body scroll lock.
 */

import React, { useEffect, useCallback, useRef } from 'react';
import { createPortal } from 'react-dom';

// Modals can be layered (resource exploration → query/merge details). Only the
// topmost layer handles Escape; scroll remains locked until the last one closes.
const modalStack: symbol[] = [];
let previousBodyOverflow = '';

/**
 * Modal wrapper component for consistent styling
 */
export const ModalWrapper: React.FC<{
  isOpen: boolean;
  onClose: () => void;
  children: React.ReactNode;
  maxWidth?: number | string;
  height?: number | string;
  maxHeight?: number | string;
  backdropBackground?: string;
  backdropBlur?: number;
  ariaLabel?: string;
}> = ({
  isOpen,
  onClose,
  children,
  maxWidth = 1200,
  height = '85vh',
  maxHeight = 960,
  backdropBackground = 'rgba(0, 0, 0, 0.92)',
  backdropBlur = 8,
  ariaLabel,
}) => {
  const modalId = useRef(Symbol('modal'));
  const dialogRef = useRef<HTMLDivElement>(null);
  const onCloseRef = useRef(onClose);
  useEffect(() => { onCloseRef.current = onClose; }, [onClose]);
  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (modalStack[modalStack.length - 1] !== modalId.current) return;
    if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      onCloseRef.current();
    } else if (e.key === 'Tab') {
      const dialog = dialogRef.current;
      if (!dialog) return;
      const candidates = [...dialog.querySelectorAll<HTMLElement>('button, a[href], input, select, textarea, [tabindex]:not([tabindex="-1"])')];
      const focusable = candidates.filter(el => !el.matches(':disabled, input[type="hidden"]') && !el.closest('[hidden], [inert]') && el.getClientRects().length > 0);
      const first = focusable[0], last = focusable[focusable.length - 1];
      if (!first) { e.preventDefault(); dialog.focus(); }
      else if (!dialog.contains(document.activeElement) || document.activeElement === dialog || (e.shiftKey && document.activeElement === first) || (!e.shiftKey && document.activeElement === last)) {
        e.preventDefault(); (e.shiftKey ? last : first).focus();
      }
    }
  }, []);

  useEffect(() => {
    if (!isOpen) return;
    const id = modalId.current;
    const trigger = document.activeElement as HTMLElement | null;
    if (!modalStack.length) previousBodyOverflow = document.body.style.overflow;
    modalStack.push(id);
    document.addEventListener('keydown', handleKeyDown);
    document.body.style.overflow = 'hidden';
    if (!dialogRef.current?.contains(document.activeElement)) dialogRef.current?.focus({ preventScroll: true });
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
      const wasTopmost = modalStack[modalStack.length - 1] === id;
      const index = modalStack.indexOf(id);
      if (index !== -1) modalStack.splice(index, 1);
      if (!modalStack.length) document.body.style.overflow = previousBodyOverflow;
      if (wasTopmost && trigger?.isConnected) trigger.focus({ preventScroll: true });
    };
  }, [isOpen, handleKeyDown]);

  if (!isOpen) return null;

  const isDark = document.documentElement.getAttribute('data-theme') !== 'light' 
    && !document.documentElement.classList.contains('theme-light');
  const modalBg = isDark ? '#0a0a1a' : '#f8fafc';
  const borderColor = isDark ? '#2a2a4a' : '#e2e8f0';

  return createPortal(
    <div
      ref={dialogRef}
      tabIndex={-1}
      role="dialog"
      aria-modal="true"
      aria-label={ariaLabel}
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
          background: backdropBackground,
          backdropFilter: `blur(${backdropBlur}px)`,
        }}
        onClick={onClose}
      />
      <div
        style={{
          position: 'relative',
          width: '100%',
          maxWidth,
          height,
          maxHeight,
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
    </div>,
    document.body,
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
