import React, { useEffect } from 'react';
import { createPortal } from 'react-dom';

interface PortalModalProps {
  isOpen: boolean;
  onClose: () => void;
  children: React.ReactNode;
  title?: string;
  width?: number | string;
  height?: number | string;
}

export function PortalModal({
  isOpen,
  onClose,
  children,
  title,
  width = '90vw',
  height = '85vh',
}: PortalModalProps) {
  useEffect(() => {
    if (!isOpen) return;

    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };

    document.addEventListener('keydown', handleEscape);
    const originalOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    return () => {
      document.removeEventListener('keydown', handleEscape);
      document.body.style.overflow = originalOverflow;
    };
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  return createPortal(
    <div
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 10000,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      }}
    >
      {/* Backdrop */}
      <div
        style={{
          position: 'absolute',
          inset: 0,
          background: 'rgba(0, 0, 0, 0.75)',
          backdropFilter: 'blur(4px)',
        }}
        onClick={onClose}
      />
      
      {/* Modal Content */}
      <div
        style={{
          position: 'relative',
          width: typeof width === 'number' ? `${width}px` : width,
          maxWidth: '95vw',
          height: typeof height === 'number' ? `${height}px` : height,
          maxHeight: '95vh',
          background: 'linear-gradient(180deg, #14142a 0%, #0c0c1a 100%)',
          borderRadius: 12,
          border: '1px solid rgba(124,58,237,0.3)',
          boxShadow: '0 0 60px rgba(0,0,0,0.8), 0 0 30px rgba(124,58,237,0.15)',
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        {title && (
          <div
            style={{
              flexShrink: 0,
              padding: '16px 24px',
              borderBottom: '1px solid rgba(124,58,237,0.2)',
              background: 'rgba(0,0,0,0.2)',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}
          >
            <span
              style={{
                fontWeight: 600,
                color: 'white',
                fontSize: 16,
                fontFamily: 'system-ui, sans-serif',
              }}
            >
              {title}
            </span>
            <button
              onClick={onClose}
              style={{
                background: 'none',
                border: 'none',
                color: 'rgba(255,255,255,0.4)',
                cursor: 'pointer',
                fontSize: 20,
                padding: 4,
                lineHeight: 1,
                borderRadius: 4,
                transition: 'color 0.2s',
              }}
              onMouseEnter={(e) => (e.currentTarget.style.color = 'rgba(255,255,255,0.8)')}
              onMouseLeave={(e) => (e.currentTarget.style.color = 'rgba(255,255,255,0.4)')}
            >
              ×
            </button>
          </div>
        )}
        
        {/* Body */}
        <div
          style={{
            flex: 1,
            overflow: 'auto',
            minHeight: 0,
          }}
        >
          {children}
        </div>
      </div>
    </div>,
    document.body
  );
}
