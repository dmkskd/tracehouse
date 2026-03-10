/**
 * SafeText - Drop-in replacement for drei's <Text> that falls back to <Html>
 * when running in a file:// context (null origin) where troika's blob-URL
 * workers are blocked by the browser.
 */

import React from 'react';
import { Text, Html } from '@react-three/drei';

const isNullOrigin =
  typeof window !== 'undefined' && (window.origin === 'null' || window.origin === null);

/**
 * Subset of drei Text props that we forward to the Html fallback.
 * The real <Text> accepts the full set; the fallback only maps the
 * most commonly used ones to CSS.
 */
interface SafeTextProps {
  position?: [number, number, number];
  rotation?: [number, number, number];
  fontSize?: number;
  color?: string;
  anchorX?: 'left' | 'center' | 'right';
  anchorY?: 'top' | 'middle' | 'bottom';
  fillOpacity?: number;
  fontWeight?: number | string;
  raycast?: unknown;
  children?: React.ReactNode;
  // Forward anything else to drei Text when available
  [key: string]: unknown;
}

const ANCHOR_X_TO_ALIGN: Record<string, string> = {
  left: 'left',
  center: 'center',
  right: 'right',
};

function HtmlFallback({
  position,
  fontSize = 0.3,
  color = 'white',
  anchorX = 'center',
  fillOpacity = 1,
  fontWeight,
  children,
}: SafeTextProps) {
  return (
    <Html
      position={position}
      center={anchorX === 'center'}
      style={{
        fontSize: `${fontSize * 80}px`,
        color,
        opacity: fillOpacity,
        fontWeight: fontWeight as React.CSSProperties['fontWeight'],
        textAlign: ANCHOR_X_TO_ALIGN[anchorX] as React.CSSProperties['textAlign'],
        whiteSpace: 'nowrap',
        pointerEvents: 'none',
        userSelect: 'none',
      }}
    >
      {children}
    </Html>
  );
}

export const SafeText: React.FC<SafeTextProps> = isNullOrigin
  ? HtmlFallback
  : (Text as unknown as React.FC<SafeTextProps>);
