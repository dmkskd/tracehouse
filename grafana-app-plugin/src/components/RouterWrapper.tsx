/**
 * RouterWrapper - Ensures components have Router context
 * 
 * This wrapper catches the case where a component tries to use
 * react-router hooks outside of a Router context.
 */
import React, { ComponentType } from 'react';
import { useLocation } from 'react-router-dom';

// Test component that verifies Router context exists
function RouterContextTest({ children }: { children: React.ReactNode }) {
  try {
    // This will throw if no Router context
    useLocation();
    return <>{children}</>;
  } catch {
    return (
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        color: 'rgba(255,255,255,0.6)',
        padding: 40,
        textAlign: 'center',
      }}>
        <div>
          <div style={{ fontSize: 32, marginBottom: 16 }}>⚠️</div>
          <div>Router context not available</div>
        </div>
      </div>
    );
  }
}

export function withRouterContext<P extends object>(
  WrappedComponent: ComponentType<P>
): React.FC<P> {
  return function WithRouterContext(props: P) {
    return (
      <RouterContextTest>
        <WrappedComponent {...props} />
      </RouterContextTest>
    );
  };
}
