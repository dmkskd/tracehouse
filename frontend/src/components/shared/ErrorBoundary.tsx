/**
 * ErrorBoundary - Generic error boundary component for React applications
 * 
 * This component catches JavaScript errors anywhere in its child component tree,
 * logs those errors, and displays a fallback UI instead of crashing the whole app.
 */

import { Component, type ReactNode, type ErrorInfo } from 'react';

/**
 * Props for the ErrorBoundary component
 */
export interface ErrorBoundaryProps {
  /** Child components to render */
  children: ReactNode;
  /** Optional custom fallback UI to display on error */
  fallback?: ReactNode | ((error: Error, resetError: () => void) => ReactNode);
  /** Optional callback when an error is caught */
  onError?: (error: Error, errorInfo: ErrorInfo) => void;
  /** Optional callback when the error boundary is reset */
  onReset?: () => void;
  /** Optional title for the error display */
  errorTitle?: string;
  /** Optional description for the error display */
  errorDescription?: string;
  /** Whether to show the error message details */
  showErrorDetails?: boolean;
  /** Optional CSS class for the error container */
  className?: string;
}

/**
 * State for the ErrorBoundary component
 */
interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
  errorInfo: ErrorInfo | null;
}

/**
 * ErrorBoundary - A reusable error boundary component
 * 
 * Catches errors in child components and displays a fallback UI.
 * Supports custom fallback rendering and error callbacks.
 * 
 * @example
 * ```tsx
 * <ErrorBoundary
 *   fallback={<div>Something went wrong</div>}
 *   onError={(error) => console.error(error)}
 * >
 *   <MyComponent />
 * </ErrorBoundary>
 * ```
 */
export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null,
    };
  }

  static getDerivedStateFromError(error: Error): Partial<ErrorBoundaryState> {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    this.setState({ errorInfo });
    
    // Call the optional error callback
    if (this.props.onError) {
      this.props.onError(error, errorInfo);
    }
    
    // Log the error to console in development
    if (process.env.NODE_ENV === 'development') {
      console.error('ErrorBoundary caught an error:', error);
      console.error('Component stack:', errorInfo.componentStack);
    }
  }

  /**
   * Reset the error boundary state
   */
  resetError = (): void => {
    this.setState({
      hasError: false,
      error: null,
      errorInfo: null,
    });
    
    if (this.props.onReset) {
      this.props.onReset();
    }
  };

  render(): ReactNode {
    const {
      children,
      fallback,
      errorTitle = 'Something went wrong',
      errorDescription = 'An error occurred while rendering this component.',
      showErrorDetails = true,
      className = '',
    } = this.props;

    if (this.state.hasError && this.state.error) {
      // If a custom fallback is provided
      if (fallback) {
        if (typeof fallback === 'function') {
          return fallback(this.state.error, this.resetError);
        }
        return fallback;
      }

      {/* Default error UI */}
      return (
        <div
          className={`
            flex items-center justify-center p-6 
            bg-red-50 dark:bg-red-900/20 
            border border-red-200 dark:border-red-800 
            rounded-lg ${className}
          `}
          role="alert"
        >
          <div className="text-center max-w-md">
            <div className="text-2xl mb-4 font-bold text-red-500">!</div>
            <h3 className="text-lg font-semibold text-red-800 dark:text-red-200 mb-2">
              {errorTitle}
            </h3>
            <p className="text-sm text-red-600 dark:text-red-300 mb-4">
              {errorDescription}
            </p>
            
            {showErrorDetails && this.state.error && (
              <div className="mb-4 p-3 bg-red-100 dark:bg-red-900/30 rounded text-left">
                <p className="text-xs text-red-700 dark:text-red-300 font-mono break-all">
                  {this.state.error.message}
                </p>
              </div>
            )}
            
            <button
              onClick={this.resetError}
              className="
                px-4 py-2 text-sm font-medium
                bg-red-600 text-white rounded-md
                hover:bg-red-700 transition-colors
                focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-offset-2
              "
            >
              Try Again
            </button>
          </div>
        </div>
      );
    }

    return children;
  }
}

export default ErrorBoundary;
