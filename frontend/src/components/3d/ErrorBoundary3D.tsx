/**
 * ErrorBoundary3D - Specialized error boundary for 3D rendering failures
 * 
 * This component catches errors in 3D visualization components and provides
 * automatic fallback to 2D representations when WebGL or Three.js fails.
 * 
 * Design: If 3D rendering fails, fall back to 2D table/chart representations
 */

import { Component, type ReactNode, type ErrorInfo } from 'react';

/**
 * Props for the ErrorBoundary3D component
 */
export interface ErrorBoundary3DProps {
  /** Child 3D components to render */
  children: ReactNode;
  /** 2D fallback component to render when 3D fails */
  fallback2D?: ReactNode;
  /** Optional callback when an error is caught */
  onError?: (error: Error, errorInfo: ErrorInfo) => void;
  /** Optional callback when switching to 2D fallback */
  onFallback?: () => void;
  /** Whether to show the "Switch to 2D" button */
  showSwitchButton?: boolean;
  /** Custom error title */
  errorTitle?: string;
  /** Custom error description */
  errorDescription?: string;
  /** Optional CSS class for the container */
  className?: string;
}

/**
 * State for the ErrorBoundary3D component
 */
interface ErrorBoundary3DState {
  hasError: boolean;
  error: Error | null;
  errorInfo: ErrorInfo | null;
  showFallback2D: boolean;
}

/**
 * Check if WebGL is supported in the browser
 */
export function isWebGLSupported(): boolean {
  try {
    const canvas = document.createElement('canvas');
    const gl = canvas.getContext('webgl') || canvas.getContext('experimental-webgl');
    return gl !== null;
  } catch {
    return false;
  }
}

/**
 * Check if WebGL2 is supported in the browser
 */
export function isWebGL2Supported(): boolean {
  try {
    const canvas = document.createElement('canvas');
    const gl = canvas.getContext('webgl2');
    return gl !== null;
  } catch {
    return false;
  }
}

/**
 * ErrorBoundary3D - Specialized error boundary for 3D visualizations
 * 
 * Catches errors in Three.js/React Three Fiber components and provides
 * automatic fallback to 2D representations.
 * 
 * @example
 * ```tsx
 * <ErrorBoundary3D
 *   fallback2D={<PartsTable2D parts={parts} />}
 *   onFallback={() => console.log('Switched to 2D')}
 * >
 *   <Scene3D config={config}>
 *     <PartsVisualization parts={parts} />
 *   </Scene3D>
 * </ErrorBoundary3D>
 * ```
 */
export class ErrorBoundary3D extends Component<ErrorBoundary3DProps, ErrorBoundary3DState> {
  constructor(props: ErrorBoundary3DProps) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null,
      showFallback2D: false,
    };
  }

  static getDerivedStateFromError(error: Error): Partial<ErrorBoundary3DState> {
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
      console.error('ErrorBoundary3D caught an error:', error);
      console.error('Component stack:', errorInfo.componentStack);
    }
  }

  /**
   * Switch to 2D fallback view
   */
  switchTo2D = (): void => {
    this.setState({ showFallback2D: true });
    
    if (this.props.onFallback) {
      this.props.onFallback();
    }
  };

  /**
   * Try to render 3D again
   */
  retry3D = (): void => {
    this.setState({
      hasError: false,
      error: null,
      errorInfo: null,
      showFallback2D: false,
    });
  };

  /**
   * Determine if the error is likely a WebGL/3D rendering issue
   */
  is3DError(error: Error): boolean {
    const errorMessage = error.message.toLowerCase();
    const webglKeywords = [
      'webgl',
      'three',
      'canvas',
      'gl_',
      'shader',
      'texture',
      'buffer',
      'renderer',
      'context',
      'gpu',
    ];
    
    return webglKeywords.some(keyword => errorMessage.includes(keyword));
  }

  render(): ReactNode {
    const {
      children,
      fallback2D,
      showSwitchButton = true,
      errorTitle = '3D Visualization Error',
      errorDescription = 'Unable to render 3D visualization. This may be due to WebGL not being supported or a rendering error.',
      className = '',
    } = this.props;

    // If user chose to show 2D fallback
    if (this.state.showFallback2D && fallback2D) {
      return (
        <div className={`relative ${className}`}>
          {/* 2D Fallback indicator */}
          <div className="absolute top-2 right-2 z-10">
            <button
              onClick={this.retry3D}
              className="
                px-3 py-1.5 text-xs font-medium
                bg-blue-600 text-white rounded-md
                hover:bg-blue-700 transition-colors
                flex items-center gap-1
              "
              title="Try 3D view again"
            >
              Try 3D
            </button>
          </div>
          {fallback2D}
        </div>
      );
    }

    // If there's an error
    if (this.state.hasError && this.state.error) {
      const webglSupported = isWebGLSupported();
      
      return (
        <div
          className={`
            h-full min-h-[300px] flex items-center justify-center 
            bg-gray-100 dark:bg-gray-800 rounded-lg ${className}
          `}
          role="alert"
        >
          <div className="text-center p-6 max-w-md">
            <div className="text-2xl mb-4 font-bold text-red-500">!</div>
            <h3 className="text-lg font-semibold text-gray-700 dark:text-gray-300 mb-2">
              {errorTitle}
            </h3>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
              {errorDescription}
            </p>
            
            {/* WebGL support status */}
            <div className="mb-4 p-3 bg-gray-200 dark:bg-gray-700 rounded text-left text-xs">
              <div className="flex items-center gap-2 mb-1">
                <span className={webglSupported ? 'text-green-500' : 'text-red-500'}>
                  {webglSupported ? '✓' : '✗'}
                </span>
                <span className="text-gray-600 dark:text-gray-400">
                  WebGL {webglSupported ? 'supported' : 'not supported'}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <span className={isWebGL2Supported() ? 'text-green-500' : 'text-yellow-500'}>
                  {isWebGL2Supported() ? 'OK' : '!'}
                </span>
                <span className="text-gray-600 dark:text-gray-400">
                  WebGL2 {isWebGL2Supported() ? 'supported' : 'not available'}
                </span>
              </div>
            </div>
            
            {/* Error details */}
            <div className="mb-4 p-3 bg-red-50 dark:bg-red-900/20 rounded text-left">
              <p className="text-xs text-red-600 dark:text-red-400 font-mono break-all">
                {this.state.error.message}
              </p>
            </div>
            
            {/* Action buttons */}
            <div className="flex flex-col sm:flex-row gap-2 justify-center">
              <button
                onClick={this.retry3D}
                className="
                  px-4 py-2 text-sm font-medium
                  bg-gray-600 text-white rounded-md
                  hover:bg-gray-700 transition-colors
                "
              >
                Try Again
              </button>
              
              {showSwitchButton && fallback2D && (
                <button
                  onClick={this.switchTo2D}
                  className="
                    px-4 py-2 text-sm font-medium
                    bg-blue-600 text-white rounded-md
                    hover:bg-blue-700 transition-colors
                  "
                >
                  Use 2D View
                </button>
              )}
            </div>
          </div>
        </div>
      );
    }

    return children;
  }
}

export default ErrorBoundary3D;
