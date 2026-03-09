import axios from 'axios';

/**
 * @deprecated This axios client targets the Python backend which is being
 * replaced by direct ClickHouse access via @tracehouse/core services.
 * New code should use the service layer from ClickHouseProvider instead.
 * This file will be removed once all consumers are migrated.
 */

// Create axios instance with default configuration
const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for adding connection ID
api.interceptors.request.use(
  (config) => {
    // Connection ID can be added here if needed
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor for unwrapping standardized responses and error handling
api.interceptors.response.use(
  (response) => {
    // Unwrap standardized API response format
    // Backend wraps responses as { success: true, data: <actual_data>, timestamp: ... }
    if (response.data && typeof response.data === 'object' && 'success' in response.data && 'data' in response.data) {
      response.data = response.data.data;
    }
    return response;
  },
  (error) => {
    // Extract the most useful error message
    let errorMessage = 'Unknown error';
    
    if (error.response) {
      // Server responded with error status
      const { status, data } = error.response;
      
      // Try to extract error message from response body
      if (data) {
        if (typeof data === 'string') {
          errorMessage = data;
        } else if (data.detail) {
          // FastAPI error format
          errorMessage = typeof data.detail === 'string' 
            ? data.detail 
            : data.detail.message || JSON.stringify(data.detail);
        } else if (data.error) {
          errorMessage = typeof data.error === 'string'
            ? data.error
            : data.error.message || JSON.stringify(data.error);
        } else if (data.message) {
          errorMessage = data.message;
        } else {
          errorMessage = JSON.stringify(data);
        }
      }
      
      // Add status code context
      switch (status) {
        case 401:
          errorMessage = `Authentication failed: ${errorMessage}`;
          break;
        case 403:
          errorMessage = `Access denied: ${errorMessage}`;
          break;
        case 404:
          errorMessage = `Not found: ${errorMessage}`;
          break;
        case 500:
          errorMessage = `Server error: ${errorMessage}`;
          break;
        case 502:
        case 503:
          errorMessage = `Service unavailable: ${errorMessage}`;
          break;
        case 504:
          errorMessage = `Request timeout: ${errorMessage}`;
          break;
      }
      
      console.error('[API] %d error:', status, errorMessage);
    } else if (error.request) {
      // Request made but no response received
      errorMessage = 'Network error: No response received from server';
      console.error('[API] Network error:', error.message);
    } else {
      // Error setting up request
      errorMessage = `Request error: ${error.message}`;
      console.error('[API] Request error:', error.message);
    }
    
    // Create a new error with the extracted message
    const enhancedError = new Error(errorMessage);
    (enhancedError as Error & { originalError: unknown }).originalError = error;
    
    return Promise.reject(enhancedError);
  }
);

// API response types
export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
  };
  timestamp: string;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
}

export default api;
