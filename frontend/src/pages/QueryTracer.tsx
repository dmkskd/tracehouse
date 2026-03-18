/**
 * QueryTracer - Main page component for query tracing and debugging
 * 
 * Provides a unified view for trace logs, EXPLAIN output, and OpenTelemetry spans
 * to help debug query performance issues and understand execution flow.
 * 
 */

import React, { useEffect, useCallback, useState } from 'react';
import { useParams, useSearchParams, useNavigate } from '../hooks/useAppLocation';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useTraceStore, type ExplainType, type TraceLog } from '../stores/traceStore';
import { TraceLogViewer } from '../components/tracing/TraceLogViewer';
import { ExplainViewer } from '../components/tracing/ExplainViewer';
import { useCapabilityCheck } from '../components/shared/RequiresCapability';
import { formatMicroseconds, formatDurationMs } from '../utils/formatters';
import {
  Scene3D,
  PipelineVisualization,
  createSceneConfig,
  ErrorBoundary3D,
  PipelineFallback2D,
  type PipelineNode,
} from '../components/3d';

/**
 * Query ID input component for manual entry
 */
const QueryIdInput: React.FC<{
  queryId: string;
  onChange: (queryId: string) => void;
  onSubmit: () => void;
}> = ({ queryId, onChange, onSubmit }) => {
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      onSubmit();
    }
  };
  
  return (
    <div className="flex items-center space-x-2">
      <input
        type="text"
        value={queryId}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Enter query ID..."
        className="flex-1 px-3 py-2 text-sm font-mono border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
      />
      <button
        onClick={onSubmit}
        disabled={!queryId.trim()}
        className={`
          px-4 py-2 text-sm font-medium rounded-md transition-colors
          ${!queryId.trim()
            ? 'bg-gray-300 text-gray-500 cursor-not-allowed dark:bg-gray-600'
            : 'bg-blue-600 text-white hover:bg-blue-700'
          }
        `}
      >
        Load Trace
      </button>
    </div>
  );
};

/**
 * OpenTelemetry spans viewer
 */
const SpansViewer: React.FC<{
  spans: Array<{
    trace_id: string;
    span_id: string;
    parent_span_id: string;
    operation_name: string;
    start_time_us: number;
    finish_time_us: number;
    duration_us: number;
    duration_ms: number;
    is_root_span: boolean;
    attributes: Record<string, unknown>;
  }>;
  isLoading: boolean;
  error: string | null;
}> = ({ spans, isLoading, error }) => {
  const [expandedSpans, setExpandedSpans] = useState<Set<string>>(new Set());
  
  const toggleSpan = (spanId: string) => {
    const newExpanded = new Set(expandedSpans);
    if (newExpanded.has(spanId)) {
      newExpanded.delete(spanId);
    } else {
      newExpanded.add(spanId);
    }
    setExpandedSpans(newExpanded);
  };
  
  const formatDuration = formatMicroseconds;
  
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-32">
        <div className="text-center">
          <div className="animate-spin inline-block w-6 h-6 border-2 border-gray-400 border-t-gray-600 rounded-full mb-2"></div>
          <div className="text-sm text-gray-500 dark:text-gray-400">
            Loading OpenTelemetry spans...
          </div>
        </div>
      </div>
    );
  }
  
  if (error) {
    return (
      <div className="p-4 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg">
        <div className="flex items-start">
          <span className="text-yellow-500 mr-2 font-bold">i</span>
          <div>
            <div className="font-medium text-yellow-700 dark:text-yellow-400">
              OpenTelemetry spans not available
            </div>
            <div className="text-sm text-yellow-600 dark:text-yellow-300 mt-1">
              {error}
            </div>
          </div>
        </div>
      </div>
    );
  }
  
  if (spans.length === 0) {
    return (
      <div className="text-center text-gray-500 dark:text-gray-400 py-8">
        <div className="text-2xl mb-2 font-light">--</div>
        <div className="text-sm">No OpenTelemetry spans found for this query</div>
        <div className="text-xs mt-1">
          OpenTelemetry tracing may not be enabled on the ClickHouse server
        </div>
      </div>
    );
  }
  
  return (
    <div className="space-y-2">
      {spans.map((span) => (
        <div
          key={span.span_id}
          className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden"
        >
          <div
            className="flex items-center p-3 bg-gray-50 dark:bg-gray-800 cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-750"
            onClick={() => toggleSpan(span.span_id)}
          >
            <span className={`transform transition-transform mr-2 ${expandedSpans.has(span.span_id) ? 'rotate-90' : ''}`}>
              ▶
            </span>
            <span className={`px-2 py-0.5 text-xs rounded mr-2 ${span.is_root_span ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400' : 'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-400'}`}>
              {span.is_root_span ? 'ROOT' : 'SPAN'}
            </span>
            <span className="font-medium text-gray-800 dark:text-white flex-1">
              {span.operation_name}
            </span>
            <span className="text-sm text-gray-500 dark:text-gray-400">
              {formatDuration(span.duration_us)}
            </span>
          </div>
          
          {expandedSpans.has(span.span_id) && (
            <div className="p-3 border-t border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900">
              <div className="grid grid-cols-2 gap-2 text-sm">
                <div>
                  <span className="text-gray-500 dark:text-gray-400">Span ID:</span>
                  <span className="ml-2 font-mono text-gray-700 dark:text-gray-300">{span.span_id}</span>
                </div>
                <div>
                  <span className="text-gray-500 dark:text-gray-400">Parent:</span>
                  <span className="ml-2 font-mono text-gray-700 dark:text-gray-300">
                    {span.parent_span_id || 'None'}
                  </span>
                </div>
                <div>
                  <span className="text-gray-500 dark:text-gray-400">Duration:</span>
                  <span className="ml-2 text-gray-700 dark:text-gray-300">
                    {formatDuration(span.duration_us)} ({span.duration_ms.toFixed(2)}ms)
                  </span>
                </div>
                <div>
                  <span className="text-gray-500 dark:text-gray-400">Trace ID:</span>
                  <span className="ml-2 font-mono text-gray-700 dark:text-gray-300 text-xs">{span.trace_id}</span>
                </div>
              </div>
              
              {Object.keys(span.attributes).length > 0 && (
                <div className="mt-3">
                  <div className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Attributes
                  </div>
                  <div className="bg-gray-50 dark:bg-gray-800 rounded p-2 text-xs font-mono">
                    {Object.entries(span.attributes).map(([key, value]) => (
                      <div key={key}>
                        <span className="text-orange-600 dark:text-orange-400">{key}</span>
                        <span className="text-gray-500">: </span>
                        <span className="text-green-600 dark:text-green-400">{JSON.stringify(value)}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      ))}
    </div>
  );
};

/**
 * Timeline visualization - shows thread activity as horizontal bars
 */
const TimelineViewer: React.FC<{
  logs: Array<{
    event_time_microseconds: string;
    thread_id: number;
    thread_name: string;
    source: string;
    message: string;
    level: string;
  }>;
  isLoading: boolean;
}> = ({ logs, isLoading }) => {
  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
        <span style={{ color: 'rgba(255,255,255,0.5)' }}>Loading...</span>
      </div>
    );
  }

  if (logs.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: 40, color: 'rgba(255,255,255,0.4)' }}>
        No trace logs to visualize
      </div>
    );
  }

  // Parse timestamps and find time range
  const parsed = logs.map(log => ({
    ...log,
    time: new Date(log.event_time_microseconds?.replace(' ', 'T') + 'Z').getTime(),
  }));
  
  const minTime = Math.min(...parsed.map(l => l.time));
  const maxTime = Math.max(...parsed.map(l => l.time));
  const totalDuration = maxTime - minTime || 1;

  // Find coordinator thread
  const coordinatorThreadId = parsed.find(l => l.thread_name?.includes('TCPHandler'))?.thread_id ?? parsed[0]?.thread_id;

  // Group by thread and calculate spans
  const threadMap = new Map<number, { 
    threadId: number; 
    threadName: string; 
    spans: Array<{ start: number; end: number; source: string; message: string }>;
  }>();

  parsed.forEach((log, i) => {
    if (!threadMap.has(log.thread_id)) {
      threadMap.set(log.thread_id, { 
        threadId: log.thread_id, 
        threadName: log.thread_name, 
        spans: [] 
      });
    }
    
    // Find end time (next log from same thread, or next coordinator log)
    let endTime = log.time;
    for (let j = i + 1; j < parsed.length; j++) {
      if (parsed[j].thread_id === log.thread_id) {
        endTime = parsed[j].time;
        break;
      }
      if (log.thread_id !== coordinatorThreadId && parsed[j].thread_id === coordinatorThreadId) {
        endTime = parsed[j].time;
        break;
      }
    }
    
    threadMap.get(log.thread_id)!.spans.push({
      start: log.time,
      end: endTime,
      source: log.source,
      message: log.message,
    });
  });

  // Sort threads: coordinator first, then by first activity
  const threads = Array.from(threadMap.values()).sort((a, b) => {
    if (a.threadId === coordinatorThreadId) return -1;
    if (b.threadId === coordinatorThreadId) return 1;
    const aFirst = a.spans[0]?.start ?? 0;
    const bFirst = b.spans[0]?.start ?? 0;
    return aFirst - bFirst;
  });

  // Color palette for threads
  const getThreadColor = (threadId: number) => {
    if (threadId === coordinatorThreadId) return '#64b5f6';
    const hue = ((threadId * 137) % 360);
    return `hsl(${hue}, 60%, 55%)`;
  };

  const formatDuration = formatDurationMs;

  return (
    <div style={{ padding: 16 }}>
      {/* Time axis */}
      <div style={{ 
        display: 'flex', 
        justifyContent: 'space-between', 
        fontSize: 10, 
        color: 'rgba(255,255,255,0.4)',
        marginBottom: 8,
        marginLeft: 150,
      }}>
        <span>0ms</span>
        <span>{formatDuration(totalDuration)}</span>
      </div>

      {/* Thread rows */}
      {threads.map((thread) => (
        <div key={thread.threadId} style={{ display: 'flex', alignItems: 'center', marginBottom: 4 }}>
          {/* Thread label */}
          <div style={{ 
            width: 140, 
            fontSize: 11, 
            color: getThreadColor(thread.threadId),
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            paddingRight: 8,
          }} title={`${thread.threadName} (${thread.threadId})`}>
            {thread.threadId === coordinatorThreadId && '● '}
            {thread.threadName}
          </div>

          {/* Timeline bar */}
          <div style={{ 
            flex: 1, 
            height: 20, 
            background: 'rgba(255,255,255,0.05)', 
            borderRadius: 3,
            position: 'relative',
            overflow: 'hidden',
          }}>
            {thread.spans.map((span, i) => {
              const left = ((span.start - minTime) / totalDuration) * 100;
              const width = Math.max(0.5, ((span.end - span.start) / totalDuration) * 100);
              const duration = span.end - span.start;
              
              return (
                <div
                  key={i}
                  title={`${span.source}: ${span.message.slice(0, 100)}... (${formatDuration(duration)})`}
                  style={{
                    position: 'absolute',
                    left: `${left}%`,
                    width: `${width}%`,
                    height: '100%',
                    background: getThreadColor(thread.threadId),
                    opacity: duration > 100 ? 0.9 : 0.5,
                    borderRadius: 2,
                  }}
                />
              );
            })}
          </div>

          {/* Total duration for thread */}
          <div style={{ 
            width: 70, 
            fontSize: 10, 
            color: 'rgba(255,255,255,0.5)',
            textAlign: 'right',
            paddingLeft: 8,
            fontFamily: 'monospace',
          }}>
            {formatDuration(thread.spans.reduce((sum, s) => sum + (s.end - s.start), 0))}
          </div>
        </div>
      ))}

      {/* Legend */}
      <div style={{ 
        marginTop: 16, 
        paddingTop: 12, 
        borderTop: '1px solid rgba(255,255,255,0.1)',
        fontSize: 10,
        color: 'rgba(255,255,255,0.4)',
      }}>
        <span style={{ color: '#64b5f6' }}>● Coordinator thread</span>
        <span style={{ marginLeft: 16 }}>{threads.length} threads</span>
        <span style={{ marginLeft: 16 }}>Total: {formatDuration(totalDuration)}</span>
      </div>
    </div>
  );
};

/**
 * Tab navigation for different trace views
 */
const TraceTabs: React.FC<{
  activeTab: 'logs' | 'timeline' | 'explain' | 'spans';
  onTabChange: (tab: 'logs' | 'timeline' | 'explain' | 'spans') => void;
  logCount: number;
  spanCount: number;
  hasTextLog: boolean;
  hasOpenTelemetry: boolean;
}> = ({ activeTab, onTabChange, logCount, spanCount, hasTextLog, hasOpenTelemetry }) => {
  const tabs = [
    { id: 'logs' as const, label: 'Trace Logs', count: logCount, icon: null, unavailable: !hasTextLog },
    { id: 'timeline' as const, label: 'Timeline', count: null, icon: null, unavailable: !hasTextLog },
    { id: 'explain' as const, label: 'EXPLAIN', count: null, icon: null, unavailable: false },
    { id: 'spans' as const, label: 'OpenTelemetry', count: spanCount, icon: null, unavailable: !hasOpenTelemetry },
  ];
  
  return (
    <div className="flex border-b border-gray-200 dark:border-gray-700">
      {tabs.map((tab) => (
        <button
          key={tab.id}
          onClick={() => !tab.unavailable && onTabChange(tab.id)}
          title={tab.unavailable ? `Requires system.${tab.id === 'spans' ? 'opentelemetry_span_log' : 'text_log'} (not available)` : undefined}
          className={`
            flex items-center px-4 py-3 text-sm font-medium border-b-2 transition-colors
            ${tab.unavailable
              ? 'border-transparent text-gray-400 dark:text-gray-600 cursor-not-allowed opacity-50'
              : activeTab === tab.id
                ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                : 'border-transparent text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300'
            }
          `}
        >
          {tab.label}
          {tab.count !== null && tab.count > 0 && (
            <span className="ml-2 px-2 py-0.5 text-xs bg-gray-100 dark:bg-gray-700 rounded-full">
              {tab.count}
            </span>
          )}
        </button>
      ))}
    </div>
  );
};

/**
 * View mode toggle for EXPLAIN tab (Text vs 3D)
 */
const ExplainViewModeToggle: React.FC<{
  mode: 'text' | '3d';
  onModeChange: (mode: 'text' | '3d') => void;
  disabled: boolean;
}> = ({ mode, onModeChange, disabled }) => (
  <div className="flex items-center space-x-1 bg-gray-100 dark:bg-gray-700 rounded-lg p-1">
    <button
      onClick={() => onModeChange('text')}
      disabled={disabled}
      className={`
        px-3 py-1 text-xs font-medium rounded-md transition-colors
        ${disabled ? 'opacity-50 cursor-not-allowed' : ''}
        ${mode === 'text'
          ? 'bg-white dark:bg-gray-600 text-blue-600 dark:text-blue-400 shadow-sm'
          : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
        }
      `}
    >
      Text
    </button>
    <button
      onClick={() => onModeChange('3d')}
      disabled={disabled}
      className={`
        px-3 py-1 text-xs font-medium rounded-md transition-colors
        ${disabled ? 'opacity-50 cursor-not-allowed' : ''}
        ${mode === '3d'
          ? 'bg-white dark:bg-gray-600 text-blue-600 dark:text-blue-400 shadow-sm'
          : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
        }
      `}
    >
      3D View
    </button>
  </div>
);

/**
 * 3D Pipeline Visualization wrapper
 */
const Pipeline3DView: React.FC<{
  explainResult: { explain_type: string; output: string; parsed_tree: Record<string, unknown> | null } | null;
  traceLogs: TraceLog[];
  onNodeClick: (node: PipelineNode) => void;
}> = ({ explainResult, traceLogs, onNodeClick }) => {
  const sceneConfig = createSceneConfig({
    performanceMode: false,
    enableAnimations: true,
    cameraPosition: [0, 5, 12],
  });

  if (!explainResult || !explainResult.output) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="text-center text-gray-500 dark:text-gray-400">
          <div className="text-2xl mb-2 font-light">--</div>
          <div className="text-sm">
            Execute EXPLAIN PIPELINE to see the 3D visualization
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-[500px] w-full">
      <ErrorBoundary3D
        fallback2D={
          <PipelineFallback2D
            pipeline={explainResult}
            traceLogs={traceLogs}
            onNodeClick={onNodeClick}
          />
        }
        errorTitle="3D Pipeline Visualization Error"
        errorDescription="Unable to render 3D pipeline visualization. You can use the 2D tree view instead."
        onError={(error) => {
          console.error('Pipeline 3D visualization error:', error);
        }}
      >
        <Scene3D config={sceneConfig}>
          <PipelineVisualization
            pipeline={explainResult}
            traceLogs={traceLogs}
            onNodeClick={onNodeClick}
          />
        </Scene3D>
      </ErrorBoundary3D>
    </div>
  );
};

/**
 * Main QueryTracer component
 */
export const QueryTracer: React.FC = () => {
  const { queryId: urlQueryId } = useParams<{ queryId: string }>();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  
  const services = useClickHouseServices();
  const { available: hasTextLog, probing: probingTextLog } = useCapabilityCheck(['text_log']);
  const { available: hasOpenTelemetry, probing: probingOtel } = useCapabilityCheck(['opentelemetry_span_log']);
  const {
    selectedQueryId,
    selectedQuery,
    traceLogs,
    explainResult,
    selectedExplainType,
    openTelemetrySpans,
    logFilter,
    isLoadingLogs,
    isLoadingExplain,
    isLoadingSpans,
    error,
    setSelectedQueryId,
    setSelectedQuery,
    setTraceLogs,
    setExplainResult,
    setSelectedExplainType,
    setOpenTelemetrySpans,
    setLogFilter,
    setIsLoadingLogs,
    setIsLoadingExplain,
    setIsLoadingSpans,
    setError,
    clearTrace,
  } = useTraceStore();
  
  const [activeTab, setActiveTab] = useState<'logs' | 'timeline' | 'explain' | 'spans'>('logs');
  const [inputQueryId, setInputQueryId] = useState('');
  
  // View mode for EXPLAIN tab (text vs 3D)
  const [explainViewMode, setExplainViewMode] = useState<'text' | '3d'>('text');
  
  // Selected pipeline node in 3D view
  const [selectedPipelineNode, setSelectedPipelineNode] = useState<PipelineNode | null>(null);
  
  // Get query from URL params if provided
  const urlQuery = searchParams.get('query');
  
  // Initialize from URL params
  useEffect(() => {
    if (urlQueryId && urlQueryId !== selectedQueryId) {
      setSelectedQueryId(urlQueryId);
      setInputQueryId(urlQueryId);
    }
    if (urlQuery && urlQuery !== selectedQuery) {
      setSelectedQuery(urlQuery);
    }
  }, [urlQueryId, urlQuery]);
  
  // Fetch trace logs when query ID changes
  const fetchLogs = useCallback(async () => {
    if (!services || !selectedQueryId) return;
    
    setIsLoadingLogs(true);
    setError(null);
    
    try {
      const logs = await services.traceService.getQueryLogs(
        selectedQueryId,
        logFilter.logLevels
      );
      setTraceLogs(logs);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to fetch trace logs';
      setError(errorMessage);
    } finally {
      setIsLoadingLogs(false);
    }
  }, [services, selectedQueryId, logFilter.logLevels]);
  
  // Fetch OpenTelemetry spans
  const fetchSpans = useCallback(async () => {
    if (!services || !selectedQueryId) return;
    
    setIsLoadingSpans(true);
    
    try {
      const spans = await services.traceService.getOpenTelemetrySpans(selectedQueryId);
      setOpenTelemetrySpans(spans);
    } catch (err) {
      // Spans might not be available, don't show as error
      console.log('OpenTelemetry spans not available:', err);
      setOpenTelemetrySpans([]);
    } finally {
      setIsLoadingSpans(false);
    }
  }, [services, selectedQueryId]);
  
  // Execute EXPLAIN query
  const executeExplain = useCallback(async (query: string, type: ExplainType) => {
    if (!services || !query.trim()) return;
    
    setIsLoadingExplain(true);
    setError(null);
    setSelectedQuery(query);
    
    try {
      const result = await services.traceService.executeExplain(query, type);
      setExplainResult(result);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to execute EXPLAIN';
      setError(errorMessage);
    } finally {
      setIsLoadingExplain(false);
    }
  }, [services]);
  
  // Load trace data when query ID is set
  useEffect(() => {
    if (selectedQueryId && services) {
      fetchLogs();
      fetchSpans();
    }
  }, [selectedQueryId, services]);
  
  // Handle manual query ID submission
  const handleQueryIdSubmit = () => {
    if (inputQueryId.trim()) {
      navigate(`/trace/${encodeURIComponent(inputQueryId.trim())}`);
    }
  };
  
  // Handle pipeline node click in 3D view
  const handlePipelineNodeClick = useCallback((node: PipelineNode) => {
    setSelectedPipelineNode(prev => prev?.id === node.id ? null : node);
  }, []);
  
  // Handle filter change and refetch
  const handleFilterChange = (filter: typeof logFilter) => {
    setLogFilter(filter);
  };
  
  // Refetch logs when filter changes
  useEffect(() => {
    if (selectedQueryId && services) {
      fetchLogs();
    }
  }, [logFilter]);
  
  // No connection selected
  if (!services) {
    return (
      <div className="space-y-6">
        <h2 className="text-2xl font-bold text-gray-800 dark:text-white">
          Query Tracer
        </h2>
        <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-6 text-center">
          <div className="text-yellow-600 dark:text-yellow-400 text-lg mb-2 font-bold">!</div>
          <div className="text-yellow-700 dark:text-yellow-300">
            Please select a connection to use the Query Tracer
          </div>
        </div>
      </div>
    );
  }
  
  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-gray-800 dark:text-white">
          Query Tracer
        </h2>
        {selectedQueryId && (
          <button
            onClick={() => {
              clearTrace();
              setInputQueryId('');
              navigate('/trace');
            }}
            className="px-3 py-1 text-sm text-gray-600 hover:text-gray-800 dark:text-gray-400 dark:hover:text-gray-200"
          >
            Clear
          </button>
        )}
      </div>
      
      {/* Query ID input */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
        <div className="text-sm text-gray-500 dark:text-gray-400 mb-2">
          Query ID
        </div>
        <QueryIdInput
          queryId={inputQueryId}
          onChange={setInputQueryId}
          onSubmit={handleQueryIdSubmit}
        />
        {selectedQueryId && (
          <div className="mt-2 text-sm">
            <span className="text-gray-500 dark:text-gray-400">Current: </span>
            <span className="font-mono text-gray-700 dark:text-gray-300">{selectedQueryId}</span>
          </div>
        )}
      </div>
      
      {/* Main content */}
      {selectedQueryId ? (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
          {/* Tabs */}
          <TraceTabs
            activeTab={activeTab}
            onTabChange={setActiveTab}
            logCount={traceLogs.length}
            spanCount={openTelemetrySpans.length}
            hasTextLog={hasTextLog || probingTextLog}
            hasOpenTelemetry={hasOpenTelemetry || probingOtel}
          />
          
          {/* Tab content */}
          <div className="min-h-[500px]">
            {activeTab === 'logs' && (
              hasTextLog || probingTextLog ? (
              <TraceLogViewer
                logs={traceLogs}
                isLoading={isLoadingLogs}
                error={error}
                filter={logFilter}
                onFilterChange={handleFilterChange}
                onRefresh={fetchLogs}
                queryId={selectedQueryId}
              />
              ) : (
                <div style={{ padding: 32, textAlign: 'center', fontSize: 12, color: 'var(--text-muted)' }}>
                  Trace Logs requires system.text_log (not available on this server)
                </div>
              )
            )}

            {activeTab === 'timeline' && (
              hasTextLog || probingTextLog ? (
              <TimelineViewer
                logs={traceLogs}
                isLoading={isLoadingLogs}
              />
              ) : (
                <div style={{ padding: 32, textAlign: 'center', fontSize: 12, color: 'var(--text-muted)' }}>
                  Timeline requires system.text_log (not available on this server)
                </div>
              )
            )}
            
            {activeTab === 'explain' && (
              <div className="h-full flex flex-col">
                {/* View mode toggle header */}
                <div className="flex items-center justify-between p-4 border-b border-gray-200 dark:border-gray-700">
                  <div className="text-sm text-gray-500 dark:text-gray-400">
                    {explainViewMode === '3d' && selectedExplainType === 'PIPELINE' 
                      ? 'Interactive 3D Pipeline Visualization' 
                      : 'EXPLAIN Output'}
                  </div>
                  <ExplainViewModeToggle
                    mode={explainViewMode}
                    onModeChange={setExplainViewMode}
                    disabled={!explainResult || selectedExplainType !== 'PIPELINE'}
                  />
                </div>
                
                {/* Content based on view mode */}
                <div className="flex-1 overflow-hidden">
                  {explainViewMode === 'text' || selectedExplainType !== 'PIPELINE' ? (
                    <ExplainViewer
                      result={explainResult}
                      selectedType={selectedExplainType}
                      isLoading={isLoadingExplain}
                      error={activeTab === 'explain' ? error : null}
                      query={selectedQuery}
                      onTypeChange={setSelectedExplainType}
                      onExecute={executeExplain}
                    />
                  ) : (
                    <div className="h-full p-4 space-y-4">
                      <Pipeline3DView
                        explainResult={explainResult}
                        traceLogs={traceLogs}
                        onNodeClick={handlePipelineNodeClick}
                      />
                      
                      {/* Selected node details */}
                      {selectedPipelineNode && (
                        <div className="bg-purple-50 dark:bg-purple-900/20 border border-purple-200 dark:border-purple-800 rounded-lg p-4">
                          <div className="flex items-start justify-between">
                            <div>
                              <h4 className="font-semibold text-purple-800 dark:text-purple-200">
                                {selectedPipelineNode.name}
                              </h4>
                              <p className="text-sm text-purple-600 dark:text-purple-300 mt-1 font-mono">
                                {selectedPipelineNode.fullText}
                              </p>
                              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-3 text-sm">
                                <div>
                                  <span className="text-gray-500 dark:text-gray-400">Depth:</span>
                                  <span className="ml-2 text-gray-700 dark:text-gray-300">
                                    {selectedPipelineNode.depth}
                                  </span>
                                </div>
                                <div>
                                  <span className="text-gray-500 dark:text-gray-400">Children:</span>
                                  <span className="ml-2 text-gray-700 dark:text-gray-300">
                                    {selectedPipelineNode.childIds.length}
                                  </span>
                                </div>
                                {selectedPipelineNode.hasCorrelatedLogs && (
                                  <div>
                                    <span className="text-amber-500 font-bold">*</span>
                                    <span className="ml-2 text-amber-600 dark:text-amber-400">
                                      {selectedPipelineNode.correlatedLogCount} correlated logs
                                    </span>
                                  </div>
                                )}
                              </div>
                              {Object.keys(selectedPipelineNode.metadata).length > 0 && (
                                <div className="mt-3">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">Metadata:</span>
                                  <div className="flex flex-wrap gap-2 mt-1">
                                    {Object.entries(selectedPipelineNode.metadata).map(([key, value]) => (
                                      <span
                                        key={key}
                                        className="px-2 py-1 text-xs bg-purple-100 dark:bg-purple-800/30 text-purple-700 dark:text-purple-300 rounded"
                                      >
                                        {key}: {value}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )}
                            </div>
                            <button
                              onClick={() => setSelectedPipelineNode(null)}
                              className="text-purple-400 hover:text-purple-600"
                            >
                              ✕
                            </button>
                          </div>
                        </div>
                      )}
                      
                      {/* Hint for PIPELINE type */}
                      {!explainResult && (
                        <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
                          <div className="flex items-center">
                            <span className="text-blue-500 text-xl mr-3 font-bold">i</span>
                            <p className="text-sm text-blue-700 dark:text-blue-300">
                              Execute EXPLAIN PIPELINE on a query to see the 3D visualization.
                              Switch to Text view to enter and execute a query.
                            </p>
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            )}
            
            {activeTab === 'spans' && (
              <div className="p-4">
                {hasOpenTelemetry || probingOtel ? (
                <SpansViewer
                  spans={openTelemetrySpans}
                  isLoading={isLoadingSpans}
                  error={null}
                />
                ) : (
                  <div style={{ textAlign: 'center', fontSize: 12, color: 'var(--text-muted)', padding: 32 }}>
                    OpenTelemetry requires system.opentelemetry_span_log (not available on this server)
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      ) : (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-8 text-center">
          <div className="text-2xl mb-4 font-light text-gray-400">?</div>
          <div className="text-lg font-medium text-gray-700 dark:text-gray-300 mb-2">
            No Query Selected
          </div>
          <div className="text-sm text-gray-500 dark:text-gray-400 max-w-md mx-auto">
            Enter a query ID above to view its trace logs and execution plan,
            or select a query from the Query Monitor page.
          </div>
        </div>
      )}
    </div>
  );
};

export default QueryTracer;
