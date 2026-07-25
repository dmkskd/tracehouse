import React, { useMemo, useState } from 'react';
import {
  buildExecutionFlowNodes,
  parseExecutionAnalysis,
  parseMergeTreeReadDetails,
  parseOperatorDetailFields,
  type ExecutionAnalysisNode,
  type SelectionRatio,
} from './executionAnalysisParser';

interface ExecutionAnalysisVisualProps {
  output: string;
}

const OPERATOR_COLORS: Record<string, string> = {
  Expression: '#8b5cf6',
  Limit: '#6366f1',
  Sorting: '#f97316',
  Aggregating: '#06b6d4',
  Filter: '#f59e0b',
  ReadFromMergeTree: '#10b981',
  ReadFromRemote: '#14b8a6',
  Join: '#ef4444',
  Union: '#84cc16',
  Distinct: '#ec4899',
  Window: '#2dd4bf',
};

const OPERATOR_DESCRIPTIONS: Record<string, string> = {
  Expression: 'expression and projection',
  Limit: 'row limit',
  Sorting: 'ORDER BY',
  Aggregating: 'GROUP BY and aggregates',
  Filter: 'row filter',
  ReadFromMergeTree: 'MergeTree read',
  ReadFromRemote: 'remote read',
  Join: 'join',
  Union: 'stream union',
  Distinct: 'deduplication',
  Window: 'window functions',
};

const MONO = "'Share Tech Mono','Fira Code',ui-monospace,monospace";
const PLAN_GRID = 'minmax(310px, 1.6fr) minmax(130px, .75fr) minmax(130px, .75fr) minmax(170px, .9fr) minmax(105px, .55fr)';

function operatorColor(name: string): string {
  return OPERATOR_COLORS[name] ?? '#94a3b8';
}

function description(node: ExecutionAnalysisNode): string {
  return node.description ?? OPERATOR_DESCRIPTIONS[node.name] ?? '';
}

function SummaryMetric({
  label,
  value,
  detail,
}: {
  label: string;
  value?: string;
  detail?: string;
}) {
  return (
    <div style={{
      minWidth: 0,
      padding: '10px 14px',
      borderLeft: '1px solid var(--border-secondary)',
    }}>
      <div style={{
        color: 'var(--text-muted)',
        fontSize: 8,
        fontWeight: 600,
        letterSpacing: '0.9px',
        textTransform: 'uppercase',
      }}>
        {label}
      </div>
      <div
        title={value}
        style={{
          marginTop: 4,
          overflow: 'hidden',
          color: value ? 'var(--text-primary)' : 'var(--text-muted)',
          fontFamily: MONO,
          fontSize: 12,
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
      >
        {value ?? '—'}
      </div>
      {detail && (
        <div
          title={detail}
          style={{
            marginTop: 2,
            overflow: 'hidden',
            color: 'var(--text-muted)',
            fontFamily: MONO,
            fontSize: 8,
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {detail}
        </div>
      )}
    </div>
  );
}

function TimingCell({ node }: { node: ExecutionAnalysisNode }) {
  if (node.timings.length === 0) {
    return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  }

  const color = operatorColor(node.name);
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
      {node.timings.map((timing, index) => (
        <div key={`${timing.label ?? 'operator'}-${index}`} title={`${timing.duration} · ${timing.share.toFixed(1)}% of execution`}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
            <span style={{
              minWidth: 0,
              overflow: 'hidden',
              color: 'var(--text-muted)',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}>
              {timing.label ?? 'operator'}
            </span>
            <span style={{ flexShrink: 0, color: 'var(--text-secondary)' }}>
              {timing.duration}
              <span style={{ marginLeft: 5, color: 'var(--text-muted)' }}>
                {timing.share.toFixed(1)}%
              </span>
            </span>
          </div>
          <div style={{
            height: 2,
            marginTop: 3,
            overflow: 'hidden',
            background: 'var(--bg-tertiary)',
          }}>
            <div style={{
              width: `${Math.max(1, Math.min(100, timing.share))}%`,
              height: '100%',
              background: color,
            }} />
          </div>
        </div>
      ))}
    </div>
  );
}

function ParallelismCell({ node }: { node: ExecutionAnalysisNode }) {
  if (node.timings.length === 0) {
    return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
      {node.timings.map((timing, index) => (
        <div key={`${timing.label ?? 'operator'}-${index}`}>
          <span style={{ color: 'var(--text-secondary)' }}>
            {timing.parallelism.toFixed(2)}×
          </span>
          <span style={{ color: 'var(--text-muted)' }}> / {timing.maxParallelism}</span>
          {timing.processors && (
            <div
              title={`min ${timing.processors.min} · median ${timing.processors.median} · max ${timing.processors.max} · sum ${timing.processors.sum}`}
              style={{ marginTop: 2, color: 'var(--text-muted)', fontSize: 8 }}
            >
              {timing.processors.count} proc.
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

function SelectionIndicator({ ratio }: { ratio?: SelectionRatio }) {
  if (!ratio || ratio.total === 0) {
    return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  }

  const selectedPct = (ratio.selected / ratio.total) * 100;
  const prunedPct = Math.max(0, 100 - selectedPct);
  return (
    <div title={`${ratio.selected} of ${ratio.total} selected · ${prunedPct.toFixed(1)}% pruned`}>
      <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 7 }}>
        <span style={{ color: 'var(--text-secondary)' }}>
          {ratio.selected}/{ratio.total}
        </span>
        <span style={{
          color: prunedPct > 0 ? 'var(--accent-green)' : 'var(--text-muted)',
          fontSize: 8,
        }}>
          {prunedPct.toFixed(1)}% pruned
        </span>
      </div>
      <div style={{
        height: 3,
        marginTop: 4,
        overflow: 'hidden',
        borderRadius: 2,
        background: 'rgba(var(--color-success-rgb), 0.14)',
      }}>
        <div style={{
          width: `${Math.max(1, Math.min(100, selectedPct))}%`,
          height: '100%',
          background: prunedPct > 0 ? 'var(--color-warning)' : 'var(--text-muted)',
        }} />
      </div>
    </div>
  );
}

function OperatorDetailLines({
  lines,
  compact = false,
}: {
  lines: string[];
  compact?: boolean;
}) {
  const fields = useMemo(() => parseOperatorDetailFields(lines), [lines]);
  return (
    <div style={{
      overflow: 'hidden',
      border: compact ? '1px solid var(--border-secondary)' : 'none',
      borderRadius: compact ? 4 : 0,
      background: compact ? 'var(--bg-card)' : 'transparent',
      fontFamily: MONO,
    }}>
      {fields.map((field, index) => {
        const hasValue = field.value !== undefined;
        return (
          <div
            key={`${field.raw}-${index}`}
            style={{
              display: hasValue ? 'grid' : 'block',
              gridTemplateColumns: 'minmax(78px, 138px) minmax(0, 1fr)',
              gap: '6px 12px',
              padding: compact ? '4px 7px' : '3px 0',
              borderTop: index > 0 ? '1px solid var(--border-secondary)' : 'none',
              lineHeight: 1.45,
            }}
          >
            {field.label ? (
              <>
                <span style={{
                  color: hasValue ? 'var(--text-tertiary)' : 'var(--text-secondary)',
                  fontSize: hasValue ? 8 : 9,
                  fontWeight: 650,
                  letterSpacing: hasValue ? '0.55px' : '0',
                  textTransform: hasValue ? 'uppercase' : 'none',
                }}>
                  {field.label}
                </span>
                {hasValue && (
                  <span style={{
                    minWidth: 0,
                    color: 'var(--text-secondary)',
                    fontSize: 9,
                    overflowWrap: 'anywhere',
                    whiteSpace: 'pre-wrap',
                  }}>
                    {field.value}
                  </span>
                )}
              </>
            ) : (
              <span style={{
                color: 'var(--text-secondary)',
                fontSize: 9,
                fontWeight: 550,
                overflowWrap: 'anywhere',
                whiteSpace: 'pre-wrap',
              }}>
                {field.raw}
              </span>
            )}
          </div>
        );
      })}
    </div>
  );
}

function MergeTreeReadDetails({ node }: { node: ExecutionAnalysisNode }) {
  const read = useMemo(() => parseMergeTreeReadDetails(node.details), [node.details]);
  const hasStructuredDetails = Boolean(
    read.readType
    || read.parts !== undefined
    || read.granules !== undefined
    || read.outputColumns.length
    || read.prewhere
    || read.indexes.length,
  );

  if (!hasStructuredDetails) {
    return <OperatorDetailLines lines={node.details} />;
  }

  return (
    <div style={{ fontFamily: MONO, fontSize: 9 }}>
      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: 0,
        border: '1px solid var(--border-secondary)',
        borderRadius: 5,
        background: 'var(--bg-card)',
      }}>
        {[
          ['Read type', read.readType],
          ['Parts read', read.parts],
          ['Granules read', read.granules],
          ['Ranges', read.ranges],
        ].map(([label, value], index) => (
          <div key={String(label)} style={{
            minWidth: 105,
            padding: '7px 10px',
            borderLeft: index > 0 ? '1px solid var(--border-secondary)' : 'none',
          }}>
            <div style={{ color: 'var(--text-muted)', fontSize: 8, textTransform: 'uppercase' }}>
              {label}
            </div>
            <div style={{ marginTop: 3, color: 'var(--text-secondary)' }}>
              {value ?? '—'}
            </div>
          </div>
        ))}
      </div>

      {read.outputColumns.length > 0 && (
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 10, marginTop: 9 }}>
          <div style={{
            width: 62,
            paddingTop: 3,
            flexShrink: 0,
            color: 'var(--text-muted)',
            fontSize: 8,
            textTransform: 'uppercase',
          }}>
            Columns
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
            {read.outputColumns.map(column => (
              <span key={column} style={{
                padding: '2px 6px',
                border: '1px solid var(--border-secondary)',
                borderRadius: 3,
                background: 'var(--bg-card)',
                color: 'var(--text-secondary)',
              }}>
                {column}
              </span>
            ))}
          </div>
        </div>
      )}

      {read.prewhere && (
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 10, marginTop: 8 }}>
          <div style={{
            width: 62,
            paddingTop: 3,
            flexShrink: 0,
            color: 'var(--color-warning)',
            fontSize: 8,
            textTransform: 'uppercase',
          }}>
            Prewhere
          </div>
          <code style={{
            padding: '3px 7px',
            border: '1px solid rgba(var(--color-warning-rgb), 0.18)',
            borderRadius: 3,
            background: 'rgba(var(--color-warning-rgb), 0.05)',
            color: 'var(--text-secondary)',
            lineHeight: 1.45,
            wordBreak: 'break-word',
          }}>
            {read.prewhere}
          </code>
        </div>
      )}

      {read.indexes.length > 0 && (
        <div style={{ marginTop: 11 }}>
          <div style={{
            marginBottom: 5,
            color: 'var(--text-muted)',
            fontSize: 8,
            letterSpacing: '0.7px',
            textTransform: 'uppercase',
          }}>
            Index pruning
          </div>
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'minmax(110px, .55fr) minmax(220px, 1.4fr) minmax(120px, .65fr) minmax(135px, .7fr)',
            minWidth: 690,
            overflow: 'hidden',
            border: '1px solid var(--border-secondary)',
            borderRadius: 5,
            background: 'var(--bg-card)',
          }}>
            {['Index', 'Condition', 'Parts', 'Granules'].map(heading => (
              <div key={heading} style={{
                padding: '5px 8px',
                borderBottom: '1px solid var(--border-secondary)',
                background: 'var(--bg-secondary)',
                color: 'var(--text-muted)',
                fontSize: 8,
                textTransform: 'uppercase',
              }}>
                {heading}
              </div>
            ))}
            {read.indexes.map((index, rowIndex) => {
              const borderBottom = rowIndex < read.indexes.length - 1
                ? '1px solid var(--border-secondary)'
                : 'none';
              const indexLabel = index.name ? `${index.type} · ${index.name}` : index.type;
              const condition = index.condition ?? '—';
              return (
                <React.Fragment key={`${index.type}-${index.name ?? rowIndex}`}>
                  <div style={{ padding: '7px 8px', borderBottom, color: 'var(--text-primary)' }}>
                    {indexLabel}
                    {index.keys.length > 0 && (
                      <div style={{ marginTop: 2, color: 'var(--text-muted)', fontSize: 8 }}>
                        {index.keys.join(', ')}
                      </div>
                    )}
                  </div>
                  <div
                    title={condition}
                    style={{
                      padding: '7px 8px',
                      overflow: 'hidden',
                      borderBottom,
                      color: condition === 'true' ? 'var(--text-muted)' : 'var(--text-secondary)',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {condition}
                  </div>
                  <div style={{ padding: '7px 8px', borderBottom }}>
                    <SelectionIndicator ratio={index.parts} />
                  </div>
                  <div style={{ padding: '7px 8px', borderBottom }}>
                    <SelectionIndicator ratio={index.granules} />
                  </div>
                </React.Fragment>
              );
            })}
          </div>
        </div>
      )}

      <details style={{ marginTop: 8 }}>
        <summary style={{ color: 'var(--text-secondary)', fontSize: 9, cursor: 'pointer' }}>
          Raw operator details
        </summary>
        <div style={{
          maxHeight: 220,
          marginTop: 7,
          overflow: 'auto',
        }}>
          <OperatorDetailLines lines={node.details} compact />
        </div>
      </details>
    </div>
  );
}

function PlanRow({ node }: { node: ExecutionAnalysisNode }) {
  const [expanded, setExpanded] = useState(false);
  const color = operatorColor(node.name);
  const canExpand = node.details.length > 0;

  const cellStyle: React.CSSProperties = {
    minWidth: 0,
    padding: '9px 12px',
    borderBottom: '1px solid var(--border-secondary)',
    color: 'var(--text-secondary)',
    fontFamily: MONO,
    fontSize: 9,
  };

  return (
    <>
      <div style={{
        ...cellStyle,
        position: 'relative',
        display: 'flex',
        alignItems: 'flex-start',
        paddingLeft: 12 + Math.min(node.depth * 18, 126),
      }}>
        {node.depth > 0 && (
          <>
            <span style={{
              position: 'absolute',
              top: 0,
              bottom: 0,
              left: 17 + Math.min((node.depth - 1) * 18, 108),
              width: 1,
              background: 'var(--border-secondary)',
            }} />
            <span style={{
              position: 'absolute',
              top: 17,
              left: 17 + Math.min((node.depth - 1) * 18, 108),
              width: 10,
              height: 1,
              background: 'var(--border-secondary)',
            }} />
          </>
        )}
        <button
          type="button"
          disabled={!canExpand}
          aria-expanded={canExpand ? expanded : undefined}
          onClick={() => canExpand && setExpanded(value => !value)}
          style={{
            display: 'flex',
            minWidth: 0,
            alignItems: 'flex-start',
            gap: 8,
            padding: 0,
            border: 0,
            background: 'transparent',
            color: 'inherit',
            font: 'inherit',
            textAlign: 'left',
            cursor: canExpand ? 'pointer' : 'default',
          }}
        >
          <span style={{
            width: 7,
            height: 7,
            marginTop: 3,
            flexShrink: 0,
            borderRadius: 2,
            background: color,
          }} />
          <span style={{ minWidth: 0 }}>
            <span style={{ display: 'flex', alignItems: 'baseline', gap: 7, minWidth: 0 }}>
              <span style={{ color: 'var(--text-primary)', fontWeight: 650 }}>
                {canExpand ? (expanded ? '▾ ' : '▸ ') : ''}{node.name}
              </span>
              {description(node) && (
                <span
                  title={description(node)}
                  style={{
                    overflow: 'hidden',
                    color: 'var(--text-muted)',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {description(node)}
                </span>
              )}
            </span>
          </span>
        </button>
      </div>

      <div style={{ ...cellStyle, textAlign: 'right' }}>
        {node.io ? (
          <>
            <span>{node.io.inputRows} → {node.io.outputRows}</span>
            {node.io.retainedRows && (
              <span style={{ marginLeft: 5, color: 'var(--text-muted)' }}>
                ({node.io.retainedRows})
              </span>
            )}
          </>
        ) : <span style={{ color: 'var(--text-muted)' }}>—</span>}
      </div>

      <div style={{ ...cellStyle, textAlign: 'right' }}>
        {node.io
          ? `${node.io.inputBytes} → ${node.io.outputBytes}`
          : <span style={{ color: 'var(--text-muted)' }}>—</span>}
      </div>

      <div style={cellStyle}>
        <TimingCell node={node} />
      </div>

      <div style={{ ...cellStyle, textAlign: 'right' }}>
        <ParallelismCell node={node} />
      </div>

      {expanded && (
        <div style={{
          gridColumn: '1 / -1',
          padding: `5px 12px 6px ${38 + Math.min(node.depth * 18, 126)}px`,
          borderBottom: '1px solid var(--border-secondary)',
          background: 'var(--bg-card)',
        }}>
          {node.name === 'ReadFromMergeTree' ? (
            <MergeTreeReadDetails node={node} />
          ) : (
            <OperatorDetailLines lines={node.details} />
          )}
        </div>
      )}
    </>
  );
}

export const ExecutionAnalysisVisual: React.FC<ExecutionAnalysisVisualProps> = ({ output }) => {
  const parsed = useMemo(() => parseExecutionAnalysis(output), [output]);
  const summary = parsed.summary;
  const visualNodes = useMemo(
    () => buildExecutionFlowNodes(parsed.nodes),
    [parsed.nodes],
  );

  return (
    <div style={{
      width: '100%',
      minHeight: '100%',
      boxSizing: 'border-box',
      padding: '16px 20px 32px',
      background: 'var(--bg-primary)',
    }}>
      <div style={{
        overflow: 'hidden',
        border: '1px solid var(--border-primary)',
        borderRadius: 6,
        background: 'var(--bg-card)',
      }}>
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(6, minmax(120px, 1fr))',
          minWidth: 780,
          marginLeft: -1,
        }}>
          <SummaryMetric label="Total" value={summary.totalTime} />
          <SummaryMetric label="Planning" value={summary.planningTime} />
          <SummaryMetric label="Execution" value={summary.executionTime} />
          <SummaryMetric
            label="Rows read"
            value={summary.readRows}
            detail={summary.rowsPerSecond ? `${summary.rowsPerSecond} rows/s` : undefined}
          />
          <SummaryMetric
            label="Data read"
            value={summary.readBytes}
            detail={summary.bytesPerSecond}
          />
          <SummaryMetric label="Peak memory" value={summary.peakMemory} />
        </div>
      </div>

      <div style={{
        display: 'flex',
        alignItems: 'baseline',
        justifyContent: 'space-between',
        gap: 12,
        margin: '18px 0 7px',
      }}>
        <div style={{
          color: 'var(--text-tertiary)',
          fontSize: 9,
          fontWeight: 650,
          letterSpacing: '1px',
          textTransform: 'uppercase',
        }}>
          Data flow
        </div>
        <div style={{ color: 'var(--text-muted)', fontFamily: MONO, fontSize: 8 }}>
          {visualNodes.length} operators · source → result
        </div>
      </div>

      {visualNodes.length > 0 ? (
        <div style={{
          overflow: 'auto',
          border: '1px solid var(--border-primary)',
          borderRadius: 6,
          background: 'var(--bg-card)',
        }}>
          <div style={{
            display: 'grid',
            gridTemplateColumns: PLAN_GRID,
            minWidth: 860,
          }}>
            {['Operator', 'Rows in → out', 'Bytes in → out', 'Time', 'Parallelism'].map((heading, index) => (
              <div key={heading} style={{
                padding: '7px 12px',
                borderBottom: '1px solid var(--border-primary)',
                background: 'var(--bg-secondary)',
                color: 'var(--text-muted)',
                fontSize: 8,
                fontWeight: 600,
                letterSpacing: '0.7px',
                textAlign: index > 0 && index < 3 ? 'right' : index === 4 ? 'right' : 'left',
                textTransform: 'uppercase',
              }}>
                {heading}
              </div>
            ))}
            {visualNodes.map(node => (
              <PlanRow key={node.id} node={node} />
            ))}
          </div>
        </div>
      ) : (
        <div role="status" style={{
          padding: '14px',
          border: '1px solid var(--border-primary)',
          borderRadius: 6,
          background: 'var(--bg-card)',
          color: 'var(--text-muted)',
          fontFamily: MONO,
          fontSize: 9,
        }}>
          Plan shape unavailable for this server output. See Raw plan.
        </div>
      )}

      {summary.output && (
        <div style={{
          marginTop: 10,
          padding: '7px 10px',
          border: '1px solid var(--border-secondary)',
          borderRadius: 5,
          background: 'var(--bg-code)',
          color: 'var(--text-muted)',
          fontFamily: MONO,
          fontSize: 8,
          lineHeight: 1.5,
          wordBreak: 'break-word',
        }}>
          <span style={{ color: 'var(--text-tertiary)' }}>Output </span>
          {summary.output}
        </div>
      )}
    </div>
  );
};
