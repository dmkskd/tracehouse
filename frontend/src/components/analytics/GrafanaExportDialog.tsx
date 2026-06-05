import React, { useMemo } from 'react';
import type { ChartType } from './metaLanguage';
import type { GrafanaExportCapability } from '@tracehouse/core/services/grafana-export';

export interface GrafanaDashboardOption {
  uid: string;
  title: string;
  folderTitle?: string;
}

export interface GrafanaPanelOption {
  id: number;
  title: string;
}

export interface GrafanaExportOptions {
  dashboardMode: 'existing' | 'new';
  dashboardUid: string;
  dashboardTitle: string;
  panelMode: 'new' | 'replace';
  panelId?: number;
  panelTitle: string;
  width: number;
  height: number;
  maxRows: number;
}

export interface GrafanaPanelSummary {
  type: string;
  visual: string;
  data: string;
  layout: string;
  capabilities: GrafanaExportCapability[];
}

interface GrafanaExportDialogProps {
  options: GrafanaExportOptions;
  dashboards: GrafanaDashboardOption[];
  panels: GrafanaPanelOption[];
  error: string | null;
  isLoadingTargets: boolean;
  panelSummary: GrafanaPanelSummary | null;
  jsonPreview: string;
  showJsonPreview: boolean;
  viewMode: 'table' | 'chart' | 'queries';
  chartType: ChartType;
  onOptionsChange: React.Dispatch<React.SetStateAction<GrafanaExportOptions | null>>;
  onJsonPreviewToggle: React.Dispatch<React.SetStateAction<boolean>>;
  onClose: () => void;
  onExport: (options: GrafanaExportOptions) => void;
}

function capabilityLabel(capability: GrafanaExportCapability): string {
  if (capability.decision === 'hide') return 'Not exported';
  if (capability.level === 'supported') return 'Mapped';
  if (capability.level === 'partial') return 'Partial';
  return 'Unsupported';
}

function capabilityColor(capability: GrafanaExportCapability): string {
  if (capability.level === 'supported') return 'var(--accent-green)';
  if (capability.level === 'partial') return '#d29922';
  return '#f85149';
}

export const GrafanaExportDialog: React.FC<GrafanaExportDialogProps> = ({
  options,
  dashboards,
  panels,
  error,
  isLoadingTargets,
  panelSummary,
  jsonPreview,
  showJsonPreview,
  viewMode,
  chartType,
  onOptionsChange,
  onJsonPreviewToggle,
  onClose,
  onExport,
}) => {
  const fieldStyle = useMemo<React.CSSProperties>(() => ({
    width: '100%',
    padding: '8px 10px',
    background: 'var(--bg-input, var(--bg-tertiary))',
    color: 'var(--text-primary)',
    border: '1px solid var(--border-input, var(--border-primary))',
    borderRadius: 6,
    fontSize: 12,
    boxSizing: 'border-box',
  }), []);
  const maxRowsDisabled = viewMode !== 'chart' || ['line', 'area', 'grouped_line'].includes(chartType);
  const exportDisabled = options.dashboardMode === 'existing' && !options.dashboardUid;

  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 100000, background: 'rgba(0,0,0,0.78)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24 }}>
      <div style={{ width: 520, maxWidth: '100%', background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)', borderRadius: 8, boxShadow: '0 18px 70px rgba(0,0,0,0.65)', overflow: 'hidden' }}>
        <div style={{ padding: '14px 16px', borderBottom: '1px solid var(--border-primary)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div>
            <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' }}>Export to Grafana</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 3 }}>Choose destination, panel name, layout, and dense chart limits.</div>
          </div>
          <button onClick={onClose}
            style={{ background: 'transparent', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', fontSize: 18, lineHeight: 1 }}>×</button>
        </div>

        <div style={{ padding: 16, display: 'grid', gap: 12 }}>
          <label style={{ display: 'grid', gap: 6 }}>
            <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Destination dashboard</span>
            <select
              value={options.dashboardMode === 'new' ? '__new__' : options.dashboardUid}
              onChange={e => {
                const value = e.target.value;
                onOptionsChange(prev => prev && (value === '__new__'
                  ? { ...prev, dashboardMode: 'new', dashboardUid: '', panelMode: 'new', panelId: undefined }
                  : { ...prev, dashboardMode: 'existing', dashboardUid: value, panelMode: 'new', panelId: undefined }));
              }}
              style={fieldStyle}
            >
              {dashboards.map(d => (
                <option key={d.uid} value={d.uid}>{d.folderTitle ? `${d.folderTitle} / ` : ''}{d.title}</option>
              ))}
              <option value="__new__">Create new dashboard</option>
            </select>
          </label>

          {options.dashboardMode === 'new' && (
            <label style={{ display: 'grid', gap: 6 }}>
              <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' }}>New dashboard title</span>
              <input
                value={options.dashboardTitle}
                onChange={e => onOptionsChange(prev => prev && ({ ...prev, dashboardTitle: e.target.value }))}
                style={fieldStyle}
              />
            </label>
          )}

          {options.dashboardMode === 'existing' && (
            <label style={{ display: 'grid', gap: 6 }}>
              <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Destination panel</span>
              <select
                value={options.panelMode === 'replace' && options.panelId != null ? String(options.panelId) : '__new__'}
                onChange={e => {
                  const value = e.target.value;
                  const selected = panels.find(p => String(p.id) === value);
                  onOptionsChange(prev => prev && (value === '__new__'
                    ? { ...prev, panelMode: 'new', panelId: undefined }
                    : { ...prev, panelMode: 'replace', panelId: Number(value), panelTitle: selected?.title?.replace(/\s+·\s+gexp-v\d+$/, '') || prev.panelTitle }));
                }}
                style={fieldStyle}
              >
                <option value="__new__">Create new panel</option>
                {panels.map(p => <option key={p.id} value={p.id}>Replace: {p.title}</option>)}
              </select>
            </label>
          )}

          <label style={{ display: 'grid', gap: 6 }}>
            <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Panel title</span>
            <input
              value={options.panelTitle}
              onChange={e => onOptionsChange(prev => prev && ({ ...prev, panelTitle: e.target.value }))}
              style={fieldStyle}
            />
          </label>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 10 }}>
            <label style={{ display: 'grid', gap: 6 }}>
              <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Width (grid cols)</span>
              <input type="number" min={4} max={24} value={options.width}
                onChange={e => onOptionsChange(prev => prev && ({ ...prev, width: Math.max(4, Math.min(24, Number(e.target.value) || 18)) }))}
                title="Grafana dashboard width in a 24-column grid. 24 is full width."
                style={fieldStyle} />
            </label>
            <label style={{ display: 'grid', gap: 6 }}>
              <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Height (grid rows)</span>
              <input type="number" min={4} max={36} value={options.height}
                onChange={e => onOptionsChange(prev => prev && ({ ...prev, height: Math.max(4, Math.min(36, Number(e.target.value) || 10)) }))}
                title="Grafana dashboard height in grid rows, not pixels."
                style={fieldStyle} />
            </label>
            <label style={{ display: 'grid', gap: 6 }}>
              <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Max rows</span>
              <input type="number" min={0} max={200} value={options.maxRows}
                onChange={e => onOptionsChange(prev => prev && ({ ...prev, maxRows: Math.max(0, Math.min(200, Number(e.target.value) || 0)) }))}
                disabled={maxRowsDisabled}
                style={{ ...fieldStyle, opacity: maxRowsDisabled ? 0.45 : 1 }} />
            </label>
          </div>

          {(error || isLoadingTargets) && (
            <div style={{ fontSize: 11, color: error ? '#f85149' : 'var(--text-muted)' }}>
              {error ? `Grafana export issue: ${error}. Importable JSON was copied if export failed.` : 'Loading Grafana dashboards…'}
            </div>
          )}

          {panelSummary && (
            <div style={{ display: 'grid', gap: 8, padding: 12, border: '1px solid var(--border-primary)', borderRadius: 6, background: 'var(--bg-tertiary)' }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.04em' }}>Grafana panel preview</div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 8 }}>
                {[
                  ['Panel', panelSummary.type],
                  ['Visual', panelSummary.visual],
                  ['Data', panelSummary.data],
                  ['Layout', panelSummary.layout],
                ].map(([label, value]) => (
                  <div key={label} style={{ minWidth: 0 }}>
                    <div style={{ fontSize: 10, color: 'var(--text-tertiary)', textTransform: 'uppercase', fontWeight: 600 }}>{label}</div>
                    <div style={{ marginTop: 2, fontSize: 12, color: 'var(--text-secondary)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }} title={value}>{value}</div>
                  </div>
                ))}
              </div>
              {panelSummary.capabilities.length > 0 && (
                <div style={{ display: 'grid', gap: 6, borderTop: '1px solid var(--border-primary)', paddingTop: 8 }}>
                  {panelSummary.capabilities.map((cap, i) => (
                    <div key={`${cap.tracehouseFeature}-${i}`} style={{ display: 'grid', gap: 2 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, minWidth: 0 }}>
                        <span style={{ fontSize: 10, fontWeight: 700, color: capabilityColor(cap), textTransform: 'uppercase', flexShrink: 0 }}>{capabilityLabel(cap)}</span>
                        <span style={{ fontSize: 11, color: 'var(--text-secondary)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }} title={cap.tracehouseFeature}>
                          {cap.tracehouseFeature}
                        </span>
                        {cap.grafanaFeature && (
                          <span style={{ fontSize: 10, color: 'var(--text-tertiary)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }} title={cap.grafanaFeature}>
                            → {cap.grafanaFeature}
                          </span>
                        )}
                      </div>
                      {cap.level !== 'supported' && (
                        <div style={{ fontSize: 10, color: 'var(--text-muted)', lineHeight: 1.35 }}>{cap.message}</div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          <div style={{ borderTop: '1px solid var(--border-primary)', paddingTop: 12 }}>
            <button
              onClick={() => onJsonPreviewToggle(v => !v)}
              style={{ width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, padding: '8px 10px', borderRadius: 6, border: '1px solid var(--border-primary)', background: 'var(--bg-tertiary)', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: 12, fontWeight: 600 }}
            >
              <span>{showJsonPreview ? 'Hide generated panel JSON' : 'Show generated panel JSON'}</span>
              <span style={{ color: 'var(--text-muted)', fontSize: 14, lineHeight: 1 }}>{showJsonPreview ? '⌃' : '⌄'}</span>
            </button>
            {showJsonPreview && (
              <textarea
                readOnly
                value={jsonPreview}
                style={{ marginTop: 8, width: '100%', height: 180, resize: 'vertical', fontFamily: "'Share Tech Mono','Fira Code',monospace", fontSize: 10, lineHeight: 1.5, background: 'var(--bg-code, var(--bg-primary))', color: 'var(--text-secondary)', border: '1px solid var(--border-primary)', borderRadius: 6, padding: 10 }}
              />
            )}
          </div>
        </div>

        <div style={{ padding: '12px 16px', borderTop: '1px solid var(--border-primary)', display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
          <button onClick={onClose}
            style={{ padding: '7px 14px', fontSize: 12, borderRadius: 6, border: '1px solid var(--border-primary)', background: 'var(--bg-secondary)', color: 'var(--text-secondary)', cursor: 'pointer' }}>
            Cancel
          </button>
          <button onClick={() => onExport(options)}
            disabled={exportDisabled}
            style={{ padding: '7px 14px', fontSize: 12, borderRadius: 6, border: '1px solid var(--border-primary)', background: 'var(--bg-primary)', color: 'var(--text-primary)', cursor: exportDisabled ? 'not-allowed' : 'pointer', opacity: exportDisabled ? 0.5 : 1, fontWeight: 600 }}>
            Export
          </button>
        </div>
      </div>
    </div>
  );
};
