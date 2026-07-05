import React from 'react';
import type { ObjectStorageProfileSummary } from '@tracehouse/core';
import {
  buildObjectStorageViewModel,
  type ObjectStorageEventGroup,
  type ObjectStorageKpi,
  type ObjectStorageMetric,
  type ObjectStorageTimeView,
} from '../../../../utils/objectStorageViewModel';
import { useProfileEventDescriptionsStore } from '../../../../stores/profileEventDescriptionsStore';

interface ObjectStorageTabProps {
  summary: ObjectStorageProfileSummary;
}

const statusColor: Record<ObjectStorageKpi['status'], string> = {
  ok: '#3fb950',
  watch: '#d29922',
  high: '#f85149',
  neutral: '#58a6ff',
};

const cardStyle: React.CSSProperties = {
  background: 'var(--bg-card)',
  border: '1px solid var(--border-secondary)',
  borderRadius: 8,
  padding: 16,
};

const sectionTitleStyle: React.CSSProperties = {
  fontSize: 11,
  fontWeight: 600,
  color: 'var(--text-secondary)',
  textTransform: 'uppercase',
  letterSpacing: '1px',
};

const metricCellStyle: React.CSSProperties = {
  minWidth: 0,
  padding: '2px 0 4px',
};

export const ObjectStorageTab: React.FC<ObjectStorageTabProps> = ({ summary }) => {
  const descriptions = useProfileEventDescriptionsStore((state) => state.descriptions);
  const vm = buildObjectStorageViewModel(summary, descriptions);

  return (
    <div style={{ padding: 20, height: '100%', overflow: 'auto' }}>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <CostPanel vm={vm} />
        <EfficiencyPanel kpis={vm.efficiency} />
        {vm.time.map((time) => <TimePanel key={time.title} time={time} />)}
        {vm.reliability.length > 0 && <MetricPanel title="Retries & Errors" metrics={vm.reliability} />}
        {vm.context.length > 0 && <MetricPanel title="Iceberg Metadata" metrics={vm.context} />}
        <RawEvents groups={vm.rawEventGroups} />
      </div>
    </div>
  );
};

const CostPanel: React.FC<{ vm: ReturnType<typeof buildObjectStorageViewModel> }> = ({ vm }) => (
  <section style={cardStyle}>
    <div style={{ display: 'grid', gridTemplateColumns: '240px minmax(0, 1fr)', gap: 20, alignItems: 'start' }}>
      <div>
        <div title={vm.cost.note} style={{ ...sectionTitleStyle, cursor: 'help' }}>
          Cost Estimate <span style={{ color: 'var(--text-muted)' }}>*</span>
        </div>
        <div style={{ fontFamily: 'monospace', fontSize: 32, lineHeight: 1.1, color: 'var(--text-primary)', marginTop: 10 }}>
          {vm.cost.total}
        </div>
        <div
          title={vm.cost.pricingTooltip}
          style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 8, cursor: 'help', textDecoration: 'underline dotted', textUnderlineOffset: 3 }}
        >
          {vm.cost.pricingProfile}
        </div>
        <div style={{ height: 10, display: 'flex', overflow: 'hidden', borderRadius: 999, background: 'var(--bg-tertiary)', border: '1px solid var(--border-secondary)', margin: '14px 0 8px' }}>
          <div title={vm.cost.requestLegend} style={{ width: vm.cost.requestBarWidth, background: '#58a6ff' }} />
          <div title={vm.cost.transferLegend} style={{ width: vm.cost.transferBarWidth, background: '#3fb950' }} />
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, fontSize: 10, color: 'var(--text-muted)' }}>
          <span>{vm.cost.requestLegend}</span>
          <span>{vm.cost.transferLegend}</span>
        </div>
      </div>
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, marginBottom: 12 }}>
          <div style={sectionTitleStyle}>Cost Inputs</div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(210px, 250px))', gap: '14px 22px', justifyContent: 'start' }}>
          {vm.cost.lines.map((line) => (
            <div key={line.label} style={metricCellStyle}>
              <div title={line.label} style={{ fontFamily: 'monospace', fontSize: 11, color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {line.label}
              </div>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 6, marginTop: 3, minWidth: 0 }}>
                <span style={{ fontFamily: 'monospace', fontSize: 16, color: 'var(--text-primary)', minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{line.countOrBytes}</span>
                <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>→ {line.cost}</span>
              </div>
              <div title={line.detail} style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 3, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{line.detail}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  </section>
);

const EfficiencyPanel: React.FC<{ kpis: ObjectStorageKpi[] }> = ({ kpis }) => (
  <section style={cardStyle}>
    <div style={{ ...sectionTitleStyle, marginBottom: 10 }}>Request Metrics</div>
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 250px))', gap: '14px 22px', justifyContent: 'start' }}>
      {kpis.map((kpi) => (
        <div key={kpi.label} title={kpi.hint} style={metricCellStyle}>
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: 10, alignItems: 'center', marginBottom: 4 }}>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)', fontWeight: 600 }}>{kpi.label}</div>
            {kpi.status !== 'neutral' && (
              <span style={{ fontSize: 10, color: statusColor[kpi.status], background: 'var(--bg-tertiary)', borderRadius: 999, padding: '2px 7px' }}>{kpi.status}</span>
            )}
          </div>
          <div style={{ fontFamily: 'monospace', fontSize: 18, color: 'var(--text-primary)', marginBottom: 4 }}>{kpi.value}</div>
          <div style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-muted)', marginBottom: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{kpi.formula}</div>
        </div>
      ))}
    </div>
  </section>
);

const TimePanel: React.FC<{ time: ObjectStorageTimeView }> = ({ time }) => (
  <section style={cardStyle}>
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, alignItems: 'baseline', marginBottom: 14 }}>
      <div>
        <div style={sectionTitleStyle}>{time.title}</div>
        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>{time.subtitle}</div>
      </div>
      <div style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{time.total.label}: {time.total.value}</div>
    </div>
    <div style={{ display: 'grid', gridTemplateColumns: '260px minmax(0, 1fr) 90px', gap: 12, alignItems: 'center' }}>
      <div style={{ fontFamily: 'monospace', fontSize: 11, color: 'var(--text-primary)', fontWeight: 600 }}>{time.total.label}</div>
      <div style={{ height: 18, background: time.totalColor, borderRadius: 4, border: '1px solid var(--border-secondary)' }} />
      <div style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--text-primary)', textAlign: 'right' }}>{time.total.value}</div>
      {time.segments.map((segment) => (
        <React.Fragment key={segment.label}>
          <div title={segment.detail} style={{ fontFamily: 'monospace', fontSize: 11, color: 'var(--text-secondary)', paddingLeft: 20 }}>{segment.label}</div>
          <div style={{ height: 14, background: 'transparent', borderRadius: 4, overflow: 'hidden' }}>
            <div style={{ width: `${segment.widthPercent}%`, height: '100%', background: segment.color }} />
          </div>
          <div style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--text-primary)', textAlign: 'right' }}>{segment.value}</div>
        </React.Fragment>
      ))}
    </div>
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 18, marginTop: 14 }}>
      {time.segments.map((segment) => (
        <div key={segment.label} title={segment.detail} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: 'var(--text-secondary)' }}>
          <span style={{ width: 10, height: 10, borderRadius: 3, background: segment.color, border: '1px solid var(--border-secondary)' }} />
          <span>{segment.label}</span>
        </div>
      ))}
    </div>
  </section>
);

const MetricPanel: React.FC<{ title: string; metrics: ObjectStorageMetric[] }> = ({ title, metrics }) => (
  <section style={cardStyle}>
    <div style={{ ...sectionTitleStyle, marginBottom: 12 }}>{title}</div>
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: 14 }}>
      {metrics.map((metric) => <MetricRow key={metric.label} metric={metric} />)}
    </div>
  </section>
);

const MetricRow: React.FC<{ metric: ObjectStorageMetric }> = ({ metric }) => (
  <div style={{ minWidth: 0 }}>
    <div title={metric.label} style={{ fontFamily: 'monospace', fontSize: 11, color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{metric.label}</div>
    <div style={{ display: 'flex', alignItems: 'baseline', gap: 6, marginTop: 4, minWidth: 0 }}>
      <span style={{ fontFamily: 'monospace', fontSize: 16, color: 'var(--text-primary)' }}>{metric.value}</span>
      {metric.detail && <span style={{ fontSize: 10, color: 'var(--text-muted)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{metric.detail}</span>}
    </div>
  </div>
);

const RawEvents: React.FC<{ groups: ObjectStorageEventGroup[] }> = ({ groups }) => (
  <section style={cardStyle}>
    <details>
      <summary style={{ cursor: 'pointer', color: 'var(--text-secondary)', fontSize: 12, fontWeight: 600 }}>Raw ProfileEvents</summary>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 18, marginTop: 14 }}>
        {groups.map((group) => (
          <div key={group.title}>
            <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.8px', marginBottom: 8 }}>{group.title}</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 7 }}>
              {group.events.map((event) => (
                <div key={event.label} style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) auto', gap: 12 }}>
                  <div title={event.label} style={{ fontFamily: 'monospace', fontSize: 11, color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{event.label}</div>
                  <div style={{ fontFamily: 'monospace', fontSize: 11, color: 'var(--text-primary)' }}>{event.value}</div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </details>
  </section>
);
