import React, { useState, useEffect } from 'react';
import { getDataSourceSrv, type DataSourceInstanceSettings } from '@grafana/runtime';

interface DatasourceSelectorProps {
  value: string | null;
  onChange: (uid: string, name: string) => void;
}

export function DatasourceSelector({ value, onChange }: DatasourceSelectorProps) {
  const [datasources, setDatasources] = useState<DataSourceInstanceSettings[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    try {
      const list = getDataSourceSrv().getList({ type: 'grafana-clickhouse-datasource' });
      setDatasources(list);
    } catch (e) {
      console.error('Failed to get datasources:', e);
    } finally {
      setIsLoading(false);
    }
  }, []);

  if (isLoading) {
    return (
      <div style={{ padding: 16, color: 'rgba(255,255,255,0.5)', fontFamily: 'monospace', fontSize: 12 }}>
        Loading datasources...
      </div>
    );
  }

  if (datasources.length === 0) {
    return (
      <div style={{
        padding: 24,
        background: 'rgba(248,81,73,0.1)',
        border: '1px solid rgba(248,81,73,0.3)',
        borderRadius: 8,
        color: '#f85149',
        fontFamily: 'system-ui, sans-serif',
        fontSize: 14,
      }}>
        <div style={{ fontWeight: 600, marginBottom: 8 }}>No ClickHouse Datasources Found</div>
        <div style={{ color: 'rgba(255,255,255,0.7)', fontSize: 13 }}>
          Please configure a <code style={{ background: 'rgba(255,255,255,0.1)', padding: '2px 6px', borderRadius: 4 }}>grafana-clickhouse-datasource</code> first.
        </div>
        <div style={{ marginTop: 12, fontSize: 12, color: 'rgba(255,255,255,0.5)' }}>
          Go to Configuration → Data Sources → Add data source → ClickHouse
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <label style={{
        fontSize: 11,
        fontWeight: 500,
        color: 'rgba(255,255,255,0.5)',
        whiteSpace: 'nowrap',
      }}>
        Datasource:
      </label>
      <select
        value={value || ''}
        onChange={(e) => {
          const ds = datasources.find(d => d.uid === e.target.value);
          if (ds) {
            onChange(ds.uid, ds.name);
          }
        }}
        style={{
          padding: '6px 28px 6px 10px',
          borderRadius: 4,
          border: '1px solid rgba(255,255,255,0.15)',
          background: 'rgba(255,255,255,0.05)',
          color: 'white',
          fontSize: 12,
          fontFamily: 'system-ui, sans-serif',
          cursor: 'pointer',
          outline: 'none',
          appearance: 'none',
          backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 24 24' fill='none' stroke='rgba(255,255,255,0.5)' stroke-width='2'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E")`,
          backgroundRepeat: 'no-repeat',
          backgroundPosition: 'right 8px center',
          minWidth: 150,
        }}
      >
        <option value="" style={{ background: '#1a1a2e', color: 'rgba(255,255,255,0.5)' }}>
          Select datasource...
        </option>
        {datasources.map(ds => (
          <option key={ds.uid} value={ds.uid} style={{ background: '#1a1a2e', color: 'white' }}>
            {ds.name}
          </option>
        ))}
      </select>
    </div>
  );
}
