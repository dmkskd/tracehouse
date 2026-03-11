import React, { useState, useEffect, useMemo } from 'react';
import { getDataSourceSrv, type DataSourceInstanceSettings } from '@grafana/runtime';
import { Select, Alert } from '@grafana/ui';

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

  const options = useMemo(
    () => datasources.map(ds => ({ label: ds.name, value: ds.uid })),
    [datasources],
  );

  if (isLoading) {
    return null;
  }

  if (datasources.length === 0) {
    return (
      <Alert title="No ClickHouse Datasources Found" severity="error">
        Please configure a <code>grafana-clickhouse-datasource</code> first.
        Go to Configuration → Data Sources → Add data source → ClickHouse.
      </Alert>
    );
  }

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <label style={{ fontSize: 12, whiteSpace: 'nowrap', opacity: 0.6 }}>
        Datasource
      </label>
      <Select
        options={options}
        value={value}
        onChange={(v) => {
          const ds = datasources.find(d => d.uid === v.value);
          if (ds) {
            onChange(ds.uid, ds.name);
          }
        }}
        placeholder="Select datasource..."
        width={30}
      />
    </div>
  );
}
