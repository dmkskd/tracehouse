import React from 'react';
import { Select } from '@grafana/ui';
import { useClusterStore } from '@frontend/stores/clusterStore';

export function ClusterSelector() {
  const { clusterName, availableClusters, detected, switchCluster } = useClusterStore();

  // Don't show if detection hasn't run or there are no clusters
  if (!detected || availableClusters.length === 0) {
    return null;
  }

  const options = [
    ...availableClusters.map(c => ({
      label: c.name,
      value: c.name,
      description: `${c.replicaCount} replica${c.replicaCount !== 1 ? 's' : ''}${c.shardCount > 1 ? ` · ${c.shardCount} shards` : ''}`,
    })),
    { label: 'Single node', value: '__none__', description: 'No cluster (local queries only)' },
  ];

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <label style={{ fontSize: 12, whiteSpace: 'nowrap', opacity: 0.6 }}>
        Cluster
      </label>
      <Select
        options={options}
        value={clusterName ?? '__none__'}
        onChange={(v) => {
          switchCluster(v.value === '__none__' ? null : v.value ?? null);
        }}
        width={20}
      />
    </div>
  );
}
