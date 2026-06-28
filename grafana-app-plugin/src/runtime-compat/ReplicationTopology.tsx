import React from 'react';
import { useClickHouseServices, type ClickHouseServices } from '../ServiceProvider';
import { useConnectionStore } from '../stores/connectionStore';
import { useClusterStore } from '@frontend/stores/clusterStore';
import { createRuntimeComponent } from './runtimeComponent';

export interface ReplicationTopologyProps {
  database: string;
  table: string;
}

interface ReplicationTopologyRuntimeProps extends ReplicationTopologyProps {
  services: ClickHouseServices | null;
  isConnected: boolean;
  detected: unknown;
}

const RuntimeReplicationTopology = createRuntimeComponent<ReplicationTopologyRuntimeProps>(
  'replicationTopology',
  'ReplicationTopologyRuntime',
  'Loading replication topology...',
  500
);

export const ReplicationTopology: React.FC<ReplicationTopologyProps> = ({ database, table }) => {
  const services = useClickHouseServices();
  const { activeProfileId, profiles } = useConnectionStore();
  const { detected } = useClusterStore();
  const activeProfile = profiles.find((profile) => profile.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;

  return (
    <RuntimeReplicationTopology
      database={database}
      table={table}
      services={services}
      isConnected={isConnected}
      detected={detected}
    />
  );
};

export default ReplicationTopology;
