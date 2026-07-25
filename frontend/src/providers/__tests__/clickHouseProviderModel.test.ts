import { describe, expect, it } from 'vitest';
import type { ConnectionConfig } from '@tracehouse/core';
import { connectionServiceIdentity } from '../clickHouseProviderModel';

const CONFIG: ConnectionConfig = {
  host: 'localhost',
  port: 8123,
  user: 'default',
  password: '',
  database: 'default',
  secure: false,
  connect_timeout: 10,
  send_receive_timeout: 30,
};

describe('connectionServiceIdentity', () => {
  it('changes when a connection timeout changes', () => {
    expect(connectionServiceIdentity('local', CONFIG)).not.toBe(
      connectionServiceIdentity('local', {
        ...CONFIG,
        send_receive_timeout: 120,
      }),
    );
    expect(connectionServiceIdentity('local', CONFIG)).not.toBe(
      connectionServiceIdentity('local', {
        ...CONFIG,
        connect_timeout: 20,
      }),
    );
  });

  it('returns no identity without an active config', () => {
    expect(connectionServiceIdentity(null, null)).toBeNull();
  });
});
