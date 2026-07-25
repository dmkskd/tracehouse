import type { ConnectionConfig } from '@tracehouse/core';

/**
 * Identity of the live adapter configuration.
 *
 * Any field consumed while constructing an adapter must participate so saving
 * connection settings rebuilds the services that own that adapter.
 */
export function connectionServiceIdentity(
  profileId: string | null,
  config: ConnectionConfig | null,
  proxyUrl?: string | null,
): string | null {
  if (!config) return null;

  return JSON.stringify([
    profileId,
    config.host,
    config.port,
    config.user,
    config.password,
    config.database,
    config.secure,
    config.connect_timeout,
    config.send_receive_timeout,
    proxyUrl ?? 'direct',
  ]);
}
