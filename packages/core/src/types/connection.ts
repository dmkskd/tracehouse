export interface ConnectionConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  secure: boolean;
  connect_timeout: number;
  send_receive_timeout: number;
  /**
   * When true and the host looks like a ClickHouse Cloud endpoint
   * (*.clickhouse.cloud), the adapter rewrites the hostname to use
   * the sticky routing subdomain pattern:
   *   `<session>.sticky.<original-host>`
   *
   * This tells the Envoy LB to pin all requests to the same replica,
   * preventing system table views from "flipping" between polls.
   *
   * Requires replica-aware routing to be enabled on the CH Cloud service.
   * @see https://clickhouse.com/docs/en/manage/replica-aware-routing
   */
  useCloudStickyRouting?: boolean;
}

export interface ConnectionProfile {
  id: string;
  name: string;
  config: ConnectionConfig;
  created_at: string;
  updated_at: string;
  last_connected_at: string | null;
}

export interface ConnectionTestResult {
  success: boolean;
  server_version?: string;
  server_timezone?: string;
  server_display_name?: string;
  error_message?: string;
  error_type?: string;
  latency_ms?: number;
}
