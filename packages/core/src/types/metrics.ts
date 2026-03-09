export interface ServerMetrics {
  timestamp: string;
  cpu_usage: number;
  memory_used: number;
  memory_total: number;
  disk_read_bytes: number;
  disk_write_bytes: number;
  uptime_seconds: number;
}

export interface HistoricalMetricsPoint {
  timestamp: number; // Unix timestamp in ms
  cpu_usage: number;
  memory_used: number;
  memory_total: number;
  disk_read_rate: number;  // bytes per second
  disk_write_rate: number; // bytes per second
  network_send_rate?: number; // bytes per second
  network_recv_rate?: number; // bytes per second
}

export interface ClusterHistoricalMetricsPoint extends HistoricalMetricsPoint {
  hostname: string;
}

export interface ThresholdConfig {
  cpu_warning: number;
  memory_warning: number;
  query_duration_warning: number;
  parts_count_warning: number;
}
