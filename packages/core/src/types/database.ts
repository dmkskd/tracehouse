export interface DatabaseInfo {
  name: string;
  engine: string;
  table_count: number;
  total_bytes: number;
}

export interface TableInfo {
  database: string;
  name: string;
  engine: string;
  total_rows: number;
  total_bytes: number;
  partition_key: string | null;
  sorting_key: string | null;
  is_merge_tree: boolean;
}

export interface ColumnSchema {
  name: string;
  type: string;
  default_kind: string;
  default_expression: string;
  comment: string;
  is_in_partition_key: boolean;
  is_in_sorting_key: boolean;
  is_in_primary_key: boolean;
  is_in_sampling_key: boolean;
}

export interface PartInfo {
  partition_id: string;
  name: string;
  rows: number;
  bytes_on_disk: number;
  modification_time: string;
  level: number;
  primary_key_bytes_in_memory: number;
}

export interface PartColumnInfo {
  column_name: string;
  type: string;
  compressed_bytes: number;
  uncompressed_bytes: number;
  compression_ratio: number;
  codec: string;
  is_in_partition_key: boolean;
  is_in_sorting_key: boolean;
  is_in_primary_key: boolean;
}

export interface PartDetailInfo {
  partition_id: string;
  name: string;
  rows: number;
  bytes_on_disk: number;
  modification_time: string;
  level: number;
  min_block_number: number;
  max_block_number: number;
  marks_count: number;
  marks_bytes: number;
  data_compressed_bytes: number;
  data_uncompressed_bytes: number;
  compression_ratio: number;
  primary_key_bytes_in_memory: number;
  min_date: string;
  max_date: string;
  disk_name: string;
  path: string;
  part_type: string;
  partition_key: string;
  sorting_key: string;
  primary_key: string;
  columns: PartColumnInfo[];
}


export interface PartDataResponse {
  columns: string[];
  rows: unknown[][];
  total_rows_in_part: number;
  returned_rows: number;
}
