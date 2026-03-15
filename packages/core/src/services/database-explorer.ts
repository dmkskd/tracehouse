import type { IClickHouseAdapter } from '../adapters/types.js';
import type { DatabaseInfo, TableInfo, PartInfo, PartDetailInfo, ColumnSchema, PartColumnInfo, PartDataResponse, ChFunction } from '../types/database.js';
import type { PartLineage } from '../types/lineage.js';
import {
  LIST_DATABASES,
  LIST_TABLES,
  GET_TABLE_SCHEMA,
  GET_TABLE_PARTS,
  GET_PART_DETAIL,
  GET_PART_COLUMNS,
  GET_TABLE_KEYS,
  GET_PART_ROW_COUNT,
  GET_TABLE_COLUMN_NAMES,
  GET_PART_DATA,
} from '../queries/database-queries.js';
import { buildQuery, tagQuery } from '../queries/builder.js';
import { TAB_DATABASES, sourceTag } from '../queries/source-tags.js';
import {
  mapDatabaseInfo,
  mapTableInfo,
  mapPartInfo,
  mapPartDetailInfo,
  mapColumnSchema,
  mapPartColumnInfo,
} from '../mappers/database-mappers.js';
import { buildLineageTree } from '../lineage/builder.js';

export class DatabaseExplorerError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'DatabaseExplorerError';
  }
}

export class DatabaseExplorer {
  constructor(private adapter: IClickHouseAdapter) {}

  async listDatabases(): Promise<DatabaseInfo[]> {
    try {
      const rows = await this.adapter.executeQuery(tagQuery(LIST_DATABASES, sourceTag(TAB_DATABASES, 'listDatabases')));
      return rows.map(mapDatabaseInfo);
    } catch (error) {
      throw new DatabaseExplorerError('Failed to list databases', error as Error);
    }
  }

  async listTables(database: string): Promise<TableInfo[]> {
    const sql = buildQuery(LIST_TABLES, { database });
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_DATABASES, 'listTables')));
      return rows.map(mapTableInfo);
    } catch (error) {
      throw new DatabaseExplorerError('Failed to list tables', error as Error);
    }
  }

  async getTableSchema(database: string, table: string): Promise<ColumnSchema[]> {
    const sql = buildQuery(GET_TABLE_SCHEMA, { database, table });
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_DATABASES, 'tableSchema')));
      return rows.map(mapColumnSchema);
    } catch (error) {
      throw new DatabaseExplorerError('Failed to get table schema', error as Error);
    }
  }

  async getTableParts(database: string, table: string): Promise<PartInfo[]> {
    const sql = buildQuery(GET_TABLE_PARTS, { database, table });
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_DATABASES, 'tableParts')));
      return rows.map(mapPartInfo);
    } catch (error) {
      throw new DatabaseExplorerError('Failed to get table parts', error as Error);
    }
  }

  async getPartDetail(database: string, table: string, partName: string): Promise<PartDetailInfo | null> {
    const detailSql = tagQuery(buildQuery(GET_PART_DETAIL, { database, table, part_name: partName }), sourceTag(TAB_DATABASES, 'partDetail'));
    const columnsSql = tagQuery(buildQuery(GET_PART_COLUMNS, { database, table, part_name: partName }), sourceTag(TAB_DATABASES, 'partColumns'));
    const keysSql = tagQuery(buildQuery(GET_TABLE_KEYS, { database, table }), sourceTag(TAB_DATABASES, 'tableKeys'));

    try {
      const [detailRows, columnRows, keyRows] = await Promise.all([
        this.adapter.executeQuery(detailSql),
        this.adapter.executeQuery(columnsSql),
        this.adapter.executeQuery(keysSql),
      ]);

      if (detailRows.length === 0) return null;

      const keys = (keyRows[0] ?? {}) as Record<string, unknown>;
      const partitionKey = String(keys.partition_key ?? '');
      const sortingKey = String(keys.sorting_key ?? '');
      const primaryKey = String(keys.primary_key ?? '');

      // Remap query column aliases to what mapPartColumnInfo expects:
      //   query returns: column, compressed, uncompressed
      //   mapper expects: column_name, compressed_bytes, uncompressed_bytes
      const columns = columnRows.map(r => {
        const raw = r as Record<string, unknown>;
        const compressed = Number(raw.compressed ?? raw.compressed_bytes ?? 0);
        const uncompressed = Number(raw.uncompressed ?? raw.uncompressed_bytes ?? 0);
        const remapped: Record<string, unknown> = {
          ...raw,
          column_name: raw.column ?? raw.column_name,
          compressed_bytes: compressed,
          uncompressed_bytes: uncompressed,
          // Calculate compression_ratio: uncompressed / compressed
          compression_ratio: compressed > 0 ? uncompressed / compressed : 0,
        };
        const col = mapPartColumnInfo(remapped);
        return {
          ...col,
          is_in_partition_key: partitionKey.includes(col.column_name),
          is_in_sorting_key: sortingKey.includes(col.column_name),
          is_in_primary_key: primaryKey.includes(col.column_name),
        };
      });

      const detail = detailRows[0] as Record<string, unknown>;
      // Remap 'marks' to 'marks_count' if needed, and compute compression_ratio
      const remappedDetail: Record<string, unknown> = {
        ...detail,
        marks_count: detail.marks ?? detail.marks_count,
        compression_ratio:
          detail.compression_ratio ??
          (Number(detail.data_compressed_bytes) > 0
            ? Number(detail.data_uncompressed_bytes) / Number(detail.data_compressed_bytes)
            : 0),
        partition_key: keys.partition_key,
        sorting_key: keys.sorting_key,
        primary_key: keys.primary_key,
      };

      return mapPartDetailInfo(remappedDetail, columns);
    } catch (error) {
      throw new DatabaseExplorerError('Failed to get part detail', error as Error);
    }
  }

  async getPartLineage(database: string, table: string, partName: string): Promise<PartLineage> {
    try {
      return await buildLineageTree(this.adapter, database, table, partName);
    } catch (error) {
      throw new DatabaseExplorerError('Failed to get part lineage', error as Error);
    }
  }

  async getPartColumns(database: string, table: string, partName: string): Promise<PartColumnInfo[]> {
    const columnsSql = tagQuery(buildQuery(GET_PART_COLUMNS, { database, table, part_name: partName }), sourceTag(TAB_DATABASES, 'partColumns'));
    const keysSql = tagQuery(buildQuery(GET_TABLE_KEYS, { database, table }), sourceTag(TAB_DATABASES, 'tableKeys'));

    try {
      const [columnRows, keyRows] = await Promise.all([
        this.adapter.executeQuery(columnsSql),
        this.adapter.executeQuery(keysSql),
      ]);

      const keys = (keyRows[0] ?? {}) as Record<string, unknown>;
      const partitionKey = String(keys.partition_key ?? '');
      const sortingKey = String(keys.sorting_key ?? '');
      const primaryKey = String(keys.primary_key ?? '');

      return columnRows.map(r => {
        const raw = r as Record<string, unknown>;
        const compressed = Number(raw.compressed ?? raw.compressed_bytes ?? 0);
        const uncompressed = Number(raw.uncompressed ?? raw.uncompressed_bytes ?? 0);
        const remapped: Record<string, unknown> = {
          ...raw,
          column_name: raw.column ?? raw.column_name,
          compressed_bytes: compressed,
          uncompressed_bytes: uncompressed,
          // Calculate compression_ratio: uncompressed / compressed
          compression_ratio: compressed > 0 ? uncompressed / compressed : 0,
        };
        const col = mapPartColumnInfo(remapped);
        return {
          ...col,
          is_in_partition_key: partitionKey.includes(col.column_name),
          is_in_sorting_key: sortingKey.includes(col.column_name),
          is_in_primary_key: primaryKey.includes(col.column_name),
        };
      });
    } catch (error) {
      throw new DatabaseExplorerError('Failed to get part columns', error as Error);
    }
  }

  async getFunctions(): Promise<ChFunction[]> {
    try {
      const rows = await this.adapter.executeQuery(tagQuery(
        'SELECT name, is_aggregate, case_insensitive, alias_to, create_query, origin, description FROM system.functions ORDER BY name',
        sourceTag(TAB_DATABASES, 'getFunctions'),
      ));
      return rows as unknown as ChFunction[];
    } catch (error) {
      throw new DatabaseExplorerError('Failed to get functions', error as Error);
    }
  }

  async getPartData(database: string, table: string, partName: string, limit = 100): Promise<PartDataResponse> {
    try {
      // Build queries with proper backtick escaping for table names
      const countSql = tagQuery(`
        SELECT count() AS cnt
        FROM \`${database}\`.\`${table}\`
        WHERE _part = '${partName.replace(/'/g, "\\'")}'
      `, sourceTag(TAB_DATABASES, 'partDataCount'));
      const countRows = await this.adapter.executeQuery(countSql);
      const totalRows = Number((countRows[0] as Record<string, unknown>)?.cnt ?? 0);

      // Get column names
      const columnsSql = tagQuery(buildQuery(GET_TABLE_COLUMN_NAMES, { database, table }), sourceTag(TAB_DATABASES, 'columnNames'));
      const columnRows = await this.adapter.executeQuery(columnsSql);
      const columns = columnRows.map(r => String((r as Record<string, unknown>).name ?? ''));

      if (columns.length === 0) {
        return { columns: [], rows: [], total_rows_in_part: 0, returned_rows: 0 };
      }

      // Get sample data from the part
      const dataSql = tagQuery(`
        SELECT *
        FROM \`${database}\`.\`${table}\`
        WHERE _part = '${partName.replace(/'/g, "\\'")}'
        LIMIT ${limit}
      `, sourceTag(TAB_DATABASES, 'partDataSample'));
      const dataRows = await this.adapter.executeQuery(dataSql);

      // Convert rows to array format
      const rows: unknown[][] = dataRows.map(row => {
        const r = row as Record<string, unknown>;
        return columns.map(col => {
          const val = r[col];
          // Serialize special types
          if (val === null || val === undefined) return null;
          if (val instanceof Date) return val.toISOString();
          if (typeof val === 'object') return JSON.stringify(val);
          return val;
        });
      });

      return {
        columns,
        rows,
        total_rows_in_part: totalRows,
        returned_rows: rows.length,
      };
    } catch (error) {
      throw new DatabaseExplorerError('Failed to get part data', error as Error);
    }
  }
}
