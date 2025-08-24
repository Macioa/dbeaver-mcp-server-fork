import { spawn, ChildProcess } from 'child_process';
import {
  DBeaverConnection,
  QueryResult,
  SchemaInfo,
  ExportOptions,
  ConnectionTest,
  DatabaseStats
} from './types.js';

export class DBeaverClient {
  private timeout: number;
  private debug: boolean;

  constructor(executablePath?: string, timeout: number = 30000, debug: boolean = false) {
    this.timeout = timeout;
    this.debug = debug;
  }

  async executeQuery(connection: DBeaverConnection, query: string, password?: string): Promise<QueryResult> {
    try {
      const args = this.buildPsqlArgs(connection, query);
      return await this.executePsql(args, password);
    } catch (error) {
      throw new Error(`Failed to execute query: ${error}`);
    }
  }

  async executeWriteQuery(connection: DBeaverConnection, query: string, password?: string): Promise<QueryResult> {
    try {
      const args = this.buildPsqlArgs(connection, query);
      return await this.executePsqlWrite(args, password);
    } catch (error) {
      throw new Error(`Failed to execute write query: ${error}`);
    }
  }

  private buildPsqlArgs(connection: DBeaverConnection, query: string): string[] {
    const args: string[] = [];

    // Add host if specified
    if (connection.host) {
      args.push('-h', connection.host);
    }

    // Add port if specified
    if (connection.port) {
      args.push('-p', connection.port.toString());
    }

    // Add username if specified
    if (connection.user) {
      args.push('-U', connection.user);
    }

    // Add database if specified
    if (connection.database) {
      args.push('-d', connection.database);
    }

    // Add query
    args.push('-c', query);

    return args;
  }

  private async executePsql(args: string[], password?: string): Promise<QueryResult> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Query execution timed out'));
      }, this.timeout);

      const proc = spawn('psql', args, {
        stdio: ['pipe', 'pipe', 'pipe'],
        env: {
          ...process.env,
          PGPASSWORD: password || process.env.PGPASSWORD || ''
        }
      });

      let stdout = '';
      let stderr = '';

      proc.stdout.on('data', (data) => {
        stdout += data.toString();
      });

      proc.stderr.on('data', (data) => {
        stderr += data.toString();
      });

      proc.on('close', (code) => {
        clearTimeout(timeout);

        if (code !== 0) {
          reject(new Error(`psql failed with code ${code}: ${stderr}`));
          return;
        }

        try {
          const result = this.parsePsqlOutput(stdout);
          resolve(result);
        } catch (error) {
          reject(new Error(`Failed to parse psql output: ${error}`));
        }
      });

      proc.on('error', (error) => {
        clearTimeout(timeout);
        reject(new Error(`Failed to execute psql: ${error.message}`));
      });
    });
  }

  private async executePsqlWrite(args: string[], password?: string): Promise<QueryResult> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Write query execution timed out'));
      }, this.timeout);

      const proc = spawn('psql', args, {
        stdio: ['pipe', 'pipe', 'pipe'],
        env: {
          ...process.env,
          PGPASSWORD: password || process.env.PGPASSWORD || ''
        }
      });

      let stdout = '';
      let stderr = '';

      proc.stdout.on('data', (data) => {
        stdout += data.toString();
      });

      proc.stderr.on('data', (data) => {
        stderr += data.toString();
      });

      proc.on('close', (code) => {
        clearTimeout(timeout);

        if (code !== 0) {
          reject(new Error(`psql failed with code ${code}: ${stderr}`));
          return;
        }

        try {
          const result = this.parsePsqlWriteOutput(stdout);
          resolve(result);
        } catch (error) {
          reject(new Error(`Failed to parse psql write output: ${error}`));
        }
      });

      proc.on('error', (error) => {
        clearTimeout(timeout);
        reject(new Error(`Failed to execute psql: ${error.message}`));
      });
    });
  }

  private parsePsqlOutput(output: string): QueryResult {
    const lines = output.trim().split('\n');

    if (lines.length === 0) {
      return { columns: [], rows: [], rowCount: 0, executionTime: 0 };
    }

    // First line contains column headers
    const headerLine = lines[0];
    const columns = headerLine.split('|').map(col => col.trim()).filter(col => col.length > 0);

    // Parse data rows (skip header and separator lines)
    const rows: any[][] = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (line.length === 0 || line.match(/^[+-]+$/)) {
        continue; // Skip empty lines and separator lines
      }

      const values = line.split('|').map(val => val.trim());
      if (values.length === columns.length) {
        rows.push(values);
      }
    }

    return {
      columns,
      rows,
      rowCount: rows.length,
      executionTime: 0
    };
  }

  private parsePsqlWriteOutput(output: string): QueryResult {
    const lines = output.trim().split('\n');
    let rowCount = 0;
    let executionTime = 0;

    for (const line of lines) {
      if (line.includes('Time:')) {
        const timeMatch = line.match(/Time: (\d+) ms/);
        if (timeMatch) {
          executionTime = parseInt(timeMatch[1]);
        }
      } else if (line.includes('Rows affected:')) {
        const rowsMatch = line.match(/Rows affected: (\d+)/);
        if (rowsMatch) {
          rowCount = parseInt(rowsMatch[1]);
        }
      }
    }

    return {
      columns: [], // No columns for write queries
      rows: [],
      rowCount: rowCount,
      executionTime: executionTime
    };
  }

  async testConnection(connection: DBeaverConnection, password?: string): Promise<ConnectionTest> {
    const startTime = Date.now();

    try {
      // Simple test query to check connectivity
      const testQuery = 'SELECT version();';
      const result = await this.executeQuery(connection, testQuery, password);

      return {
        connectionId: connection.id,
        success: true,
        responseTime: Date.now() - startTime,
        databaseVersion: this.extractVersionFromResult(result)
      };
    } catch (error) {
      return {
        connectionId: connection.id,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        responseTime: Date.now() - startTime
      };
    }
  }

  async getTableSchema(connection: DBeaverConnection, tableName: string, password?: string): Promise<SchemaInfo> {
    try {
      // Query to get table schema information with properly qualified column names
      const schemaQuery = `
        SELECT
          col.column_name,
          col.data_type,
          col.is_nullable,
          col.column_default,
          CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
          CASE WHEN col.column_default LIKE 'nextval%' THEN true ELSE false END as is_auto_increment,
          col.character_maximum_length,
          col.numeric_precision,
          col.numeric_scale,
          col.ordinal_position
        FROM information_schema.columns col
        LEFT JOIN (
          SELECT kcu.column_name
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
          WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = '${tableName}'
        ) pk ON col.column_name = pk.column_name
        WHERE col.table_name = '${tableName}'
        ORDER BY col.ordinal_position;
      `;

      const result = await this.executeQuery(connection, schemaQuery, password);

      // Parse the schema result
      const columns = result.rows.map((row: any[]) => ({
        name: row[0],
        type: row[1],
        nullable: row[2] === 'YES',
        defaultValue: row[3] || undefined,
        isPrimaryKey: row[4] === true,
        isAutoIncrement: row[5] === true,
        length: row[6] ? parseInt(row[6]) : undefined,
        precision: row[7] ? parseInt(row[7]) : undefined,
        scale: row[8] ? parseInt(row[8]) : undefined,
        position: parseInt(row[9]) || 0
      }));

      // Get index information with more details
      const indexQuery = `
        SELECT
          i.relname as index_name,
          array_to_string(array_agg(a.attname), ', ') as column_names,
          ix.indisunique as is_unique,
          ix.indisprimary as is_primary,
          am.amname as index_type,
          pg_get_indexdef(ix.indexrelid) as index_definition
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON ix.indexrelid = i.oid
        JOIN pg_am am ON i.relam = am.oid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE t.relname = '${tableName}'
        GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname, ix.indexrelid
        ORDER BY i.relname;
      `;

      const indexResult = await this.executeQuery(connection, indexQuery);
      const indexes = indexResult.rows.map((row: any[]) => ({
        name: row[0],
        columns: row[1].split(', ').filter((col: string) => col.length > 0),
        unique: row[2] === true,
        isPrimary: row[3] === true,
        type: row[4] || 'btree',
        definition: row[5]
      }));

      // Get constraint information with more details
      const constraintQuery = `
        SELECT
          con.conname as constraint_name,
          con.contype as constraint_type,
          pg_get_constraintdef(con.oid) as definition,
          CASE
            WHEN con.contype = 'f' THEN (
              SELECT relname FROM pg_class WHERE oid = con.confrelid
            )
            ELSE NULL
          END as referenced_table,
          CASE
            WHEN con.contype = 'f' THEN (
              SELECT array_to_string(array_agg(att.attname), ', ')
              FROM pg_attribute att
              WHERE att.attrelid = con.confrelid AND att.attnum = ANY(con.confkey)
            )
            ELSE NULL
          END as referenced_columns
        FROM pg_constraint con
        WHERE con.conrelid = '${tableName}'::regclass
        ORDER BY con.conname;
      `;

      const constraintResult = await this.executeQuery(connection, constraintQuery);
      const constraints = constraintResult.rows.map((row: any[]) => ({
        name: row[0],
        type: this.mapConstraintType(row[1]),
        definition: row[2],
        columns: this.extractConstraintColumns(row[2]),
        referencedTable: row[3] || undefined,
        referencedColumns: row[4] ? row[4].split(', ') : undefined
      }));

      // Get table statistics
      const statsQuery = `
        SELECT
          pg_size_pretty(pg_total_relation_size('${tableName}'::regclass)) as total_size,
          pg_size_pretty(pg_relation_size('${tableName}'::regclass)) as table_size,
          pg_size_pretty(pg_total_relation_size('${tableName}'::regclass) - pg_relation_size('${tableName}'::regclass)) as index_size,
          (SELECT count(*) FROM "${tableName}") as row_count
      `;

      let tableStats: any = undefined;
      try {
        const statsResult = await this.executeQuery(connection, statsQuery);
        if (statsResult.rows.length > 0) {
          const stats = statsResult.rows[0];
          tableStats = {
            totalSize: stats[0],
            tableSize: stats[1],
            indexSize: stats[2],
            rowCount: parseInt(stats[3]) || 0
          };
        }
      } catch (error) {
        // Stats query might fail for some tables, ignore
        tableStats = undefined;
      }

      return {
        tableName,
        columns,
        indexes,
        constraints,
        statistics: tableStats
      };
    } catch (error) {
      throw new Error(`Failed to get table schema: ${error}`);
    }
  }

  private extractIndexColumns(indexDef: string): string[] {
    // Simple parsing of index definition to extract column names
    const match = indexDef.match(/\(([^)]+)\)/);
    if (match) {
      return match[1].split(',').map(col => col.trim().replace(/"/g, ''));
    }
    return [];
  }

  private extractConstraintColumns(constraintDef: string): string[] {
    // Simple parsing of constraint definition to extract column names
    const match = constraintDef.match(/\(([^)]+)\)/);
    if (match) {
      return match[1].split(',').map(col => col.trim().replace(/"/g, ''));
    }
    return [];
  }

  private mapConstraintType(pgType: string): 'PRIMARY_KEY' | 'FOREIGN_KEY' | 'UNIQUE' | 'CHECK' {
    switch (pgType) {
      case 'p': return 'PRIMARY_KEY';
      case 'f': return 'FOREIGN_KEY';
      case 'u': return 'UNIQUE';
      case 'c': return 'CHECK';
      default: return 'CHECK';
    }
  }

  async exportData(
    connection: DBeaverConnection,
    query: string,
    options: ExportOptions,
    password?: string
  ): Promise<string> {
    try {
      // Execute the query to get data
      const result = await this.executeQuery(connection, query, password);
      
      // Format the output based on requested format
      if (options.format === 'csv') {
        return this.formatAsCSV(result.columns, result.rows, options.includeHeaders);
      } else if (options.format === 'json') {
        return this.formatAsJSON(result.columns, result.rows);
      } else {
        throw new Error(`Unsupported export format: ${options.format}`);
      }
    } catch (error) {
      throw new Error(`Export failed: ${error}`);
    }
  }

  private formatAsCSV(columns: string[], rows: any[][], includeHeaders: boolean = true): string {
    let csv = '';

    if (includeHeaders) {
      csv += columns.map(col => `"${col}"`).join(',') + '\n';
    }

    for (const row of rows) {
      csv += row.map(cell => {
        if (cell === null || cell === undefined) {
          return '';
        }
        const cellStr = String(cell);
        if (cellStr.includes(',') || cellStr.includes('"') || cellStr.includes('\n')) {
          return `"${cellStr.replace(/"/g, '""')}"`;
        }
        return cellStr;
      }).join(',') + '\n';
    }

    return csv;
  }

  private formatAsJSON(columns: string[], rows: any[][]): string {
    const jsonData = rows.map(row => {
      const obj: any = {};
      columns.forEach((col, idx) => {
        obj[col] = row[idx];
      });
      return obj;
    });

    return JSON.stringify(jsonData, null, 2);
  }

  async getDatabaseStats(connection: DBeaverConnection): Promise<DatabaseStats> {
    try {
      // Get database statistics using PostgreSQL system queries
      const statsQuery = `
        SELECT
          (SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public') as table_count,
          (SELECT pg_size_pretty(pg_database_size(current_database()))) as total_size,
          (SELECT version()) as server_version,
          (SELECT extract(epoch from now() - pg_postmaster_start_time())::int) as uptime_seconds
      `;

      const result = await this.executeQuery(connection, statsQuery);

      if (result.rows.length === 0) {
        throw new Error('Failed to retrieve database statistics');
      }

      const row = result.rows[0];

      return {
        connectionId: connection.id,
        tableCount: parseInt(row[0]) || 0,
        totalSize: row[1] || '0',
        connectionTime: 0, // This would need to be tracked separately
        serverVersion: row[2] || '',
        uptime: this.formatUptime(parseInt(row[3]) || 0)
      };
    } catch (error) {
      throw new Error(`Failed to get database stats: ${error}`);
    }
  }

  private formatUptime(seconds: number): string {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);

    if (days > 0) {
      return `${days}d ${hours}h ${minutes}m`;
    } else if (hours > 0) {
      return `${hours}h ${minutes}m`;
    } else {
      return `${minutes}m`;
    }
  }

  private extractVersionFromResult(result: QueryResult): string | undefined {
    if (result.rows.length > 0 && result.columns.includes('version')) {
      const versionIndex = result.columns.indexOf('version');
      return result.rows[0][versionIndex];
    }
    return undefined;
  }

  async listTables(connection: DBeaverConnection, schema?: string, includeViews: boolean = false, password?: string): Promise<any[]> {
    try {
      let query = `
        SELECT
          t.table_schema,
          t.table_name as name,
          t.table_type as type,
          pg_size_pretty(pg_total_relation_size(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))) as size,
          (SELECT count(*) FROM information_schema.columns WHERE table_schema = t.table_schema AND table_name = t.table_name) as column_count
        FROM information_schema.tables t
        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
      `;

      if (schema) {
        query += ` AND t.table_schema = '${schema}'`;
      }

      if (!includeViews) {
        query += ` AND t.table_type = 'BASE TABLE'`;
      }

      query += ` ORDER BY t.table_schema, t.table_name`;

      const result = await this.executeQuery(connection, query, password);

      return result.rows.map(row => ({
        schema: row[0],
        name: row[1],
        type: row[2],
        size: row[3],
        columnCount: parseInt(row[4]) || 0
      }));
    } catch (error) {
      throw new Error(`Failed to list tables: ${error}`);
    }
  }
}