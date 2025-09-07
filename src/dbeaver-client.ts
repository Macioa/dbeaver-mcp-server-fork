import { spawn, ChildProcess } from 'child_process';
import {
  DBeaverConnection,
  QueryResult,
  SchemaInfo,
  ExportOptions,
  ConnectionTest,
  DatabaseStats,
  ColumnInfo,
  IndexInfo,
  ConstraintInfo,
  TableStatistics
} from './types.js';

export class DBeaverClient {
  private timeout: number;
  private debug: boolean;

  constructor(executablePath?: string, timeout: number = 30000, debug: boolean = false) {
    this.timeout = timeout;
    this.debug = debug;
  }

  async executeQuery(connection: DBeaverConnection, query: string, password?: string, username?: string): Promise<QueryResult> {
    try {
      const args = this.buildPsqlArgs(connection, query, username);
      return await this.executePsql(args, password);
    } catch (error) {
      throw new Error(`Failed to execute query: ${error}`);
    }
  }

  async executeWriteQuery(connection: DBeaverConnection, query: string, password?: string, username?: string): Promise<QueryResult> {
    try {
      const args = this.buildPsqlArgs(connection, query, username);
      return await this.executePsqlWrite(args, password);
    } catch (error) {
      throw new Error(`Failed to execute write query: ${error}`);
    }
  }

  private buildPsqlArgs(connection: DBeaverConnection, query: string, username?: string): string[] {
    if (this.debug) {
      console.error(`[DEBUG] Building psql args for connection:`, {
        id: connection.id,
        name: connection.name,
        host: connection.host,
        port: connection.port,
        user: connection.user,
        database: connection.database,
        driver: connection.driver
      });
    }

    const args: string[] = [];

    // Add host if specified
    if (connection.host) {
      args.push('-h', connection.host);
    }

    // Add port if specified
    if (connection.port) {
      args.push('-p', connection.port.toString());
    }

    // Add username - use provided username, connection.user, or fallback to environment variable
    let finalUsername = username || connection.user;
    if (!finalUsername) {
      // Try to get username from environment variables using the {db_name}_DB_USER pattern
      const dbName = connection.name.toUpperCase().replace(/[^A-Z0-9]/g, '_');
      const usernameVarName = `${dbName}_DB_USER`;
      finalUsername = process.env[usernameVarName] || 'postgres'; // Default to postgres
    }
    
    if (finalUsername) {
      args.push('-U', finalUsername);
    }

    // Add database if specified
    if (connection.database) {
      args.push('-d', connection.database);
    }

    // Add query
    args.push('-c', query);
    
    // CRITICAL: Force psql to exit after command completion
    args.push('--no-psqlrc'); // Don't read ~/.psqlrc (prevents interactive mode)
    args.push('--no-align'); // Disable aligned output for easier parsing
    args.push('--tuples-only'); // Output tuples only, no headers/footers
    args.push('--field-separator=|'); // Use | as field separator

    if (this.debug) {
      console.error(`[DEBUG] Final psql args: ${JSON.stringify(args)}`);
    }

    return args;
  }

  private async executePsql(args: string[], password?: string): Promise<QueryResult> {
    if (this.debug) {
      console.error(`[DEBUG] Executing psql with args: ${JSON.stringify(args)}`);
      console.error(`[DEBUG] Password provided: ${password ? 'YES' : 'NO'}`);
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        if (this.debug) {
          console.error(`[DEBUG] Query timeout reached (${this.timeout}ms), killing psql process ${proc.pid}`);
        }
        proc.kill('SIGKILL');
        reject(new Error('Query execution timed out'));
      }, this.timeout);

      const proc = spawn('psql', args, {
        stdio: ['pipe', 'pipe', 'pipe'],
        env: {
          ...process.env,
          PGPASSWORD: password || process.env.PGPASSWORD || '',
          // Force psql to exit after command completion
          PGCONNECT_TIMEOUT: '10', // 10 second connection timeout
          PGOPTIONS: '-c statement_timeout=30000' // 30 second statement timeout
        },
        // Add additional safety options
        detached: false, // Don't detach from parent process
        windowsHide: true // Hide console window on Windows
      });

      // Add additional safety timeout for connection establishment
      const connectionTimeout = setTimeout(() => {
        if (proc && !proc.connected) {
          console.error(`[DEBUG] Connection timeout reached, killing psql process ${proc.pid}`);
          proc.kill('SIGKILL');
        }
      }, 15000); // 15 second connection timeout

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
        clearTimeout(connectionTimeout); // Clear the connection timeout

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
        clearTimeout(connectionTimeout); // Clear the connection timeout
        reject(new Error(`Failed to execute psql: ${error.message}`));
      });
    });
  }

  private async executePsqlWrite(args: string[], password?: string): Promise<QueryResult> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        if (this.debug) {
          console.error(`[DEBUG] Write query timeout reached (${this.timeout}ms), killing psql process ${proc.pid}`);
        }
        proc.kill('SIGKILL');
        reject(new Error('Write query execution timed out'));
      }, this.timeout);

      const proc = spawn('psql', args, {
        stdio: ['pipe', 'pipe', 'pipe'],
        env: {
          ...process.env,
          PGPASSWORD: password || process.env.PGPASSWORD || '',
          // Force psql to exit after command completion
          PGCONNECT_TIMEOUT: '10', // 10 second connection timeout
          PGOPTIONS: '-c statement_timeout=30000' // 30 second statement timeout
        },
        // Add additional safety options
        detached: false, // Don't detach from parent process
        windowsHide: true // Hide console window on Windows
      });

      // Add additional safety timeout for connection establishment
      const connectionTimeout = setTimeout(() => {
        if (proc && !proc.connected) {
          console.error(`[DEBUG] Connection timeout reached, killing psql process ${proc.pid}`);
          proc.kill('SIGKILL');
        }
      }, 15000); // 15 second connection timeout

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
        clearTimeout(connectionTimeout); // Clear the connection timeout

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
        clearTimeout(connectionTimeout); // Clear the connection timeout
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

  async testConnection(connection: DBeaverConnection, password?: string, username?: string): Promise<ConnectionTest> {
    const startTime = Date.now();
    
    try {
      // Use a simple query to test the connection
      const testQuery = 'SELECT 1 as test, version() as version';
      const result = await this.executeQuery(connection, testQuery, password);
      
      const responseTime = Date.now() - startTime;
      const databaseVersion = this.extractVersionFromResult(result);
      
      return {
        connectionId: connection.id,
        success: true,
        responseTime,
        databaseVersion
      };
    } catch (error) {
      const responseTime = Date.now() - startTime;
      return {
        connectionId: connection.id,
        success: false,
        error: error instanceof Error ? error.message : String(error),
        responseTime
      };
    }
  }

  async getTableSchema(connection: DBeaverConnection, tableName: string, password?: string, username?: string): Promise<SchemaInfo> {
    try {
      // Get table columns
      const columnsQuery = `
        SELECT
          c.column_name as name,
          c.data_type as type,
          c.is_nullable = 'YES' as nullable,
          c.column_default as default_value,
          CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
          CASE WHEN c.column_default LIKE 'nextval%' THEN true ELSE false END as is_auto_increment,
          c.character_maximum_length as length,
          c.numeric_precision as precision,
          c.numeric_scale as scale
        FROM information_schema.columns c
        LEFT JOIN (
          SELECT kcu.column_name
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
          WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = $1
        ) pk ON c.column_name = pk.column_name
        WHERE c.table_name = $1
        ORDER BY c.ordinal_position
      `;

      // Get table indexes
      const indexesQuery = `
        SELECT
          i.relname as name,
          array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns,
          ix.indisunique as unique,
          am.amname as type
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        JOIN pg_am am ON am.oid = i.relam
        WHERE t.relname = $1
        GROUP BY i.relname, ix.indisunique, am.amname
        ORDER BY i.relname
      `;

      // Get table constraints
      const constraintsQuery = `
        SELECT
          tc.constraint_name as name,
          tc.constraint_type as type,
          array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns,
          ccu.table_name as referenced_table,
          array_agg(ccu.column_name ORDER BY kcu.ordinal_position) as referenced_columns
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        LEFT JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
        WHERE tc.table_name = $1
        GROUP BY tc.constraint_name, tc.constraint_type, ccu.table_name
        ORDER BY tc.constraint_name
      `;

      // Get table statistics
      const statsQuery = `
        SELECT
          pg_size_pretty(pg_total_relation_size(quote_ident($1))) as total_size,
          pg_size_pretty(pg_relation_size(quote_ident($1))) as table_size,
          pg_size_pretty(pg_total_relation_size(quote_ident($1)) - pg_relation_size(quote_ident($1))) as index_size,
          (SELECT reltuples FROM pg_class WHERE relname = $1) as row_count
      `;

      // Execute queries in parallel for better performance
      const [columnsResult, indexesResult, constraintsResult, statsResult] = await Promise.all([
        this.executeQuery(connection, columnsQuery.replace(/\$1/g, `'${tableName}'`), password),
        this.executeQuery(connection, indexesQuery.replace(/\$1/g, `'${tableName}'`), password),
        this.executeQuery(connection, constraintsQuery.replace(/\$1/g, `'${tableName}'`), password),
        this.executeQuery(connection, statsQuery.replace(/\$1/g, `'${tableName}'`), password)
      ]);

      // Parse columns
      const columns: ColumnInfo[] = columnsResult.rows.map(row => ({
        name: row[0],
        type: row[1],
        nullable: row[2] === 'true',
        defaultValue: row[3] || undefined,
        isPrimaryKey: row[4] === 'true',
        isAutoIncrement: row[5] === 'true',
        length: row[6] ? parseInt(row[6]) : undefined,
        precision: row[7] ? parseInt(row[7]) : undefined,
        scale: row[8] ? parseInt(row[8]) : undefined
      }));

      // Parse indexes
      const indexes: IndexInfo[] = indexesResult.rows.map(row => ({
        name: row[0],
        columns: row[1] ? row[1].split(',') : [],
        unique: row[2] === 'true',
        type: row[3] || 'btree'
      }));

      // Parse constraints
      const constraints: ConstraintInfo[] = constraintsResult.rows.map(row => ({
        name: row[0],
        type: this.mapConstraintType(row[1]),
        columns: row[2] ? row[2].split(',') : [],
        referencedTable: row[3] || undefined,
        referencedColumns: row[4] ? row[4].split(',') : undefined
      }));

      // Parse statistics
      let statistics: TableStatistics | undefined;
      if (statsResult.rows.length > 0) {
        const statsRow = statsResult.rows[0];
        statistics = {
          totalSize: statsRow[0] || '0',
          tableSize: statsRow[1] || '0',
          indexSize: statsRow[2] || '0',
          rowCount: parseInt(statsRow[3]) || 0
        };
      }

      return {
        tableName,
        columns,
        indexes,
        constraints,
        statistics
      };
    } catch (error) {
      throw new Error(`Failed to get table schema: ${error}`);
    }
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

  async getDatabaseStats(connection: DBeaverConnection, password?: string, username?: string): Promise<DatabaseStats> {
    try {
      // Get database statistics using PostgreSQL system queries
      const statsQuery = `
        SELECT
          (SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public') as table_count,
          (SELECT pg_size_pretty(pg_database_size(current_database()))) as total_size,
          (SELECT version()) as server_version,
          (SELECT extract(epoch from now() - pg_postmaster_start_time())::int) as uptime_seconds
      `;

      const result = await this.executeQuery(connection, statsQuery, password);

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

  async listTables(connection: DBeaverConnection, schema?: string, includeViews: boolean = false, password?: string, username?: string): Promise<any[]> {
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