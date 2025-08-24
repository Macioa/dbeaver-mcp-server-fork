import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { DBeaverConfigParser } from './config-parser.js';
import { DBeaverClient } from './dbeaver-client.js';
import { DBeaverConnection, QueryResult, ExportOptions, BusinessInsight, TableResource } from './types.js';
import { CallToolRequestSchema, ListResourcesRequestSchema, ListToolsRequestSchema, ReadResourceRequestSchema, Tool, McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { validateQuery, sanitizeConnectionId, formatError, convertToCSV } from './utils.js';
// CSV functionality will be handled in utils

class DBeaverMCPServer {
  private server: Server;
  private configParser: DBeaverConfigParser;
  private dbeaverClient: DBeaverClient;
  private debug: boolean;
  private insights: BusinessInsight[] = [];
  private insightsFile: string;

  constructor() {
    this.debug = process.env.DBEAVER_DEBUG === 'true';
    this.insightsFile = path.join(os.tmpdir(), 'dbeaver-mcp-insights.json');
    
    this.server = new Server(
      {
        name: 'dbeaver-mcp-server',
        version: '1.1.0',
      },
      {
        capabilities: {
          tools: {},
          resources: {},
        },
      }
    );

    this.configParser = new DBeaverConfigParser({
      debug: this.debug,
      timeout: parseInt(process.env.DBEAVER_TIMEOUT || '30000'),
      executablePath: process.env.DBEAVER_PATH
    });

    this.dbeaverClient = new DBeaverClient(
      process.env.DBEAVER_PATH,
      parseInt(process.env.DBEAVER_TIMEOUT || '30000'),
      this.debug
    );

    this.loadInsights();
    this.setupResourceHandlers();
    this.setupToolHandlers();
    this.setupErrorHandling();
  }

  private log(message: string, level: 'info' | 'error' | 'debug' = 'info') {
    if (level === 'debug' && !this.debug) return;
    
    const timestamp = new Date().toISOString();
    const prefix = `[${timestamp}] [${level.toUpperCase()}]`;
    
    if (level === 'error') {
      console.error(`${prefix} ${message}`);
    } else {
      console.error(`${prefix} ${message}`);
    }
  }

  private async executePsqlDirect(connection: DBeaverConnection, query: string, password?: string, timeout: number = 30000): Promise<QueryResult> {
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        reject(new Error('Query execution timed out'));
      }, timeout);

      const args = [];
      if (connection.host) args.push('-h', connection.host);
      if (connection.port) args.push('-p', connection.port.toString());
      if (connection.user) args.push('-U', connection.user);
      if (connection.database) args.push('-d', connection.database);
      args.push('-c', query);

      const proc = spawn('psql', args, {
        stdio: ['pipe', 'pipe', 'pipe'],
        env: {
          ...process.env,
          PGPASSWORD: password || process.env.PGPASSWORD || 'postgres'
        }
      });

      let stdout = '';
      let stderr = '';
      proc.stdout.on('data', (data) => { stdout += data.toString(); });
      proc.stderr.on('data', (data) => { stderr += data.toString(); });

      proc.on('close', (code) => {
        clearTimeout(timeoutId);
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
        clearTimeout(timeoutId);
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

  private loadInsights() {
    try {
      if (fs.existsSync(this.insightsFile)) {
        const data = fs.readFileSync(this.insightsFile, 'utf-8');
        this.insights = JSON.parse(data);
      }
    } catch (error) {
      this.log(`Failed to load insights: ${error}`, 'debug');
      this.insights = [];
    }
  }

  private saveInsights() {
    try {
      fs.writeFileSync(this.insightsFile, JSON.stringify(this.insights, null, 2));
    } catch (error) {
      this.log(`Failed to save insights: ${error}`, 'error');
    }
  }

  private setupErrorHandling() {
    process.on('uncaughtException', (error) => {
      this.log(`Uncaught exception: ${error.message}`, 'error');
      if (this.debug) {
        this.log(error.stack || '', 'debug');
      }
      process.exit(1);
    });

    process.on('unhandledRejection', (reason, promise) => {
      this.log(`Unhandled rejection at: ${promise}, reason: ${reason}`, 'error');
      if (this.debug) {
        this.log(String(reason), 'debug');
      }
    });
  }

  private setupResourceHandlers() {
    // List all available database resources (table schemas)
    this.server.setRequestHandler(ListResourcesRequestSchema, async () => {
      try {
        const connections = await this.configParser.parseConnections();
        const allResources: any[] = [];
        
        for (const connection of connections) {
          const resources: any[] = [];
          
          try {
            const tables = await this.dbeaverClient.listTables(connection);
            
            for (const table of tables) {
              resources.push({
                connectionId: connection.id,
                tableName: table.name || table.table_name,
                schema: table.schema || table.table_schema,
                uri: `table://${connection.id}/${table.schema || table.table_schema}/${table.name || table.table_name}`
              });
            }
          } catch (error) {
            this.log(`Failed to get tables for connection ${connection.id}: ${error}`, 'debug');
          }
          
          allResources.push(...resources);
        }

        return { resources: allResources };
      } catch (error) {
        this.log(`Failed to list resources: ${error}`, 'error');
        return { resources: [] };
      }
    });

    // Get schema information for a specific table
    this.server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
      try {
        const uri = new URL(request.params.uri);
        const pathParts = uri.pathname.split('/').filter(p => p);
        
        if (pathParts.length < 2 || pathParts[pathParts.length - 1] !== 'schema') {
          throw new Error("Invalid resource URI format");
        }

        const connectionId = uri.hostname;
        const tableName = pathParts[pathParts.length - 2];

        const connection = await this.configParser.getConnection(connectionId);
        if (!connection) {
          throw new Error(`Connection not found: ${connectionId}`);
        }

        const schema = await this.dbeaverClient.getTableSchema(connection, tableName);

        return {
          contents: [
            {
              uri: request.params.uri,
              mimeType: "application/json",
              text: JSON.stringify(schema, null, 2),
            },
          ],
        };
      } catch (error) {
        throw new McpError(ErrorCode.InvalidParams, `Failed to read resource: ${formatError(error)}`);
      }
    });
  }

  private setupToolHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      const tools: Tool[] = [
        {
          name: 'list_connections',
          description: 'List all available DBeaver database connections',
          inputSchema: {
            type: 'object',
            properties: {
              includeDetails: {
                type: 'boolean',
                description: 'Include detailed connection information',
                default: false
              }
            }
          },
        },
        {
          name: 'get_connection_info',
          description: 'Get detailed information about a specific DBeaver connection',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection',
              },
            },
            required: ['connectionId'],
          },
        },
        {
          name: 'execute_query',
          description: 'Execute a SQL query on a specific DBeaver connection (read-only queries)',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection to use'
              },
              query: {
                type: 'string',
                description: 'The SQL query to execute (SELECT statements only)'
              },
              maxRows: {
                type: 'number',
                description: 'Maximum number of rows to return (default: 1000)',
                default: 1000
              },
              password: {
                type: 'string',
                description: 'Database password for the connection (optional, will use environment variable if not provided)'
              }
            },
            required: ['connectionId', 'query']
          }
        },
        {
          name: 'write_query',
          description: 'Execute INSERT, UPDATE, or DELETE queries on a specific DBeaver connection',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection to use'
              },
              query: {
                type: 'string',
                description: 'The SQL query to execute (INSERT, UPDATE, DELETE statements only)'
              },
              password: {
                type: 'string',
                description: 'Database password for the connection (optional, will use environment variable if not provided)'
              }
            },
            required: ['connectionId', 'query']
          }
        },
        {
          name: 'create_table',
          description: 'Create new tables in the database',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection'
              },
              query: {
                type: 'string',
                description: 'CREATE TABLE statement'
              },
              password: {
                type: 'string',
                description: 'Database password for the connection (optional, will use environment variable if not provided)'
              }
            },
            required: ['connectionId', 'query']
          }
        },
        {
          name: 'alter_table',
          description: 'Modify existing table schema (add columns, rename tables, etc.)',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection'
              },
              query: {
                type: 'string',
                description: 'ALTER TABLE statement'
              },
              password: {
                type: 'string',
                description: 'Database password for the connection (optional, will use environment variable if not provided)'
              }
            },
            required: ['connectionId', 'query']
          }
        },
        {
          name: 'drop_table',
          description: 'Remove a table from the database with safety confirmation',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection'
              },
              tableName: {
                type: 'string',
                description: 'Name of the table to drop'
              },
              confirm: {
                type: 'boolean',
                description: 'Safety confirmation flag (must be true)'
              },
              password: {
                type: 'string',
                description: 'Database password for the connection (optional, will use environment variable if not provided)'
              }
            },
            required: ['connectionId', 'tableName', 'confirm']
          }
        },
        {
          name: 'get_table_schema',
          description: 'Get schema information for a specific table',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection'
              },
              tableName: {
                type: 'string',
                description: 'The name of the table to describe'
              },
              includeIndexes: {
                type: 'boolean',
                description: 'Include index information',
                default: true
              },
              password: {
                type: 'string',
                description: 'Database password for the connection (optional, will use environment variable if not provided)'
              }
            },
            required: ['connectionId', 'tableName']
          }
        },
        {
          name: 'export_data',
          description: 'Export query results to various formats (CSV, JSON, etc.)',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection'
              },
              query: {
                type: 'string',
                description: 'The SQL query to execute for export (SELECT only)'
              },
              format: {
                type: 'string',
                enum: ['csv', 'json', 'xml', 'excel'],
                description: 'Export format',
                default: 'csv'
              },
              includeHeaders: {
                type: 'boolean',
                description: 'Include column headers in export',
                default: true
              },
              maxRows: {
                type: 'number',
                description: 'Maximum number of rows to export',
                default: 10000
              },
              password: {
                type: 'string',
                description: 'Database password for the connection (optional, will use environment variable if not provided)'
              }
            },
            required: ['connectionId', 'query']
          }
        },
        {
          name: 'test_connection',
          description: 'Test connectivity to a DBeaver connection',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection to test',
              },
            },
            required: ['connectionId'],
          },
        },
        {
          name: 'get_database_stats',
          description: 'Get statistics and information about a database',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection',
              },
            },
            required: ['connectionId'],
          },
        },
        {
          name: 'list_tables',
          description: 'List all tables in a database',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection'
              },
              schema: {
                type: 'string',
                description: 'Specific schema to list tables from (optional)'
              },
              includeViews: {
                type: 'boolean',
                description: 'Include views in the results',
                default: false
              },
              password: {
                type: 'string',
                description: 'Database password for the connection (optional, will use environment variable if not provided)'
              }
            },
            required: ['connectionId']
          }
        },
        {
          name: 'append_insight',
          description: 'Add a business insight or analysis note to the memo',
          inputSchema: {
            type: 'object',
            properties: {
              insight: {
                type: 'string',
                description: 'The business insight or analysis note to store',
              },
              connection: {
                type: 'string',
                description: 'Optional connection ID to associate with this insight',
              },
              tags: {
                type: 'array',
                items: { type: 'string' },
                description: 'Optional tags to categorize the insight',
              },
            },
            required: ['insight'],
          },
        },
        {
          name: 'list_insights',
          description: 'List all stored business insights and analysis notes',
          inputSchema: {
            type: 'object',
            properties: {
              connection: {
                type: 'string',
                description: 'Filter insights by connection ID (optional)',
              },
              tags: {
                type: 'array',
                items: { type: 'string' },
                description: 'Filter insights by tags (optional)',
              },
            },
          },
        },
        {
          name: 'list_db_passwords',
          description: 'List all environment variables ending with db_password (case-insensitive)',
          inputSchema: {
            type: 'object',
            properties: {},
            required: [],
          },
        },
        {
          name: 'set_db_password',
          description: 'Set a database password in the .env file with an appropriately named environment variable',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection to set password for'
              },
              password: {
                type: 'string',
                description: 'The database password to set'
              },
              customVarName: {
                type: 'string',
                description: 'Custom environment variable name (optional, will auto-generate if not provided)'
              }
            },
            required: ['connectionId', 'password']
          }
        },
      ];

      return { tools };
    });

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;
      
      try {
        this.log(`Executing tool: ${name} with args: ${JSON.stringify(args)}`, 'debug');
        
        switch (name) {
          case 'list_connections':
            return await this.handleListConnections(args as { includeDetails?: boolean });
            
          case 'get_connection_info':
            return await this.handleGetConnectionInfo(args as { connectionId: string });
            
          case 'execute_query':
            return await this.handleExecuteQuery(args as { 
              connectionId: string; 
              query: string; 
              maxRows?: number;
              password?: string;
            });

          case 'write_query':
            return await this.handleWriteQuery(args as { 
              connectionId: string; 
              query: string; 
              password?: string;
            });

          case 'create_table':
            return await this.handleCreateTable(args as { 
              connectionId: string; 
              query: string; 
              password?: string;
            });

          case 'alter_table':
            return await this.handleAlterTable(args as { 
              connectionId: string; 
              query: string; 
              password?: string;
            });

          case 'drop_table':
            return await this.handleDropTable(args as { 
              connectionId: string; 
              tableName: string; 
              confirm: boolean 
            });
            
          case 'get_table_schema':
            return await this.handleGetTableSchema(args as { 
              connectionId: string; 
              tableName: string; 
              includeIndexes?: boolean;
              password?: string;
            });
            
          case 'export_data':
            return await this.handleExportData(args as { 
              connectionId: string; 
              query: string; 
              format: string; 
              includeHeaders: boolean; 
              maxRows: number;
              password?: string;
            });
            
          case 'test_connection':
            return await this.handleTestConnection(args as { connectionId: string });
            
          case 'get_database_stats':
            return await this.handleGetDatabaseStats(args as { connectionId: string });
            
          case 'list_tables':
            return await this.handleListTables(args as { 
              connectionId: string; 
              schema?: string; 
              includeViews?: boolean 
            });

          case 'append_insight':
            return await this.handleAppendInsight(args as { 
              insight: string; 
              connection?: string; 
              tags?: string[] 
            });

          case 'list_insights':
            return await this.handleListInsights(args as { 
              connection?: string; 
              tags?: string[] 
            });
            
          case 'list_db_passwords':
            return await this.handleListDbPasswords();
            
          case 'set_db_password':
            return await this.handleSetDbPassword(args as {
              connectionId: string;
              password: string;
              customVarName?: string;
            });
            
          default:
            throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${name}`);
        }
      } catch (error: any) {
        this.log(`Tool execution failed: ${error}`, 'error');
        
        if (error instanceof McpError) {
          throw error;
        }
        
        throw new McpError(
          ErrorCode.InternalError,
          `Tool execution failed: ${formatError(error)}`
        );
      }
    });
  }

  private async handleListConnections(args: { includeDetails?: boolean }) {
    const connections = await this.configParser.parseConnections();
    
    if (args.includeDetails) {
      return {
        content: [{
          type: 'text' as const,
          text: JSON.stringify(connections, null, 2),
        }],
      };
    }
    
    const simplified = connections.map(conn => ({
      id: conn.id,
      name: conn.name,
      driver: conn.driver,
      host: conn.host,
      database: conn.database,
      folder: conn.folder
    }));
    
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(simplified, null, 2),
      }],
    };
  }

  private async handleGetConnectionInfo(args: { connectionId: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const connection = await this.configParser.getConnection(connectionId);
    
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }
    
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(connection, null, 2),
      }],
    };
  }

  private async handleExecuteQuery(args: {
    connectionId: string;
    query: string;
    maxRows?: number;
    password?: string;
  }) {
    const connection = await this.configParser.getConnection(args.connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${args.connectionId}`);
    }

    // Validate query - only SELECT queries allowed
    const trimmedQuery = args.query.trim();
    if (!trimmedQuery.toLowerCase().startsWith('select')) {
      throw new McpError(ErrorCode.InvalidParams, 'Only SELECT queries are allowed for execute_query');
    }

    // Apply maxRows limit if specified
    let finalQuery = trimmedQuery;
    if (args.maxRows && args.maxRows > 0) {
      finalQuery = `${trimmedQuery} LIMIT ${args.maxRows}`;
    }

    try {
      const result = await this.executePsqlDirect(connection, finalQuery, args.password);
      const response = {
        columns: result.columns,
        rows: result.rows,
        rowCount: result.rows.length,
        executionTime: Date.now() // Simple execution time tracking
      };
      return { content: [{ type: 'text' as const, text: JSON.stringify(response, null, 2), }], };
    } catch (error) {
      throw new McpError(ErrorCode.InternalError, `Query execution failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private async handleWriteQuery(args: { connectionId: string; query: string; password?: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const query = args.query.trim();
    
    // Validate query type
    const lowerQuery = query.toLowerCase();
    if (lowerQuery.startsWith('select')) {
      throw new McpError(ErrorCode.InvalidParams, 'Use execute_query for SELECT operations');
    }
    
    if (!(lowerQuery.startsWith('insert') || lowerQuery.startsWith('update') || lowerQuery.startsWith('delete'))) {
      throw new McpError(ErrorCode.InvalidParams, 'Only INSERT, UPDATE, or DELETE operations are allowed with write_query');
    }

    // Additional validation
    const validationError = validateQuery(query);
    if (validationError) {
      throw new McpError(ErrorCode.InvalidParams, validationError);
    }
    
    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }
    
    const result = await this.dbeaverClient.executeWriteQuery(connection, query, args.password);
    
    const response = {
      query: query,
      connection: connection.name,
      executionTime: result.executionTime,
      affectedRows: result.rowCount,
      success: true
    };
    
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(response, null, 2),
      }],
    };
  }

  private async handleCreateTable(args: { connectionId: string; query: string; password?: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const query = args.query.trim();
    
    if (!query.toLowerCase().startsWith('create table')) {
      throw new McpError(ErrorCode.InvalidParams, 'Only CREATE TABLE statements are allowed');
    }
    
    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }
    
    const result = await this.dbeaverClient.executeWriteQuery(connection, query, args.password);
    
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify({ 
          success: true, 
          message: 'Table created successfully',
          executionTime: result.executionTime 
        }, null, 2),
      }],
    };
  }

  private async handleAlterTable(args: { connectionId: string; query: string; password?: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const query = args.query.trim();
    
    if (!query.toLowerCase().startsWith('alter table')) {
      throw new McpError(ErrorCode.InvalidParams, 'Only ALTER TABLE statements are allowed');
    }
    
    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }
    
    const result = await this.dbeaverClient.executeWriteQuery(connection, query, args.password);
    
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify({ 
          success: true, 
          message: 'Table altered successfully',
          executionTime: result.executionTime 
        }, null, 2),
      }],
    };
  }

  private async handleDropTable(args: { connectionId: string; tableName: string; confirm: boolean; password?: string }) {
    if (!args.confirm) {
      throw new McpError(ErrorCode.InvalidParams, 'Safety confirmation required. Set confirm to true to proceed.');
    }

    const connectionId = sanitizeConnectionId(args.connectionId);
    const tableName = args.tableName.trim();
    
    if (!tableName) {
      throw new McpError(ErrorCode.InvalidParams, 'Table name is required');
    }

    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }

    const dropQuery = `DROP TABLE IF EXISTS "${tableName}" CASCADE;`;
    
    try {
      const result = await this.dbeaverClient.executeWriteQuery(connection, dropQuery, args.password);
      
      return {
        content: [{
          type: 'text' as const,
          text: JSON.stringify({
            success: true,
            message: `Table ${tableName} dropped successfully`,
            result: result
          }, null, 2)
        }]
      };
    } catch (error) {
      throw new McpError(ErrorCode.InternalError, `Failed to drop table: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private async handleGetTableSchema(args: { connectionId: string; tableName: string; includeIndexes?: boolean; password?: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const tableName = args.tableName.trim();
    
    if (!tableName) {
      throw new McpError(ErrorCode.InvalidParams, 'Table name is required');
    }

    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }

    try {
      const schema = await this.dbeaverClient.getTableSchema(connection, tableName, args.password);
      
      return {
        content: [{
          type: 'text' as const,
          text: JSON.stringify(schema, null, 2)
        }]
      };
    } catch (error) {
      throw new McpError(ErrorCode.InternalError, `Failed to get table schema: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private async handleExportData(args: { 
    connectionId: string; 
    query: string; 
    format: string; 
    includeHeaders: boolean; 
    maxRows: number;
    password?: string;
  }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const query = args.query.trim();
    
    if (!query) {
      throw new McpError(ErrorCode.InvalidParams, 'Query is required');
    }

    // Validate query - only SELECT queries for export
    if (!query.toLowerCase().startsWith('select')) {
      throw new McpError(ErrorCode.InvalidParams, 'Only SELECT queries are allowed for export');
    }

    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }

    try {
      // Use the enhanced exportData method from DBeaverClient
      const exportOptions: ExportOptions = {
        format: args.format as 'csv' | 'json' | 'xml' | 'excel',
        includeHeaders: args.includeHeaders,
        maxRows: args.maxRows
      };
      
      const result = await this.dbeaverClient.exportData(connection, query, exportOptions, args.password);
      
      return {
        content: [{
          type: 'text' as const,
          text: result
        }]
      };
    } catch (error) {
      throw new McpError(ErrorCode.InternalError, `Export failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private async handleTestConnection(args: { connectionId: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const connection = await this.configParser.getConnection(connectionId);
    
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }
    
    const testResult = await this.dbeaverClient.testConnection(connection);
    
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(testResult, null, 2),
      }],
    };
  }

  private async handleGetDatabaseStats(args: { connectionId: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const connection = await this.configParser.getConnection(connectionId);
    
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }
    
    const stats = await this.dbeaverClient.getDatabaseStats(connection);
    
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(stats, null, 2),
      }],
    };
  }

  private async handleListTables(args: {
    connectionId: string;
    schema?: string;
    includeViews?: boolean;
    password?: string;
  }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    
    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }

    try {
      const tables = await this.dbeaverClient.listTables(connection, args.schema, args.includeViews || false, args.password);
      
      return {
        content: [{
          type: 'text' as const,
          text: JSON.stringify(tables, null, 2)
        }]
      };
    } catch (error) {
      throw new McpError(ErrorCode.InternalError, `Failed to list tables: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private async handleAppendInsight(args: { insight: string; connection?: string; tags?: string[] }) {
    if (!args.insight || args.insight.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Insight text is required');
    }

    const newInsight: BusinessInsight = {
      id: Date.now(),
      insight: args.insight.trim(),
      created_at: new Date().toISOString(),
      connection: args.connection,
      tags: args.tags || []
    };

    this.insights.push(newInsight);
    this.saveInsights();

    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify({ 
          success: true, 
          message: 'Insight added successfully',
          id: newInsight.id 
        }, null, 2),
      }],
    };
  }

  private async handleListInsights(args: { connection?: string; tags?: string[] }) {
    let filteredInsights = [...this.insights];

    if (args.connection) {
      filteredInsights = filteredInsights.filter(insight => 
        insight.connection === args.connection
      );
    }

    if (args.tags && args.tags.length > 0) {
      filteredInsights = filteredInsights.filter(insight => 
        insight.tags && args.tags!.some(tag => insight.tags!.includes(tag))
      );
    }

    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(filteredInsights, null, 2),
      }],
    };
  }

  private async handleListDbPasswords() {
    const dbPasswordVars: { [key: string]: string } = {};
    
    // Collect all environment variables ending with 'db_password' (case-insensitive)
    for (const [key, value] of Object.entries(process.env)) {
      if (key.toLowerCase().endsWith('db_password')) {
        dbPasswordVars[key] = value || '';
      }
    }
    
    const response = {
      dbPasswords: dbPasswordVars,
      count: Object.keys(dbPasswordVars).length,
      message: `Found ${Object.keys(dbPasswordVars).length} database password environment variables`
    };
    
    return {
      content: [{ type: 'text' as const, text: JSON.stringify(response, null, 2) }],
    };
  }

  private async handleSetDbPassword(args: {
    connectionId: string;
    password: string;
    customVarName?: string;
  }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const password = args.password;
    const customVarName = args.customVarName || `DB_PASSWORD_${connectionId.toUpperCase().replace(/-/g, '_')}`;

    if (!password) {
      throw new McpError(ErrorCode.InvalidParams, 'Password cannot be empty');
    }

    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }

    try {
      // Attempt to set the password in the .env file
      const envFilePath = path.join(os.homedir(), '.env');
      let envContent = '';
      if (fs.existsSync(envFilePath)) {
        envContent = fs.readFileSync(envFilePath, 'utf-8');
      }

      const newEnvContent = envContent.replace(
        new RegExp(`^${customVarName}=.*$`, 'm'),
        `${customVarName}=${password}`
      );

      if (newEnvContent !== envContent) {
        fs.writeFileSync(envFilePath, newEnvContent);
        this.log(`Password set for ${customVarName} in ${envFilePath}`);
      } else {
        this.log(`Password for ${customVarName} already exists in ${envFilePath}`);
      }

      return {
        content: [{
          type: 'text' as const,
          text: JSON.stringify({
            success: true,
            message: `Password set for ${customVarName} in ${envFilePath}`,
            connection: connection.name,
            varName: customVarName,
            password: password
          }, null, 2)
        }]
      };
    } catch (error) {
      this.log(`Failed to set DB password: ${error}`, 'error');
      throw new McpError(ErrorCode.InternalError, `Failed to set DB password: ${formatError(error)}`);
    }
  }

  async run() {
    try {
      // Validate DBeaver workspace
      if (!this.configParser.isWorkspaceValid()) {
        throw new Error('DBeaver workspace not found. Please run DBeaver at least once to create the workspace.');
      }

      const transport = new StdioServerTransport();
      await this.server.connect(transport);
      
      this.log('DBeaver MCP server started successfully');
      
      if (this.debug) {
        const debugInfo = this.configParser.getDebugInfo();
        this.log(`Debug info: ${JSON.stringify(debugInfo, null, 2)}`, 'debug');
      }
      
    } catch (error) {
      this.log(`Failed to start server: ${formatError(error)}`, 'error');
      process.exit(1);
    }
  }
}

// Handle CLI arguments
if (process.argv.includes('--help') || process.argv.includes('-h')) {
  console.log(`
DBeaver MCP Server v1.1.0

Usage: dbeaver-mcp-server [options]

Options:
  -h, --help     Show this help message
  --version      Show version information
  --debug        Enable debug logging

Environment Variables:
  DBEAVER_PATH      Path to DBeaver executable
  DBEAVER_TIMEOUT   Query timeout in milliseconds (default: 30000)
  DBEAVER_DEBUG     Enable debug logging (true/false)

Features:
  - Universal database support via DBeaver connections
  - Read and write operations with safety checks
  - Schema introspection and table management
  - Data export in multiple formats
  - Business insights tracking
  - Resource-based schema browsing

For more information, visit: https://github.com/srthkdev/dbeaver-mcp-server
`);
  process.exit(0);
}

if (process.argv.includes('--version')) {
  console.log('1.1.0');
  process.exit(0);
}

// Start the server
const server = new DBeaverMCPServer();
server.run().catch((error) => {
  console.error('Server startup failed:', error);
  process.exit(1);
});