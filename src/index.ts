import { spawn, ChildProcess } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { config } from 'dotenv';
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
    // Load environment variables from .env file in MCP server directory
    const mcpDir = path.dirname(new URL(import.meta.url).pathname);
    const envPath = path.join(mcpDir, '.env');
    
    if (fs.existsSync(envPath)) {
      this.log(`Loading .env from MCP server directory: ${envPath}`, 'debug');
      config({ path: envPath });
    } else {
      this.log(`No .env file found in MCP server directory, using system environment variables only`, 'debug');
      config();
    }
    
    this.debug = process.env.DBEAVER_DEBUG === 'true';
    this.insightsFile = path.join(path.dirname(new URL(import.meta.url).pathname), 'dbeaver-mcp-insights.json');
    
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

  private getTimeoutConfig() {
    return {
      queryTimeout: parseInt(process.env.DBEAVER_TIMEOUT || '30000'),
      connectionTimeout: parseInt(process.env.DBEAVER_CONNECTION_TIMEOUT || '15000'),
      maxTimeout: parseInt(process.env.DBEAVER_MAX_TIMEOUT || '120000'),
      heartbeatInterval: parseInt(process.env.DBEAVER_HEARTBEAT_INTERVAL || '5000'),
      gracefulKillDelay: parseInt(process.env.DBEAVER_GRACEFUL_KILL_DELAY || '1000')
    };
  }

  private killProcessTree(proc: ChildProcess, reason: string) {
    if (!proc || !proc.pid) return;
    
    try {
      this.log(`Killing process tree for PID ${proc.pid}: ${reason}`, 'debug');
      
      // First try graceful termination
      proc.kill('SIGTERM');
      
      // Force kill after a short delay if still running
      const timeoutConfig = this.getTimeoutConfig();
      setTimeout(() => {
        try {
          if (proc && !proc.killed) {
            proc.kill('SIGKILL');
            // Also try to kill process group
            process.kill(-proc.pid!, 'SIGKILL');
          }
        } catch (killError) {
          this.log(`Error force killing process: ${killError}`, 'debug');
        }
      }, timeoutConfig.gracefulKillDelay);
      
    } catch (error) {
      this.log(`Error killing process: ${error}`, 'debug');
    }
  }

    private async executePsqlDirect(connection: DBeaverConnection, query: string, password?: string, username?: string, timeout?: number): Promise<QueryResult> {
    const timeoutConfig = this.getTimeoutConfig();
    // Use provided timeout, environment variable, or default
    const queryTimeout = timeout || timeoutConfig.queryTimeout;
    
    // Ensure timeout doesn't exceed maximum
    const finalTimeout = Math.min(queryTimeout, timeoutConfig.maxTimeout);
    
    // Adjust timeout based on query type for better safety
    let adjustedTimeout = finalTimeout;
    const lowerQuery = query.toLowerCase().trim();
    if (lowerQuery.includes('vacuum') || lowerQuery.includes('analyze') || lowerQuery.includes('reindex')) {
      adjustedTimeout = Math.min(finalTimeout * 2, timeoutConfig.maxTimeout); // Longer timeout for maintenance operations
    } else if (lowerQuery.includes('create index') || lowerQuery.includes('drop index')) {
      adjustedTimeout = Math.min(finalTimeout * 1.5, timeoutConfig.maxTimeout); // Medium timeout for index operations
    }
    
    // Validate connection parameters
    if (!connection.host || !connection.port || !connection.database) {
      throw new Error('Invalid connection: host, port, and database are required');
    }
    
    // Get username from parameter, connection, or environment variables
    let finalUsername = username || connection.user;
    if (!finalUsername) {
      // Try to get username from environment variables using the {db_name}_DB_USER pattern
      const dbName = connection.name.toUpperCase().replace(/[^A-Z0-9]/g, '_');
      const usernameVarName = `${dbName}_DB_USER`;
      finalUsername = process.env[usernameVarName] || 'postgres'; // Default to postgres
    }
    
    // Log timeout configuration for debugging
    if (this.debug) {
      this.log(`Executing psql with timeout: ${adjustedTimeout}ms (base: ${finalTimeout}ms, max: ${timeoutConfig.maxTimeout}ms)`, 'debug');
      this.log(`Query: ${query.substring(0, 100)}${query.length > 100 ? '...' : ''}`, 'debug');
      this.log(`Connection: ${connection.host}:${connection.port}/${connection.database}`, 'debug');
      this.log(`Username: ${finalUsername}`, 'debug');
    }
    
    return new Promise((resolve, reject) => {
      let proc: ChildProcess | null = null;
      
      const timeoutId = setTimeout(() => {
        if (proc) {
          this.log(`Query timeout reached (${adjustedTimeout}ms), killing psql process ${proc.pid}`, 'debug');
          this.killProcessTree(proc, 'query timeout');
        }
        reject(new Error(`Query execution timed out after ${adjustedTimeout}ms`));
      }, adjustedTimeout);

      const args = [];
      if (connection.host) args.push('-h', connection.host);
      if (connection.port) args.push('-p', connection.port.toString());
      if (finalUsername) args.push('-U', finalUsername);
      if (connection.database) args.push('-d', connection.database);
      
      // CRITICAL: Force psql to exit after command completion
      args.push('-c', query);
      args.push('--no-psqlrc'); // Don't read ~/.psqlrc (prevents interactive mode)
      args.push('--no-align'); // Disable aligned output for easier parsing
      args.push('--tuples-only'); // Output tuples only, no headers/footers
      args.push('--field-separator=|'); // Use | as field separator

      proc = spawn('psql', args, {
        stdio: ['pipe', 'pipe', 'pipe'],
        env: {
          ...process.env,
          PGPASSWORD: password || process.env.PGPASSWORD || 'postgres',
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
          this.log(`Connection timeout reached, killing psql process ${proc.pid}`, 'debug');
          this.killProcessTree(proc, 'connection timeout');
        }
      }, Math.min(adjustedTimeout / 2, timeoutConfig.connectionTimeout)); // Connection timeout is half of query timeout or configured connection timeout

      // CRITICAL: Add network operation timeout detection
      const networkTimeout = setTimeout(() => {
        if (proc && !proc.killed && !hasOutput) {
          this.log(`Network operation timeout detected, process may be hanging on network`, 'debug');
          // Don't kill yet, but log the warning
        }
      }, 15000); // 15 second network timeout

      // Add heartbeat check to detect hanging processes
      let lastActivity = Date.now();
      let hasOutput = false;
      const heartbeatInterval = setInterval(() => {
        if (proc && !proc.killed) {
          const timeSinceLastActivity = Date.now() - lastActivity;
          
          // If we have no output and no activity for a while, consider it hanging
          if (!hasOutput && timeSinceLastActivity > Math.min(adjustedTimeout / 2, 10000)) {
            this.log(`No output detected for ${timeSinceLastActivity}ms, process may be hanging`, 'debug');
          }
          
          // CRITICAL: If no output for extended period, force quit psql
          if (!hasOutput && timeSinceLastActivity > Math.min(adjustedTimeout * 0.7, 20000)) {
            this.log(`Forcing psql quit due to no output for ${timeSinceLastActivity}ms`, 'debug');
            try {
              // Try to send quit command to psql stdin
              if (proc.stdin && !proc.stdin.destroyed) {
                proc.stdin.write('\\q\n');
              }
            } catch (error) {
              this.log(`Failed to send quit command: ${error}`, 'debug');
            }
          }
          
          if (timeSinceLastActivity > adjustedTimeout) {
            this.log(`Heartbeat timeout detected, killing hanging psql process ${proc.pid}`, 'debug');
            this.killProcessTree(proc, 'heartbeat timeout');
            clearInterval(heartbeatInterval);
          }
        }
      }, timeoutConfig.heartbeatInterval); // Check at configured interval

      let stdout = '';
      let stderr = '';
      if (proc.stdout) proc.stdout.on('data', (data) => { 
        stdout += data.toString(); 
        lastActivity = Date.now(); // Update activity timestamp
        hasOutput = true; // Mark that we've received output
      });
      if (proc.stderr) proc.stderr.on('data', (data) => { 
        stderr += data.toString(); 
        lastActivity = Date.now(); // Update activity timestamp
        hasOutput = true; // Mark that we've received output
      });

      proc.on('close', (code) => {
        clearTimeout(timeoutId);
        clearTimeout(connectionTimeout);
        clearTimeout(networkTimeout);
        clearInterval(heartbeatInterval);
        clearInterval(processValidation);
        
        // Handle different psql exit codes
        if (code === null) {
          reject(new Error('psql process terminated without exit code'));
          return;
        }
        
        // psql exit codes:
        // 0 = success
        // 1 = SQL error (table not found, syntax error, etc.) - but command executed
        // 2 = connection/authentication error
        // 124 = timeout kill (from our timeout handler)
        
        if (code === 0) {
          // Success - parse output normally
          try {
            const result = this.parsePsqlOutput(stdout);
            resolve(result);
          } catch (error) {
            reject(new Error(`Failed to parse psql output: ${error}`));
          }
        } else if (code === 1) {
          // SQL error - still return result but include error info
          try {
            const result = this.parsePsqlOutput(stdout);
            // Add error information to the result
            result.error = stderr.trim();
            result.exitCode = code;
            resolve(result);
          } catch (error) {
            reject(new Error(`Failed to parse psql output: ${error}`));
          }
        } else if (code === 2) {
          // Connection/authentication error
          reject(new Error(`psql connection failed (code ${code}): ${stderr}`));
        } else if (code === 124) {
          // Timeout kill
          reject(new Error(`psql query timed out and was killed (code ${code})`));
        } else {
          // Unknown error code
          reject(new Error(`psql failed with unexpected code ${code}: ${stderr}`));
        }
      });
      
      proc.on('error', (error) => {
        clearTimeout(timeoutId);
        clearTimeout(connectionTimeout);
        clearTimeout(networkTimeout);
        clearInterval(heartbeatInterval);
        clearInterval(processValidation);
        this.log(`psql process error: ${error.message}`, 'error');
        reject(new Error(`Failed to execute psql: ${error.message}`));
      });

      // Add process exit handler for additional safety
      proc.on('exit', (code, signal) => {
        if (code !== null && code !== 0) {
          this.log(`psql process exited with code ${code}`, 'debug');
        }
        if (signal) {
          this.log(`psql process killed with signal ${signal}`, 'debug');
        }
      });

      // CRITICAL: Validate process is actually running and not stuck
      const processValidation = setInterval(() => {
        if (proc && !proc.killed) {
          // Check if process is actually consuming CPU/memory
          try {
            const procInfo = process.kill(proc.pid!, 0); // Check if process exists
            if (!procInfo) {
              this.log(`Process validation failed - process may be stuck`, 'debug');
            }
          } catch (error) {
            this.log(`Process validation error: ${error}`, 'debug');
          }
        }
      }, 10000); // Check every 10 seconds

      // Add process disconnect handler
      proc.on('disconnect', () => {
        this.log(`psql process disconnected`, 'debug');
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

    // Clean up any hanging processes on exit
    process.on('exit', () => {
      this.log('Server shutting down, cleaning up processes...', 'debug');
    });

    process.on('SIGINT', () => {
      this.log('Received SIGINT, shutting down gracefully...', 'info');
      process.exit(0);
    });

    process.on('SIGTERM', () => {
      this.log('Received SIGTERM, shutting down gracefully...', 'info');
      process.exit(0);
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
          name: 'workflow_requirements',
          description: '🚨 GLOBAL WORKFLOW REQUIREMENTS: ALL database operations MUST follow this sequence: 1) list_connections FIRST, 2) get_connection_info + list_db_passwords TOGETHER, 3) prompt user and set_db_password if password not found, 4) ALWAYS provide password parameter. This prevents hanging connections and ensures proper authentication.',
          inputSchema: {
            type: 'object',
            properties: {},
            required: []
          }
        },
        {
           name: 'list_connections',
          description: '⚠️ FIRST STEP: List all available DBeaver database connections. ALWAYS call this before any database operations to see available connections.',
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
          description: '⚠️ SECOND STEP: Get detailed information about a specific DBeaver connection. ALWAYS call this after list_connections to verify connection details. MUST be used together with list_db_passwords to check password configuration.',
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
          description: '⚠️ DATABASE OPERATION: Execute a SQL query on a specific DBeaver connection (read-only queries). REQUIRES: 1) list_connections, 2) get_connection_info + list_db_passwords TOGETHER, 3) set_db_password if needed, 4) ALWAYS provide password and username parameters. Missing password verification causes hanging connections.',
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
                description: 'Database password for the connection (REQUIRED - must be provided to avoid hanging connections)'
              },
              username: {
                type: 'string',
                description: 'Database username for the connection (REQUIRED - use postgres if unsure)'
              }
            },
            required: ['connectionId', 'query', 'password', 'username']
          }
        },
        {
          name: 'write_query',
          description: '⚠️ DATABASE OPERATION: Execute INSERT, UPDATE, or DELETE queries on a specific DBeaver connection. REQUIRES: 1) list_connections, 2) get_connection_info + list_db_passwords TOGETHER, 3) set_db_password if needed, 4) ALWAYS provide password and username parameters. Missing password verification causes hanging connections.',
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
                description: 'Database password for the connection (REQUIRED - must be provided to avoid hanging connections)'
              },
              username: {
                type: 'string',
                description: 'Database username for the connection (REQUIRED - use postgres if unsure)'
              }
            },
            required: ['connectionId', 'query', 'password', 'username']
          }
        },
        {
          name: 'create_table',
          description: '⚠️ DATABASE OPERATION: Create new tables in the database. REQUIRES: 1) list_connections, 2) get_connection_info + list_db_passwords TOGETHER, 3) set_db_password if needed, 4) ALWAYS provide password and username parameters. Missing password verification causes hanging connections.',
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
                description: 'Database password for the connection (REQUIRED - must be provided to avoid hanging connections)'
              },
              username: {
                type: 'string',
                description: 'Database username for the connection (REQUIRED - use postgres if unsure)'
              }
            },
            required: ['connectionId', 'query', 'password', 'username']
          }
        },
        {
          name: 'alter_table',
          description: '⚠️ DATABASE OPERATION: Modify existing table schema (add columns, rename tables, etc.). REQUIRES: 1) list_connections, 2) get_connection_info + list_db_passwords TOGETHER, 3) set_db_password if needed, 4) ALWAYS provide password and username parameters. Missing password verification causes hanging connections.',
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
                description: 'Database password for the connection (REQUIRED - must be provided to avoid hanging connections)'
              },
              username: {
                type: 'string',
                description: 'Database username for the connection (REQUIRED - use postgres if unsure)'
              }
            },
            required: ['connectionId', 'query', 'password', 'username']
          }
        },
        {
          name: 'drop_table',
          description: '⚠️ DATABASE OPERATION: Remove a table from the database with safety confirmation. REQUIRES: 1) list_connections, 2) get_connection_info + list_db_passwords TOGETHER, 3) set_db_password if needed, 4) ALWAYS provide password and username parameters. Missing password verification causes hanging connections.',
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
                description: 'Database password for the connection (REQUIRED - must be provided to avoid hanging connections)'
              },
              username: {
                type: 'string',
                description: 'Database username for the connection (REQUIRED - use postgres if unsure)'
              }
            },
            required: ['connectionId', 'tableName', 'confirm', 'password', 'username']
          }
        },
        {
          name: 'get_table_schema',
          description: '⚠️ DATABASE OPERATION: Get schema information for a specific table. REQUIRES: 1) list_connections, 2) get_connection_info + list_db_passwords TOGETHER, 3) set_db_password if needed, 4) ALWAYS provide password and username parameters. Missing password verification causes hanging connections.',
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
                description: 'Database password for the connection (REQUIRED - must be provided to avoid hanging connections)'
              },
              username: {
                type: 'string',
                description: 'Database username for the connection (REQUIRED - use postgres if unsure)'
              }
            },
            required: ['connectionId', 'tableName', 'password', 'username']
          }
        },
        {
          name: 'export_data',
          description: '⚠️ DATABASE OPERATION: Export query results to various formats (CSV, JSON, etc.). REQUIRES: 1) list_connections, 2) get_connection_info + list_db_passwords TOGETHER, 3) set_db_password if needed, 4) ALWAYS provide password and username parameters. Missing password verification causes hanging connections.',
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
                description: 'Database password for the connection (REQUIRED - must be provided to avoid hanging connections)'
              },
              username: {
                type: 'string',
                description: 'Database username for the connection (REQUIRED - use postgres if unsure)'
              }
            },
            required: ['connectionId', 'query', 'password', 'username']
          }
        },
        {
          name: 'test_connection',
          description: '⚠️ DATABASE OPERATION: Test connectivity to a DBeaver connection. REQUIRES: 1) list_connections, 2) get_connection_info + list_db_passwords TOGETHER, 3) set_db_password if needed, 4) ALWAYS provide password and username parameters. Missing password verification causes hanging connections.',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection to test',
              },
              password: {
                type: 'string',
                description: 'Database password for the connection (REQUIRED - must be provided to avoid hanging connections)'
              },
              username: {
                type: 'string',
                description: 'Database username for the connection (REQUIRED - use postgres if unsure)'
              },
            },
            required: ['connectionId', 'password', 'username'],
          },
        },
        {
          name: 'get_database_stats',
          description: '⚠️ DATABASE OPERATION: Get statistics and information about a database. REQUIRES: 1) list_connections, 2) get_connection_info + list_db_passwords TOGETHER, 3) set_db_password if needed, 4) ALWAYS provide password and username parameters. Missing password verification causes hanging connections.',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection',
              },
              password: {
                type: 'string',
                description: 'Database password for the connection (REQUIRED - must be provided to avoid hanging connections)'
              },
              username: {
                type: 'string',
                description: 'Database username for the connection (REQUIRED - use postgres if unsure)'
              },
            },
            required: ['connectionId', 'password', 'username'],
          },
        },
        {
          name: 'list_tables',
          description: '⚠️ DATABASE OPERATION: List all tables in a database. REQUIRES: 1) list_connections, 2) get_connection_info + list_db_passwords TOGETHER, 3) set_db_password if needed, 4) ALWAYS provide password and username parameters. Missing password verification causes hanging connections.',
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
                description: 'Database password for the connection (REQUIRED - must be provided to avoid hanging connections)'
              },
              username: {
                type: 'string',
                description: 'Database username for the connection (REQUIRED - use postgres if unsure)'
              }
            },
            required: ['connectionId', 'password', 'username']
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
          description: '⚠️ CRITICAL PASSWORD STEP: List all environment variables ending with db_password and db_username (case-insensitive). MUST be called together with get_connection_info to verify both connection details AND password/username configuration. If no login match is found between the connection list and password list, prompt the user to update their .env file or manually provide username and password and wait for user input.',
          inputSchema: {
            type: 'object',
            properties: {},
            required: [],
          },
        },
        {
          name: 'set_db_password',
          description: '⚠️ PASSWORD CONFIGURATION STEP: Set a database password and username in the .env file with appropriately named environment variables. Call this if list_db_passwords shows no passwords or if you need to set a new password. This is REQUIRED to prevent hanging connections.',
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
              username: {
                type: 'string',
                description: 'The database username to set (optional, defaults to postgres if not provided)'
              },
              customVarName: {
                type: 'string',
                description: 'Custom environment variable name (optional, will auto-generate if not provided)'
              }
            },
            required: ['connectionId', 'password']
          }
        },
        {
          name: 'debug_connection',
          description: '🔍 DEBUG TOOL: Get detailed connection information for troubleshooting connection issues. Shows parsed JDBC URL details and connection parameters.',
          inputSchema: {
            type: 'object',
            properties: {
              connectionId: {
                type: 'string',
                description: 'The ID or name of the DBeaver connection to debug'
              }
            },
            required: ['connectionId']
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
          case 'workflow_requirements':
            return await this.handleWorkflowRequirements();
            
          case 'list_connections':
            return await this.handleListConnections(args as { includeDetails?: boolean });
            
          case 'get_connection_info':
            return await this.handleGetConnectionInfo(args as { connectionId: string });
            
          case 'execute_query':
            return await this.handleExecuteQuery(args as { 
              connectionId: string; 
              query: string; 
              maxRows?: number;
              password: string;
              username: string;
            });

          case 'write_query':
            return await this.handleWriteQuery(args as { 
              connectionId: string; 
              query: string; 
              password: string;
              username: string;
            });

          case 'create_table':
            return await this.handleCreateTable(args as { 
              connectionId: string; 
              query: string; 
              password: string;
              username: string;
            });

          case 'alter_table':
            return await this.handleAlterTable(args as { 
              connectionId: string; 
              query: string; 
              password: string;
              username: string;
            });

          case 'drop_table':
            return await this.handleDropTable(args as { 
              connectionId: string; 
              tableName: string; 
              confirm: boolean;
              password: string;
              username: string;
            });
            
          case 'get_table_schema':
            return await this.handleGetTableSchema(args as { 
              connectionId: string; 
              tableName: string; 
              includeIndexes?: boolean;
              password: string;
              username: string;
            });
            
          case 'export_data':
            return await this.handleExportData(args as { 
              connectionId: string; 
              query: string; 
              format: string; 
              includeHeaders: boolean; 
              maxRows: number;
              password: string;
              username: string;
            });
            
          case 'test_connection':
            return await this.handleTestConnection(args as { connectionId: string; password: string; username: string });
            
          case 'get_database_stats':
            return await this.handleGetDatabaseStats(args as { connectionId: string; password: string; username: string });
            
          case 'list_tables':
            return await this.handleListTables(args as { 
              connectionId: string; 
              schema?: string; 
              includeViews?: boolean;
              password: string;
              username: string;
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
              username?: string;
              customVarName?: string;
            });
            
          case 'debug_connection':
            return await this.handleDebugConnection(args as { connectionId: string });
            
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
    password: string;
    username: string;
  }) {
    // Validate required password parameter
    if (!args.password || args.password.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Password parameter is required for all database operations to prevent hanging connections');
    }

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
      const result = await this.executePsqlDirect(connection, finalQuery, args.password, args.username);
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

  private async handleWriteQuery(args: { connectionId: string; query: string; password: string; username: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const query = args.query.trim();
    
    // Validate required password parameter
    if (!args.password || args.password.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Password parameter is required for all database operations to prevent hanging connections');
    }
    
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
    
    const result = await this.dbeaverClient.executeWriteQuery(connection, query, args.password, args.username);
    
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

  private async handleCreateTable(args: { connectionId: string; query: string; password: string; username: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const query = args.query.trim();
    
    // Validate required password parameter
    if (!args.password || args.password.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Password parameter is required for all database operations to prevent hanging connections');
    }
    
    if (!query.toLowerCase().startsWith('create table')) {
      throw new McpError(ErrorCode.InvalidParams, 'Only CREATE TABLE statements are allowed');
    }
    
    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }
    
    const result = await this.dbeaverClient.executeWriteQuery(connection, query, args.password, args.username);
    
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

  private async handleAlterTable(args: { connectionId: string; query: string; password: string; username: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const query = args.query.trim();
    
    // Validate required password parameter
    if (!args.password || args.password.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Password parameter is required for all database operations to prevent hanging connections');
    }
    
    if (!query.toLowerCase().startsWith('alter table')) {
      throw new McpError(ErrorCode.InvalidParams, 'Only ALTER TABLE statements are allowed');
    }
    
    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }
    
    const result = await this.dbeaverClient.executeWriteQuery(connection, query, args.password, args.username);
    
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

  private async handleDropTable(args: { connectionId: string; tableName: string; confirm: boolean; password: string; username: string }) {
    if (!args.confirm) {
      throw new McpError(ErrorCode.InvalidParams, 'Safety confirmation required. Set confirm to true to proceed.');
    }

    // Validate required password parameter
    if (!args.password || args.password.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Password parameter is required for all database operations to prevent hanging connections');
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

  private async handleGetTableSchema(args: { connectionId: string; tableName: string; includeIndexes?: boolean; password: string; username: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const tableName = args.tableName.trim();
    
    // Validate required password parameter
    if (!args.password || args.password.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Password parameter is required for all database operations to prevent hanging connections');
    }
    
    if (!tableName) {
      throw new McpError(ErrorCode.InvalidParams, 'Table name is required');
    }

    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }

    try {
      const schema = await this.dbeaverClient.getTableSchema(connection, tableName, args.password, args.username);
      
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
    password: string;
    username: string;
  }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const query = args.query.trim();
    
    // Validate required password parameter
    if (!args.password || args.password.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Password parameter is required for all database operations to prevent hanging connections');
    }
    
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

  private async handleTestConnection(args: { connectionId: string; password: string; username: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    
    // Validate required password parameter
    if (!args.password || args.password.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Password parameter is required for all database operations to prevent hanging connections');
    }
    
    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }

    if (this.debug) {
      this.log(`Testing connection: ${connectionId}`, 'debug');
      this.log(`Connection details: ${JSON.stringify({
        id: connection.id,
        name: connection.name,
        host: connection.host,
        port: connection.port,
        user: connection.user,
        database: connection.database,
        driver: connection.driver
      }, null, 2)}`, 'debug');
      this.log(`Password provided: ${args.password ? 'YES' : 'NO'}`, 'debug');
    }
    
    const testResult = await this.dbeaverClient.testConnection(connection, args.password, args.username);
    
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(testResult, null, 2),
      }],
    };
  }

  private async handleGetDatabaseStats(args: { connectionId: string; password: string; username: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    
    // Validate required password parameter
    if (!args.password || args.password.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Password parameter is required for all database operations to prevent hanging connections');
    }
    
    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }
    
    const stats = await this.dbeaverClient.getDatabaseStats(connection, args.password, args.username);
    
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
    password: string;
    username: string;
  }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    
    // Validate required password parameter
    if (!args.password || args.password.trim().length === 0) {
      throw new McpError(ErrorCode.InvalidParams, 'Password parameter is required for all database operations to prevent hanging connections');
    }
    
    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }

    try {
      const tables = await this.dbeaverClient.listTables(connection, args.schema, args.includeViews || false, args.password, args.username);
      
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

  private async handleWorkflowRequirements() {
    const workflow = {
      title: '🚨 GLOBAL WORKFLOW REQUIREMENTS FOR ALL DATABASE OPERATIONS',
      description: 'This workflow MUST be followed for ANY database operation to prevent hanging connections and ensure proper authentication.',
      steps: [
        {
          step: 1,
          tool: 'list_connections',
          description: 'FIRST STEP - List all available DBeaver database connections',
          required: true,
          note: 'ALWAYS call this before any database operations'
        },
        {
          step: 2,
          tool: 'get_connection_info + list_db_passwords',
          description: 'SECOND STEP - Get connection details AND verify password configuration',
          required: true,
          note: 'MUST be called TOGETHER to verify both connection details AND password configuration'
        },
        {
          step: 3,
          tool: 'set_db_password',
          description: 'Set database password if none found',
          required: false,
          note: 'Only needed if list_db_passwords shows no passwords'
        },
        {
          step: 4,
          tool: 'password parameter',
          description: 'ALWAYS provide password parameter',
          required: true,
          note: 'Required for ALL database operations to prevent hanging connections'
        }
      ],
      criticalNotes: [
        '⚠️ Missing password verification causes hanging connections',
        '⚠️ Skipping any step will result in connection failures',
        '⚠️ This workflow applies to ALL database operation tools',
        '⚠️ Password must be provided even if set_db_password was called'
      ],
      toolsRequiringWorkflow: [
        'execute_query', 'write_query', 'create_table', 'alter_table', 
        'drop_table', 'get_table_schema', 'export_data', 'test_connection',
        'get_database_stats', 'list_tables'
      ]
    };
    
    return {
      content: [{ type: 'text' as const, text: JSON.stringify(workflow, null, 2) }],
    };
  }

  private async handleListDbPasswords() {
    const dbPasswordVars: { [key: string]: string } = {};
    const dbUsernameVars: { [key: string]: string } = {};
    
    // Collect all environment variables ending with 'db_password' or 'db_user' (case-insensitive)
    for (const [key, value] of Object.entries(process.env)) {
      if (key.toLowerCase().endsWith('db_password')) {
        dbPasswordVars[key] = value || '';
      } else if (key.toLowerCase().endsWith('db_user')) {
        dbUsernameVars[key] = value || '';
      }
    }
    
    // Check if .env file exists in MCP server directory and provide debugging info
    const mcpDir = path.dirname(new URL(import.meta.url).pathname);
    const envFilePath = path.join(mcpDir, '.env');
    const envFileExists = fs.existsSync(envFilePath);
    
    let envFileContent = '';
    
    if (envFileExists) {
      try {
        envFileContent = fs.readFileSync(envFilePath, 'utf-8');
      } catch (error) {
        this.log(`Error reading .env file: ${error}`, 'error');
      }
    }
    
    const response = {
      dbPasswords: dbPasswordVars,
      dbUsernames: dbUsernameVars,
      passwordCount: Object.keys(dbPasswordVars).length,
      usernameCount: Object.keys(dbUsernameVars).length,
      message: `Found ${Object.keys(dbPasswordVars).length} database password and ${Object.keys(dbUsernameVars).length} username environment variables`,
      debug: {
        envFileExists,
        envFilePath,
        envFileContent: envFileContent ? envFileContent.split('\n').filter((line: string) => line.trim() && !line.startsWith('#')) : [],
        allEnvVars: Object.keys(process.env).filter(key => key.toLowerCase().includes('password') || key.toLowerCase().includes('username'))
      }
    };
    
    this.log(`List DB Passwords called. Found ${Object.keys(dbPasswordVars).length} password vars and ${Object.keys(dbUsernameVars).length} username vars. .env exists: ${envFileExists}`, 'debug');
    
    return {
      content: [{ type: 'text' as const, text: JSON.stringify(response, null, 2) }],
    };
  }

  private async handleSetDbPassword(args: {
    connectionId: string;
    password: string;
    username?: string;
    customVarName?: string;
  }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const password = args.password;
    const username = args.username || 'postgres'; // Default to postgres if not provided

    if (!password) {
      throw new McpError(ErrorCode.InvalidParams, 'Password cannot be empty');
    }

    const connection = await this.configParser.getConnection(connectionId);
    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }

    const dbName = connection.name.toUpperCase().replace(/[^A-Z0-9]/g, '_');
    const customVarName = args.customVarName || `${dbName}_DB_PASSWORD`;
    const customUsernameVarName = `${dbName}_DB_USER`;

    try {
      // Attempt to set the password and username in the .env file in MCP server directory
      const mcpDir = path.dirname(new URL(import.meta.url).pathname);
      const envFilePath = path.join(mcpDir, '.env');
      
      let envContent = '';
      
      // Check if .env exists, otherwise create new one
      if (fs.existsSync(envFilePath)) {
        envContent = fs.readFileSync(envFilePath, 'utf-8');
        this.log(`Using existing .env file: ${envFilePath}`, 'debug');
      } else {
        // Create new .env file in MCP server directory
        this.log(`Creating new .env file in MCP server directory: ${envFilePath}`, 'debug');
      }

      // Update password
      let newEnvContent = envContent.replace(
        new RegExp(`^${customVarName}=.*$`, 'm'),
        `${customVarName}=${password}`
      );

      // Update username
      newEnvContent = newEnvContent.replace(
        new RegExp(`^${customUsernameVarName}=.*$`, 'm'),
        `${customUsernameVarName}=${username}`
      );

      // If neither existed, add them
      if (newEnvContent === envContent) {
        if (!envContent.endsWith('\n') && envContent.length > 0) {
          newEnvContent += '\n';
        }
        newEnvContent += `${customVarName}=${password}\n${customUsernameVarName}=${username}\n`;
      }

      fs.writeFileSync(envFilePath, newEnvContent);
      this.log(`Password and username set for ${connection.name} in ${envFilePath}`);

      return {
        content: [{
          type: 'text' as const,
          text: JSON.stringify({
            success: true,
            message: `Password and username set for ${connection.name} in ${envFilePath}`,
            connection: connection.name,
            passwordVarName: customVarName,
            usernameVarName: customUsernameVarName,
            password: password,
            username: username
          }, null, 2)
        }]
      };
    } catch (error) {
      this.log(`Failed to set DB password and username: ${error}`, 'error');
      throw new McpError(ErrorCode.InternalError, `Failed to set DB password and username: ${formatError(error)}`);
    }
  }

  private async handleDebugConnection(args: { connectionId: string }) {
    const connectionId = sanitizeConnectionId(args.connectionId);
    const connection = await this.configParser.getConnection(connectionId);

    if (!connection) {
      throw new McpError(ErrorCode.InvalidParams, `Connection not found: ${connectionId}`);
    }

    const debugInfo = await this.configParser.getConnectionDebugInfo(connectionId);

    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(debugInfo, null, 2)
      }]
    };
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
  DBEAVER_PATH                    Path to DBeaver executable
  DBEAVER_TIMEOUT                 Query timeout in milliseconds (default: 30000)
  DBEAVER_CONNECTION_TIMEOUT      Connection timeout in milliseconds (default: 15000)
  DBEAVER_MAX_TIMEOUT             Maximum allowed timeout in milliseconds (default: 120000)
  DBEAVER_HEARTBEAT_INTERVAL      Heartbeat check interval in milliseconds (default: 5000)
  DBEAVER_GRACEFUL_KILL_DELAY    Graceful kill delay in milliseconds (default: 1000)
  DBEAVER_DEBUG                   Enable debug logging (true/false)

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