#!/usr/bin/env node

// Simple test script to verify psql integration
import { DBeaverClient } from './dist/dbeaver-client.js';

async function testPsql() {
  const client = new DBeaverClient();
  
  // Test connection details (you'll need to set these)
  const testConnection = {
    id: 'test',
    name: 'Test Connection',
    driver: 'postgresql',
    host: process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.PG_PORT) || 5432,
    user: process.env.PG_USER || 'postgres',
    database: process.env.PG_DATABASE || 'postgres',
    url: '',
    folder: '',
    description: '',
    readonly: false,
    properties: {}
  };

  try {
    console.log('Testing psql connection...');
    
    // Test basic query
    const result = await client.executeQuery(testConnection, 'SELECT version();');
    console.log('Query result:', JSON.stringify(result, null, 2));
    
    // Test write query (INSERT)
    console.log('\nTesting write query (INSERT)...');
    const writeResult = await client.executeWriteQuery(testConnection, 'CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name TEXT);');
    console.log('Write query result:', JSON.stringify(writeResult, null, 2));
    
    // Test table listing
    const tables = await client.listTables(testConnection);
    console.log('\nTables:', JSON.stringify(tables, null, 2));
    
    // Test enhanced table schema
    console.log('\nTesting enhanced table schema...');
    try {
      const schema = await client.getTableSchema(testConnection, 'test_table');
      console.log('Table schema:', JSON.stringify(schema, null, 2));
    } catch (error) {
      console.log('Schema test failed (table might not exist):', error.message);
    }
    
    // Test database stats
    const stats = await client.getDatabaseStats(testConnection);
    console.log('\nDatabase stats:', JSON.stringify(stats, null, 2));
    
  } catch (error) {
    console.error('Test failed:', error.message);
  }
}

testPsql();
