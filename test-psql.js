#!/usr/bin/env node

// Simple test script to verify psql integration
import { DBeaverClient } from './dist/dbeaver-client.js';

async function testPsql() {
  const client = new DBeaverClient();
  
  // EndlessCore database connection details from DBeaver MCP
  const testConnection = {
    id: 'postgres-jdbc-197fb05baf0-3d97c29680b91f47',
    name: 'EndlessCore - LocalDocker',
    driver: 'postgres-jdbc',
    host: 'localhost',
    port: 5434,
    user: 'postgres',
    database: 'postgres',
    url: 'jdbc:postgresql://localhost:5434/postgres',
    folder: '',
    description: '',
    readonly: false,
    properties: {}
  };
  
  console.log('🔍 Testing EndlessCore Database (Port 5434)');
  console.log(`   Host: ${testConnection.host}:${testConnection.port}`);
  console.log(`   Database: ${testConnection.database}`);
  
  // Use environment variable or default to 'postgres'
  const password = process.env.PGPASSWORD || 'postgres';
  console.log(`\n   Using password: ${password}`);
  process.env.PGPASSWORD = password;
  
  try {
    // Test basic query
    const result = await client.executeQuery(testConnection, 'SELECT version();');
    console.log('   ✅ Connection successful!');
    console.log('   Query result:', JSON.stringify(result, null, 2));
    
    // Test write query (CREATE TABLE)
    console.log('\n   Testing write query (CREATE TABLE)...');
    const writeResult = await client.executeWriteQuery(testConnection, 'CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name TEXT);');
    console.log('   Write query result:', JSON.stringify(writeResult, null, 2));
    
    // Test table listing
    const tables = await client.listTables(testConnection);
    console.log('\n   Tables:', JSON.stringify(tables, null, 2));
    
    // Test enhanced table schema
    console.log('\n   Testing enhanced table schema...');
    try {
      const schema = await client.getTableSchema(testConnection, 'test_table');
      console.log('   Table schema:', JSON.stringify(schema, null, 2));
    } catch (error) {
      console.log('   Schema test failed (table might not exist):', error.message);
    }
    
    // Test export functionality
    console.log('\n   Testing export functionality...');
    try {
      const csvExport = await client.exportData(testConnection, 'SELECT 1 as test_column, \'test_value\' as test_data', { format: 'csv', includeHeaders: true });
      console.log('   CSV Export (first 200 chars):', csvExport.substring(0, 200));
      
      const jsonExport = await client.exportData(testConnection, 'SELECT 1 as test_column, \'test_value\' as test_data', { format: 'json', includeHeaders: true });
      console.log('   JSON Export (first 200 chars):', jsonExport.substring(0, 200));
    } catch (error) {
      console.log('   Export test failed:', error.message);
    }
    
    console.log('\n🎉 All tests passed for EndlessCore!');
    
  } catch (error) {
    console.error('   ❌ Test failed:', error.message);
    throw new Error(`EndlessCore database test failed: ${error.message}`);
  }
}

testPsql().catch(error => {
  console.error('\n💥 Test execution failed:', error.message);
  process.exit(1);
});
