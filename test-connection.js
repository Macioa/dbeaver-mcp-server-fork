#!/usr/bin/env node

// Test script to verify test_connection functionality
import { DBeaverClient } from './dist/dbeaver-client.js';

async function testConnectionFunctionality() {
  console.log('🧪 Testing test_connection functionality...');
  
  const client = new DBeaverClient();
  
  // Test with a mock connection object
  const mockConnection = {
    id: 'test-connection',
    name: 'Test Connection',
    driver: 'postgres-jdbc',
    host: 'localhost',
    port: 5432,
    user: 'testuser',
    database: 'testdb',
    url: 'jdbc:postgresql://localhost:5432/testdb',
    folder: '',
    description: 'Test connection for validation',
    readonly: false,
    properties: {}
  };
  
  console.log('   Mock connection created:', mockConnection.name);
  
  try {
    // Test the testConnection method
    console.log('   Testing testConnection method...');
    const result = await client.testConnection(mockConnection);
    
    console.log('   ✅ testConnection method executed successfully!');
    console.log('   Result:', JSON.stringify(result, null, 2));
    
    // Verify the result structure
    if (result.connectionId === mockConnection.id && 
        typeof result.success === 'boolean' && 
        typeof result.responseTime === 'number') {
      console.log('   ✅ Result structure is correct');
    } else {
      console.log('   ❌ Result structure is incorrect');
    }
    
    console.log('\n🎉 test_connection functionality test passed!');
    
  } catch (error) {
    console.error('   ❌ Test failed:', error.message);
    throw new Error(`test_connection functionality test failed: ${error.message}`);
  }
}

testConnectionFunctionality().catch(error => {
  console.error('\n💥 Test execution failed:', error.message);
  process.exit(1);
});

