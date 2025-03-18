"""Tests for SQLite connector functionality."""

import os
import sys
import unittest
import tempfile
import shutil

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_mcp_server.connectors.sqlite import SQLiteConnector
from db_mcp_server.query.executor import QueryExecutor
from db_mcp_server.schema.manager import SchemaManager


class TestSQLiteConnector(unittest.TestCase):
    """Test case for SQLite connector."""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary directory for the test database
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, 'test.db')
        self.connector = SQLiteConnector(db_path=self.db_path)
        self.connector.connect()
        self.query_executor = QueryExecutor(self.connector)
        self.schema_manager = SchemaManager(self.connector)

    def tearDown(self):
        """Clean up after tests."""
        # Close the database connection
        if self.connector:
            self.connector.close()
        
        # Remove the temporary directory
        shutil.rmtree(self.test_dir)

    def test_create_table(self):
        """Test creating a table."""
        # Define table columns
        columns = [
            {
                'name': 'id',
                'type': 'INTEGER',
                'primary_key': True,
                'nullable': False
            },
            {
                'name': 'name',
                'type': 'TEXT',
                'nullable': False
            },
            {
                'name': 'age',
                'type': 'INTEGER',
                'nullable': True
            }
        ]
        
        # Create the table
        result = self.schema_manager.create_table('users', columns)
        self.assertTrue(result.get('success', False))
        
        # Verify table exists
        tables = self.connector.list_tables()
        self.assertIn('users', tables)
        
        # Verify schema
        schema = self.schema_manager.get_table_schema('users')
        self.assertEqual(len(schema), 3)
        
        # Check column properties
        id_col = next((col for col in schema if col['name'] == 'id'), None)
        self.assertIsNotNone(id_col)
        self.assertTrue(id_col['primary_key'])
        self.assertFalse(id_col['nullable'])
        
        name_col = next((col for col in schema if col['name'] == 'name'), None)
        self.assertIsNotNone(name_col)
        self.assertEqual(name_col['type'], 'TEXT')
        self.assertFalse(name_col['nullable'])
        
        age_col = next((col for col in schema if col['name'] == 'age'), None)
        self.assertIsNotNone(age_col)
        self.assertEqual(age_col['type'], 'INTEGER')
        self.assertTrue(age_col['nullable'])

    def test_crud_operations(self):
        """Test CRUD operations."""
        # Create a test table
        columns = [
            {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
            {'name': 'name', 'type': 'TEXT'},
            {'name': 'email', 'type': 'TEXT'}
        ]
        self.schema_manager.create_table('contacts', columns)
        
        # 1. Insert records
        data1 = {'name': 'John Doe', 'email': 'john@example.com'}
        result = self.query_executor.insert('contacts', data1)
        self.assertTrue(result.get('success', False))
        id1 = result.get('last_insert_id')
        self.assertIsNotNone(id1)
        
        data2 = {'name': 'Jane Smith', 'email': 'jane@example.com'}
        result = self.query_executor.insert('contacts', data2)
        self.assertTrue(result.get('success', False))
        id2 = result.get('last_insert_id')
        self.assertIsNotNone(id2)
        
        # 2. Query records
        records = self.query_executor.select('contacts')
        self.assertEqual(len(records), 2)
        
        # Query with where clause
        records = self.query_executor.select('contacts', where={'name': 'John Doe'})
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['name'], 'John Doe')
        self.assertEqual(records[0]['email'], 'john@example.com')
        
        # 3. Update a record
        update_data = {'email': 'john.doe@example.com'}
        result = self.query_executor.update('contacts', update_data, where={'id': id1})
        self.assertTrue(result.get('success', False))
        
        # Verify update
        records = self.query_executor.select('contacts', where={'id': id1})
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['email'], 'john.doe@example.com')
        
        # 4. Delete a record
        result = self.query_executor.delete('contacts', where={'id': id2})
        self.assertTrue(result.get('success', False))
        
        # Verify deletion
        records = self.query_executor.select('contacts')
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['id'], id1)

    def test_transactions(self):
        """Test transaction support."""
        # Create a test table
        columns = [
            {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
            {'name': 'value', 'type': 'TEXT'}
        ]
        self.schema_manager.create_table('test_transactions', columns)
        
        # Start a transaction
        self.connector.begin_transaction()
        
        # Insert first record
        self.query_executor.insert('test_transactions', {'value': 'value1'})
        
        # Insert second record
        self.query_executor.insert('test_transactions', {'value': 'value2'})
        
        # Check records are visible within transaction
        records = self.query_executor.select('test_transactions')
        self.assertEqual(len(records), 2)
        
        # Rollback the transaction
        self.connector.rollback_transaction()
        
        # Verify no records after rollback
        records = self.query_executor.select('test_transactions')
        self.assertEqual(len(records), 0)
        
        # Test commit
        self.connector.begin_transaction()
        self.query_executor.insert('test_transactions', {'value': 'committed'})
        self.connector.commit_transaction()
        
        # Verify record persists after commit
        records = self.query_executor.select('test_transactions')
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['value'], 'committed')


if __name__ == '__main__':
    unittest.main()
