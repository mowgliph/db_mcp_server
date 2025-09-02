"""Schema management implementation."""

from typing import Dict, List, Optional, Any, Union
import sqlalchemy as sa
from sqlalchemy import MetaData, Table, Column, Index
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.sql import text

from ..utils.errors import SchemaError, QueryError


class SchemaManager:
    """Manages database schema operations across different database types."""
    
    def __init__(self, connector):
        """Initialize schema manager.
        
        Args:
            connector: A database connector instance.
        """
        self.connector = connector
        self.metadata = MetaData()
    
    def list_tables(self) -> List[str]:
        """List all tables in the database.
        
        Returns:
            A list of table names.
            
        Raises:
            SchemaError: If table listing fails.
        """
        try:
            return self.connector.list_tables()
        except Exception as e:
            raise SchemaError("Failed to list tables", e)
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Get schema information for a table.
        
        Args:
            table_name: The name of the table.
            
        Returns:
            A list of dictionaries describing the table columns and their properties.
            
        Raises:
            SchemaError: If schema retrieval fails.
        """
        try:
            return self.connector.get_table_schema(table_name)
        except Exception as e:
            raise SchemaError(f"Failed to get schema for table {table_name}", e)
    
    def create_table(self, table_name: str, columns: List[Dict]) -> Dict:
        """Create a new table.
        
        Args:
            table_name: The name of the new table.
            columns: A list of dictionaries defining the columns and their properties.
                Each dictionary should have:
                - name: Column name
                - type: Column type
                - nullable: Whether the column can be NULL (default: True)
                - primary_key: Whether the column is a primary key (default: False)
                - default: Default value (optional)
                
        Returns:
            A dictionary containing information about the table creation.
            
        Raises:
            SchemaError: If table creation fails.
        """
        try:
            # Convert generic type names to database-specific types if needed
            normalized_columns = self._normalize_column_types(columns)
            return self.connector.create_table(table_name, normalized_columns)
        except Exception as e:
            raise SchemaError(f"Failed to create table {table_name}", e)
    
    def _normalize_column_types(self, columns: List[Dict]) -> List[Dict]:
        """Normalize column types based on database type.
        
        This converts generic type names to database-specific types.
        
        Args:
            columns: A list of column definitions.
            
        Returns:
            A list of column definitions with normalized types.
        """
        db_type = self._get_db_type()
        normalized = []
        
        for column in columns:
            normalized_column = column.copy()
            
            # Convert generic types to database-specific types
            if 'type' in column:
                col_type = column['type'].upper()
                
                # Integer types
                if col_type in ('INT', 'INTEGER'):
                    if db_type == 'sqlite':
                        normalized_column['type'] = 'INTEGER'
                    elif db_type == 'postgres':
                        normalized_column['type'] = 'INTEGER'
                    elif db_type in ('mysql', 'mariadb'):
                        normalized_column['type'] = 'INT'
                    elif db_type == 'mssql':
                        normalized_column['type'] = 'INT'
                
                # String types
                elif col_type in ('STRING', 'TEXT', 'VARCHAR'):
                    if 'size' in column and col_type != 'TEXT':
                        size = column['size']
                        if db_type == 'sqlite':
                            normalized_column['type'] = 'TEXT'  # SQLite doesn't care about size
                        elif db_type == 'postgres':
                            normalized_column['type'] = f'VARCHAR({size})'
                        elif db_type in ('mysql', 'mariadb'):
                            normalized_column['type'] = f'VARCHAR({size})'
                        elif db_type == 'mssql':
                            normalized_column['type'] = f'NVARCHAR({size})'
                    else:
                        if db_type == 'sqlite':
                            normalized_column['type'] = 'TEXT'
                        elif db_type == 'postgres':
                            normalized_column['type'] = 'TEXT'
                        elif db_type in ('mysql', 'mariadb'):
                            normalized_column['type'] = 'TEXT'
                        elif db_type == 'mssql':
                            normalized_column['type'] = 'NVARCHAR(MAX)'
                
                # Boolean types
                elif col_type in ('BOOL', 'BOOLEAN'):
                    if db_type == 'sqlite':
                        normalized_column['type'] = 'INTEGER'  # 0 or 1
                    elif db_type == 'postgres':
                        normalized_column['type'] = 'BOOLEAN'
                    elif db_type in ('mysql', 'mariadb'):
                        normalized_column['type'] = 'TINYINT(1)'
                    elif db_type == 'mssql':
                        normalized_column['type'] = 'BIT'
                
                # Date and time types
                elif col_type in ('DATETIME', 'TIMESTAMP'):
                    if db_type == 'sqlite':
                        normalized_column['type'] = 'TEXT'  # ISO format
                    elif db_type == 'postgres':
                        normalized_column['type'] = 'TIMESTAMP'
                    elif db_type in ('mysql', 'mariadb'):
                        normalized_column['type'] = 'DATETIME'
                    elif db_type == 'mssql':
                        normalized_column['type'] = 'DATETIME2'
            
            normalized.append(normalized_column)
        
        return normalized
    
    def _get_db_type(self) -> str:
        """Get the database type from the connector.
        
        Returns:
            The database type as a string.
        """
        # This is a simple implementation - in a real-world scenario,
        # you would get this from the connector or connection configuration
        if hasattr(self.connector, 'db_type'):
            return self.connector.db_type
        
        # Try to infer from connector class name
        class_name = self.connector.__class__.__name__.lower()
        if 'sqlite' in class_name:
            return 'sqlite'
        elif 'postgres' in class_name:
            return 'postgres'
        elif 'mysql' in class_name:
            return 'mysql'
        elif 'mssql' in class_name:
            return 'mssql'
        
        return 'unknown'
    
    def drop_table(self, table_name: str) -> Dict:
        """Drop a table.
        
        Args:
            table_name: The name of the table to drop.
            
        Returns:
            A dictionary containing information about the table dropping.
            
        Raises:
            SchemaError: If table dropping fails.
        """
        try:
            return self.connector.drop_table(table_name)
        except Exception as e:
            raise SchemaError(f"Failed to drop table {table_name}", e)
    
    def create_index(self, table_name: str, index_name: str, columns: List[str], 
                     unique: bool = False) -> Dict:
        """Create an index on a table.
        
        Args:
            table_name: The name of the table.
            index_name: The name of the index to create.
            columns: List of column names to include in the index.
            unique: Whether the index should enforce uniqueness.
            
        Returns:
            A dictionary containing information about the index creation.
            
        Raises:
            SchemaError: If index creation fails.
        """
        try:
            return self.connector.create_index(table_name, index_name, columns, unique)
        except Exception as e:
            raise SchemaError(f"Failed to create index {index_name} on table {table_name}", e)
    
    def drop_index(self, index_name: str) -> Dict:
        """Drop an index.
        
        Args:
            index_name: The name of the index to drop.
            
        Returns:
            A dictionary containing information about the index dropping.
            
        Raises:
            SchemaError: If index dropping fails.
        """
        try:
            return self.connector.drop_index(index_name)
        except Exception as e:
            raise SchemaError(f"Failed to drop index {index_name}", e)
    
    def alter_table(self, table_name: str, operations: List[Dict]) -> Dict:
        """Alter a table structure.
        
        Args:
            table_name: The name of the table to alter.
            operations: A list of operations to perform on the table.
                Each operation should be a dictionary with:
                - operation: Type of operation ("add_column", "drop_column", "modify_column")
                - column: Column name
                - definition: Column definition (for add/modify operations)
                
        Returns:
            A dictionary containing information about the table alteration.
            
        Raises:
            SchemaError: If table alteration fails.
        """
        db_type = self._get_db_type()
        
        if db_type == 'sqlite':
            return self._alter_table_sqlite(table_name, operations)
        
        # For other databases, build and execute ALTER TABLE statements
        try:
            results = {}
            
            for op in operations:
                operation = op.get('operation', '').lower()
                column = op.get('column', '')
                definition = op.get('definition', {})
                
                if not operation or not column:
                    raise SchemaError(f"Invalid alteration operation: {op}")
                
                query = f"ALTER TABLE {table_name} "
                
                if operation == 'add_column':
                    if not definition:
                        raise SchemaError("Column definition required for add_column operation")
                    
                    col_type = definition.get('type', 'TEXT')
                    nullable = "NULL" if definition.get('nullable', True) else "NOT NULL"
                    default = f"DEFAULT {definition['default']}" if 'default' in definition else ""
                    
                    query += f"ADD COLUMN {column} {col_type} {nullable} {default}"
                
                elif operation == 'drop_column':
                    query += f"DROP COLUMN {column}"
                
                elif operation == 'modify_column':
                    if not definition:
                        raise SchemaError("Column definition required for modify_column operation")
                    
                    if db_type in ('postgres', 'mysql', 'mariadb'):
                        col_type = definition.get('type', 'TEXT')
                        query += f"ALTER COLUMN {column} TYPE {col_type}"
                        
                        # For MySQL, nullable and default changes are in the same statement
                        if db_type in ('mysql', 'mariadb'):
                            nullable = "NULL" if definition.get('nullable', True) else "NOT NULL"
                            default = f"DEFAULT {definition['default']}" if 'default' in definition else ""
                            query += f" {nullable} {default}"
                    
                    elif db_type == 'mssql':
                        col_type = definition.get('type', 'NVARCHAR(MAX)')
                        query += f"ALTER COLUMN {column} {col_type}"
                
                else:
                    raise SchemaError(f"Unsupported operation: {operation}")
                
                result = self.connector.execute_query(query)
                results[f"{operation}_{column}"] = result
            
            return {"operations": results}
        
        except Exception as e:
            raise SchemaError(f"Failed to alter table {table_name}", e)
    
    def _alter_table_sqlite(self, table_name: str, operations: List[Dict]) -> Dict:
        """Alter a table in SQLite (which has limited ALTER TABLE support).
        
        This uses the table recreation approach since SQLite doesn't support many ALTER TABLE operations.
        
        Args:
            table_name: The name of the table to alter.
            operations: A list of operations to perform on the table.
                
        Returns:
            A dictionary containing information about the table alteration.
            
        Raises:
            SchemaError: If table alteration fails.
        """
        try:
            # Get current table schema
            schema = self.get_table_schema(table_name)
            if not schema:
                raise SchemaError(f"Table {table_name} not found or has no columns")
            
            # Begin transaction
            self.connector.begin_transaction()
            
            # Create a new table with the desired structure
            new_columns = []
            old_columns = []
            unchanged_columns = []
            
            # Process current columns
            for column in schema:
                old_columns.append(column['name'])
                
                # Check if this column should be modified or dropped
                should_drop = False
                modified_definition = None
                
                for op in operations:
                    if op.get('column') == column['name']:
                        if op.get('operation') == 'drop_column':
                            should_drop = True
                            break
                        elif op.get('operation') == 'modify_column':
                            modified_definition = op.get('definition', {})
                            break
                
                if should_drop:
                    continue
                
                if modified_definition:
                    # Add modified column to new table
                    new_def = {
                        'name': column['name'],
                        'type': modified_definition.get('type', column['type']),
                        'nullable': modified_definition.get('nullable', column['nullable']),
                        'primary_key': modified_definition.get('primary_key', column['primary_key']),
                    }
                    
                    if 'default' in modified_definition:
                        new_def['default'] = modified_definition['default']
                    elif 'default_value' in column and column['default_value'] is not None:
                        new_def['default'] = column['default_value']
                    
                    new_columns.append(new_def)
                    unchanged_columns.append(column['name'])
                else:
                    # Add unchanged column to new table
                    new_def = {
                        'name': column['name'],
                        'type': column['type'],
                        'nullable': column['nullable'],
                        'primary_key': column['primary_key'],
                    }
                    
                    if 'default_value' in column and column['default_value'] is not None:
                        new_def['default'] = column['default_value']
                    
                    new_columns.append(new_def)
                    unchanged_columns.append(column['name'])
            
            # Add new columns
            for op in operations:
                if op.get('operation') == 'add_column':
                    column_name = op.get('column')
                    definition = op.get('definition', {})
                    
                    if not column_name or not definition:
                        continue
                    
                    # Check if column already exists in schema
                    if any(col['name'] == column_name for col in schema):
                        continue
                    
                    new_columns.append({
                        'name': column_name,
                        'type': definition.get('type', 'TEXT'),
                        'nullable': definition.get('nullable', True),
                        'primary_key': definition.get('primary_key', False),
                        'default': definition.get('default')
                    })
            
            # Create temporary table
            temp_table = f"temp_{table_name}"
            self.connector.create_table(temp_table, new_columns)
            
            # Copy data from old table to new table
            columns_to_copy = ', '.join(unchanged_columns)
            self.connector.execute_query(
                f"INSERT INTO {temp_table} ({columns_to_copy}) SELECT {columns_to_copy} FROM {table_name}"
            )
            
            # Drop old table
            self.connector.drop_table(table_name)
            
            # Rename temp table to original name
            self.connector.execute_query(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
            
            # Commit transaction
            self.connector.commit_transaction()
            
            return {"success": True, "message": f"Table {table_name} altered successfully"}
        
        except Exception as e:
            # Rollback transaction on error
            try:
                self.connector.rollback_transaction()
            except:
                pass
            
            raise SchemaError(f"Failed to alter table {table_name}", e)
