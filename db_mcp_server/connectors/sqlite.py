"""SQLite database connector implementation."""

import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple, Union
from pathlib import Path

from .base import DatabaseConnector
from ..utils.errors import ConnectionError, QueryError


class SQLiteConnector(DatabaseConnector):
    """SQLite database connector implementation."""
    
    def __init__(self, db_path: str, **kwargs):
        """Initialize SQLite connector.
        
        Args:
            db_path: Path to the SQLite database file.
            **kwargs: Additional connection parameters.
        """
        self.db_path = db_path
        self.connection = None
        self.connection_params = kwargs
        self.in_transaction = False
    
    def connect(self, **kwargs) -> sqlite3.Connection:
        """Connect to SQLite database.
        
        Args:
            **kwargs: Additional connection parameters to override initialization parameters.
            
        Returns:
            SQLite connection object.
            
        Raises:
            ConnectionError: If connection fails.
        """
        params = {**self.connection_params, **kwargs}
        
        try:
            # Ensure directory exists
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
            # Connect to database
            self.connection = sqlite3.connect(self.db_path, **params)
            
            # Configure connection for dictionary access
            self.connection.row_factory = sqlite3.Row
            
            return self.connection
        
        except Exception as e:
            raise ConnectionError(f"Failed to connect to SQLite database at {self.db_path}", e)
    
    def close(self) -> None:
        """Close the database connection.
        
        Raises:
            ConnectionError: If closing the connection fails.
        """
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
            except Exception as e:
                raise ConnectionError("Failed to close SQLite connection", e)
    
    def _ensure_connection(self) -> sqlite3.Connection:
        """Ensure a database connection exists.
        
        Returns:
            SQLite connection object.
            
        Raises:
            ConnectionError: If no connection exists and cannot be established.
        """
        if not self.connection:
            return self.connect()
        return self.connection
    
    def execute_query(self, query: str, params: Optional[Union[Dict, List, Tuple]] = None) -> Dict:
        """Execute a query and return results.
        
        Args:
            query: The SQL query to execute.
            params: Parameters to bind to the query.
            
        Returns:
            Dictionary containing the query results or execution status.
            
        Raises:
            QueryError: If query execution fails.
        """
        conn = self._ensure_connection()
        
        try:
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # For SELECT queries, return the results
            if query.strip().upper().startswith(('SELECT', 'PRAGMA')):
                results = [dict(row) for row in cursor.fetchall()]
                cursor.close()
                return {"results": results, "row_count": len(results)}
            
            # For other queries, commit if not in transaction and return status
            if not self.in_transaction:
                conn.commit()
            
            rowcount = cursor.rowcount
            lastrowid = cursor.lastrowid
            cursor.close()
            
            return {
                "success": True,
                "affected_rows": rowcount if rowcount >= 0 else 0,
                "last_insert_id": lastrowid if lastrowid else None
            }
        
        except Exception as e:
            if not self.in_transaction:
                conn.rollback()
            raise QueryError(f"SQLite query execution failed: {query}", e)
    
    def get_records(self, table_name: str, columns: Optional[List[str]] = None, 
                   where: Optional[Dict] = None, limit: Optional[int] = None) -> List[Dict]:
        """Get records from a table with optional filtering.
        
        Args:
            table_name: The name of the table to query.
            columns: List of columns to retrieve. If None, retrieves all columns.
            where: Dictionary of column-value pairs for filtering records.
            limit: Maximum number of records to return.
            
        Returns:
            A list of dictionaries representing the retrieved records.
            
        Raises:
            QueryError: If record retrieval fails.
        """
        # Build the query
        column_clause = "*"
        if columns:
            column_clause = ", ".join(columns)
        
        query = f"SELECT {column_clause} FROM {table_name}"
        params = []
        
        # Add WHERE clause if specified
        if where:
            where_conditions = []
            for column, value in where.items():
                where_conditions.append(f"{column} = ?")
                params.append(value)
            
            if where_conditions:
                query += " WHERE " + " AND ".join(where_conditions)
        
        # Add LIMIT clause if specified
        if limit is not None:
            query += f" LIMIT {limit}"
        
        # Execute the query
        result = self.execute_query(query, params)
        return result.get("results", [])
    
    def insert_record(self, table_name: str, data: Dict) -> Dict:
        """Insert a new record into a table.
        
        Args:
            table_name: The name of the table.
            data: Dictionary of column-value pairs to insert.
            
        Returns:
            A dictionary containing information about the inserted record.
            
        Raises:
            QueryError: If record insertion fails.
        """
        if not data:
            raise QueryError("No data provided for insertion")
        
        columns = list(data.keys())
        placeholders = ["?"] * len(columns)
        values = list(data.values())
        
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        return self.execute_query(query, values)
    
    def update_record(self, table_name: str, data: Dict, where: Dict) -> Dict:
        """Update records in a table.
        
        Args:
            table_name: The name of the table.
            data: Dictionary of column-value pairs to update.
            where: Dictionary of column-value pairs for filtering records to update.
            
        Returns:
            A dictionary containing information about the update operation.
            
        Raises:
            QueryError: If record update fails.
        """
        if not data:
            raise QueryError("No data provided for update")
        
        if not where:
            raise QueryError("No conditions provided for update")
        
        # Build SET clause
        set_clauses = []
        params = []
        
        for column, value in data.items():
            set_clauses.append(f"{column} = ?")
            params.append(value)
        
        # Build WHERE clause
        where_clauses = []
        for column, value in where.items():
            where_clauses.append(f"{column} = ?")
            params.append(value)
        
        query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}"
        
        return self.execute_query(query, params)
    
    def delete_record(self, table_name: str, where: Dict) -> Dict:
        """Delete records from a table.
        
        Args:
            table_name: The name of the table.
            where: Dictionary of column-value pairs for filtering records to delete.
            
        Returns:
            A dictionary containing information about the delete operation.
            
        Raises:
            QueryError: If record deletion fails.
        """
        if not where:
            raise QueryError("No conditions provided for delete operation")
        
        # Build WHERE clause
        where_clauses = []
        params = []
        
        for column, value in where.items():
            where_clauses.append(f"{column} = ?")
            params.append(value)
        
        query = f"DELETE FROM {table_name} WHERE {' AND '.join(where_clauses)}"
        
        return self.execute_query(query, params)
    
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Get schema information for a table.
        
        Args:
            table_name: The name of the table.
            
        Returns:
            A list of dictionaries describing the table columns and their properties.
            
        Raises:
            QueryError: If schema retrieval fails.
        """
        query = f"PRAGMA table_info({table_name})"
        result = self.execute_query(query)
        
        schema = []
        for column in result.get("results", []):
            schema.append({
                "name": column["name"],
                "type": column["type"],
                "nullable": not column["notnull"],
                "primary_key": bool(column["pk"]),
                "default_value": column["dflt_value"]
            })
        
        return schema
    
    def list_tables(self) -> List[str]:
        """List all tables in the database.
        
        Returns:
            A list of table names.
            
        Raises:
            QueryError: If table listing fails.
        """
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        result = self.execute_query(query)
        
        return [row["name"] for row in result.get("results", [])]
    
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
            QueryError: If table creation fails.
        """
        if not columns:
            raise QueryError("No columns provided for table creation")
        
        column_defs = []
        
        for column in columns:
            col_def = f"{column['name']} {column['type']}"
            
            if column.get('primary_key'):
                col_def += " PRIMARY KEY"
            
            if not column.get('nullable', True):
                col_def += " NOT NULL"
            
            if 'default' in column:
                default_value = column['default']
                if isinstance(default_value, str):
                    default_value = f"'{default_value}'"
                col_def += f" DEFAULT {default_value}"
            
            column_defs.append(col_def)
        
        query = f"CREATE TABLE {table_name} ({', '.join(column_defs)})"
        
        return self.execute_query(query)
    
    def drop_table(self, table_name: str) -> Dict:
        """Drop a table.
        
        Args:
            table_name: The name of the table to drop.
            
        Returns:
            A dictionary containing information about the table dropping.
            
        Raises:
            QueryError: If table dropping fails.
        """
        query = f"DROP TABLE IF EXISTS {table_name}"
        return self.execute_query(query)
    
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
            QueryError: If index creation fails.
        """
        if not columns:
            raise QueryError("No columns provided for index creation")
        
        uniqueness = "UNIQUE" if unique else ""
        column_list = ", ".join(columns)
        
        query = f"CREATE {uniqueness} INDEX {index_name} ON {table_name} ({column_list})"
        
        return self.execute_query(query)
    
    def drop_index(self, index_name: str) -> Dict:
        """Drop an index.
        
        Args:
            index_name: The name of the index to drop.
            
        Returns:
            A dictionary containing information about the index dropping.
            
        Raises:
            QueryError: If index dropping fails.
        """
        query = f"DROP INDEX IF EXISTS {index_name}"
        return self.execute_query(query)
    
    def begin_transaction(self) -> None:
        """Begin a transaction.
        
        Raises:
            QueryError: If transaction initiation fails.
        """
        if self.in_transaction:
            raise QueryError("Transaction already in progress")
        
        self._ensure_connection()
        self.execute_query("BEGIN TRANSACTION")
        self.in_transaction = True
    
    def commit_transaction(self) -> None:
        """Commit the current transaction.
        
        Raises:
            QueryError: If transaction commit fails.
        """
        if not self.in_transaction:
            raise QueryError("No transaction in progress")
        
        try:
            conn = self._ensure_connection()
            conn.commit()
            self.in_transaction = False
        except Exception as e:
            raise QueryError("Failed to commit transaction", e)
    
    def rollback_transaction(self) -> None:
        """Rollback the current transaction.
        
        Raises:
            QueryError: If transaction rollback fails.
        """
        if not self.in_transaction:
            raise QueryError("No transaction in progress")
        
        try:
            conn = self._ensure_connection()
            conn.rollback()
            self.in_transaction = False
        except Exception as e:
            raise QueryError("Failed to rollback transaction", e)
