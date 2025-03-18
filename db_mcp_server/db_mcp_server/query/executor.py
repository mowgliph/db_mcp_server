"""Query executor implementation."""

from typing import Any, Dict, List, Optional, Union, Tuple
from .builder import QueryBuilder
from ..utils.errors import QueryError


class QueryExecutor:
    """Executes SQL queries through a database connector."""
    
    def __init__(self, connector):
        """Initialize query executor.
        
        Args:
            connector: A database connector instance.
        """
        self.connector = connector
        self.query_builder = self._create_query_builder()
    
    def _create_query_builder(self) -> QueryBuilder:
        """Create a query builder instance configured for the current database type.
        
        Returns:
            QueryBuilder instance.
        """
        db_type = self._get_db_type()
        return QueryBuilder(db_type)
    
    def _get_db_type(self) -> str:
        """Get the database type from the connector.
        
        Returns:
            The database type as a string.
        """
        # Try to get db_type attribute if it exists
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
    
    def select(self, table_name: str, columns: Optional[List[str]] = None,
               where: Optional[Dict[str, Any]] = None, order_by: Optional[List[str]] = None,
               limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Execute a SELECT query.
        
        Args:
            table_name: The name of the table to query.
            columns: List of columns to retrieve. If None, retrieves all columns.
            where: Dictionary of column-value pairs for filtering records.
            order_by: List of columns to order by (prefix with '-' for descending order).
            limit: Maximum number of records to return.
            offset: Number of records to skip.
            
        Returns:
            A list of dictionaries representing the retrieved records.
            
        Raises:
            QueryError: If the query execution fails.
        """
        try:
            query, params = self.query_builder.select(
                table_name, columns, where, order_by, limit, offset
            )
            result = self.connector.execute_query(query, params)
            return result.get("results", [])
        except Exception as e:
            raise QueryError(f"Failed to execute SELECT query on table {table_name}", e)
    
    def insert(self, table_name: str, data: Dict[str, Any]) -> Dict:
        """Execute an INSERT query.
        
        Args:
            table_name: The name of the table.
            data: Dictionary of column-value pairs to insert.
            
        Returns:
            A dictionary containing information about the insert operation.
            
        Raises:
            QueryError: If the query execution fails.
        """
        try:
            query, params = self.query_builder.insert(table_name, data)
            return self.connector.execute_query(query, params)
        except Exception as e:
            raise QueryError(f"Failed to execute INSERT query on table {table_name}", e)
    
    def update(self, table_name: str, data: Dict[str, Any], where: Dict[str, Any]) -> Dict:
        """Execute an UPDATE query.
        
        Args:
            table_name: The name of the table.
            data: Dictionary of column-value pairs to update.
            where: Dictionary of column-value pairs for filtering records to update.
            
        Returns:
            A dictionary containing information about the update operation.
            
        Raises:
            QueryError: If the query execution fails.
        """
        try:
            query, params = self.query_builder.update(table_name, data, where)
            return self.connector.execute_query(query, params)
        except Exception as e:
            raise QueryError(f"Failed to execute UPDATE query on table {table_name}", e)
    
    def delete(self, table_name: str, where: Dict[str, Any]) -> Dict:
        """Execute a DELETE query.
        
        Args:
            table_name: The name of the table.
            where: Dictionary of column-value pairs for filtering records to delete.
            
        Returns:
            A dictionary containing information about the delete operation.
            
        Raises:
            QueryError: If the query execution fails.
        """
        try:
            query, params = self.query_builder.delete(table_name, where)
            return self.connector.execute_query(query, params)
        except Exception as e:
            raise QueryError(f"Failed to execute DELETE query on table {table_name}", e)
    
    def execute_raw(self, query: str, params: Optional[Union[Dict, List, Tuple]] = None) -> Dict:
        """Execute a raw SQL query.
        
        Args:
            query: The SQL query to execute.
            params: Parameters to bind to the query.
            
        Returns:
            A dictionary containing the query results or execution status.
            
        Raises:
            QueryError: If the query execution fails.
        """
        try:
            return self.connector.execute_query(query, params)
        except Exception as e:
            raise QueryError("Failed to execute raw query", e)
    
    def begin_transaction(self) -> None:
        """Begin a transaction.
        
        Raises:
            QueryError: If transaction initiation fails.
        """
        try:
            self.connector.begin_transaction()
        except Exception as e:
            raise QueryError("Failed to begin transaction", e)
    
    def commit_transaction(self) -> None:
        """Commit the current transaction.
        
        Raises:
            QueryError: If transaction commit fails.
        """
        try:
            self.connector.commit_transaction()
        except Exception as e:
            raise QueryError("Failed to commit transaction", e)
    
    def rollback_transaction(self) -> None:
        """Rollback the current transaction.
        
        Raises:
            QueryError: If transaction rollback fails.
        """
        try:
            self.connector.rollback_transaction()
        except Exception as e:
            raise QueryError("Failed to rollback transaction", e)
