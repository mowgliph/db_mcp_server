"""Base database connector definition."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union


class DatabaseConnector(ABC):
    """Abstract base class for database connectors.
    
    This class defines the interface that all database connectors must implement,
    ensuring consistent behavior across different database backends.
    """
    
    @abstractmethod
    def connect(self, **kwargs) -> Any:
        """Establish a connection to the database.
        
        Args:
            **kwargs: Connection parameters specific to the database type.
            
        Returns:
            A database connection object.
            
        Raises:
            ConnectionError: If the connection cannot be established.
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the database connection.
        
        Raises:
            ConnectionError: If there is an error closing the connection.
        """
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: Optional[Union[Dict, List, Tuple]] = None) -> Dict:
        """Execute a query and return results.
        
        Args:
            query: The SQL query to execute.
            params: Parameters to bind to the query.
            
        Returns:
            A dictionary containing the query results or execution status.
            
        Raises:
            QueryError: If there is an error executing the query.
        """
        pass
    
    @abstractmethod
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
            QueryError: If there is an error retrieving the records.
        """
        pass
    
    @abstractmethod
    def insert_record(self, table_name: str, data: Dict) -> Dict:
        """Insert a new record into a table.
        
        Args:
            table_name: The name of the table.
            data: Dictionary of column-value pairs to insert.
            
        Returns:
            A dictionary containing information about the inserted record.
            
        Raises:
            QueryError: If there is an error inserting the record.
        """
        pass
    
    @abstractmethod
    def update_record(self, table_name: str, data: Dict, where: Dict) -> Dict:
        """Update records in a table.
        
        Args:
            table_name: The name of the table.
            data: Dictionary of column-value pairs to update.
            where: Dictionary of column-value pairs for filtering records to update.
            
        Returns:
            A dictionary containing information about the update operation.
            
        Raises:
            QueryError: If there is an error updating the records.
        """
        pass
    
    @abstractmethod
    def delete_record(self, table_name: str, where: Dict) -> Dict:
        """Delete records from a table.
        
        Args:
            table_name: The name of the table.
            where: Dictionary of column-value pairs for filtering records to delete.
            
        Returns:
            A dictionary containing information about the delete operation.
            
        Raises:
            QueryError: If there is an error deleting the records.
        """
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> List[Dict]:
        """Get schema information for a table.
        
        Args:
            table_name: The name of the table.
            
        Returns:
            A list of dictionaries describing the table columns and their properties.
            
        Raises:
            QueryError: If there is an error retrieving the schema.
        """
        pass
    
    @abstractmethod
    def list_tables(self) -> List[str]:
        """List all tables in the database.
        
        Returns:
            A list of table names.
            
        Raises:
            QueryError: If there is an error listing the tables.
        """
        pass
    
    @abstractmethod
    def create_table(self, table_name: str, columns: List[Dict]) -> Dict:
        """Create a new table.
        
        Args:
            table_name: The name of the new table.
            columns: A list of dictionaries defining the columns and their properties.
            
        Returns:
            A dictionary containing information about the table creation.
            
        Raises:
            QueryError: If there is an error creating the table.
        """
        pass
    
    @abstractmethod
    def drop_table(self, table_name: str) -> Dict:
        """Drop a table.
        
        Args:
            table_name: The name of the table to drop.
            
        Returns:
            A dictionary containing information about the table dropping.
            
        Raises:
            QueryError: If there is an error dropping the table.
        """
        pass
    
    @abstractmethod
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
            QueryError: If there is an error creating the index.
        """
        pass
    
    @abstractmethod
    def drop_index(self, index_name: str) -> Dict:
        """Drop an index.
        
        Args:
            index_name: The name of the index to drop.
            
        Returns:
            A dictionary containing information about the index dropping.
            
        Raises:
            QueryError: If there is an error dropping the index.
        """
        pass
    
    @abstractmethod
    def begin_transaction(self) -> None:
        """Begin a transaction.
        
        Raises:
            QueryError: If there is an error beginning the transaction.
        """
        pass
    
    @abstractmethod
    def commit_transaction(self) -> None:
        """Commit the current transaction.
        
        Raises:
            QueryError: If there is an error committing the transaction.
        """
        pass
    
    @abstractmethod
    def rollback_transaction(self) -> None:
        """Rollback the current transaction.
        
        Raises:
            QueryError: If there is an error rolling back the transaction.
        """
        pass
