"""Database connector factory."""

from typing import Dict, Optional, Any
from .base import DatabaseConnector
from ..utils.errors import ConnectionError


def get_connector(db_type: str, **kwargs) -> DatabaseConnector:
    """Get the appropriate database connector based on database type.
    
    Args:
        db_type: The type of database (sqlite, postgres, mysql, mssql)
        **kwargs: Connection parameters specific to the database type
        
    Returns:
        An instance of the appropriate DatabaseConnector implementation
        
    Raises:
        ConnectionError: If the specified database type is not supported
    """
    db_type = db_type.lower()
    
    if db_type == 'sqlite':
        from .sqlite import SQLiteConnector
        return SQLiteConnector(**kwargs)
    
    elif db_type == 'postgres':
        from .postgres import PostgreSQLConnector
        return PostgreSQLConnector(**kwargs)
    
    elif db_type in ('mysql', 'mariadb'):
        from .mysql import MySQLConnector
        return MySQLConnector(**kwargs)
    
    elif db_type == 'mssql':
        from .mssql import MSSQLConnector
        return MSSQLConnector(**kwargs)
    
    else:
        raise ConnectionError(f"Unsupported database type: {db_type}")
